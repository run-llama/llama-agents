"""Tests for the git HTTP server backed by dulwich and S3."""

from __future__ import annotations

import io
import shutil
import time
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
import pytest
from aiomoto import mock_aws
from dulwich.object_format import DEFAULT_OBJECT_FORMAT
from dulwich.objects import Blob, Commit, ShaFile, Tree
from dulwich.pack import UnpackedObject, write_pack_data
from dulwich.protocol import ZERO_SHA
from dulwich.refs import Ref
from dulwich.repo import Repo
from fastapi import FastAPI, Request
from fastapi.responses import Response
from httpx import ASGITransport
from llama_agents.control_plane.code_repo.git_server import (
    handle_git_request,
    handle_git_request_readonly,
)
from llama_agents.control_plane.code_repo.storage import CodeRepoStorage

from .conftest import create_bucket, create_test_repo, make_storage


def _add_commit_to_repo(repo: Repo, filename: bytes, content: bytes) -> Commit:
    """Add another commit to an existing repo."""
    blob = Blob.from_string(content)
    repo.object_store.add_object(blob)

    # Build tree from previous commit's tree plus new file
    prev_commit = cast(Commit, repo[repo.refs[Ref(b"refs/heads/main")]])
    old_tree = cast(Tree, repo[prev_commit.tree])
    tree = Tree()
    for item in old_tree.items():
        tree.add(*item)
    tree.add(filename, 0o100644, blob.id)
    repo.object_store.add_object(tree)

    commit = Commit()
    commit.tree = tree.id
    commit.parents = [prev_commit.id]
    commit.author = commit.committer = b"Test User <test@example.com>"
    commit.commit_time = commit.author_time = int(time.time())
    commit.commit_timezone = commit.author_timezone = 0
    commit.encoding = b"UTF-8"
    commit.message = b"Second commit"
    repo.object_store.add_object(commit)
    repo.refs[Ref(b"refs/heads/main")] = commit.id
    return commit


def _sha_file_to_unpacked(obj: ShaFile) -> UnpackedObject:
    """Convert a dulwich ShaFile to an UnpackedObject for pack writing."""
    raw = obj.as_raw_string()
    return UnpackedObject(
        obj.type_num,
        decomp_chunks=[raw],
        decomp_len=len(raw),
        sha=obj.id,
    )


def _collect_repo_objects(repo: Repo) -> list[ShaFile]:
    """Walk a repo and collect all objects reachable from refs."""
    objects: list[ShaFile] = []
    seen: set[bytes] = set()
    include = [sha for sha in repo.refs.allkeys() if not sha == b"HEAD"]
    include_shas = [repo.refs[ref] for ref in include]
    for entry in repo.get_walker(include=include_shas):
        commit_obj = entry.commit
        if commit_obj.id not in seen:
            objects.append(commit_obj)
            seen.add(commit_obj.id)
        tree_obj = cast(Tree, repo[commit_obj.tree])
        if tree_obj.id not in seen:
            objects.append(tree_obj)
            seen.add(tree_obj.id)
        for item in tree_obj.items():
            obj = repo[item.sha]
            if obj.id not in seen:
                objects.append(obj)
                seen.add(obj.id)
    return objects


def _build_receive_pack_body(
    objects: list[ShaFile],
    old_sha: bytes,
    new_sha: bytes,
    ref_name: str = "refs/heads/main",
) -> bytes:
    """Build a git-receive-pack request body with ref update and pack data."""
    body = io.BytesIO()

    # Write the ref update pkt-line
    old_hex = old_sha.decode("ascii")
    new_hex = new_sha.decode("ascii")
    ref_line = (
        f"{old_hex} {new_hex} {ref_name}\x00 report-status side-band-64k"
    ).encode("ascii")
    pkt_line = f"{len(ref_line) + 4:04x}".encode("ascii") + ref_line
    body.write(pkt_line)
    body.write(b"0000")  # flush-pkt

    # Write pack data
    pack_buf = io.BytesIO()
    unpacked = [_sha_file_to_unpacked(obj) for obj in objects]
    write_pack_data(
        pack_buf.write,
        iter(unpacked),
        DEFAULT_OBJECT_FORMAT,
        num_records=len(unpacked),
    )
    body.write(pack_buf.getvalue())

    return body.getvalue()


def _make_test_app(
    storage: CodeRepoStorage,
    on_push_complete: Any = None,
    readonly: bool = False,
) -> FastAPI:
    """Create a minimal FastAPI app wired to the git handlers."""
    app = FastAPI()

    if readonly:

        @app.api_route("/git/{git_path:path}", methods=["GET", "POST"])
        async def git_readonly_handler(request: Request, git_path: str) -> Response:
            return await handle_git_request_readonly(
                request=request,
                deployment_id="test-deploy",
                git_path=git_path,
                storage=storage,
            )

    else:

        @app.api_route("/git/{git_path:path}", methods=["GET", "POST"])
        async def git_handler(request: Request, git_path: str) -> Response:
            return await handle_git_request(
                request=request,
                deployment_id="test-deploy",
                git_path=git_path,
                storage=storage,
                on_push_complete=on_push_complete,
            )

    return app


@pytest.mark.asyncio
async def test_info_refs_receive_pack_empty_repo() -> None:
    """GET /info/refs?service=git-receive-pack on empty repo returns valid discovery."""
    with mock_aws():
        create_bucket()
        storage = make_storage()
        app = _make_test_app(storage)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-receive-pack"}
            )
            assert response.status_code == 200
            assert b"service=git-receive-pack" in response.content
            content_type = response.headers.get("content-type", "")
            assert "application/x-git-receive-pack-advertisement" in content_type


@pytest.mark.asyncio
async def test_info_refs_upload_pack_existing_repo(tmp_path: Path) -> None:
    """GET /info/refs?service=git-upload-pack returns refs from existing repo."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        # Upload a repo with a commit
        repo_path = tmp_path / "repo"
        repo = create_test_repo(repo_path)
        head_sha = repo.refs[Ref(b"refs/heads/main")]
        await storage.upload_repo("test-deploy", repo_path)

        app = _make_test_app(storage)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )
            assert response.status_code == 200
            assert b"service=git-upload-pack" in response.content
            # The response should contain the commit SHA
            assert head_sha in response.content
            content_type = response.headers.get("content-type", "")
            assert "application/x-git-upload-pack-advertisement" in content_type


@pytest.mark.asyncio
async def test_push_to_empty_repo_triggers_callback_and_uploads(
    tmp_path: Path,
) -> None:
    """Full push to empty repo: uploads to S3 and calls on_push_complete."""
    with mock_aws():
        create_bucket()
        storage = make_storage()
        mock_callback = AsyncMock()

        # Create a local repo with a commit to push from
        local_repo_path = tmp_path / "local"
        local_repo = create_test_repo(local_repo_path)
        head_sha = local_repo.refs[Ref(b"refs/heads/main")]

        app = _make_test_app(storage, on_push_complete=mock_callback)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # Step 1: info/refs discovery
            refs_response = await client.get(
                "/git/info/refs", params={"service": "git-receive-pack"}
            )
            assert refs_response.status_code == 200

            # Step 2: Build and send the pack
            objects = _collect_repo_objects(local_repo)
            body = _build_receive_pack_body(objects, ZERO_SHA, head_sha)

            pack_response = await client.post(
                "/git/git-receive-pack",
                content=body,
                headers={
                    "Content-Type": "application/x-git-receive-pack-request",
                },
            )
            assert pack_response.status_code == 200

        # Verify the repo was uploaded to S3
        assert await storage.repo_exists("test-deploy")

        # Verify the callback was called
        mock_callback.assert_called_once()
        call_args = mock_callback.call_args
        assert call_args[0][0] == "test-deploy"  # deployment_id
        assert call_args[0][1] == head_sha.decode()  # new_sha
        assert call_args[0][2] == "main"  # git_ref


@pytest.mark.asyncio
async def test_push_with_chunked_transfer_encoding(tmp_path: Path) -> None:
    """Push with Transfer-Encoding: chunked succeeds (ASGI de-chunks the body)."""
    with mock_aws():
        create_bucket()
        storage = make_storage()
        mock_callback = AsyncMock()

        local_repo_path = tmp_path / "local"
        local_repo = create_test_repo(local_repo_path)
        head_sha = local_repo.refs[Ref(b"refs/heads/main")]

        app = _make_test_app(storage, on_push_complete=mock_callback)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            refs_response = await client.get(
                "/git/info/refs", params={"service": "git-receive-pack"}
            )
            assert refs_response.status_code == 200

            objects = _collect_repo_objects(local_repo)
            body = _build_receive_pack_body(objects, ZERO_SHA, head_sha)

            # Send with Transfer-Encoding: chunked header to simulate what
            # dulwich's HTTP client does via urllib3. The ASGI server de-chunks
            # the body, but the header is preserved in the WSGI environ.
            pack_response = await client.post(
                "/git/git-receive-pack",
                content=body,
                headers={
                    "Content-Type": "application/x-git-receive-pack-request",
                    "Transfer-Encoding": "chunked",
                },
            )
            assert pack_response.status_code == 200

        assert await storage.repo_exists("test-deploy")
        mock_callback.assert_called_once()


@pytest.mark.asyncio
async def test_push_updates_existing_repo(tmp_path: Path) -> None:
    """Push to an existing repo updates S3 and fires callback with new SHA."""
    with mock_aws():
        create_bucket()
        storage = make_storage()
        mock_callback = AsyncMock()

        # Upload initial repo
        repo_path = tmp_path / "repo"
        repo = create_test_repo(repo_path)
        first_sha = repo.refs[Ref(b"refs/heads/main")]
        await storage.upload_repo("test-deploy", repo_path)

        # Add a second commit
        second_commit = _add_commit_to_repo(repo, b"file2.txt", b"second file")
        second_sha = second_commit.id

        app = _make_test_app(storage, on_push_complete=mock_callback)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # info/refs discovery
            refs_response = await client.get(
                "/git/info/refs", params={"service": "git-receive-pack"}
            )
            assert refs_response.status_code == 200

            # Build pack with all objects (server may already have some,
            # but sending duplicates is safe in git protocol)
            objects = _collect_repo_objects(repo)
            body = _build_receive_pack_body(objects, first_sha, second_sha)

            pack_response = await client.post(
                "/git/git-receive-pack",
                content=body,
                headers={
                    "Content-Type": "application/x-git-receive-pack-request",
                },
            )
            assert pack_response.status_code == 200

        # Verify callback was called with the new SHA
        mock_callback.assert_called_once()
        call_args = mock_callback.call_args
        assert call_args[0][0] == "test-deploy"
        assert call_args[0][1] == second_sha.decode()
        assert call_args[0][2] == "main"

        # Download from S3 and verify both commits are present
        downloaded = await storage.download_repo("test-deploy")
        assert downloaded is not None
        try:
            dl_repo = Repo(str(downloaded))
            assert dl_repo.refs[Ref(b"refs/heads/main")] == second_sha
            # Walk history to verify both commits exist
            walker = dl_repo.get_walker()
            commit_shas = [entry.commit.id for entry in walker]
            assert second_sha in commit_shas
            assert first_sha in commit_shas
        finally:
            shutil.rmtree(downloaded.parent, ignore_errors=True)


@pytest.mark.asyncio
async def test_readonly_rejects_receive_pack(tmp_path: Path) -> None:
    """The readonly handler returns 403 for git-receive-pack POST requests."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        # Need an existing repo so we don't get 404 first
        repo_path = tmp_path / "repo"
        create_test_repo(repo_path)
        await storage.upload_repo("test-deploy", repo_path)

        app = _make_test_app(storage, readonly=True)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            # POST to git-receive-pack should be rejected
            response = await client.post(
                "/git/git-receive-pack",
                content=b"",
                headers={
                    "Content-Type": "application/x-git-receive-pack-request",
                },
            )
            assert response.status_code == 403
            assert b"Push not allowed" in response.content

            # info/refs for receive-pack also contains "git-receive-pack" in path
            # but since the path is "info/refs", not "git-receive-pack",
            # it passes through to dulwich. We verify upload-pack still works.
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )
            assert response.status_code == 200


@pytest.mark.asyncio
async def test_readonly_returns_404_for_missing_repo() -> None:
    """The readonly handler returns 404 when no repo exists."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        app = _make_test_app(storage, readonly=True)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )
            assert response.status_code == 404
            assert b"No code has been pushed" in response.content


@pytest.mark.asyncio
async def test_readonly_serves_upload_pack(tmp_path: Path) -> None:
    """The readonly handler successfully serves git-upload-pack requests."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        repo_path = tmp_path / "repo"
        repo = create_test_repo(repo_path)
        head_sha = repo.refs[Ref(b"refs/heads/main")]
        await storage.upload_repo("test-deploy", repo_path)

        app = _make_test_app(storage, readonly=True)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )
            assert response.status_code == 200
            assert head_sha in response.content
            content_type = response.headers.get("content-type", "")
            assert "application/x-git-upload-pack-advertisement" in content_type


@pytest.mark.asyncio
async def test_handle_git_request_closes_repo_handles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The read/write handler closes each dulwich Repo it opens."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        repo_path = tmp_path / "repo"
        create_test_repo(repo_path)
        await storage.upload_repo("test-deploy", repo_path)

        close_calls = 0
        original_close = Repo.close

        def _spy_close(self: Repo) -> None:
            nonlocal close_calls
            close_calls += 1
            original_close(self)

        monkeypatch.setattr(Repo, "close", _spy_close)

        app = _make_test_app(storage)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )

        assert response.status_code == 200
        assert close_calls == 1


@pytest.mark.asyncio
async def test_handle_git_request_readonly_closes_repo_after_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The readonly handler closes its dulwich Repo after streaming completes."""
    with mock_aws():
        create_bucket()
        storage = make_storage()

        repo_path = tmp_path / "repo"
        create_test_repo(repo_path)
        await storage.upload_repo("test-deploy", repo_path)

        close_calls = 0
        original_close = Repo.close

        def _spy_close(self: Repo) -> None:
            nonlocal close_calls
            close_calls += 1
            original_close(self)

        monkeypatch.setattr(Repo, "close", _spy_close)

        app = _make_test_app(storage, readonly=True)

        async with httpx.AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            response = await client.get(
                "/git/info/refs", params={"service": "git-upload-pack"}
            )

        assert response.status_code == 200
        assert close_calls == 1
