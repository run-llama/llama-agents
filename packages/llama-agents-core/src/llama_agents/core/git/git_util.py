"""
Git utilities for exploring, cloning, and parsing llama-deploy repositories.

Backed by the pure-Python ``dulwich`` library so the host does not need a
``git`` binary on PATH.
"""

import asyncio
import contextlib
import io
import ipaddress
import re
import shutil
import socket
import tempfile
import urllib.parse
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from dulwich import porcelain
from dulwich.client import get_transport_and_path
from dulwich.errors import NotGitRepository
from dulwich.objects import ObjectID
from dulwich.refs import Ref
from dulwich.repo import Repo
from dulwich.walk import Walker

_HEAD_REF = Ref(b"HEAD")
_REFS_HEADS = b"refs/heads/"
_REFS_TAGS = b"refs/tags/"
_REFS_REMOTES = b"refs/remotes/"
_DETACHED_BRANCH_REF = Ref(b"refs/heads/_llama_checkout")


@contextlib.contextmanager
def _discover_repo(start: Path | None = None) -> Iterator[Repo]:
    """Discover the repo from start (or cwd), yield it, and guarantee close."""
    try:
        repo = Repo.discover(start=str(start or Path.cwd()))
    except NotGitRepository as e:
        raise GitAccessError("Not a git repository") from e
    try:
        yield repo
    finally:
        repo.close()


def parse_github_repo_url(repo_url: str) -> tuple[str, str]:
    """
    Parse GitHub repository URL to extract owner and repo name.

    Args:
        repo_url: GitHub repository URL (various formats supported)

    Returns:
        Tuple of (owner, repo_name)

    Raises:
        ValueError: If URL format is not recognized
    """
    url = repo_url.rstrip("/").removesuffix(".git")

    patterns = [
        r"https://github\.com/([^/]+)/([^/]+)",
        r"git@github\.com:([^/]+)/([^/]+)",
        r"github\.com/([^/]+)/([^/]+)",
    ]

    for pattern in patterns:
        match = re.match(pattern, url)
        if match:
            return match.group(1), match.group(2)

    raise ValueError(f"Could not parse GitHub repository URL: {repo_url}")


class GitAccessError(Exception):
    """Error raised when a user reportable git error occurs, e.g connection fails, cannot access repository, timeout, ref not found, etc."""

    def __init__(self, message: str):
        self.message = message
        super().__init__(message)


_ALLOWED_SCHEMES = {"https", "http"}

FULL_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA_LIKE_RE = re.compile(r"^[0-9a-f]{7,40}$")


def validate_git_url(url: str) -> None:
    """Validate that a git URL uses an allowed scheme.

    Raises GitAccessError for dangerous or unrecognised URL schemes such as
    ``ext::`` (which lets git invoke arbitrary commands) or URLs that start
    with ``-`` (which could be interpreted as flags).

    Note: this does NOT check whether the hostname resolves to a private
    network address.  Callers that accept user-supplied URLs should also
    call :func:`validate_git_url_no_ssrf` to guard against SSRF.
    """
    if url.startswith("-"):
        raise GitAccessError(f"Invalid git URL (starts with '-'): {url}")

    if url.startswith("ext::"):
        raise GitAccessError(f"Disallowed git URL scheme 'ext': {url}")

    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme not in _ALLOWED_SCHEMES:
        raise GitAccessError(
            f"Disallowed git URL scheme '{scheme}': {url}. "
            f"Allowed schemes: {', '.join(sorted(_ALLOWED_SCHEMES))}"
        )


def validate_git_url_no_ssrf(url: str) -> None:
    """Validate git URL scheme AND reject hostnames that resolve to private IPs.

    Use this at API boundaries where the URL comes from user input.
    """
    validate_git_url(url)

    parsed = urllib.parse.urlparse(url)
    hostname = parsed.hostname
    if not hostname:
        raise GitAccessError(f"Invalid git URL (no hostname): {url}")

    _check_hostname_not_private(hostname)


def _check_hostname_not_private(hostname: str) -> None:
    """Resolve hostname and raise GitAccessError if it points to a private/internal IP."""
    try:
        addr_infos = socket.getaddrinfo(hostname, None)
    except socket.gaierror as e:
        raise GitAccessError(f"DNS resolution failed for {hostname}") from e

    for addr_info in addr_infos:
        ip = ipaddress.ip_address(addr_info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise GitAccessError(
                f"Git URL resolves to a private network address: {hostname}"
            )


@dataclass
class GitCloneResult:
    git_sha: str
    git_ref: str | None = None


def _split_basic_auth(basic_auth: str | None) -> tuple[str | None, str | None]:
    """Parse a ``user:password`` style string into separate components."""
    if not basic_auth:
        return None, None
    if ":" not in basic_auth:
        # Treat as a token-only credential (passed in the user slot)
        return basic_auth, None
    user, _, password = basic_auth.partition(":")
    return user, password


def _resolved_git_ref_for_head(repo: Repo) -> str | None:
    """Read the symbolic HEAD ref and return a friendly branch/tag name (or None)."""
    head_ref = repo.refs.read_ref(_HEAD_REF)
    if head_ref is None:
        return None
    if head_ref.startswith(b"ref: "):
        head_ref = head_ref[5:]
    # If the read produced an actual SHA (40 hex), HEAD is detached.
    if FULL_SHA_RE.match(head_ref.decode(errors="replace")):
        # Try to find a tag pointing at HEAD
        head_sha = repo.refs[_HEAD_REF]
        for ref_name in repo.refs.subkeys(Ref(_REFS_TAGS)):
            tag_ref = Ref(_REFS_TAGS + ref_name)
            try:
                if repo.refs[tag_ref] == head_sha:
                    return ref_name.decode()
            except KeyError:
                continue
        return None
    if head_ref.startswith(_REFS_HEADS):
        return head_ref[len(_REFS_HEADS) :].decode()
    if head_ref.startswith(_REFS_TAGS):
        return head_ref[len(_REFS_TAGS) :].decode()
    return head_ref.decode()


def resolve_ref_in_repo(repo: Repo, git_ref: str) -> bytes | None:
    """Resolve a git ref string to a commit SHA in a dulwich Repo.

    Tries named refs (bare path, heads, tags), full SHA, then short SHA
    prefix.  For named refs, annotated tags are peeled to the underlying
    commit.

    Returns the SHA as bytes, or ``None`` if the ref cannot be found.
    Raises :class:`GitAccessError` when a short SHA prefix is ambiguous.
    """
    ref_bytes = git_ref.encode()

    for candidate in (
        Ref(ref_bytes),
        Ref(_REFS_HEADS + ref_bytes),
        Ref(_REFS_TAGS + ref_bytes),
    ):
        try:
            return repo.get_peeled(candidate)
        except KeyError:
            continue

    if FULL_SHA_RE.match(git_ref):
        try:
            return repo[ref_bytes].id
        except (AssertionError, KeyError):
            pass

    if SHA_LIKE_RE.match(git_ref):
        try:
            matching_shas = repo.object_store.iter_prefix(ref_bytes)
            first = next(matching_shas)
            if next(matching_shas, None) is not None:
                raise GitAccessError(f"Git ref is ambiguous: {git_ref}")
            return first
        except StopIteration:
            pass
        except GitAccessError:
            raise
        except (KeyError, OSError):
            pass

    return None


def _checkout_ref(repo: Repo, git_ref: str) -> None:
    """Update HEAD to point at the requested ref.

    Raises GitAccessError if the ref cannot be resolved or if a short SHA-like
    ref is ambiguous.
    """
    target_sha = resolve_ref_in_repo(repo, git_ref)

    # Also try remote tracking refs (only present in cloned repos)
    if target_sha is None:
        ref_bytes = git_ref.encode()
        try:
            target_sha = repo.get_peeled(Ref(_REFS_REMOTES + b"origin/" + ref_bytes))
        except KeyError:
            pass

    if target_sha is None:
        raise GitAccessError(f"Git ref not found: {git_ref}")

    repo.refs.set_symbolic_ref(_HEAD_REF, _DETACHED_BRANCH_REF)
    repo.refs[_DETACHED_BRANCH_REF] = ObjectID(target_sha)
    porcelain.reset(repo, "hard", target_sha.decode())


def clone_repo_sync(
    repository_url: str,
    git_ref: str | None = None,
    basic_auth: str | None = None,
    dest_dir: Path | str | None = None,
    depth: int | None = None,
    git_sha: str | None = None,
) -> GitCloneResult:
    """
    Clone a repository and checkout a specific ref, if provided. If user reportable access errors occur, raises a GitAccessError.

    Args:
        repository_url: The URL of the repository to clone
        git_ref: The git reference to checkout, if provided. May be a branch
            name, tag, full 40-character commit SHA, or a short SHA-like
            prefix. SHA-like refs are resolved after clone so they do not get
            misclassified as branch names.
        git_sha: Optional pinned commit SHA to check out explicitly. When set,
            this takes precedence over ``git_ref`` for checkout behavior and
            depth selection.
        basic_auth: The basic auth to use to clone the repository, in
            ``user:password`` form. Token-only credentials are also accepted
            (passed via the URL user component).
        dest_dir: The directory to clone the repository to, if provided. The
            directory must not already contain a checkout — partial state is
            not handled. When omitted, a temporary directory is used and
            cleaned up after the function returns.
        depth: Optional shallow clone depth. ``depth=1`` requests only the
            most recent commit on the cloned ref. Defaults to a full clone
            for backwards compatibility.

    Returns:
        GitCloneResult: A dataclass containing the git SHA and resolved git ref (e.g. main if None was provided)
    """
    validate_git_url(repository_url)
    user, password = _split_basic_auth(basic_auth)

    cleanup_temp: Path | None = None
    if dest_dir is None:
        cleanup_temp = Path(tempfile.mkdtemp(prefix="llama_clone_"))
        target_path = cleanup_temp
    else:
        target_path = Path(dest_dir)
        target_path.mkdir(parents=True, exist_ok=True)

    explicit_git_sha = bool(git_sha)
    is_sha_ref = bool(git_ref and SHA_LIKE_RE.match(git_ref))
    branch_arg: bytes | None = None
    if not explicit_git_sha and git_ref and not is_sha_ref:
        branch_arg = git_ref.encode()

    # When the caller pinned a specific SHA, depth=1 of the default branch
    # may not contain the requested commit. dulwich does not have a clean
    # "shallow fetch this SHA" path, so fall back to a full clone in that
    # case to guarantee the SHA is reachable.
    effective_depth = depth
    if explicit_git_sha or is_sha_ref:
        effective_depth = None

    transport_kwargs: dict[str, Any] = {}
    if user is not None:
        transport_kwargs["username"] = user
    if password is not None:
        transport_kwargs["password"] = password

    try:
        try:
            repo = porcelain.clone(
                source=repository_url,
                target=str(target_path),
                depth=effective_depth,
                branch=branch_arg,
                checkout=True,
                errstream=io.BytesIO(),
                **transport_kwargs,
            )
        except Exception as e:
            # Dulwich surfaces network/protocol failures as a mix of
            # HangupException, NotGitRepository, OSError, and assorted
            # GitProtocolError subclasses — normalize them all here.
            raise GitAccessError(
                f"Failed to clone repository {repository_url}: {e}"
            ) from e

        try:
            resolved_ref: str | None = git_ref

            if git_sha is not None:
                # Dulwich cannot fetch a specific SHA at clone time, so check
                # out the requested commit after the clone.
                _checkout_ref(repo, git_sha)
                head_sha_bytes = repo.head()
            else:
                try:
                    head_sha_bytes = repo.head()
                except KeyError as e:
                    raise GitAccessError(
                        f"Cloned repository {repository_url} has no HEAD"
                    ) from e

            if git_sha is None and git_ref and is_sha_ref:
                # Preserve the historical compatibility path for SHA-shaped
                # git_ref values that callers still pass through the generic API.
                _checkout_ref(repo, git_ref)
                head_sha_bytes = repo.head()
                resolved_ref = git_ref
            elif git_ref is None:
                resolved_ref = _resolved_git_ref_for_head(repo)

            return GitCloneResult(git_sha=head_sha_bytes.decode(), git_ref=resolved_ref)
        finally:
            repo.close()
    finally:
        if cleanup_temp is not None:
            shutil.rmtree(cleanup_temp, ignore_errors=True)


async def clone_repo(
    repository_url: str,
    git_ref: str | None = None,
    basic_auth: str | None = None,
    dest_dir: Path | str | None = None,
    depth: int | None = None,
    git_sha: str | None = None,
) -> GitCloneResult:
    """Clone a repository without blocking the event loop."""
    return await asyncio.to_thread(
        clone_repo_sync,
        repository_url,
        git_ref,
        basic_auth,
        dest_dir,
        depth,
        git_sha,
    )


def validate_deployment_file(repo_dir: Path, deployment_file_path: str) -> bool:
    """
    Validate that the deployment file exists in the repository.

    Args:
        repo_dir: The directory of the repository
        deployment_file_path: The path to the deployment file, relative to the repository root

    Returns:
        True if the deployment file exists and appears to be valid, False otherwise
    """
    deployment_file = repo_dir / deployment_file_path
    if not deployment_file.exists():
        return False
    with open(deployment_file, "r") as f:
        try:
            loaded = yaml.safe_load(f)
            if not isinstance(loaded, dict):
                return False
            if "name" not in loaded:
                return False
            if not isinstance(loaded["name"], str):
                return False
            if "services" not in loaded:
                return False
            if not isinstance(loaded["services"], dict):
                return False
            return True  # good nuff for now. Eventually this should parse it into a model validated format
        except yaml.YAMLError:
            return False


def _probe_remote(
    repository_url: str, user: str | None = None, password: str | None = None
) -> bool:
    """Run an ls-remote-equivalent against ``repository_url``."""
    transport_kwargs: dict[str, Any] = {}
    if user is not None:
        transport_kwargs["username"] = user
    if password is not None:
        transport_kwargs["password"] = password
    try:
        client, path = get_transport_and_path(repository_url, **transport_kwargs)
    except Exception:
        return False
    try:
        client.get_refs(path.encode() if isinstance(path, str) else path)
        return True
    except Exception:
        return False


def validate_git_public_access_sync(repository_url: str) -> bool:
    """Check if a git repository is publicly accessible without authentication."""
    validate_git_url_no_ssrf(repository_url)
    return _probe_remote(repository_url)


async def validate_git_public_access(repository_url: str) -> bool:
    """Check if a git repository is publicly accessible without blocking."""
    return await asyncio.to_thread(validate_git_public_access_sync, repository_url)


def validate_git_credential_access_sync(repository_url: str, basic_auth: str) -> bool:
    """Check if a credential provides access to a git repository."""
    validate_git_url_no_ssrf(repository_url)
    user, password = _split_basic_auth(basic_auth)
    return _probe_remote(repository_url, user=user, password=password)


async def validate_git_credential_access(repository_url: str, basic_auth: str) -> bool:
    """Check credentialed git access without blocking the event loop."""
    return await asyncio.to_thread(
        validate_git_credential_access_sync, repository_url, basic_auth
    )


def is_git_repo() -> bool:
    try:
        with _discover_repo():
            return True
    except GitAccessError:
        return False


def list_remotes() -> list[str]:
    with _discover_repo() as repo:
        config = repo.get_config()
        urls: list[str] = []
        for section in config.sections():
            if len(section) == 2 and section[0] == b"remote":
                try:
                    url = config.get(section, b"url")
                except KeyError:
                    continue
                urls.append(url.decode())
        return urls


def get_current_branch() -> str | None:
    with _discover_repo() as repo:
        head_ref = repo.refs.read_ref(_HEAD_REF)
        if head_ref is None:
            return None
        if head_ref.startswith(b"ref: "):
            head_ref = head_ref[5:]
        if not head_ref.startswith(_REFS_HEADS):
            return None
        branch = head_ref[len(_REFS_HEADS) :].decode()
        return branch or None


def get_commit_sha_for_ref(ref: str) -> str | None:
    with _discover_repo() as repo:
        ref_bytes = ref.encode()
        for candidate in (
            Ref(ref_bytes),
            Ref(_REFS_HEADS + ref_bytes),
            Ref(_REFS_TAGS + ref_bytes),
            Ref(_REFS_REMOTES + ref_bytes),
        ):
            try:
                return repo.refs[candidate].decode()
            except KeyError:
                continue

        if FULL_SHA_RE.match(ref):
            try:
                obj = repo[ref_bytes]
                return obj.id.decode()
            except KeyError:
                return None
        return None


def get_git_root(start: Path | None = None) -> Path:
    with _discover_repo(start) as repo:
        return Path(repo.path)


def working_tree_has_changes() -> bool:
    """
    Returns True if the working tree has uncommitted or untracked changes.
    Safe to call; returns False if unable to determine.
    """
    try:
        status = porcelain.status(str(Path.cwd()))
    except Exception:
        return False
    staged = status.staged or {}
    has_staged = any(staged.get(key) for key in ("add", "delete", "modify"))
    return bool(has_staged or status.unstaged or status.untracked)


def get_unpushed_commits_count() -> int | None:
    """
    Returns the number of local commits ahead of the upstream.

    - Returns an integer >= 0 when an upstream is configured
    - Returns None when no upstream is configured
    - Returns 0 if the status cannot be determined
    """
    try:
        with _discover_repo() as repo:
            try:
                head_ref = repo.refs.read_ref(_HEAD_REF)
            except Exception:
                return 0
            if not head_ref or not head_ref.startswith(b"ref: "):
                # Detached HEAD — no upstream concept
                return None
            branch_full = head_ref[5:]
            if not branch_full.startswith(_REFS_HEADS):
                return None
            branch_name = branch_full[len(_REFS_HEADS) :]

            config = repo.get_config()
            try:
                merge_ref = config.get((b"branch", branch_name), b"merge")
                remote = config.get((b"branch", branch_name), b"remote")
            except KeyError:
                return None

            if not merge_ref.startswith(_REFS_HEADS):
                return None
            upstream_branch = merge_ref[len(_REFS_HEADS) :]
            tracking_ref = Ref(_REFS_REMOTES + remote + b"/" + upstream_branch)

            try:
                local_sha = repo.refs[_HEAD_REF]
                upstream_sha = repo.refs[tracking_ref]
            except KeyError:
                return 0

            try:
                walker = Walker(
                    repo.object_store,
                    [local_sha],
                    exclude=[upstream_sha],
                    max_entries=1001,
                )
                count = sum(1 for _ in walker)
            except Exception:
                return 0
            return count
    except GitAccessError:
        return 0
