#!/usr/bin/env -S uv run --script
# /// script
# dependencies = ["click"]
# ///
"""
Local development environment for cloud_llama_deploy.

  up     — create/ensure cluster and start tilt
  down   — tear down tilt resources, retaining data
  down --delete — also delete the kind cluster (kind target only)

Targets:
  kind            — (default) creates a kind cluster
  docker-desktop  — uses Docker Desktop's built-in Kubernetes
  k3s             — uses the local k3s cluster in the default context
"""

import os
import subprocess
import sys
from pathlib import Path
from textwrap import dedent

import click

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
NAMESPACE = "llama-agents"
APPS_NAMESPACE = "llama-agents-apps"

TARGETS = ("kind", "docker-desktop", "k3s")
K8S_CONTEXTS = {
    "kind": "kind-kind",
    "docker-desktop": "docker-desktop",
    "k3s": "default",
}


def run(
    cmd: list[str], check: bool = True, capture: bool = False
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd, check=False, capture_output=capture, text=True, cwd=PROJECT_ROOT
    )
    if check and result.returncode != 0:
        if capture and result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(1)
    return result


# ---------------------------------------------------------------------------
# kind helpers
# ---------------------------------------------------------------------------


def kind_cluster_exists() -> bool:
    result = run(["kind", "get", "clusters"], check=False, capture=True)
    return "kind" in result.stdout


def ensure_kind_cluster() -> None:
    if kind_cluster_exists():
        result = run(
            ["kind", "export", "kubeconfig", "--name", "kind"],
            check=False,
            capture=True,
        )
        if result.returncode == 0:
            return
        print("Cluster 'kind' exists but is broken, recreating...")
        run(["kind", "delete", "cluster", "--name", "kind"])

    print("Creating kind cluster 'kind'...")

    kind_config = dedent("""\
        kind: Cluster
        apiVersion: kind.x-k8s.io/v1alpha4
        nodes:
        - role: control-plane
          kubeadmConfigPatches:
          - |
            kind: InitConfiguration
            nodeRegistration:
              kubeletExtraArgs:
                node-labels: "ingress-ready=true"
          extraPortMappings:
          - containerPort: 80
            hostPort: 8090
            protocol: TCP
          - containerPort: 443
            hostPort: 8444
            protocol: TCP
    """)

    import tempfile

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(kind_config)
        config_path = f.name

    try:
        run(
            [
                "kind",
                "create",
                "cluster",
                "--name",
                "kind",
                "--config",
                config_path,
            ]
        )
    finally:
        os.unlink(config_path)

    install_ingress_controller("kind")


# ---------------------------------------------------------------------------
# docker-desktop helpers
# ---------------------------------------------------------------------------


def ensure_docker_desktop_cluster() -> None:
    context = K8S_CONTEXTS["docker-desktop"]
    result = run(
        ["kubectl", "--context", context, "cluster-info"],
        check=False,
        capture=True,
    )
    if result.returncode != 0:
        print(
            f"Kubernetes context '{context}' is not reachable.\n"
            "Enable Kubernetes in Docker Desktop → Settings → Kubernetes.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Switch to the docker-desktop context
    run(["kubectl", "config", "use-context", context])
    install_ingress_controller("docker-desktop")


def ensure_k3s_cluster() -> None:
    context = K8S_CONTEXTS["k3s"]
    result = run(
        ["kubectl", "--context", context, "cluster-info"],
        check=False,
        capture=True,
    )
    if result.returncode != 0:
        print(
            f"Kubernetes context '{context}' is not reachable.",
            file=sys.stderr,
        )
        sys.exit(1)

    run(["kubectl", "config", "use-context", context])


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------

INGRESS_MANIFESTS = {
    "kind": "https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/kind/deploy.yaml",
    "docker-desktop": "https://raw.githubusercontent.com/kubernetes/ingress-nginx/main/deploy/static/provider/cloud/deploy.yaml",
}


def install_ingress_controller(target: str) -> None:
    result = run(
        ["kubectl", "get", "namespace", "ingress-nginx"], check=False, capture=True
    )
    if result.returncode == 0:
        return

    print("Installing nginx ingress controller...")
    run(["kubectl", "apply", "-f", INGRESS_MANIFESTS[target]])

    import time

    start = time.time()
    while time.time() - start < 10:
        result = run(
            [
                "kubectl",
                "get",
                "pods",
                "--namespace",
                "ingress-nginx",
                "--selector=app.kubernetes.io/component=controller",
                "--no-headers",
            ],
            check=False,
            capture=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            break
        time.sleep(1)

    run(
        [
            "kubectl",
            "wait",
            "--namespace",
            "ingress-nginx",
            "--for=condition=ready",
            "pod",
            "--selector=app.kubernetes.io/component=controller",
            "--timeout=300s",
        ]
    )


target_option = click.option(
    "--target",
    type=click.Choice(TARGETS),
    default=None,
    envvar="DEV_TARGET",
    help="Kubernetes target cluster. [default: kind]",
)

apps_ns_option = click.option(
    "--apps-namespace",
    "apps_namespace",
    is_flag=False,
    flag_value=APPS_NAMESPACE,
    default=None,
    envvar="DEV_APPS_NAMESPACE",
    help=(
        "Run in split-namespace mode: put LlamaDeployment CRs and app "
        f"resources in this namespace (default: {APPS_NAMESPACE}). "
        "Unset = single-namespace mode."
    ),
)


def resolve_target(ctx: click.Context, target: str | None) -> str:
    return target or ctx.obj.get("target") or "kind"


def resolve_apps_namespace(
    ctx: click.Context, apps_namespace: str | None
) -> str | None:
    return apps_namespace or ctx.obj.get("apps_namespace")


@click.group()
@target_option
@apps_ns_option
@click.pass_context
def cli(ctx: click.Context, target: str | None, apps_namespace: str | None) -> None:
    """Local development environment for cloud_llama_deploy."""
    ctx.ensure_object(dict)
    ctx.obj["target"] = target
    ctx.obj["apps_namespace"] = apps_namespace


@cli.command()
@target_option
@apps_ns_option
@click.pass_context
def up(ctx: click.Context, target: str | None, apps_namespace: str | None) -> None:
    """Create/ensure cluster and start tilt."""
    target = resolve_target(ctx, target)
    apps_namespace = resolve_apps_namespace(ctx, apps_namespace)

    # Check required tools
    version_cmds: dict[str, list[str]] = {
        "kubectl": ["kubectl", "version", "--client"],
        "docker": ["docker", "--version"],
        "tilt": ["tilt", "version"],
    }
    if target == "kind":
        version_cmds["kind"] = ["kind", "--version"]

    for tool, cmd in version_cmds.items():
        if run(cmd, check=False, capture=True).returncode != 0:
            print(f"Missing required tool: {tool}", file=sys.stderr)
            sys.exit(1)

    if target == "kind":
        ensure_kind_cluster()
    elif target == "docker-desktop":
        ensure_docker_desktop_cluster()
    else:
        ensure_k3s_cluster()

    # Ensure namespaces exist
    for ns in [NAMESPACE] + ([apps_namespace] if apps_namespace else []):
        result = run(["kubectl", "get", "namespace", ns], check=False, capture=True)
        if result.returncode != 0:
            run(["kubectl", "create", "namespace", ns])

    if not (PROJECT_ROOT / ".env").exists():
        print(
            "Note: no .env file found. GitHub integration requires GITHUB_APP_PRIVATE_KEY, GITHUB_APP_CLIENT_ID, GITHUB_APP_NAME, GITHUB_APP_SECRET."
        )

    ingress_port = "8090" if target == "kind" else "80"
    print("Starting tilt...")
    print("  API:     http://localhost:8011")
    print("  Tilt UI: http://localhost:10350")
    print(f"  Ingress: *.127.0.0.1.nip.io:{ingress_port}")
    if apps_namespace:
        print(f"  Apps namespace: {apps_namespace}")
    tilt_args = [
        "tilt",
        "up",
        "-f",
        str(PROJECT_ROOT / "operator" / "Tiltfile"),
        "--",
        target,
    ]
    if apps_namespace:
        tilt_args += ["--apps-namespace", apps_namespace]
    os.execvp("tilt", tilt_args)


@cli.command()
@click.option("--delete", is_flag=True, help="Also delete the kind cluster")
@target_option
@apps_ns_option
@click.pass_context
def down(
    ctx: click.Context,
    delete: bool,
    target: str | None,
    apps_namespace: str | None,
) -> None:
    """Tear down tilt resources. Use --delete to also remove the cluster (kind only)."""
    target = resolve_target(ctx, target)
    apps_namespace = resolve_apps_namespace(ctx, apps_namespace)

    tilt_args = [
        "tilt",
        "down",
        "-f",
        str(PROJECT_ROOT / "operator" / "Tiltfile"),
        "--",
        target,
    ]
    if apps_namespace:
        tilt_args += ["--apps-namespace", apps_namespace]
    run(tilt_args, check=False)

    if delete:
        if target != "kind":
            print("--delete only applies to the kind target", file=sys.stderr)
            return
        if kind_cluster_exists():
            print("Deleting kind cluster 'kind'...")
            run(["kind", "delete", "cluster", "--name", "kind"])


@cli.command()
@target_option
@apps_ns_option
@click.pass_context
def status(ctx: click.Context, target: str | None, apps_namespace: str | None) -> None:
    """Show cluster and deployment status."""
    target = resolve_target(ctx, target)
    apps_namespace = resolve_apps_namespace(ctx, apps_namespace)
    context = K8S_CONTEXTS[target]

    if target == "kind":
        if not kind_cluster_exists():
            print("No kind cluster 'kind' found")
            return
        run(["kind", "export", "kubeconfig", "--name", "kind"])
    else:
        result = run(
            ["kubectl", "--context", context, "cluster-info"],
            check=False,
            capture=True,
        )
        if result.returncode != 0:
            print(f"Kubernetes context '{context}' is not reachable")
            return
        run(["kubectl", "config", "use-context", context])

    run(["kubectl", "get", "pods", "-n", NAMESPACE], check=False)
    if apps_namespace and apps_namespace != NAMESPACE:
        print()
        run(["kubectl", "get", "pods", "-n", apps_namespace], check=False)


if __name__ == "__main__":
    cli()
