#!/usr/bin/env python3

"""Create an XGC2 trusted build-artifact manifest for Debian outputs."""

import argparse
import hashlib
import json
import pathlib
import subprocess
from datetime import datetime, timezone


def deb_field(path: pathlib.Path, field: str) -> str:
    return subprocess.check_output(
        ["dpkg-deb", "-f", str(path), field], text=True
    ).strip()


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_manifest(arguments: argparse.Namespace) -> pathlib.Path:
    deb_dir = pathlib.Path(arguments.deb_dir)
    output_dir = pathlib.Path(arguments.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    debs = []
    for deb in sorted(deb_dir.glob("*.deb")):
        architecture = deb_field(deb, "Architecture")
        if architecture not in (arguments.architecture, "all"):
            raise SystemExit(
                f"artifact architecture mismatch: {deb.name} is {architecture}, "
                f"expected {arguments.architecture} or all"
            )
        debs.append(
            {
                "file": deb.name,
                "package": deb_field(deb, "Package"),
                "version": deb_field(deb, "Version"),
                "architecture": architecture,
                "sha256": sha256(deb),
                "size": deb.stat().st_size,
            }
        )
    if not debs:
        raise SystemExit(f"no .deb artifacts found in {deb_dir}")

    manifest = {
        "schema": "xgc2.build-artifact.v1",
        "product": arguments.product,
        "source_sha": arguments.source_sha,
        "version": arguments.product_version,
        "distribution": arguments.distribution,
        "architecture": arguments.architecture,
        "ci": {
            "run_id": str(arguments.ci_run_id),
            "workflow": arguments.ci_workflow,
            "workflow_ref": arguments.ci_workflow_ref,
        },
        "created_at": datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z"),
        "debs": debs,
    }
    destination = output_dir / (
        f"{arguments.product}_{arguments.distribution}_"
        f"{arguments.architecture}.build.json"
    )
    destination.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return destination


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--deb-dir", required=True)
    build.add_argument("--output-dir", required=True)
    build.add_argument("--product", required=True)
    build.add_argument("--product-version", required=True)
    build.add_argument("--distribution", required=True)
    build.add_argument("--architecture", required=True)
    build.add_argument("--source-sha", required=True)
    build.add_argument("--ci-run-id", required=True)
    build.add_argument("--ci-workflow", required=True)
    build.add_argument("--ci-workflow-ref", required=True)
    build.set_defaults(function=build_manifest)
    arguments = parser.parse_args()
    arguments.function(arguments)


if __name__ == "__main__":
    main()
