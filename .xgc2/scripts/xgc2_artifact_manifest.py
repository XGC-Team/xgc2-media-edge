#!/usr/bin/env python3

"""Create and verify Media Edge build artifacts for the XGC2 release train."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import shutil
import subprocess
from typing import Any


BUILD_SCHEMA = "xgc2.build-artifact.v2"
EMPTY_DEPENDENCY_SET_DIGEST = (
    "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"
)
PREPARE_ACTIONS = {"ci", "release", "compatibility-verify"}
SOURCE_SHA = re.compile(r"^[0-9a-f]{40}(?:[0-9a-f]{24})?$")
BUILD_FIELDS = {
    "schema",
    "product",
    "source_sha",
    "version",
    "distribution",
    "architecture",
    "prepareAction",
    "dependencySetDigest",
    "dependencyMode",
    "dependencies",
    "ci",
    "debs",
}
CI_FIELDS = {"run_id", "workflow", "workflow_ref"}
DEB_FIELDS = {"file", "package", "version", "architecture", "sha256", "size"}


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


def validate_contract(
    *, prepare_action: str, dependency_set_digest: str, dependency_mode: str
) -> None:
    if prepare_action not in PREPARE_ACTIONS:
        raise ValueError("prepareAction is invalid")
    if dependency_set_digest != EMPTY_DEPENDENCY_SET_DIGEST:
        raise ValueError("Media Edge dependencySetDigest must identify the empty set")
    if dependency_mode != "locked-source":
        raise ValueError("Media Edge has no dependencies and requires locked-source mode")


def deb_entry(path: pathlib.Path, requested_architecture: str) -> dict[str, Any]:
    architecture = deb_field(path, "Architecture")
    if architecture not in (requested_architecture, "all"):
        raise ValueError(
            f"artifact architecture mismatch: {path.name} is {architecture}, "
            f"expected {requested_architecture} or all"
        )
    return {
        "file": path.name,
        "package": deb_field(path, "Package"),
        "version": deb_field(path, "Version"),
        "architecture": architecture,
        "sha256": sha256(path),
        "size": path.stat().st_size,
    }


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def build_manifest(arguments: argparse.Namespace) -> pathlib.Path:
    validate_contract(
        prepare_action=arguments.prepare_action,
        dependency_set_digest=arguments.dependency_set_digest,
        dependency_mode=arguments.dependency_mode,
    )
    if SOURCE_SHA.fullmatch(arguments.source_sha) is None:
        raise ValueError("source SHA must contain 40 or 64 lowercase hexadecimal characters")
    deb_dir = pathlib.Path(arguments.deb_dir)
    debs = [
        deb_entry(path, arguments.architecture)
        for path in sorted(deb_dir.glob("*.deb"))
    ]
    if not debs:
        raise ValueError(f"no .deb artifacts found in {deb_dir}")
    manifest = {
        "schema": BUILD_SCHEMA,
        "product": arguments.product,
        "source_sha": arguments.source_sha,
        "version": arguments.product_version,
        "distribution": arguments.distribution,
        "architecture": arguments.architecture,
        "prepareAction": arguments.prepare_action,
        "dependencySetDigest": arguments.dependency_set_digest,
        "dependencyMode": arguments.dependency_mode,
        "dependencies": [],
        "ci": {
            "run_id": str(arguments.ci_run_id),
            "workflow": arguments.ci_workflow,
            "workflow_ref": arguments.ci_workflow_ref,
        },
        "debs": debs,
    }
    destination = pathlib.Path(arguments.output_dir) / (
        f"{arguments.product}_{arguments.distribution}_"
        f"{arguments.architecture}.build.json"
    )
    write_json(destination, manifest)
    return destination


def validate_deb(path: pathlib.Path, declared: Any, architecture: str) -> None:
    if not isinstance(declared, dict) or set(declared) != DEB_FIELDS:
        raise ValueError("deb manifest fields are not exact")
    actual = deb_entry(path, architecture)
    if declared != actual:
        raise ValueError(f"deb metadata or digest mismatch: {path.name}")


def verify_manifest(arguments: argparse.Namespace) -> None:
    validate_contract(
        prepare_action=arguments.prepare_action,
        dependency_set_digest=arguments.dependency_set_digest,
        dependency_mode=arguments.dependency_mode,
    )
    artifact_root = pathlib.Path(arguments.artifact_dir).resolve(strict=True)
    expected = {
        "product": arguments.product,
        "source_sha": arguments.source_sha,
        "version": arguments.product_version,
        "distribution": arguments.distribution,
        "architecture": arguments.architecture,
        "prepareAction": arguments.prepare_action,
        "dependencySetDigest": arguments.dependency_set_digest,
        "dependencyMode": arguments.dependency_mode,
        "dependencies": [],
    }
    candidates: list[tuple[pathlib.Path, dict[str, Any], list[pathlib.Path]]] = []
    for manifest_path in sorted(artifact_root.rglob("*.build.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if not isinstance(manifest, dict) or set(manifest) != BUILD_FIELDS:
            raise ValueError(f"build manifest fields are not exact: {manifest_path}")
        if manifest.get("schema") != BUILD_SCHEMA:
            raise ValueError(f"unsupported build manifest schema: {manifest_path}")
        if any(manifest.get(key) != value for key, value in expected.items()):
            continue
        ci = manifest.get("ci")
        if not isinstance(ci, dict) or set(ci) != CI_FIELDS or not all(ci.values()):
            raise ValueError(f"CI identity is invalid: {manifest_path}")
        if str(ci["run_id"]) != str(arguments.ci_run_id):
            continue
        declared_debs = manifest.get("debs")
        if not isinstance(declared_debs, list) or not declared_debs:
            raise ValueError(f"deb list is empty: {manifest_path}")
        resolved: list[pathlib.Path] = []
        for declared in declared_debs:
            filename = declared.get("file") if isinstance(declared, dict) else None
            if not isinstance(filename, str) or pathlib.Path(filename).name != filename:
                raise ValueError(f"unsafe deb filename: {filename!r}")
            matches = sorted(artifact_root.rglob(filename))
            if len(matches) != 1 or not matches[0].is_file():
                raise ValueError(f"expected exactly one artifact named {filename}")
            validate_deb(matches[0], declared, arguments.architecture)
            resolved.append(matches[0])
        candidates.append((manifest_path, manifest, resolved))
    if len(candidates) != 1:
        raise ValueError(f"expected exactly one matching build manifest, found {len(candidates)}")
    manifest_path, _manifest, debs = candidates[0]
    deb_output = pathlib.Path(arguments.deb_output_dir)
    manifest_output = pathlib.Path(arguments.manifest_output_dir)
    deb_output.mkdir(parents=True, exist_ok=True)
    manifest_output.mkdir(parents=True, exist_ok=True)
    for deb in debs:
        shutil.copy2(deb, deb_output / deb.name)
    shutil.copy2(manifest_path, manifest_output / manifest_path.name)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    subparsers = result.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--product", required=True)
    common.add_argument("--product-version", required=True)
    common.add_argument("--distribution", required=True)
    common.add_argument("--architecture", required=True)
    common.add_argument("--source-sha", required=True)
    common.add_argument("--ci-run-id", required=True)
    common.add_argument("--prepare-action", required=True, choices=sorted(PREPARE_ACTIONS))
    common.add_argument("--dependency-set-digest", required=True)
    common.add_argument("--dependency-mode", required=True, choices=["locked-source"])

    build = subparsers.add_parser("build", parents=[common])
    build.add_argument("--deb-dir", required=True)
    build.add_argument("--output-dir", required=True)
    build.add_argument("--ci-workflow", required=True)
    build.add_argument("--ci-workflow-ref", required=True)
    build.set_defaults(function=build_manifest)

    verify = subparsers.add_parser("verify-build", parents=[common])
    verify.add_argument("--artifact-dir", required=True)
    verify.add_argument("--deb-output-dir", required=True)
    verify.add_argument("--manifest-output-dir", required=True)
    verify.set_defaults(function=verify_manifest)
    return result


def main() -> int:
    arguments = parser().parse_args()
    arguments.function(arguments)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
