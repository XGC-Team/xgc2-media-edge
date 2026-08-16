#!/usr/bin/env python3
"""Guard the media-edge build manifest contract used by central staging."""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import re
import tempfile
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
GENERATOR = SCRIPT_DIR / "xgc2_artifact_manifest.py"
TOP_LEVEL_KEYS = {
    "schema",
    "product",
    "source_sha",
    "version",
    "distribution",
    "architecture",
    "created_at",
    "ci",
    "debs",
}
DEB_KEYS = {"file", "package", "version", "architecture", "sha256", "size"}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> int:
    spec = importlib.util.spec_from_file_location("media_edge_manifest", GENERATOR)
    require(spec is not None and spec.loader is not None, "cannot load generator")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    with tempfile.TemporaryDirectory(prefix="xgc2-media-edge-manifest-") as directory:
        root = Path(directory)
        deb_dir = root / "debs"
        output_dir = root / "manifests"
        deb_dir.mkdir()
        deb = deb_dir / "xgc2-media-edge_0.2.0-1~focal_amd64.deb"
        deb.write_bytes(b"manifest-contract-fixture")

        fields = {
            "Package": "xgc2-media-edge",
            "Version": "0.2.0-1~focal",
            "Architecture": "amd64",
        }
        module.deb_field = lambda _path, field: fields[field]
        destination = module.build_manifest(
            argparse.Namespace(
                deb_dir=str(deb_dir),
                output_dir=str(output_dir),
                product="xgc2-media-edge",
                product_version="0.2.0-1",
                distribution="focal",
                architecture="amd64",
                source_sha="a" * 40,
                ci_run_id="12345",
                ci_workflow="ci",
                ci_workflow_ref=(
                    "lxk36/xgc2-media-edge/.github/workflows/ci.yml@refs/heads/main"
                ),
            )
        )
        manifest = json.loads(destination.read_text(encoding="utf-8"))

        require(set(manifest) == TOP_LEVEL_KEYS, "unexpected manifest fields")
        require(
            manifest["schema"] == "xgc2.build-artifact.v1",
            "build manifest schema regressed",
        )
        created_at = dt.datetime.fromisoformat(manifest["created_at"].replace("Z", "+00:00"))
        require(created_at.tzinfo is not None, "created_at must include a timezone")
        require(manifest["version"] == "0.2.0-1", "version field is invalid")
        require(
            isinstance(manifest["debs"], list) and len(manifest["debs"]) == 1,
            "debs must contain exactly the fixture package",
        )
        entry = manifest["debs"][0]
        require(set(entry) == DEB_KEYS, "unexpected deb manifest fields")
        require(entry["file"] == deb.name, "deb file field is invalid")
        require(entry["package"] == fields["Package"], "deb package is invalid")
        require(entry["version"] == fields["Version"], "deb version is invalid")
        require(
            entry["architecture"] == fields["Architecture"],
            "deb architecture is invalid",
        )
        require(
            entry["sha256"] == hashlib.sha256(deb.read_bytes()).hexdigest(),
            "deb SHA256 is invalid",
        )
        require(entry["size"] == deb.stat().st_size, "deb size is invalid")
        require(manifest["ci"]["run_id"] == "12345", "CI identity is invalid")

        verified_debs = root / "verified-debs"
        verified_manifests = root / "verified-manifests"
        module.verify_manifest(
            argparse.Namespace(
                artifact_dir=str(root),
                deb_output_dir=str(verified_debs),
                manifest_output_dir=str(verified_manifests),
                product="xgc2-media-edge",
                product_version="0.2.0-1",
                distribution="focal",
                architecture="amd64",
                source_sha="a" * 40,
                ci_run_id="12345",
            )
        )
        require((verified_debs / deb.name).is_file(), "verified deb was not copied")
        require(
            (verified_manifests / destination.name).is_file(),
            "verified manifest was not copied",
        )

    workflows = {
        name: (SCRIPT_DIR.parent.parent / ".github" / "workflows" / name).read_text(
            encoding="utf-8"
        )
        for name in ("ci.yml", "release.yml")
    }
    for name, workflow in workflows.items():
        require("verify-build" in workflow, f"{name} does not verify its manifest")
        for action in re.findall(r"uses:\s+actions/[^@\s]+@([^\s#]+)", workflow):
            require(
                re.fullmatch(r"[0-9a-f]{40}", action) is not None,
                f"{name} contains a floating GitHub Action",
            )
    release = workflows["release.yml"]
    require("run_cpp_quality" not in release, "optional C++ quality gate returned")
    require("run_source_tests" not in release, "optional source gate returned")
    require(
        "if: ${{ inputs.prepare_action == 'release' }}" not in release,
        "compatibility artifacts are not uploaded",
    )
    require(
        re.search(r"(?m)^  source-tests:\n(?!\s+if:)", release) is not None,
        "release source quality may be skipped",
    )
    require(
        re.search(r"(?m)^  package-compliance:\n(?!\s+if:)", release) is not None,
        "release package quality may be skipped",
    )

    print("Build artifact manifest contract checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
