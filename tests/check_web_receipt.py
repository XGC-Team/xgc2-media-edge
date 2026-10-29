#!/usr/bin/env python3
"""Focused contract tests for the portable Media Edge Web receipt verifier.

The verifier is the packaging gate: target builders must accept the current
known-good player and reject a changed Web source, a changed lock file or a
changed embedded player artifact. It must also have no bypass switch.
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VERIFIER = os.path.join(REPO, ".xgc2", "scripts", "verify_web_receipt.py")
FIXTURE_FILES = (
    "web/package.json",
    "web/package-lock.json",
    "web/tsconfig.json",
    "web/vite.config.ts",
    "web/scripts/minify.mjs",
    "web/scripts/build-receipt.mjs",
)
FIXTURE_TREES = ("web/src", "internal/mediaedge/player")
RECEIPT = "internal/mediaedge/player/build-receipt.json"


def run(root, *arguments):
    result = subprocess.run(
        [sys.executable, VERIFIER, "--root", root] + list(arguments),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    return result.returncode, result.stdout, result.stderr


def fixture(directory):
    for name in FIXTURE_FILES:
        source = os.path.join(REPO, *name.split("/"))
        destination = os.path.join(directory, *name.split("/"))
        parent = os.path.dirname(destination)
        if not os.path.isdir(parent):
            os.makedirs(parent)
        shutil.copy2(source, destination)
    for tree in FIXTURE_TREES:
        source = os.path.join(REPO, *tree.split("/"))
        destination = os.path.join(directory, *tree.split("/"))
        if os.path.isdir(destination):
            shutil.rmtree(destination)
        shutil.copytree(source, destination)


def path(root, name):
    return os.path.join(root, *name.split("/"))


def expect_accept(root):
    code, stdout, stderr = run(root)
    if code != 0 or "verified" not in stdout:
        raise AssertionError("known-good build was rejected: code=%d out=%r err=%r" %
                             (code, stdout, stderr))


def expect_reject(root, fragment, arguments=()):
    code, stdout, stderr = run(root, *arguments)
    if code == 0:
        raise AssertionError("drifted build was accepted: out=%r" % stdout)
    if fragment not in stderr:
        raise AssertionError("rejection did not name %r: %r" % (fragment, stderr))


def mutate_source(root):
    with open(path(root, "web/src/styles.css"), "a", encoding="utf-8") as stream:
        stream.write("\n/* drift */\n")


def mutate_lock(root):
    # Still valid JSON, but no longer the locked bytes the player was built from.
    with open(path(root, "web/package-lock.json"), "a", encoding="utf-8") as stream:
        stream.write("\n")


def mutate_artifact(root):
    with open(path(root, "internal/mediaedge/player/player.js"), "a", encoding="utf-8") as stream:
        stream.write("\n")


def refresh_source_hash(root, name):
    receipt_path = path(root, RECEIPT)
    with open(receipt_path, "r", encoding="utf-8") as stream:
        receipt = json.load(stream)
    with open(path(root, name), "rb") as stream:
        digest = hashlib.sha256(stream.read()).hexdigest()
    for entry in receipt["source"]:
        if entry["path"] == name:
            entry["sha256"] = digest
            break
    else:
        raise AssertionError("fixture receipt has no source entry for " + name)
    with open(receipt_path, "w", encoding="utf-8") as stream:
        json.dump(receipt, stream, indent=2)
        stream.write("\n")


def mutate_dependency_specifier(root):
    manifest_path = path(root, "web/package.json")
    with open(manifest_path, "r", encoding="utf-8") as stream:
        manifest = json.load(stream)
    manifest["dependencies"]["react"] = "19.2.0"
    with open(manifest_path, "w", encoding="utf-8") as stream:
        json.dump(manifest, stream, indent=2)
        stream.write("\n")
    # A hash-only verifier would accept this; the lock identity must still fail.
    refresh_source_hash(root, "web/package.json")


def case(name, mutate, fragment, arguments=()):
    with tempfile.TemporaryDirectory(prefix="xgc2-media-web-receipt-") as directory:
        fixture(directory)
        if mutate is not None:
            mutate(directory)
        if arguments:
            expect_reject(directory, fragment, arguments)
        else:
            expect_reject(directory, fragment)
    print("ok %s" % name)


def main():
    expect_accept(REPO)
    print("ok current known-good build")

    with tempfile.TemporaryDirectory(prefix="xgc2-media-web-receipt-") as directory:
        fixture(directory)
        os.unlink(path(directory, RECEIPT))
        expect_reject(directory, "missing Web build receipt")
    print("ok missing receipt")

    case("changed web source", mutate_source, "Web source changed")
    case("changed web lock", mutate_lock, "web/package-lock.json")
    case("changed embedded artifact", mutate_artifact, "embedded player artifact changed")
    case("changed dependency specifier", mutate_dependency_specifier, "specifier drifted")

    with tempfile.TemporaryDirectory(prefix="xgc2-media-web-receipt-") as directory:
        fixture(directory)
        with open(path(directory, "web/src/extra.ts"), "w", encoding="utf-8") as stream:
            stream.write("export const drift = true\n")
        expect_reject(directory, "source set differs")
    print("ok extra web source")

    with tempfile.TemporaryDirectory(prefix="xgc2-media-web-receipt-") as directory:
        fixture(directory)
        expect_reject(directory, "unrecognized arguments", ("--skip",))
    print("ok no bypass flag")

    print("web receipt verifier tests passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
