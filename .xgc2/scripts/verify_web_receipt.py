#!/usr/bin/env python3
"""Verify the committed Media Edge Web player receipt without Node.

The React player, its minified assets and ``build-receipt.json`` are produced
once by the maintained Noble Web builder (``npm run build``). Target Debian
builders are not Web builders: they consume these exact committed bytes, so
they must fail closed when the receipt, the Web source/lock or the embedded
player artifacts have drifted apart.

This verifier is deliberately portable (Python 3.6, standard library only) so
every bionic/focal/jammy/noble target image can run it. It proves:

* the receipt has the current schema and builder identity;
* the receipt's source set is exactly the Web build input set, and every
  source file still has the recorded SHA256;
* the manifest, lock file and receipt agree on the four locked dependencies
  (specifier, locked version, installed version and lock integrity);
* the embedded ``player.js``/``player.css`` bytes still have the recorded
  SHA256.

The installed-dependency file digests in the receipt cannot be recomputed
without ``node_modules``; the Web builder's ``npm run verify:build`` remains the
full check of the installed tree and the source-test job still rebuilds and
git-diff-checks the committed receipt and assets.
"""

import argparse
import hashlib
import json
import os
import sys

SCHEMA_VERSION = 1
BUILDER = "npm run build"
TOP_LEVEL = ("schemaVersion", "builder", "source", "dependencies", "artifacts")
FIXED_SOURCE = (
    "web/package.json",
    "web/package-lock.json",
    "web/tsconfig.json",
    "web/vite.config.ts",
    "web/scripts/minify.mjs",
    "web/scripts/build-receipt.mjs",
)
SOURCE_ROOT = "web/src"
ARTIFACTS = (
    "internal/mediaedge/player/player.css",
    "internal/mediaedge/player/player.js",
)
DEPENDENCIES = ("@xgc2/ui-react", "@xgc2/ui-policy", "react", "react-dom")
DEPENDENCY_KEYS = ("specifier", "lockedVersion", "installedVersion",
                   "lockIntegrity", "installedFilesSha256")
RECEIPT = "internal/mediaedge/player/build-receipt.json"


class ReceiptError(Exception):
    """The committed Web receipt does not describe this checkout."""


def sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_json(path):
    with open(path, "r", encoding="utf-8") as stream:
        return json.load(stream)


def relative(root, path):
    return os.path.relpath(path, root).replace(os.sep, "/")


def source_files(root):
    files = []
    for name in FIXED_SOURCE:
        path = os.path.join(root, *name.split("/"))
        if os.path.islink(path) or not os.path.isfile(path):
            raise ReceiptError("Web build input is missing or not a regular file: " + name)
        files.append(path)
    source_root = os.path.join(root, *SOURCE_ROOT.split("/"))
    if not os.path.isdir(source_root) or os.path.islink(source_root):
        raise ReceiptError("Web source directory is missing: " + SOURCE_ROOT)
    for directory, dirnames, filenames in os.walk(source_root):
        dirnames.sort()
        for name in dirnames:
            child = os.path.join(directory, name)
            if os.path.islink(child):
                raise ReceiptError("Web source contains a symlinked directory: " + relative(root, child))
        for name in sorted(filenames):
            path = os.path.join(directory, name)
            if os.path.islink(path) or not os.path.isfile(path):
                raise ReceiptError("Web source contains a non-regular file: " + relative(root, path))
            files.append(path)
    return files


def receipt_entries(receipt, key):
    entries = receipt.get(key)
    if not isinstance(entries, list):
        raise ReceiptError("Web build receipt %s must be a list" % key)
    recorded = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise ReceiptError("Web build receipt %s entry is invalid" % key)
        name = entry["path"]
        if not isinstance(name, str) or not name:
            raise ReceiptError("Web build receipt %s path is invalid" % key)
        if name in recorded:
            raise ReceiptError("Web build receipt repeats a %s path: %s" % (key, name))
        recorded[name] = entry["sha256"]
    return recorded


def verify_dependencies(receipt, manifest, lock):
    recorded = receipt.get("dependencies")
    if not isinstance(recorded, dict) or set(recorded) != set(DEPENDENCIES):
        raise ReceiptError("Web build receipt dependency set is not current")
    packages = lock.get("packages")
    if not isinstance(packages, dict):
        raise ReceiptError("Web package lock has no package table")
    root_lock = packages.get("", {})
    for name in DEPENDENCIES:
        wanted = (manifest.get("dependencies") or {}).get(name)
        if wanted is None:
            wanted = (manifest.get("devDependencies") or {}).get(name)
        locked = packages.get("node_modules/" + name)
        if not wanted or not isinstance(locked, dict):
            raise ReceiptError("Web dependency is missing from the manifest or lock: " + name)
        entry = recorded[name]
        if not isinstance(entry, dict) or set(entry) != set(DEPENDENCY_KEYS):
            raise ReceiptError("Web build receipt dependency entry is invalid: " + name)
        if entry["specifier"] != wanted:
            raise ReceiptError("Web dependency specifier drifted from the manifest: " + name)
        if entry["lockedVersion"] != locked.get("version"):
            raise ReceiptError("Web dependency locked version drifted from the lock: " + name)
        if entry["installedVersion"] != entry["lockedVersion"]:
            raise ReceiptError("Web dependency installed version differs from the lock: " + name)
        if entry["lockIntegrity"] != locked.get("integrity"):
            raise ReceiptError("Web dependency lock integrity drifted from the lock: " + name)
        root_wanted = (root_lock.get("dependencies") or {}).get(name)
        if root_wanted is None:
            root_wanted = (root_lock.get("devDependencies") or {}).get(name)
        if root_wanted != wanted:
            raise ReceiptError("Web manifest and lock root dependency differ: " + name)
        digest = entry["installedFilesSha256"]
        if (not isinstance(digest, str) or len(digest) != 64 or
                any(character not in "0123456789abcdef" for character in digest)):
            raise ReceiptError("Web dependency installed-file digest is invalid: " + name)


def verify(root):
    receipt_path = os.path.join(root, *RECEIPT.split("/"))
    if os.path.islink(receipt_path) or not os.path.isfile(receipt_path):
        raise ReceiptError("missing Web build receipt: " + RECEIPT)
    receipt = read_json(receipt_path)
    if not isinstance(receipt, dict) or set(receipt) != set(TOP_LEVEL):
        raise ReceiptError("Web build receipt has unexpected fields")
    if receipt.get("schemaVersion") != SCHEMA_VERSION:
        raise ReceiptError("Web build receipt schema is not current")
    if receipt.get("builder") != BUILDER:
        raise ReceiptError("Web build receipt builder is not current")

    manifest = read_json(os.path.join(root, "web", "package.json"))
    lock = read_json(os.path.join(root, "web", "package-lock.json"))

    expected = {}
    for path in source_files(root):
        expected[relative(root, path)] = path
    recorded = receipt_entries(receipt, "source")
    if set(recorded) != set(expected):
        missing = sorted(set(expected) - set(recorded))
        extra = sorted(set(recorded) - set(expected))
        raise ReceiptError(
            "Web build receipt source set differs from the checkout "
            "(missing=%s extra=%s)" % (missing, extra))
    for name in sorted(expected):
        actual = sha256_file(expected[name])
        if recorded[name] != actual:
            raise ReceiptError("Web source changed since the receipt was built: " + name)

    verify_dependencies(receipt, manifest, lock)

    artifacts = receipt_entries(receipt, "artifacts")
    if set(artifacts) != set(ARTIFACTS):
        raise ReceiptError("Web build receipt artifact set is not current")
    for name in ARTIFACTS:
        path = os.path.join(root, *name.split("/"))
        if os.path.islink(path) or not os.path.isfile(path):
            raise ReceiptError("embedded player artifact is missing: " + name)
        actual = sha256_file(path)
        if artifacts[name] != actual:
            raise ReceiptError("embedded player artifact changed since the receipt was built: " + name)
    return len(expected), len(ARTIFACTS)


def main(argv=None):
    default_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    parser = argparse.ArgumentParser(
        description="Verify the committed Media Edge Web player receipt without Node")
    parser.add_argument("--root", default=default_root,
                        help="product checkout root (default: the repository containing this script)")
    args = parser.parse_args(argv)
    root = os.path.abspath(args.root)
    try:
        sources, artifacts = verify(root)
    except ReceiptError as error:
        sys.stderr.write("web receipt verification failed: %s\n" % error)
        return 1
    except (OSError, ValueError, KeyError, TypeError) as error:
        sys.stderr.write("web receipt verification failed: %s\n" % error)
        return 1
    sys.stdout.write(
        "web receipt verified: %d source files and %d embedded player artifacts\n" %
        (sources, artifacts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
