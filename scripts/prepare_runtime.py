#!/usr/bin/env python3
"""Explicit deployment grant for one media-edge owner's private runtime root."""
import argparse
import os
import stat


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-root", required=True, help="canonical private media-edge namespace; no fallback")
    parser.add_argument("--uid", required=True, type=int, help="numeric media-edge process owner")
    parser.add_argument("--gid", required=True, type=int, help="numeric media-edge process group")
    args = parser.parse_args()
    path = args.runtime_root
    if not os.path.isabs(path) or os.path.normpath(path) != path or path in ("/", "/run", "/tmp", "/var", "/home"):
        parser.error("runtime-root must be a canonical dedicated namespace")
    if min(args.uid, args.gid) < 0:
        parser.error("uid and gid must be nonnegative")
    if os.geteuid() != 0 and (args.uid != os.geteuid() or args.gid not in (os.getegid(), *os.getgroups())):
        parser.error("changing the deployment owner requires root")
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        components = path.lstrip("/").split("/")
        for index, component in enumerate(components):
            created = False
            try:
                next_descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            except FileNotFoundError:
                os.mkdir(component, 0o755 if index < len(components) - 1 else 0o700, dir_fd=descriptor)
                created = True
                next_descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = next_descriptor
            if created and index < len(components) - 1:
                # Only new ancestors are shared traversal directories. Do not
                # let an administrator's restrictive umask hide the grant.
                os.fchmod(descriptor, 0o755)
            if index == len(components) - 1:
                info = os.fstat(descriptor)
                if not created and (info.st_uid != args.uid or info.st_gid != args.gid or stat.S_IMODE(info.st_mode) != 0o700):
                    raise PermissionError("existing runtime namespace is not already granted owned mode0700; refusing to alter it")
                if created and os.geteuid() == 0:
                    os.fchown(descriptor, args.uid, args.gid)
                os.fchmod(descriptor, 0o700)
        for child in ("control", "mediamtx"):
            try:
                os.mkdir(child, 0o700, dir_fd=descriptor)
                created = True
            except FileExistsError:
                created = False
            child_descriptor = os.open(child, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=descriptor)
            try:
                info = os.fstat(child_descriptor)
                if not created and (info.st_uid != args.uid or info.st_gid != args.gid or stat.S_IMODE(info.st_mode) != 0o700):
                    raise PermissionError("existing private child is not already granted owned mode0700")
                if created and os.geteuid() == 0:
                    os.fchown(child_descriptor, args.uid, args.gid)
                os.fchmod(child_descriptor, 0o700)
            finally:
                os.close(child_descriptor)
    finally:
        os.close(descriptor)
    print("rpc_socket=" + path + "/control/control.sock")
    print("mediamtx_runtime_dir=" + path + "/mediamtx")


if __name__ == "__main__":
    main()
