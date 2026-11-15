"""Root hash of the generated data tree. `make verify-repro` runs the generator
twice and compares this hash: same seed in, same bytes out. Contents are hashed
and paths are sorted, so the result is order-independent and depends only on the
bytes plus the deterministic, seed-driven path set."""

import hashlib
import sys
from pathlib import Path


def tree_hash(roots: list[Path]) -> str:
    h = hashlib.sha256()
    # Sort roots and files so neither argument order nor filesystem iteration
    # order can change the result.
    for root in sorted(roots):
        if not root.is_dir():
            # A missing root hashes as empty, so both runs would match and a
            # typo would silently pass as REPRODUCIBLE.
            raise FileNotFoundError(f"not a directory: {root}")
        for f in sorted(p for p in root.rglob("*") if p.is_file()):
            # Path identity includes the root's own name, so moving a file
            # from landing/ to screenshots/ changes the hash.
            h.update(f.relative_to(root.parent).as_posix().encode())
            h.update(hashlib.sha256(f.read_bytes()).digest())  # content identity
    return h.hexdigest()


if __name__ == "__main__":
    roots = [Path(arg) for arg in sys.argv[1:]] or [Path("data")]
    print(tree_hash(roots))
