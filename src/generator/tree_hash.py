"""Root hash of the generated data tree. `make verify-repro` runs the generator
twice and compares this hash: same seed in, same bytes out. Contents are hashed
and paths are sorted, so the result is order-independent and depends only on the
bytes plus the deterministic, seed-driven path set."""
import hashlib
import sys
from pathlib import Path


def tree_hash(root: Path) -> str:
    h = hashlib.sha256()
    # Sort by path so filesystem iteration order can never change the result.
    for f in sorted(p for p in root.rglob("*") if p.is_file()):
        h.update(f.relative_to(root).as_posix().encode())  # path identity
        h.update(hashlib.sha256(f.read_bytes()).digest())  # content identity
    return h.hexdigest()


if __name__ == "__main__":
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
    print(tree_hash(root))