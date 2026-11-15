"""tree_hash: the fingerprint verify-repro compares across two generator runs."""

import pytest

from src.generator.tree_hash import tree_hash


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "landing").mkdir()
    (tmp_path / "screenshots").mkdir()
    (tmp_path / "landing" / "a.parquet").write_bytes(b"rows")
    (tmp_path / "screenshots" / "b.png").write_bytes(b"pixels")
    return [tmp_path / "landing", tmp_path / "screenshots"]


def test_root_order_does_not_matter(tree):
    assert tree_hash(tree) == tree_hash(list(reversed(tree)))


def test_screenshot_bytes_change_the_hash(tree):
    landing, screenshots = tree
    before = tree_hash(tree)
    (screenshots / "b.png").write_bytes(b"other pixels")
    assert tree_hash(tree) != before


def test_moving_a_file_between_roots_changes_the_hash(tree):
    landing, screenshots = tree
    before = tree_hash(tree)
    (landing / "a.parquet").rename(screenshots / "a.parquet")
    assert tree_hash(tree) != before


def test_missing_root_fails_loudly(tree):
    landing, screenshots = tree
    with pytest.raises(FileNotFoundError):
        tree_hash([landing, screenshots.with_name("screenshotz")])
