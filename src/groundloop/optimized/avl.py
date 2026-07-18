"""A small deterministic AVL ordered set used by the optimized lane.

Python's standard library has no balanced ordered-map type.  This module keeps
the physical-index claim honest: insert, delete, and endpoint search are
worst-case logarithmic, while interval enumeration is output-sensitive.
"""

from __future__ import annotations

from dataclasses import dataclass

ScoreKey = tuple[float, str]


@dataclass(slots=True)
class _Node:
    key: ScoreKey
    left: _Node | None = None
    right: _Node | None = None
    height: int = 1
    size: int = 1


def _height(node: _Node | None) -> int:
    return node.height if node is not None else 0


def _size(node: _Node | None) -> int:
    return node.size if node is not None else 0


def _refresh(node: _Node) -> None:
    node.height = 1 + max(_height(node.left), _height(node.right))
    node.size = 1 + _size(node.left) + _size(node.right)


def _rotate_left(node: _Node) -> _Node:
    pivot = node.right
    if pivot is None:
        raise AssertionError("left rotation requires a right child")
    node.right = pivot.left
    pivot.left = node
    _refresh(node)
    _refresh(pivot)
    return pivot


def _rotate_right(node: _Node) -> _Node:
    pivot = node.left
    if pivot is None:
        raise AssertionError("right rotation requires a left child")
    node.left = pivot.right
    pivot.right = node
    _refresh(node)
    _refresh(pivot)
    return pivot


def _balance(node: _Node) -> _Node:
    _refresh(node)
    factor = _height(node.left) - _height(node.right)
    if factor > 1:
        if node.left is None:
            raise AssertionError("invalid AVL balance")
        if _height(node.left.left) < _height(node.left.right):
            node.left = _rotate_left(node.left)
        return _rotate_right(node)
    if factor < -1:
        if node.right is None:
            raise AssertionError("invalid AVL balance")
        if _height(node.right.right) < _height(node.right.left):
            node.right = _rotate_right(node.right)
        return _rotate_left(node)
    return node


def _insert(node: _Node | None, key: ScoreKey) -> _Node:
    if node is None:
        return _Node(key)
    if key < node.key:
        node.left = _insert(node.left, key)
    elif key > node.key:
        node.right = _insert(node.right, key)
    else:
        raise KeyError(f"duplicate ordered-set key: {key!r}")
    return _balance(node)


def _minimum(node: _Node) -> _Node:
    while node.left is not None:
        node = node.left
    return node


def _delete(node: _Node | None, key: ScoreKey) -> _Node | None:
    if node is None:
        raise KeyError(f"missing ordered-set key: {key!r}")
    if key < node.key:
        node.left = _delete(node.left, key)
    elif key > node.key:
        node.right = _delete(node.right, key)
    elif node.left is None:
        return node.right
    elif node.right is None:
        return node.left
    else:
        successor = _minimum(node.right)
        node.key = successor.key
        node.right = _delete(node.right, successor.key)
    return _balance(node)


@dataclass(slots=True)
class ScoreOrderedSet:
    """Ordered set of ``(score, stable_id)`` keys backed by an AVL tree."""

    _root: _Node | None = None

    def __len__(self) -> int:
        return _size(self._root)

    def add(self, key: ScoreKey) -> None:
        self._root = _insert(self._root, key)

    def remove(self, key: ScoreKey) -> None:
        self._root = _delete(self._root, key)

    def maximum_score(self) -> float | None:
        node = self._root
        if node is None:
            return None
        while node.right is not None:
            node = node.right
        return node.key[0]

    def ids_between(self, lower: float, upper: float) -> tuple[str, ...]:
        """Return IDs with scores in the half-open interval [lower, upper)."""
        if lower >= upper:
            return ()
        result: list[str] = []

        def visit(node: _Node | None) -> None:
            if node is None:
                return
            score = node.key[0]
            if score >= lower:
                visit(node.left)
            if lower <= score < upper:
                result.append(node.key[1])
            if score < upper:
                visit(node.right)

        visit(self._root)
        return tuple(result)

    def validate(self) -> None:
        """Assert ordering, cached size/height, and AVL balance invariants."""

        def check(
            node: _Node | None,
            lower: ScoreKey | None,
            upper: ScoreKey | None,
        ) -> tuple[int, int]:
            if node is None:
                return 0, 0
            if lower is not None and not lower < node.key:
                raise AssertionError("AVL lower ordering violation")
            if upper is not None and not node.key < upper:
                raise AssertionError("AVL upper ordering violation")
            left_height, left_size = check(node.left, lower, node.key)
            right_height, right_size = check(node.right, node.key, upper)
            expected_height = 1 + max(left_height, right_height)
            expected_size = 1 + left_size + right_size
            if node.height != expected_height or node.size != expected_size:
                raise AssertionError("AVL cached metadata violation")
            if abs(left_height - right_height) > 1:
                raise AssertionError("AVL balance violation")
            return expected_height, expected_size

        check(self._root, None, None)
