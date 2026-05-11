from unittest import mock as mk

from simio.heap import Heap


def test_heap_top():
    heap = Heap(max_size=4)

    heap.insert(3)
    heap.insert(1)
    heap.insert(2)
    heap.insert(4)

    assert heap.top() == 1
    assert heap.pop() == 1

    assert heap.top() == 2
    assert heap.pop() == 2

    assert heap.top() == 3
    assert heap.pop() == 3

    assert heap.top() == 4
    assert heap.pop() == 4

    assert heap.top() is None


def test_heap_remove():
    heap = Heap(max_size=7)
    heap.insert(0)
    heap.insert(3)
    heap.insert(4)
    heap.insert(5)
    heap.insert(6)
    heap.insert(2)
    heap.insert(1)

    assert heap.remove(3) == 5
    assert heap.top() == 0
    assert heap.remove(2) == 1
    assert heap.top() == 0
    assert heap.remove(0) == 0
    assert heap.top() == 2
    assert heap.remove(0) == 2
    assert heap.top() == 3
    assert heap.remove(0) == 3
    assert heap.top() == 4
    assert heap.remove(0) == 4
    assert heap.top() == 6
    assert heap.remove(0) == 6


def test_heap_push_pop():
    heap = Heap(max_size=13)

    items = [0, 2, 1, 4, 3, 5, 6, 7, 8, 12, 11, 10, 9]
    for item in items:
        heap.insert(item)

    actual_result = []
    while heap:
        actual_result.append(heap.pop())

    expected_result = sorted(items)
    assert actual_result == expected_result
    assert heap.pop() is None
    assert len(heap) == 0


def test_heap_clear():
    heap = Heap(max_size=3)

    items = [0, 1, 2]
    for item in items:
        heap.insert(item)

    heap.clear()
    assert len(heap) == 0


def test_heap_swap_callback():
    callback_mock = mk.Mock()

    heap = Heap(max_size=4, swap_callback=callback_mock)

    heap.insert(4)
    heap.insert(3)
    heap.insert(2)
    heap.insert(1)

    assert heap.pop() == 1
    assert heap.pop() == 2
    assert heap.pop() == 3
    assert heap.pop() == 4

    callback_mock.assert_has_calls([
        mk.call(1, 3, 0, 4),
        mk.call(2, 2, 0, 3),
        mk.call(3, 1, 1, 4),
        mk.call(1, 1, 0, 2),
        mk.call(0, 1, 3, 4),
        mk.call(0, 4, 1, 2),
        mk.call(0, 2, 2, 3),
        mk.call(0, 3, 1, 4),
    ])
