import itertools as it

import pytest

from simio.arq import RankedSlidingWindow, SlidingWindow


def test_sliding_window_indexing():
    window_size = 10
    window = SlidingWindow[int](window_size=window_size)
    assert window.size == 0

    with pytest.raises(IndexError):
        window[-1] = -1

    window[0] = 0
    window[9] = 9
    assert window.size == 2

    with pytest.raises(IndexError):
        window[10] = 10

    window.pop()
    assert window.size == 1

    with pytest.raises(IndexError):
        window[0] = 0

    window[10] = 10
    assert window.size == 2


@pytest.mark.parametrize('batch_size, window_size, iterations', [(10, 10, 2), (7, 10, 3)])
def test_sliding_window_pop(batch_size: int, window_size: int, iterations: int):
    window = SlidingWindow[int](window_size=window_size)

    index_gen = it.count()
    for iteration in range(iterations):
        values: list[int] = []
        for _ in range(batch_size * iteration, batch_size * (iteration + 1)):
            value = idx = next(index_gen)
            values.append(value)
            window[idx] = value

        assert window.size == len(values)

        for value in values:
            assert window.pop() == value

        assert window.size == 0
        assert window.first() is None


def test_sliding_window_move():
    window_size = 3
    window = SlidingWindow[int](window_size=window_size)

    for i in range(window_size):
        window[i] = i

    assert len(window) == 3

    assert window[0] == 0
    assert window[1] == 1
    assert window[2] == 2

    window.move(offset=1)
    assert window[1] == 1
    assert window[2] == 2
    assert window[3] is None
    assert len(window) == 2

    window.move(offset=1)
    assert window[2] == 2
    assert window[3] is None
    assert window[4] is None
    assert len(window) == 1

    window.move(offset=1)
    assert window[3] is None
    assert window[4] is None
    assert window[5] is None
    assert len(window) == 0


def test_sliding_window_iterator():
    window_size = 6
    window = SlidingWindow[int](window_size=window_size)

    for i in range(window_size):
        window[i] = i

    assert list(iter(window)) == list(range(window_size))

    window.move(offset=window_size // 2)
    for i in range(window_size, window_size + window_size // 2):
        window[i] = i

    assert list(iter(window)) == list(range(window_size // 2, window_size // 2 + window_size))


def test_ranked_sliding_window():
    window_size = 5
    window = RankedSlidingWindow[tuple[int, str]](window_size=window_size)

    window[0] = (5, 'a')
    window[1] = (3, 'b')
    window[2] = (2, 'c')
    window[3] = (1, 'd')
    window[4] = (4, 'e')

    assert window[0] == (5, 'a')
    assert window[1] == (3, 'b')
    assert window[2] == (2, 'c')
    assert window[3] == (1, 'd')
    assert window[4] == (4, 'e')
    assert len(window) == 5

    assert window.first() == (5, 'a')
    assert window.top() == (1, 'd')

    assert window.pop() == (5, 'a')
    assert len(window) == 4

    window.move(1)
    assert window.first() == (2, 'c')
    assert window[5] is None
    assert len(window) == 3

    window[5] = (0, 'a')
    assert window.first() == (2, 'c')
    assert window.top() == (0, 'a')

    window[5] = None
    assert window.top() == (1, 'd')
