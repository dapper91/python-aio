"""
Automatic Repeat Request (ARQ) protocol family utils.
"""

import itertools as it
from typing import Generator, Iterator, NamedTuple, Optional

from simio import heap as hp
from simio.heap import ComparableP


class IndexRange(NamedTuple):
    """
    Index range.
    """

    beg: int
    end: int


class SlidingWindow[ItemT]:
    """
    Sliding window.
    It can be thought of as an infinite array with limited working index range moving forward.

    :param window_size: the size of the sliding window
    """

    def __init__(self, window_size: int):
        assert window_size > 0, "windows must not be zero sized"

        self._size = 0
        self._window_size = window_size
        self._window_begin_idx = 0
        self._buffer: list[Optional[ItemT]] = [None for _ in range(window_size)]
        self._buffer_begin_idx = 0

    @property
    def size(self) -> int:
        """
        Returns the number of items in the window.
        """

        return self._size

    @property
    def full(self) -> bool:
        """
        Returns `True` if the window is full.
        """

        return self.size == self.window_size

    @property
    def window_size(self) -> int:
        """
        Returns the size of the sliding window.
        """

        return self._window_size

    @property
    def window_range(self) -> IndexRange:
        """
        Returns the first and the last index of the sliding window.
        """

        return IndexRange(
            self._window_begin_idx,
            self._window_begin_idx + self._window_size - 1,
        )

    def __str__(self) -> str:
        return str(self._buffer)

    def __repr__(self) -> str:
        return repr(self._buffer)

    def __len__(self) -> int:
        return self._size

    def __getitem__(self, index: int) -> Optional[ItemT]:
        index = self._clamp_index(index)
        return self._buffer[index]

    def __setitem__(self, index: int, item: Optional[ItemT]) -> None:
        index = self._clamp_index(index)
        if item is None and self._buffer[index] is not None:
            self._size -= 1
        if item is not None and self._buffer[index] is None:
            self._size += 1

        self._buffer[index] = item

    def __contains__(self, item: ItemT) -> bool:
        return item in self._buffer

    def __iter__(self) -> Iterator[Optional[ItemT]]:
        return it.chain(
            self._buffer[self._buffer_begin_idx:],
            self._buffer[0:self._buffer_begin_idx],
        )

    def _clamp_index(self, index: int) -> int:
        if index < self._window_begin_idx or index >= self._window_begin_idx + self._window_size:
            raise IndexError("buffer index out of window")

        window_offset = index - self._window_begin_idx
        buffer_idx = (self._buffer_begin_idx + window_offset) % self._window_size

        return buffer_idx

    def clear(self) -> None:
        """
        Clears the buffer of items keeping the window range.
        """

        for idx in range(len(self._buffer)):
            self._buffer[idx] = None

        self._buffer_begin_idx = 0
        self._size = 0

    def pop(self) -> Optional[ItemT]:
        """
        Returns the first item of the window moving the sliding window one step forward.
        """

        item = self._buffer[self._buffer_begin_idx]
        self._buffer[self._buffer_begin_idx] = None
        self._buffer_begin_idx = (self._buffer_begin_idx + 1) % self._window_size
        self._window_begin_idx += 1
        self._size -= 1

        return item

    def first(self) -> Optional[ItemT]:
        """
        Returns the first item of the window.
        """

        return self._buffer[self._buffer_begin_idx]

    def move(self, offset: int) -> None:
        """
        Moves the window forward by `offset` steps.
        """

        assert offset >= 0, "offset must be positive"

        if offset < self._window_size:
            for _ in range(offset):
                self._buffer[self._buffer_begin_idx] = None
                self._buffer_begin_idx = (self._buffer_begin_idx + 1) % self._window_size
                self._size -= 1
        else:
            self.clear()

        self._window_begin_idx += offset


class RankedSlidingWindow[ItemT: ComparableP]:
    """
    Sliding window with item rank support.

    :param window_size: the size of the sliding window
    """

    def __init__(self, window_size: int):
        self._window: SlidingWindow[tuple[ItemT, int]] = SlidingWindow(window_size)
        self._ranks: hp.Heap[tuple[ItemT, int]] = hp.Heap(max_size=window_size, swap_callback=self._swap_refs)

    def _swap_refs(
            self,
            rnk_idx1: int,
            rnk_item1: tuple[ItemT, int],
            rnk_idx2: int,
            rnk_item2: tuple[ItemT, int],
    ) -> None:
        rank1, wnd_ref1 = rnk_item1
        rank2, wnd_ref2 = rnk_item2

        wnd_item1 = self._window[wnd_ref1]
        assert wnd_item1 is not None, "refs are broken"
        item1, rnk_ref1 = wnd_item1
        assert rnk_idx1 == rnk_ref1, "refs are broken"

        wnd_item2 = self._window[wnd_ref2]
        assert wnd_item2 is not None, "refs are broken"
        item2, rnk_ref2 = wnd_item2
        assert rnk_idx2 == rnk_ref2, "refs are broken"

        self._window[wnd_ref1] = (item1, rnk_ref2)
        self._window[wnd_ref2] = (item2, rnk_ref1)

    @property
    def size(self) -> int:
        """
        Returns the number of items in the window.
        """

        return self._window.size

    @property
    def full(self) -> bool:
        """
        Returns `True` if the window is full.
        """

        return self._window.full

    @property
    def window_size(self) -> int:
        """
        Returns the size of the sliding window.
        """

        return self._window.window_size

    @property
    def window_range(self) -> IndexRange:
        """
        Returns the first and the last index of the sliding window.
        """

        return self._window.window_range

    def __str__(self) -> str:
        return str(self._window)

    def __repr__(self) -> str:
        return repr(self._window)

    def __len__(self) -> int:
        return len(self._window)

    def __getitem__(self, index: int) -> Optional[ItemT]:
        if (wnd_item := self._window[index]) is not None:
            item, rnk_ref = wnd_item
            return item

        return None

    def __setitem__(self, index: int, item: Optional[ItemT]) -> None:
        if (wnd_item := self._window[index]) is not None:
            cur_item, rnk_ref = wnd_item
            rank, wnd_ref = self._ranks.remove(rnk_ref)
            assert wnd_ref == index, "refs are broken"
            self._window[index] = None

        if item is not None:
            self._window[index] = (item, len(self._ranks))
            self._ranks.insert((item, index))

    def __contains__(self, item: ItemT) -> bool:
        for wnd_item in self._window:
            if wnd_item is not None:
                cur_item, rnk_ref = wnd_item
                if item == cur_item:
                    return True

        return False

    def __iter__(self) -> Iterator[Optional[ItemT]]:
        def walk_window() -> Generator[Optional[ItemT], None, None]:
            for wnd_item in self._window:
                if wnd_item is not None:
                    item, rnk_ref = wnd_item
                    yield item
                else:
                    yield None

        return walk_window()

    def clear(self) -> None:
        """
        Clears the buffer of items keeping the window range.
        """

        self._window.clear()
        self._ranks.clear()

    def first(self) -> Optional[ItemT]:
        """
        Returns the first item of the window.
        """

        if (wnd_item := self._window.first()) is not None:
            item, rnk_ref = wnd_item
            return item

        return None

    def pop(self) -> Optional[ItemT]:
        """
        Returns the first item of the window moving the sliding window one step forward.
        """

        if (wnd_item := self._window.first()) is not None:
            item, rnk_ref = wnd_item
            self._ranks.remove(rnk_ref)
            self._window.pop()
            return item
        else:
            self._window.pop()
            return None

    def move(self, offset: int) -> None:
        """
        Moves the window forward by `offset` steps.
        """

        if offset < self._window.window_size:
            for _ in range(offset):
                self.pop()
        else:
            self._ranks.clear()
            self._window.move(offset)

    def top(self) -> Optional[ItemT]:
        """
        Returns an item with the lowest rank.
        """

        if (top := self._ranks.top()) is not None:
            rank, wnd_ref = top
            wnd_item = self._window[wnd_ref]
            assert wnd_item is not None, "refs are broken"
            item, rnk_ref = wnd_item

            return item

        return None
