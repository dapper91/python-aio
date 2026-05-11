from typing import Any, Callable, Optional, Protocol


class ComparableP(Protocol):
    def __lt__(self, value: Any, /) -> bool: ...
    def __eq__(self, value: Any, /) -> bool: ...


class Heap[ItemT: ComparableP]:
    """
    Heap data structure.
    """

    def __init__(self, max_size: int, swap_callback: Optional[Callable[[int, ItemT, int, ItemT], None]] = None) -> None:
        self._max_size = max_size
        self._swap_callback = swap_callback
        self._heap: list[ItemT] = []

    def __len__(self) -> int:
        return len(self._heap)

    def __bool__(self) -> bool:
        return bool(self._heap)

    def __getitem__(self, index: int) -> ItemT:
        return self._heap[index]

    def clear(self) -> None:
        """
        Remove all items from the heap.
        """

        self._heap.clear()

    def insert(self, item: ItemT) -> None:
        """
        Inserts an item into the heap.

        :param item: item to be inserted
        """

        if len(self._heap) == self._max_size:
            raise IndexError("heap is full")

        item_idx = len(self._heap)
        self._heap.append(item)

        self._siftdown(item_idx)

    def remove(self, index: int) -> ItemT:
        """
        Removes an element located at the specific index in the heap.

        :param index: index of the element to be deleted
        """

        last_idx = len(self._heap) - 1
        rem_item, last_item = self._heap[index], self._heap[last_idx]
        self._heap[index], self._heap[last_idx] = last_item, rem_item
        if callback := self._swap_callback:
            callback(index, rem_item, last_idx, last_item)

        item = self._heap.pop()
        if index != len(self._heap):
            if (
                index == 0 or
                self._heap[(index - 1) // 2] == self._heap[index] or
                self._heap[(index - 1) // 2] < self._heap[index]
            ):
                self._siftup(index)
            else:
                self._siftdown(index)

        return item

    def pop(self) -> Optional[ItemT]:
        """
        Pops the smallest item from the heap.

        :return: the smallest item
        """

        if len(self._heap) == 0:
            return None
        elif len(self._heap) == 1:
            return self._heap.pop()
        else:
            top_idx, last_idx = 0, len(self._heap) - 1
            top_item, last_item = self._heap[top_idx], self._heap[last_idx]
            self._heap[top_idx], self._heap[last_idx] = last_item, top_item
            if callback := self._swap_callback:
                callback(top_idx, top_item, last_idx, last_item)

            item = self._heap.pop()
            self._siftup(0)

            return item

    def top(self) -> Optional[ItemT]:
        """
        Returns the smallest item from the map.

        :return: the smallest item
        """

        if len(self._heap) > 0:
            return self._heap[0]
        else:
            return None

    def _siftup(self, idx: int) -> None:
        item = self._heap[idx]

        left_child_idx = 2 * idx + 1
        while left_child_idx < len(self._heap):
            right_child_idx = left_child_idx + 1
            if right_child_idx >= len(self._heap) or self._heap[left_child_idx] < self._heap[right_child_idx]:
                min_child_idx = left_child_idx
            else:
                min_child_idx = right_child_idx

            child_item = self._heap[min_child_idx]
            if item < child_item or item == child_item:
                break

            self._heap[idx] = child_item
            self._heap[min_child_idx] = item
            if callback := self._swap_callback:
                callback(idx, item, min_child_idx, child_item)

            idx = min_child_idx
            left_child_idx = 2 * idx + 1

    def _siftdown(self, idx: int) -> None:
        item = self._heap[idx]

        while idx > 0:
            parent_idx = (idx - 1) // 2
            parent_item = self._heap[parent_idx]
            if item < parent_item:
                self._heap[idx] = parent_item
                self._heap[parent_idx] = item
                if callback := self._swap_callback:
                    callback(idx, item, parent_idx, parent_item)

                idx = parent_idx
            else:
                break
