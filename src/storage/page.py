"""
页式存储模型
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple

from .constants import PAGE_SIZE, ROW_ACTIVE, ROW_DELETED, PAGE_HEADER_SIZE, SLOT_SIZE

@dataclass
class Slot:
    offset: int
    length: int
    flag: int = ROW_ACTIVE

class Page:
    """简化页模型，页尾部保存槽目录(slotted page)。"""

    HEADER_SIZE = PAGE_HEADER_SIZE
    SLOT_SIZE = SLOT_SIZE

    def __init__(self, page_id: int, data: bytes | None = None):
        self.page_id = page_id
        if data is None:
            self._data = bytearray(PAGE_SIZE)
            self.free_start = self.HEADER_SIZE
            self.slots: List[Slot] = []
        else:
            self._data = bytearray(data)
            self._deserialize_header_and_slots()

    def _deserialize_header_and_slots(self) -> None:
        self.free_start = int.from_bytes(self._data[0:4], "little")
        slot_count = int.from_bytes(self._data[4:8], "little")
        self.slots = []
        slots_start = PAGE_SIZE - slot_count * self.SLOT_SIZE
        i = 0
        while i < slot_count:
            base = slots_start + i * self.SLOT_SIZE
            offset = int.from_bytes(self._data[base:base+4], "little")
            length = int.from_bytes(self._data[base+4:base+8], "little")
            flag = self._data[base+8]
            self.slots.append(Slot(offset, length, flag))
            i += 1

    def _serialize(self) -> bytes:
        # write header
        self._data[0:4] = int(self.free_start).to_bytes(4, "little")
        self._data[4:8] = int(len(self.slots)).to_bytes(4, "little")
        free_space = self._free_space()
        self._data[8:12] = int(free_space).to_bytes(4, "little")
        # write slots
        slots_start = PAGE_SIZE - len(self.slots) * self.SLOT_SIZE
        idx = 0
        for s in self.slots:
            base = slots_start + idx * self.SLOT_SIZE
            self._data[base:base+4] = int(s.offset).to_bytes(4, "little")
            self._data[base+4:base+8] = int(s.length).to_bytes(4, "little")
            self._data[base+8] = int(s.flag) & 0xFF
            idx += 1
        return bytes(self._data)

    def _free_space(self) -> int:
        slots_space = (len(self.slots) + 1) * self.SLOT_SIZE
        return PAGE_SIZE - self.free_start - max(0, slots_space)

    def has_space_for(self, length: int) -> bool:
        return self._free_space() >= length

    # page.py 确保 insert_row 方法正确
    def insert_row(self, row_data: bytes) -> Tuple[int, int]:
        length = len(row_data)
        # ensure space for row and one more slot
        required = length + self.SLOT_SIZE
        if self._free_space() < required:
            raise ValueError("Not enough space in page")

        offset = self.free_start
        # 确保有足够的空间
        if offset + length > len(self._data):
            raise ValueError("Not enough space in page data array")

        # 写入数据
        self._data[offset:offset + length] = row_data
        self.free_start += length

        # 添加槽
        self.slots.append(Slot(offset, length, ROW_ACTIVE))

        return (len(self.slots) - 1, offset)

    def iterate_rows(self):
        for idx, s in enumerate(self.slots):
            if s.flag == ROW_ACTIVE and s.length > 0:
                yield idx, bytes(self._data[s.offset:s.offset+s.length])

    def mark_deleted(self, slot_index: int) -> None:
        if 0 <= slot_index < len(self.slots):
            self.slots[slot_index].flag = ROW_DELETED

    def to_bytes(self) -> bytes:
        return self._serialize()