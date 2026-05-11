import contextlib as cl
import dataclasses as dc
import functools as ft
from enum import IntEnum
from typing import ClassVar, Generator, Optional, Self

from simio import arq

__all__ = [
    'Command',
    'Kcp',
    'KcpError',
    'KcpPacket',
    'PacketConversationIdError',
    'PacketDeserializationError',
]


class KcpError(Exception):
    """
    Base KCP protocol error
    """


class PacketDeserializationError(KcpError):
    """
    KCP packet deserialization error.
    """


class PacketConversationIdError(KcpError):
    """
    KCP conversation id mismatched.
    """


class Command(IntEnum):
    """
    KCP protocol command.
    """

    # push data
    PUSH = 81
    # ack
    ACK = 82
    # window probe (ask)
    WINDOW_ASK = 83
    # window size (tell)
    WINDOW_SIZE = 84


@dc.dataclass(frozen=True)
class KcpPacket:
    """
    KCP protocol packet.

    Packet format:

    0               4   5   6       8 (BYTE)
    +---------------+---+---+-------+
    |     conv      |cmd|frg|  wnd  |
    +---------------+---+---+-------+   8
    |     ts        |     sn        |
    +---------------+---------------+  16
    |     una       |     len       |
    +---------------+---------------+  24
    |        DATA (optional)        |
    +-------------------------------+

    :param conversation_id: conversation id
    :param command: command
    :param fragment: fragment count
    :param window: window size
    :param timestamp: timestamp (in milliseconds)
    :param serial: serial number
    :param unacked_serial: un-acknowledged serial number
    :param length: data length
    :param data: data
    """

    HEADER_SIZE: ClassVar[int] = 24

    conversation_id: int
    command: Command
    fragment: int
    window: int
    timestamp: int
    serial: int
    unacked_serial: int
    length: int
    data: bytes

    def __post_init__(self) -> None:
        if not 0 <= self.conversation_id <= 2 ** 32 - 1:
            raise ValueError("conversation id out of range")
        if not 0 <= self.fragment <= 2 ** 8 - 1:
            raise ValueError("fragment out of range")
        if not 0 <= self.window <= 2 ** 16 - 1:
            raise ValueError("window out of range")
        if not 0 <= self.timestamp <= 2 ** 32 - 1:
            raise ValueError("timestamp out of range")
        if not 0 <= self.serial <= 2 ** 32 - 1:
            raise ValueError("serial out of range")
        if not 0 <= self.unacked_serial <= 2 ** 32 - 1:
            raise ValueError("unacked serial out of range")
        if not 0 <= self.length <= 2 ** 32 - 1:
            raise ValueError("length out of range")

        if len(self.data) != self.length:
            raise ValueError("length is not correct")

    def encode(self) -> bytes:
        buffer = bytearray(self.HEADER_SIZE + self.length)

        buffer[0:4] = self.conversation_id.to_bytes(4)
        buffer[4:5] = self.command.to_bytes(1)
        buffer[5:6] = self.fragment.to_bytes(1)
        buffer[6:8] = self.window.to_bytes(2)
        buffer[8:12] = self.timestamp.to_bytes(4)
        buffer[12:16] = self.serial.to_bytes(4)
        buffer[16:20] = self.unacked_serial.to_bytes(4)
        buffer[20:24] = self.length.to_bytes(4)
        buffer[24:] = self.data

        return buffer

    @classmethod
    def decode(cls, data: bytes) -> Self:
        if len(data) < cls.HEADER_SIZE:
            raise ValueError("insufficient data")

        return cls(
            conversation_id=int.from_bytes(data[0:4]),
            command=Command(int.from_bytes(data[4:5])),
            fragment=int.from_bytes(data[5:6]),
            window=int.from_bytes(data[6:8]),
            timestamp=int.from_bytes(data[8:12]),
            serial=int.from_bytes(data[12:16]),
            unacked_serial=int.from_bytes(data[16:20]),
            length=int.from_bytes(data[20:24]),
            data=data[24:],
        )

    @classmethod
    def build_push(
            cls,
            conv_id: int,
            serial: int,
            timestamp: int,
            data: bytes,
            window: int,
            unacked_serial: int,
            fragment: int = 0,
    ) -> Self:
        return cls(
            conversation_id=conv_id,
            command=Command.PUSH,
            fragment=fragment,
            window=window,
            timestamp=timestamp,
            serial=serial,
            unacked_serial=unacked_serial,
            length=len(data),
            data=data,
        )

    @classmethod
    def build_ack(
            cls,
            conv_id: int,
            serial: int,
            timestamp: int,
            window: int,
            unacked_serial: int,
            fragment: int = 0,
    ) -> Self:
        return cls(
            conversation_id=conv_id,
            command=Command.ACK,
            fragment=fragment,
            window=window,
            timestamp=timestamp,
            serial=serial,
            unacked_serial=unacked_serial,
            length=0,
            data=b'',
        )

    @classmethod
    def build_window_size(
            cls,
            conv_id: int,
            window: int,
            unacked_serial: int,
    ) -> Self:
        return cls(
            conversation_id=conv_id,
            command=Command.WINDOW_SIZE,
            fragment=0,
            window=window,
            timestamp=0,
            serial=0,
            unacked_serial=unacked_serial,
            length=0,
            data=b'',
        )

    @classmethod
    def build_window_ask(
            cls,
            conv_id: int,
            window: int,
            unacked_serial: int,
    ) -> Self:
        return cls(
            conversation_id=conv_id,
            command=Command.WINDOW_ASK,
            fragment=0,
            window=window,
            timestamp=0,
            serial=0,
            unacked_serial=unacked_serial,
            length=0,
            data=b'',
        )

@dc.dataclass(frozen=True)
class SendPacketBufferEntry:
    serial: int
    fragment: int
    data: bytes
    send_at: float
    ack_timeout: float
    fastack: int = 0
    attempts: int = 0

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, SendPacketBufferEntry):
            return NotImplemented

        return self.send_at == other.send_at

    def __lt__(self, other: Self) -> bool:
        return self.send_at < other.send_at


@dc.dataclass(frozen=True)
@ft.total_ordering
class AckBufferEntry:
    serial: int
    fragment: int
    timestamp: int

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AckBufferEntry):
            return NotImplemented

        return self.serial == other.serial

    def __lt__(self, other: Self) -> bool:
        return self.serial < other.serial


@dc.dataclass(frozen=True)
class RecvPacketBufferEntry:
    serial: int
    fragment: int
    data: bytes


class Kcp:
    """
    KCP protocol processor.

    :param conv_id: the conversation id used to identify each connection
    :param send_buffer_size: send buffer size (in packets)
    :param recv_buffer_size: receive buffer size (in packets)
    :param max_transfer_unit: maximum transfer unit
    """

    def __init__(
            self,
            conv_id: int,
            send_buffer_size: int,
            recv_buffer_size: int,
            max_transfer_unit: int = 1400,
            fastack_threshold: int = 0,
            fastack_limit: int = 5
    ):
        self._conv_id = conv_id
        self._max_transfer_unit = max_transfer_unit
        self._max_segment_size = max_transfer_unit - KcpPacket.HEADER_SIZE

        self._fastack_threshold = fastack_threshold
        self._fastack_limit = fastack_limit
        self._slow_start_threshold = 2
        self._increment = 0
        self._send_window_size = send_buffer_size
        self._send_next_serial = 0
        self._send_window = arq.RankedSlidingWindow[SendPacketBufferEntry](window_size=send_buffer_size)

        self._min_recv_timeout = 0.1
        self._max_recv_timeout = 600.0
        self._ack_timeout = 0.2
        self._next_probe_timestamp = float('inf')

        self._recv_window_size_requested = False
        self._recv_next_serial = 0
        self._recv_window = arq.SlidingWindow[RecvPacketBufferEntry](window_size=recv_buffer_size)
        self._ack_window = arq.SlidingWindow[AckBufferEntry](window_size=recv_buffer_size)

        self._remote_window_size = 128
        self._smoothed_round_trip_time = 0.0
        self._round_trip_time_variation = 0.0

    def put_packet(self, packet_data: bytes, timestamp: float) -> bool:
        """
        Puts a packet to KCP protocol processor.
        The packet can be ignored if the packet conversation id doesn't match
        or the internal buffer is full or packet is out of receiving window range.

        :param packet_data: packet data
        :param timestamp: current timestamp
        :return: `True` if the packet has been handled, otherwise `False`
        """

        try:
            packet = KcpPacket.decode(packet_data)
        except ValueError as e:
            raise PacketDeserializationError(f"packet deserialization error: {e}") from e

        if packet.conversation_id != self._conv_id:
            raise PacketConversationIdError(packet.conversation_id)

        match packet.command:
            case Command.PUSH:
                return self._handle_push(packet)
            case Command.ACK:
                return self._handle_ack(packet, timestamp)
            case Command.WINDOW_ASK:
                return self._handle_window_ack(packet)
            case Command.WINDOW_SIZE:
                return self._handle_window_size(packet)
            case _:
                raise AssertionError("unreachable")

    @cl.contextmanager
    def process_packet(self, timestamp: float) -> Generator[Optional[bytes], None, None]:
        """
        Packet processing context.
        Yields a ready-to-send packet from KCP protocol processor.
        Packet is not removed from the internal buffer until the context is successfully closed.
        If there is no such packet `None` is yielded.

        :param timestamp: current timestamp
        :return: serialized packet or `None` if there is no ready one.
        """

        # window = min(self._remote_window_size, self._send_window.window_range)

        if self._remote_window_size == 0:
            packet = KcpPacket.build_window_ask(
                conv_id=self._conv_id,
                window=self._recv_window.window_range.end - self._recv_next_serial,
                unacked_serial=self._recv_next_serial,
            )
            yield packet.encode()

        elif self._recv_window_size_requested:
            packet = KcpPacket.build_window_size(
                conv_id=self._conv_id,
                window=self._recv_window.window_range.end - self._recv_next_serial,
                unacked_serial=self._recv_next_serial,
            )
            yield packet.encode()
            self._recv_window_size_requested = False

        elif ack_packet_entry := self._ack_window.first():
            packet = KcpPacket.build_ack(
                conv_id=self._conv_id,
                serial=ack_packet_entry.serial,
                timestamp=int(timestamp * 1000),
                window=self._recv_window.window_range.end - self._recv_next_serial,
                unacked_serial=self._recv_next_serial,
                fragment=ack_packet_entry.fragment,
            )
            yield packet.encode()
            self._ack_window.pop()

        elif send_packet_entry := self._send_window.top():
            if send_packet_entry.send_at < timestamp:
                packet = KcpPacket.build_push(
                    conv_id=self._conv_id,
                    serial=send_packet_entry.serial,
                    fragment=send_packet_entry.fragment,
                    timestamp=int(timestamp * 1000),
                    data=send_packet_entry.data,
                    window=self._recv_window.window_range.end - self._recv_next_serial,
                    unacked_serial=self._recv_next_serial,
                )
                if send_packet_entry.attempts == 0:
                    send_packet_entry.ack_timeout = self._ack_timeout
                    # send_packet_entry.send_at = timestamp + send_packet_entry.ack_timeout + self._ack_timeout / 3
                    send_packet_entry.send_at = timestamp + send_packet_entry.ack_timeout
                    send_packet_entry.attempts += 1
                else:
                    # send_packet_entry.ack_timeout += max(send_packet_entry.ack_timeout, self._ack_timeout)
                    # send_packet_entry.ack_timeout += send_packet_entry.ack_timeout / 2
                    send_packet_entry.ack_timeout += self._ack_timeout / 2
                    send_packet_entry.send_at = timestamp + self._ack_timeout

                yield packet.encode()

            elif self._fastack_threshold:
                for send_packet_entry in self._send_window:
                    if send_packet_entry is not None:
                        if send_packet_entry.fastack >= self._fastack_threshold:
                            if send_packet_entry.attempts < self._fastack_limit:
                                send_packet_entry.send_at = timestamp + send_packet_entry.ack_timeout
                                # TODO: send?
                        else:
                            yield None
                            break
                else:
                    yield None

            else:
                yield None

        else:
            yield None

    def send_data(self, data: bytes, timestamp: float) -> bool:
        fragments_cnt = len(data) // self._max_segment_size + (1 if len(data) % self._max_segment_size else 0)

        wnd_rng = self._send_window.window_range
        if self._send_next_serial + fragments_cnt - 1 > wnd_rng.end:
            return False

        for fragment in range(fragments_cnt):
            offset = fragment * self._max_segment_size
            self._send_window[self._send_next_serial] = SendPacketBufferEntry(
                serial=self._send_next_serial,
                fragment=fragment,
                data=data[offset:offset + self._max_segment_size],
                send_at=timestamp,
            )
            self._send_next_serial += 1

        return True

    def recv_data(self) -> Optional[bytes]:
        is_buffer_full = self._recv_window.full

        if (fragments := self._assemble_fragments()) is not None:
            if is_buffer_full:
                self._recv_window_size_requested = True

            return b"".join(fragment.data for fragment in fragments)

        return None

    def next_poll(self, timestamp: float) -> float:
        """
        Returns the next timestamp when
        """

        next_poll_timestamp = float('inf')

        if self._remote_window_size == 0:
            return self._next_probe_timestamp

        if self._recv_window_size_requested:
            next_poll_timestamp = min(next_poll_timestamp, timestamp)

        if len(self._ack_window) > 0:
            next_poll_timestamp = min(next_poll_timestamp, timestamp)

        if self._recv_window.first() is not None:
            next_poll_timestamp = min(next_poll_timestamp, timestamp)

        if (packet := self._send_window.top()) is not None:
            next_poll_timestamp = min(next_poll_timestamp, packet.send_at)

        return next_poll_timestamp

    def _cancel_acked_packets(self, before_serial: int) -> int:
        wnd_rng = self._send_window.window_range

        if wnd_rng.beg < before_serial:
            offset = before_serial - wnd_rng.beg
            self._send_window.move(offset=offset)
            self._send_next_serial += offset
        else:
            offset = 0

        return offset

    def _assemble_fragments(self) -> Optional[list[RecvPacketBufferEntry]]:
        fragments_buf: list[RecvPacketBufferEntry] = []
        for packet in self._recv_window:
            if packet is None:
                return None

            fragments_buf.append(packet)
            if packet.fragment == 0:
                self._recv_window.move(offset=len(fragments_buf))
                return fragments_buf

        return None

    def _handle_push(self, packet: KcpPacket) -> bool:
        self._cancel_acked_packets(before_serial=packet.unacked_serial)
        self._update_send_window_size(packet.window)

        wnd_rng = self._recv_window.window_range
        if not wnd_rng.beg <= packet.serial <= wnd_rng.end:
            # serial out of receive window range
            return False

        if self._recv_window[packet.serial] is None:
            self._recv_window[packet.serial] = RecvPacketBufferEntry(packet.serial, packet.fragment, packet.data)

            for serial in range(self._recv_next_serial, wnd_rng.end + 1):
                self._recv_next_serial = serial
                if self._recv_window[serial] is None:
                    break

        self._add_ack_packet(packet)

        return True

    def _handle_ack(self, packet: KcpPacket, timestamp: float) -> bool:
        self._cancel_acked_packets(before_serial=packet.unacked_serial)
        self._ack_timeout = self._calculate_ack_timeout(packet, timestamp)
        self._update_send_window_size(packet.window)
        self._mark_fastack(before_serial=packet.serial)

        wnd_rng = self._send_window.window_range
        if not wnd_rng.beg <= packet.serial <= wnd_rng.end:
            return False

        self._send_window[packet.serial] = None
        while self._send_window.first() is None:
            self._send_window.move(offset=1)

        return True

    def _handle_window_ack(self, packet: KcpPacket) -> bool:
        self._cancel_acked_packets(before_serial=packet.unacked_serial)
        self._update_send_window_size(packet.window)

        return True

    def _handle_window_size(self, packet: KcpPacket) -> bool:
        self._cancel_acked_packets(before_serial=packet.unacked_serial)
        self._update_send_window_size(packet.window)

        self._recv_window_size_requested = True
        return True

    def _add_ack_packet(self, packet: KcpPacket) -> bool:
        wnd_rng = self._ack_window.window_range
        if packet.serial < wnd_rng.beg:
            # serial behind ack window
            return False

        if packet.serial > wnd_rng.end:
            self._ack_window.move(packet.serial - wnd_rng.end)

        self._ack_window[packet.serial] = AckBufferEntry(packet.serial, packet.fragment, packet.timestamp)

        return True

    def _calculate_ack_timeout(self, packet: KcpPacket, timestamp: float) -> float:
        def clamp(val: float, minimum: float, maximum: float) -> float:
            return min(max(val, minimum), maximum)

        round_trip_time = max(0.0, timestamp - packet.timestamp)
        # smoothed RTT calculated as the average value across last 8 values
        if self._smoothed_round_trip_time == 0.0:
            self._smoothed_round_trip_time = round_trip_time
        else:
            self._smoothed_round_trip_time = (7 * self._smoothed_round_trip_time + round_trip_time) / 8

        # RTT variation calculated as the average difference between RTT and smoothed RTT across last 4 values
        if self._round_trip_time_variation == 0.0:
            self._round_trip_time_variation = round_trip_time / 2
        else:
            delta = abs(round_trip_time - self._smoothed_round_trip_time)
            self._round_trip_time_variation = (3 * self._round_trip_time_variation + delta) / 4

        return clamp(
            self._smoothed_round_trip_time + 4 * self._round_trip_time_variation,
            minimum=self._min_recv_timeout,
            maximum=self._max_recv_timeout,
        )

    def _mark_fastack(self, before_serial: int) -> None:
        wnd_rng = self._send_window.window_range
        for serial in range(wnd_rng.beg, min(before_serial, wnd_rng.end + 1)):
            packet = self._send_window[serial]
            if packet is not None:
                packet.fastack += 1

    def _update_send_window_size(self, window_size: int) -> None:
        self._remote_window_size = window_size

        if self._send_window_size < self._remote_window_size:
            if self._send_window_size < self._slow_start_threshold:
                self._send_window_size += 1
                self._increment += self._max_segment_size
            else:
                self._increment = max(self._increment, self._max_segment_size)
                # Use additive increase: increase the congestion window by about 1 MSS per RTT.
                # * CW - congestion window (in bytes)
                # * MSS - segment size (in bytes)
                # * RTT - roundtrip time
                # During one RTT  CW/MSS packets approximately sent. The aim is the window to grow by 1 MSS per RTT.
                # increase_per_ack = (1 MSS) / (number_of_packets_per_RTT) = MSS / (CW / MSS) = MSS * MSS / CW
                # Therefore: CW += MSS * MSS / CW
                self._increment += (self._max_segment_size ** 2) / self._increment
                self._send_window_size = self._increment // self._max_segment_size

            self._send_window_size = min(self._send_window_size, self._remote_window_size)

    def _calculate_next_probe(self) -> float:
        pass
        # if self.probe_wait == 0:
        #     self.probe_wait = uint32(IKCP_PROBE_INIT)  # Reset and detection time
        #     self.ts_probe = self.current + self.probe_wait  # The time stamp of the next detection
        # else:
        #     # Check whether the current timestamp is greater than the detection timestamp
        #     if _itimediff(self.current, self.ts_probe) >= 0:
        #         if self.probe_wait < IKCP_PROBE_INIT:
        #             self.probe_wait = uint32(IKCP_PROBE_INIT)
        #         self.probe_wait += self.probe_wait / 2
        #         if self.probe_wait > IKCP_PROBE_LIMIT:
        #             self.probe_wait = uint32(IKCP_PROBE_LIMIT)
        #         self.ts_probe = self.current + self.probe_wait
        #         self.probe |= uint32(IKCP_ASK_SEND)

    def _calculate_slow_start_threshold(self) -> float:
        # if change > 0:
        #     inflight = self.snd_nxt - self.snd_una
        #     self.ssthresh = inflight / 2
        #     if self.ssthresh < IKCP_THRESH_MIN:
        #         self.ssthresh = uint32(IKCP_THRESH_MIN)
        #     self.cwnd = self.ssthresh + resent
        #     self.incr = self.cwnd * self.mss
        # # handle lost
        # if lost:
        #     self.ssthresh = uint32(cwnd / 2)
        #     if self.ssthresh < IKCP_THRESH_MIN:
        #         self.ssthresh = IKCP_THRESH_MIN
        #     self.cwnd = uint32(1)
        #     self.incr = self.mss
        # # handle windows
        # if self.cwnd < 1:
        #     self.cwnd = uint32(1)
        #     self.incr = self.mss