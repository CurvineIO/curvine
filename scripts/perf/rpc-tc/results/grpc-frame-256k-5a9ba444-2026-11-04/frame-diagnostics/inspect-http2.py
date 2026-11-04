#!/usr/bin/env python3
"""Inspect plaintext HTTP/2 over an IPv4 TCP capture of a worker port.

Use a separate diagnostic run, not the performance acceptance run. Capture the
handshake from one veth interface, for example inside the server namespace:

  tcpdump -i "$SERVER_VETH" -s 0 -U -c 4096 -w capture.pcap 'tcp port 8997'
  python3 inspect-http2.py capture.pcap

The tcpdump process must be listening before starting the client. ``-s 0`` is
essential: a truncated TCP segment cannot establish frame boundaries. Check
tcpdump's final kernel-drop count too. Classic pcap, Ethernet/raw IP/Linux SLL
and SLL2 are supported; pcapng, IPv6, TLS, and IPv4 fragments are not. The
output summarizes only complete frames in the captured contiguous TCP prefix.
It never scans forward to guess a lost HTTP/2 boundary.
"""

import argparse
from collections import Counter
import io
import json
import os
import struct
import sys

PREFACE = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
DEFAULT_MAX_FRAME = 16384
MAGICS = {
    b"\xd4\xc3\xb2\xa1": "<",
    b"\xa1\xb2\xc3\xd4": ">",
    b"\x4d\x3c\xb2\xa1": "<",
    b"\xa1\xb2\x3c\x4d": ">",
}


def read_pcap(file):
    header = file.read(24)
    if len(header) != 24 or header[:4] not in MAGICS:
        raise ValueError("expected a complete classic pcap header (not pcapng)")
    endian = MAGICS[header[:4]]
    major, minor, _, _, snaplen, network = struct.unpack(endian + "HHIIII", header[4:])
    if (major, minor) != (2, 4) or snaplen == 0:
        raise ValueError("unsupported pcap version or invalid snapshot length")
    linktype = network & 0xFFFF
    if linktype not in (1, 101, 113, 276):
        raise ValueError(f"unsupported pcap link type {linktype}")
    while True:
        record = file.read(16)
        if not record:
            return
        if len(record) != 16:
            raise ValueError("truncated pcap packet header")
        _, _, caplen, origlen = struct.unpack(endian + "IIII", record)
        if caplen > snaplen or caplen > origlen:
            raise ValueError("invalid pcap captured/original length")
        packet = file.read(caplen)
        if len(packet) != caplen:
            raise ValueError("truncated pcap packet record")
        yield linktype, packet, caplen < origlen


def ipv4_packet(linktype, packet):
    if linktype == 101:  # DLT_RAW
        return packet
    if linktype == 1:  # DLT_EN10MB
        if len(packet) < 14:
            return None
        protocol = int.from_bytes(packet[12:14], "big")
        offset = 14
    elif linktype == 113:  # DLT_LINUX_SLL
        if len(packet) < 16:
            return None
        protocol = int.from_bytes(packet[14:16], "big")
        offset = 16
    else:  # DLT_LINUX_SLL2
        if len(packet) < 20:
            return None
        protocol = int.from_bytes(packet[:2], "big")
        offset = 20
    while protocol in (0x8100, 0x88A8, 0x9100):
        if len(packet) < offset + 4:
            return None
        protocol = int.from_bytes(packet[offset + 2 : offset + 4], "big")
        offset += 4
    return packet[offset:] if protocol == 0x0800 else None


def tcp_packet(linktype, packet):
    ip = ipv4_packet(linktype, packet)
    if ip is None or len(ip) < 20 or ip[0] >> 4 != 4 or ip[9] != 6:
        return None
    ihl = (ip[0] & 0x0F) * 4
    total = int.from_bytes(ip[2:4], "big")
    if ihl < 20 or total < ihl + 20 or len(ip) < total:
        return None
    if int.from_bytes(ip[6:8], "big") & 0x3FFF:
        return None  # Do not mistake an IPv4 fragment for a full TCP packet.
    tcp = ip[ihl:total]
    srcport, dstport, seq = struct.unpack_from("!HHI", tcp)
    tcp_header = (tcp[12] >> 4) * 4
    if tcp_header < 20 or tcp_header > len(tcp):
        return None
    src = ".".join(str(x) for x in ip[12:16])
    dst = ".".join(str(x) for x in ip[16:20])
    return src, dst, srcport, dstport, seq, tcp[13], tcp[tcp_header:]


def offset_from_base(seq, base):
    return ((seq - base + (1 << 31)) % (1 << 32)) - (1 << 31)


class Direction:
    def __init__(self):
        self.base = None
        self.segments = []

    def add(self, seq, flags, payload):
        if flags & 0x02:  # SYN consumes a sequence number.
            base = (seq + 1) & 0xFFFFFFFF
            if self.base is not None and self.base != base:
                raise ValueError("conflicting SYN sequence numbers for a connection")
            self.base = base
            seq = base
        if payload:
            self.segments.append((seq, payload))

    def contiguous(self):
        if self.base is None:
            return b"", "missing SYN; no trustworthy TCP stream start"
        segments = sorted((offset_from_base(seq, self.base), data) for seq, data in self.segments)
        out = bytearray()
        for start, data in segments:
            if start < 0:
                skip = min(-start, len(data))
                start += skip
                data = data[skip:]
            if not data:
                continue
            if start > len(out):
                return bytes(out), f"TCP gap at byte {len(out)} (next byte {start})"
            overlap = min(len(data), len(out) - start)
            if out[start : start + overlap] != data[:overlap]:
                return bytes(out[:start]), f"conflicting retransmission at byte {start}"
            out.extend(data[overlap:])
        return bytes(out), None


def http2_frames(data, client):
    if client:
        if not data.startswith(PREFACE):
            return None, "client preface missing (capture may start late or traffic is encrypted)"
        data = data[len(PREFACE) :]
    index = 0
    frames = 0
    saw_settings = False
    initial_max_frame_size = None
    settings_max = []
    data_lengths = Counter()
    data_streams = Counter()
    warning = None
    while index < len(data):
        if len(data) - index < 9:
            warning = f"partial frame header at end of captured prefix ({len(data) - index} bytes)"
            break
        size = int.from_bytes(data[index : index + 3], "big")
        kind, flags = data[index + 3], data[index + 4]
        stream = int.from_bytes(data[index + 5 : index + 9], "big") & 0x7FFFFFFF
        end = index + 9 + size
        if end > len(data):
            warning = f"partial frame at end of captured prefix (need {size} payload bytes)"
            break
        payload = data[index + 9 : end]
        if kind == 4:
            if stream != 0 or size % 6 or (flags & 1 and size):
                return None, f"invalid SETTINGS frame at byte {index}"
            if not flags & 1:
                first_settings = not saw_settings
                value_in_frame = DEFAULT_MAX_FRAME
                saw_settings = True
                for start in range(0, size, 6):
                    setting, value = struct.unpack_from("!HI", payload, start)
                    if setting == 5:
                        if not DEFAULT_MAX_FRAME <= value <= 16777215:
                            return None, f"invalid SETTINGS_MAX_FRAME_SIZE {value}"
                        settings_max.append(value)
                        value_in_frame = value
                if first_settings:
                    initial_max_frame_size = value_in_frame
        elif kind == 0:
            if stream == 0:
                return None, f"invalid DATA frame with stream id 0 at byte {index}"
            data_lengths[size] += 1
            data_streams[stream] += 1
        frames += 1
        index = end
    count = sum(data_lengths.values())
    return {
        "complete_frames": frames,
        "settings_seen": saw_settings,
        "settings_max_frame_size_updates": settings_max,
        "initial_max_frame_size": initial_max_frame_size,
        "data_frames": count,
        "data_payload_bytes": sum(length * count for length, count in data_lengths.items()),
        "data_min_length": min(data_lengths) if count else None,
        "data_max_length": max(data_lengths) if count else None,
        "data_frames_above_16384": sum(count for length, count in data_lengths.items() if length > 16384),
        "data_length_histogram": dict(sorted(data_lengths.items())),
        "data_stream_count": len(data_streams),
        "tail_warning": warning,
    }, None


def analyze(file, port):
    groups = []
    active = {}
    records = 0
    captured_truncated = 0
    for linktype, packet, truncated in read_pcap(file):
        records += 1
        captured_truncated += int(truncated)
        parsed = tcp_packet(linktype, packet)
        if parsed is None:
            continue
        src, dst, sport, dport, seq, flags, payload = parsed
        if dport == port and sport != port:
            key = (src, sport, dst, dport)
            direction = "client_to_server"
        elif sport == port and dport != port:
            key = (dst, dport, src, sport)
            direction = "server_to_client"
        else:
            continue
        conn = active.get(key)
        new_syn = direction == "client_to_server" and flags & 0x02 and not flags & 0x10
        if conn is None or (new_syn and conn["isn"] is not None and conn["isn"] != seq):
            conn = {"key": key, "isn": None, "client_to_server": Direction(), "server_to_client": Direction()}
            groups.append(conn)
            active[key] = conn
        if new_syn:
            conn["isn"] = seq
        conn[direction].add(seq, flags, payload)

    results = []
    for conn in groups:
        client, client_problem = conn["client_to_server"].contiguous()
        server, server_problem = conn["server_to_client"].contiguous()
        # Do not parse a server stream unless this connection has a verified
        # client preface; port 8997 can also carry other traffic/readiness probes.
        client_frames, invalid_client = http2_frames(client, True)
        if client_frames is None:
            continue
        server_frames, invalid_server = http2_frames(server, False)
        key = conn["key"]
        results.append({
            "client": f"{key[0]}:{key[1]}", "server": f"{key[2]}:{key[3]}",
            "client_to_server": client_frames,
            "server_to_client": server_frames,
            "client_tcp_warning": client_problem,
            "server_tcp_warning": server_problem,
            "client_http2_error": invalid_client,
            "server_http2_error": invalid_server,
        })
    return {
        "capture_packets": records,
        "snaplen_truncated_packets": captured_truncated,
        "note": "DATA lengths cover complete frames in the captured contiguous prefix only; check tcpdump kernel drops separately",
        "connections": results,
    }


def self_test():
    def frame(kind, flags, stream, payload):
        return len(payload).to_bytes(3, "big") + bytes([kind, flags]) + struct.pack("!I", stream) + payload

    def packet(src, dst, sport, dport, seq, flags, data=b""):
        tcp = struct.pack("!HHIIHHHH", sport, dport, seq, 0, (5 << 12) | flags, 0, 0, 0) + data
        src_ip = bytes(map(int, src.split(".")))
        dst_ip = bytes(map(int, dst.split(".")))
        ip = struct.pack("!BBHHHBBH4s4s", 0x45, 0, 20 + len(tcp), 0, 0, 64, 6, 0, src_ip, dst_ip) + tcp
        return b"\x00" * 12 + b"\x08\x00" + ip

    def capture(records, linktype=1):
        result = bytearray(b"\xd4\xc3\xb2\xa1" + struct.pack("<HHIIII", 2, 4, 0, 0, 65535, linktype))
        for data in records:
            result.extend(struct.pack("<IIII", 0, 0, len(data), len(data)))
            result.extend(data)
        return io.BytesIO(result)

    cip, sip, cp, sp = "192.0.2.2", "192.0.2.1", 45678, 8997
    setting = frame(4, 0, 0, struct.pack("!HI", 5, 262144))
    upstream = PREFACE + setting
    downstream = frame(4, 0, 0, b"") + frame(0, 0, 1, b"X" * 32768)
    records = [
        packet(cip, sip, cp, sp, 100, 2),
        packet(sip, cip, sp, cp, 200, 0x12),
        packet(cip, sip, cp, sp, 101, 0x18, upstream[:10]),
        packet(cip, sip, cp, sp, 111, 0x18, upstream[10:]),
        # Out of order, followed by the missing first piece and a retransmit.
        packet(sip, cip, sp, cp, 1201, 0x18, downstream[1000:]),
        packet(sip, cip, sp, cp, 201, 0x18, downstream[:1000]),
        packet(sip, cip, sp, cp, 201, 0x18, downstream[:1000]),
    ]
    result = analyze(capture(records), sp)
    assert len(result["connections"]) == 1
    conn = result["connections"][0]
    assert conn["client_to_server"]["settings_max_frame_size_updates"] == [262144]
    assert conn["server_to_client"]["data_max_length"] == 32768
    assert conn["server_to_client"]["data_frames_above_16384"] == 1
    assert conn["server_tcp_warning"] is None
    cooked = [b"\x08\x00" + b"\x00" * 18 + packet[14:] for packet in records]
    cooked_result = analyze(capture(cooked, 276), sp)
    assert cooked_result["connections"][0]["server_to_client"]["data_max_length"] == 32768
    later_settings, error = http2_frames(PREFACE + frame(4, 0, 0, b"") + setting, True)
    assert error is None
    assert later_settings["initial_max_frame_size"] == DEFAULT_MAX_FRAME
    assert later_settings["settings_max_frame_size_updates"] == [262144]
    wrapped = Direction()
    wrapped.add(0xFFFFFFF0, 2, b"")
    wrapped.add(1, 0x18, b"b")
    wrapped.add(0xFFFFFFF1, 0x18, b"a" * 16)
    assert wrapped.contiguous() == (b"a" * 16 + b"b", None)
    assert not analyze(capture(records[1:]), sp)["connections"]  # Missing client SYN.
    # A gap cannot be bridged by scanning for a later plausible frame header.
    missing = analyze(capture(records[:3] + records[4:]), sp)
    assert not missing["connections"]
    conflict = records + [packet(cip, sip, cp, sp, 101, 0x18, b"!" + upstream[1:10])]
    assert not analyze(capture(conflict), sp)["connections"]
    print("synthetic pcap tests passed")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pcap", nargs="?", help="classic pcap captured with full packet snapshots")
    parser.add_argument("--port", type=int, default=8997, help="server TCP port (default 8997)")
    parser.add_argument("--max-mib", type=int, default=512, help="reject larger captures to bound memory (default 512 MiB)")
    parser.add_argument("--self-test", action="store_true", help="exercise parsing, reassembly and missing data with synthetic packets")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if not args.pcap or not 1 <= args.port <= 65535 or args.max_mib <= 0:
        parser.error("a pcap, valid port and positive --max-mib are required")
    if os.path.getsize(args.pcap) > args.max_mib * 1024 * 1024:
        raise ValueError("capture exceeds --max-mib; use a shorter diagnostic capture or increase the limit")
    with open(args.pcap, "rb") as file:
        result = analyze(file, args.port)
    print(json.dumps(result, indent=2))
    usable = any(
        c["client_to_server"]["settings_seen"]
        and c["server_to_client"] is not None
        and c["server_to_client"]["settings_seen"]
        and c["server_to_client"]["data_frames"] > 0
        and not c["server_http2_error"]
        for c in result["connections"]
    )
    return 0 if usable else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError) as error:
        print(f"inspect-http2: {error}", file=sys.stderr)
        sys.exit(1)
