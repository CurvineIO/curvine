# Separate HTTP/2 diagnostics

These captures contain synthetic benchmark traffic on the isolated test network. They are separate 128 MiB diagnostic runs, excluded from all performance medians. The default control advertises 16,384 bytes and contains 8,444 complete 16 KiB server DATA frames, none larger. The candidate advertises 262,144 bytes and contains 527 complete 256 KiB server DATA frames. Both tcpdump logs report zero kernel drops and the parser reports no truncated packets or TCP/HTTP2 errors. The candidate capture ends with a partial frame, which is excluded: counts describe the captured contiguous prefix, not the complete workload.

The capture wrapper starts tcpdump before the worker accepts connections. Its hash replaces the server hash in the diagnostic harness result, so `provenance.json` separately records the actual server/client binaries and revisions. Acceptance runs use the real binaries directly, without the wrapper or packet capture.

To replay either capture:

```sh
gzip -dc default.pcap.gz > /tmp/curvine-default.pcap
python3 inspect-http2.py /tmp/curvine-default.pcap

gzip -dc larger.pcap.gz > /tmp/curvine-larger.pcap
python3 inspect-http2.py /tmp/curvine-larger.pcap
```

The committed JSON files are the parser output; the independent audit reproduced them exactly. To collect a new diagnostic, set `RPC_TC_CAPTURE_REAL_SERVER` to the actual server, `RPC_TC_CAPTURE_PCAP` to a fresh absolute capture path and `RPC_TC_CAPTURE_PACKETS=1000000`, then supply `capture-worker.sh` as the server argument to `bench.sh`. Use the workload and shaping values in `provenance.json`. Never use this wrapper for acceptance timing.
