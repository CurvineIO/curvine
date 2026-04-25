#!/bin/bash
# Linker wrapper that forces -lstdc++, -lgcc_s, and -lz to link statically.
# Needed because crate build scripts (e.g. librocksdb-sys) emit
# `cargo:rustc-link-lib=stdc++` which lands in the -Bdynamic section.
#
# NOTE: each -Wl flag must be a separate arg; do NOT merge into one
# comma-separated string or gcc will pass a literal "-Wl," to the linker.

set -euo pipefail

NEWARGS=()
for arg in "$@"; do
    if [ "$arg" = "-lstdc++" ]; then
        NEWARGS+=("-Wl,-Bstatic" "-lstdc++" "-Wl,-Bdynamic")
    elif [ "$arg" = "-lgcc_s" ]; then
        NEWARGS+=("-Wl,-Bstatic" "-lgcc_eh" "-lgcc" "-Wl,-Bdynamic")
    elif [ "$arg" = "-lz" ]; then
        NEWARGS+=("-Wl,-Bstatic" "-lz" "-Wl,-Bdynamic")
    else
        NEWARGS+=("$arg")
    fi
done

exec "${CC:-cc}" "${NEWARGS[@]}"
