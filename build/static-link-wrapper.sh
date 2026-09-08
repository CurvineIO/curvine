#!/bin/bash
# Cargo linker wrapper. When CURVINE_STATIC_LINK=1, forces -lstdc++, -lgcc_s,
# and -lz to link statically. Needed because crate build scripts (e.g.
# librocksdb-sys) emit `cargo:rustc-link-lib=stdc++` which lands in the
# -Bdynamic section.
#
# NOTE: gcc/clang support comma-separated `-Wl,...` lists. This wrapper
# emits `-Wl,-Bstatic` / `-Wl,-Bdynamic` as separate args around the
# affected libraries to preserve the intended linker mode transitions.
#
# Usage:
#   CURVINE_STATIC_LINK=1 cargo build --release
#
# Without the variable (default), arguments are passed through unchanged.
#
# Optional:
#   CURVINE_JINDOSDK_LIB=/path/to/libjindosdk_c.so
# Some CentOS 7 aarch64 final links can fail to resolve -ljindosdk_c after
# mixed static/dynamic transitions. Passing the exact shared object keeps the
# final binary's DT_NEEDED on libjindosdk_c.so.6 while avoiding linker search
# ambiguity.

set -euo pipefail

if [ "${CURVINE_STATIC_LINK:-0}" != "1" ]; then
    exec "${CC:-cc}" "$@"
fi

have_static_archive() {
    local archive_name="$1"
    local archive_path

    if [ -n "${CURVINE_STATIC_LIB_GCC_DIR:-}" ] && [ -f "${CURVINE_STATIC_LIB_GCC_DIR}/${archive_name}" ]; then
        return 0
    fi

    archive_path="$("${CC:-cc}" -print-file-name="${archive_name}" 2>/dev/null || true)"
    if [ -z "${archive_path}" ] || [ "${archive_path}" = "${archive_name}" ]; then
        return 1
    fi

    [ -f "${archive_path}" ]
}

NEWARGS=()
for arg in "$@"; do
    if [ "$arg" = "-lstdc++" ]; then
        if have_static_archive "libstdc++.a"; then
            if [ -n "${CURVINE_STATIC_LIB_GCC_DIR:-}" ] && [ -f "${CURVINE_STATIC_LIB_GCC_DIR}/libstdc++.a" ]; then
                NEWARGS+=("-Wl,-Bstatic" "${CURVINE_STATIC_LIB_GCC_DIR}/libstdc++.a" "-Wl,-Bdynamic")
            else
                NEWARGS+=("-Wl,-Bstatic" "-lstdc++" "-Wl,-Bdynamic")
            fi
        else
            NEWARGS+=("-lstdc++")
        fi
    elif [ "$arg" = "-lgcc_s" ]; then
        # libgcc_s.so provides both the GCC runtime helpers and the EH
        # (exception-handling) unwinder. GCC does not ship a libgcc_s.a;
        # the static equivalents are libgcc.a (runtime) + libgcc_eh.a (EH).
        if have_static_archive "libgcc_eh.a" && have_static_archive "libgcc.a"; then
            if [ -n "${CURVINE_STATIC_LIB_GCC_DIR:-}" ] \
               && [ -f "${CURVINE_STATIC_LIB_GCC_DIR}/libgcc_eh.a" ] \
               && [ -f "${CURVINE_STATIC_LIB_GCC_DIR}/libgcc.a" ]; then
                NEWARGS+=(
                    "-Wl,-Bstatic"
                    "${CURVINE_STATIC_LIB_GCC_DIR}/libgcc_eh.a"
                    "${CURVINE_STATIC_LIB_GCC_DIR}/libgcc.a"
                    "-Wl,-Bdynamic"
                )
            else
                NEWARGS+=("-Wl,-Bstatic" "-lgcc_eh" "-lgcc" "-Wl,-Bdynamic")
            fi
        else
            NEWARGS+=("-lgcc_s")
        fi
    elif [ "$arg" = "-lz" ]; then
        if have_static_archive "libz.a"; then
            NEWARGS+=("-Wl,-Bstatic" "-lz" "-Wl,-Bdynamic")
        else
            NEWARGS+=("-lz")
        fi
    elif [ "$arg" = "-ljindosdk_c" ] && [ -n "${CURVINE_JINDOSDK_LIB:-}" ] && [ -f "${CURVINE_JINDOSDK_LIB}" ]; then
        NEWARGS+=("${CURVINE_JINDOSDK_LIB}")
    else
        NEWARGS+=("$arg")
    fi
done

if [ -n "${CURVINE_LINK_WRAPPER_LOG:-}" ]; then
    {
        echo "==== $(date '+%Y-%m-%d %H:%M:%S') pid=$$ ===="
        echo "-- original"
        printf '%q ' "$@"
        echo
        echo "-- rewritten"
        printf '%q ' "${NEWARGS[@]}"
        echo
    } >> "${CURVINE_LINK_WRAPPER_LOG}"
fi

exec "${CC:-cc}" "${NEWARGS[@]}"
