"""Load the pyo3 native extension built by maturin (curvine_libsdk.abi3.so)."""

from __future__ import annotations

import importlib.util
import os
import sys

_dir = os.path.dirname(os.path.abspath(__file__))
_so_path = os.path.join(_dir, "curvine_libsdk.abi3.so")

_spec = importlib.util.spec_from_file_location(__name__, _so_path)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load native extension from {_so_path}")

_native = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_native)

_this = sys.modules[__name__]
for _name in dir(_native):
    if _name.startswith("_"):
        continue
    setattr(_this, _name, getattr(_native, _name))
