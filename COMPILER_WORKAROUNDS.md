# Compiler Workarounds

This document lists the files, functions and lines in which we have implemented compiler specific workarounds.

The goal is one day, we compilers get upgraded the items in this document can be removed.

## Workaround List

| File | Line | Function | Compiler| Workaround |
| ---- | ---- | -------- | ------- | ---------- |
| tiledb/common/interval/interval.h | 967 | `Bound normalize_bound(T bound, bool is_closed, bool is_for_upper_bound)` | VS2017 | `(void)is_for_upper_bound;` |