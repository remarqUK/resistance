"""Multi-zone-per-bar prototype.

Standalone walk-forward that allows multiple concurrent trades per pair,
one per eligible S/R zone, instead of the one-nearest-zone cap enforced
by the legacy walk-forward. The goal is to test whether trading every
qualifying zone in range per bar meaningfully increases trade count
without destroying expectancy.

This package does NOT modify any existing code paths. It imports and
reuses primitives (`generate_signal`, `check_price_exit`, zone
detection) but runs its own bar loop and produces its own trade list.
"""
