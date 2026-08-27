# indexset

[![crates.io](https://img.shields.io/crates/v/indexset.svg)](https://crates.io/crates/indexset)
[![docs](https://docs.rs/indexset/badge.svg)](https://docs.rs/indexset)

A pure-Rust two-level dynamic order-statistic b-tree.

This crate implements a compact set data structure that preserves its elements' sorted order and
allows lookups of entries by value or sorted order position.

Under the feature `concurrent` you can find a version of the BTree that can be fearlessly shared between
threads.

Both the concurrent and single-threaded versions are meant to be drop-in replacements for the stdlib BTree. This 
is mostly true for the latter but not for the former, yet.

The following table describes the variants of this data structure that are available:

| Variant                                    | tl;dr                                                     | Stability |
|--------------------------------------------|-----------------------------------------------------------|-----------|
| crate::BTreeSet                            | A single-threaded ordered set                             | Stable    |
| crate::BTreeMap                            | A single-threaded ordered map                             | Stable    |
| crate::concurrent::set::BTreeSet           | A concurrent ordered set                                  | Beta      |
| crate::concurrent::map::BTreeMap           | A concurrent ordered map                                  | Beta      |
| crate::concurrent::multimap::BTreeMultiMap | A concurrent ordered map where keys need not to be unique | Alpha     |

## Features

* `serde`: implements serialization and deserialization traits for the single-threaded trees 
* `concurrent`: enables the three concurrent variants of `BTreeSet` referenced in the table above
* `cdc`: provides helper methods to persist all concurrent trees
* `multimap`: enables `BTreeMultiMap`

# Background

This was heavily inspired by [`indexmap`](https://crates.io/crates/indexmap), and
python's [`sortedcontainers`](https://github.com/grantjenks/python-sortedcontainers).

It differs from both in that:

* `indexmap` is a hashmap that provides numerical lookups, but does not maintain order in case of removals, while
  `indexset`'s core data structure is a b-tree that irrespective of which mutating operation is run, always maintains order.
* `sortecontainers` is similar in spirit, but utilizes a different routine for balancing the tree, and relies
  on a heap for numerical lookups.

`indexset` provides the following features:

- As fast to iterate as a vec.
- Zero indirection.
- Lookups by position and range.
- Minimal amount of allocations.
- `select`(lookups by position) and `rank` operations in near constant time (not yet in the concurrent versions).

# Performance

`BTreeSet` and `BTreeMap` derive their performance much from how they are constructed, which is:

> A two-level B-Tree with a fenwick tree as a low-cost index for numerical lookups

Each node is a leaf, and each leaf is a vec with a fixed capacity of size `B`, with `1024` being the default.

The following hold:
- Iteration is very fast since it is done by inheriting vec's iter struct.
- Lookups only need two binary searches. One over `n/B` nodes and another over `B` elements: `O(log(n/B) + log(B)) = O(log(n))`.
- Insertions are constant time `O(B)` in the best case and `O(B^2)` in the worst. Removals are `O(log(n))`.

## Benchmarks

### Running the suite

| Target | Command | Coverage |
|--------|---------|----------|
| Single-threaded set and map | `cargo bench --bench single_threaded` | `indexset` and `std::collections` |
| Concurrent set and map | `cargo bench --bench concurrent --features concurrent` | Concurrent trees and mutex-protected single-threaded baselines |
| Concurrent set, map, and multimap | `cargo bench --bench concurrent --features multimap` | Adds random- and ordered-discriminator multimap workloads |
| Concurrent map alternatives | `cargo bench --bench comparison --features concurrent` | `indexset`, WorkTablesIndex, Arctic, and raw Congee maps |

### Method and units

The representative results below are Criterion median point estimates from commit `7e3ac23`, measured on
2026-08-26 on a 12-core Apple M2 Pro with 16 GB RAM, macOS 26.5.2, and
`rustc 1.97.0-nightly (ad3a598ca 2026-05-03)`. Lower is better.
The lowest comparable result for each workload or scenario is shown in **bold**.

### Single-threaded results

The set cases use either an 8-byte `u64` or a 64-byte record and start with 100,000 existing values:

| Workload | Value | `std::collections::BTreeSet` | `indexset`, cap 256 | `indexset`, cap 1024 |
|----------|-------|------------------------------|---------------------|----------------------|
| Insert a new value | `u64` (8 B) | 144.9 ns | **134.6 ns** | 168.1 ns |
| | `record_64b` (64 B) | **230.0 ns** | 264.3 ns | 564.3 ns |
| `contains` hit | `u64` (8 B) | **21.8 ns** | 23.9 ns | 23.6 ns |
| | `record_64b` (64 B) | **27.8 ns** | 31.2 ns | 33.1 ns |
| Remove hit | `u64` (8 B) | **69.6 ns** | 133.1 ns | 163.1 ns |
| | `record_64b` (64 B) | **97.6 ns** | 254.5 ns | 578.5 ns |
| `get_index` | `u64` (8 B) | N/A | 13.2 ns | **11.0 ns** |
| | `record_64b` (64 B) | N/A | 13.2 ns | **11.1 ns** |
| Full traversal | `u64` (8 B) | 92.5 µs | 37.5 µs | **37.3 µs** |
| | `record_64b` (64 B) | 108.2 µs | 63.1 µs | **61.4 µs** |
| 128-entry range traversal | `u64` (8 B) | 164.8 ns | 127.4 ns | **117.3 ns** |
| | `record_64b` (64 B) | 168.4 ns | 129.5 ns | **122.5 ns** |

The map cases use either a 16-byte `u64 -> u64` entry or a 64-byte entry with a 56-byte value and start with
100,000 existing entries:

| Workload | Entry | `std::collections::BTreeMap` | `indexset`, cap 256 | `indexset`, cap 1024 |
|----------|-------|------------------------------|---------------------|----------------------|
| Insert a new entry | `entry_16b` (16 B) | 178.5 ns | **152.5 ns** | 223.7 ns |
| | `entry_64b` (64 B) | 298.3 ns | **271.8 ns** | 563.0 ns |
| Update an existing entry | `entry_16b` (16 B) | 67.4 ns | 49.4 ns | **44.0 ns** |
| | `entry_64b` (64 B) | 77.2 ns | 75.3 ns | **66.0 ns** |
| `get` hit | `entry_16b` (16 B) | **24.7 ns** | 46.5 ns | 38.7 ns |
| | `entry_64b` (64 B) | **29.1 ns** | 51.0 ns | 40.5 ns |
| Remove hit | `entry_16b` (16 B) | **75.0 ns** | 153.4 ns | 224.3 ns |
| | `entry_64b` (64 B) | **117.0 ns** | 262.8 ns | 587.5 ns |
| Full traversal | `entry_16b` (16 B) | 89.9 µs | 59.7 µs | **59.6 µs** |
| | `entry_64b` (64 B) | 110.0 µs | 66.3 µs | **62.7 µs** |
| 128-entry range traversal | `entry_16b` (16 B) | 163.8 ns | 155.6 ns | **139.0 ns** |
| | `entry_64b` (64 B) | 163.4 ns | 165.0 ns | **148.5 ns** |

### Concurrent results

The following set results measure 10,000 operations over 12 worker threads and 100,000 existing values:

| Implementation | 90/10, 8 B | 90/10, 64 B | 50/50, 8 B | 50/50, 64 B |
|----------------|------------:|-------------:|------------:|-------------:|
| Concurrent `indexset`, cap 256 | **0.531 ms** | **0.593 ms** | **0.548 ms** | **0.728 ms** |
| Concurrent `indexset`, cap 1024 | 0.584 ms | 1.811 ms | 0.697 ms | 4.011 ms |
| Mutex-protected `indexset`, cap 256 | 1.798 ms | 2.989 ms | 2.913 ms | 6.239 ms |
| Mutex-protected `indexset`, cap 1024 | 2.024 ms | 9.801 ms | 3.829 ms | 35.458 ms |
| Mutex-protected `std::collections::BTreeSet` | 1.496 ms | 1.913 ms | 1.886 ms | 2.545 ms |

The map target uses the same operation count, thread count, and initial length:

| Implementation | 90/10, 16 B | 90/10, 64 B | 50/50, 16 B | 50/50, 64 B |
|----------------|-------------:|-------------:|-------------:|-------------:|
| Concurrent `indexset`, cap 256 | **0.562 ms** | **0.593 ms** | **0.581 ms** | **0.722 ms** |
| Concurrent `indexset`, cap 1024 | 0.659 ms | 1.829 ms | 0.790 ms | 3.945 ms |
| Mutex-protected `indexset`, cap 256 | 2.341 ms | 3.216 ms | 4.150 ms | 6.272 ms |
| Mutex-protected `indexset`, cap 1024 | 2.637 ms | 10.461 ms | 5.292 ms | 35.900 ms |
| Mutex-protected `std::collections::BTreeMap` | 1.642 ms | 1.795 ms | 1.969 ms | 2.314 ms |

### Multimap results

These multimap results use 100,000 pairs, node capacity `1024 `. `v8b` combines with an 8-byte key into a 16-byte pair; `v56b` produces a 64-byte pair. A hit query
iterates and checksums every value for its key, so dense fanout is expected to cost more than sparse fanout.

| Pair discriminator | Pair | Insert new, fanout 1-3 | Insert new, fanout 1,000-2,000 | Get hit, fanout 1-3 | Get hit, fanout 1,000-2,000 |
|--------------------|------|------------------------|--------------------------------|---------------------|-----------------------------|
| Random | `v8b` (16 B) | 322.9 ns | 295.8 ns | **648.9 ns** | 7.146 µs |
| | `v56b` (64 B) | 718.3 ns | 707.3 ns | **849.6 ns** | **7.749 µs** |
| Ordered | `v8b` (16 B) | **248.6 ns** | **209.6 ns** | 736.1 ns | **6.035 µs** |
| | `v56b` (64 B) | **610.7 ns** | **569.5 ns** | 1.694 µs | 8.390 µs |

Dense ordered mixed workloads are currently excluded pending
[`issue #68`](https://github.com/lucidarium-systems/indexset/issues/68); the remaining multimap cases retain
their correctness checks during benchmark execution.

## Limitations

* `BTreeMap` is less polished than `BTreeSet`. This crate has been optimised for a leaner `BTreeSet`.
* `Concurrent` `BtreeSet`, `BTreeMap` and `BTreeMultiMap` do not support `serde` serialization and deserialization nor are they order-statistic trees.

## Naming

This library is called `indexset` because the base data structure is `BTreeSet`. `BTreeMap` is a `BTreeSet` with
a `Pair<K, V>` item type, and `BTreeMultiMap` is one with a `MultiPair<K, V>` item.

## Changelog

See [CHANGELOG.md](https://github.com/brurucy/indexset/blob/master/CHANGELOG.md).

## Mentions

Special thanks to Christopher Bergstrom from Pathscale for funding the development of this library.
