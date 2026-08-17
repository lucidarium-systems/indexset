use crate::value_generator::{BenchMapValue, MapInsertionKind, ValueGenerator, RANGE_LEN};
use criterion::{black_box, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId};
use std::time::{Duration, Instant};

pub type IndexMap<V> = indexset::BTreeMap<u64, V>;
pub type StdMap<V> = std::collections::BTreeMap<u64, V>;

pub trait MapImplementation<V>: Sized + 'static
where
    V: BenchMapValue,
{
    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self;
    fn insert(&mut self, key: u64, value: V) -> Option<V>;
    fn get(&self, key: &u64) -> Option<&V>;
    fn remove(&mut self, key: &u64) -> Option<V>;
    fn traversal_checksum(&self) -> u64;
    fn range_checksum(&self, start: u64, end: u64) -> u64;
    fn benchmark_id(node_capacity: usize) -> String;

    fn insert_batch_benchmark_id(node_capacity: usize, _insert_count: usize) -> String {
        Self::benchmark_id(node_capacity)
    }
}

impl<V: BenchMapValue> MapImplementation<V> for IndexMap<V> {
    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self {
        let mut map = Self::with_maximum_node_size(node_capacity);
        for (key, value) in entries {
            map.insert(*key, value.clone());
        }
        map
    }

    fn insert(&mut self, key: u64, value: V) -> Option<V> {
        self.insert(key, value)
    }

    fn get(&self, key: &u64) -> Option<&V> {
        self.get(key)
    }

    fn remove(&mut self, key: &u64) -> Option<V> {
        self.remove(key)
    }

    fn traversal_checksum(&self) -> u64 {
        self.iter().fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end).fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn benchmark_id(node_capacity: usize) -> String {
        format!("indexset_cap_{node_capacity}")
    }
}

impl<V: BenchMapValue> MapImplementation<V> for StdMap<V> {
    fn build(entries: &[(u64, V)], _node_capacity: usize) -> Self {
        entries.iter().cloned().collect()
    }

    fn insert(&mut self, key: u64, value: V) -> Option<V> {
        self.insert(key, value)
    }

    fn get(&self, key: &u64) -> Option<&V> {
        self.get(key)
    }

    fn remove(&mut self, key: &u64) -> Option<V> {
        self.remove(key)
    }

    fn traversal_checksum(&self) -> u64 {
        self.iter().fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end).fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn benchmark_id(_node_capacity: usize) -> String {
        "std".to_owned()
    }

    fn insert_batch_benchmark_id(_node_capacity: usize, insert_count: usize) -> String {
        format!("std_batch_{insert_count}")
    }
}

pub fn bench_insert_batch<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    node_capacity: usize,
    insert_count: usize,
    kind: MapInsertionKind,
) where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::insert_batch_benchmark_id(node_capacity, insert_count), map_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            (
                generator.map_base_entries::<V>(),
                generator.map_insertions::<V>(insert_count, kind),
            )
        });

        b.iter_batched_ref(
            || (M::build(base_entries, node_capacity), insertions.clone()),
            |(map, insertion_batch)| {
                for (key, value) in insertion_batch.drain(..) {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn bench_insert_one<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    node_capacity: usize,
    operation_count: usize,
    kind: MapInsertionKind,
) where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity), map_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            (
                generator.map_base_entries::<V>(),
                generator.map_insertions::<V>(operation_count, kind),
            )
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let insertion_batch = insertions.clone();
                let mut map = M::build(base_entries, node_capacity);

                let start = Instant::now();
                for (key, value) in insertion_batch {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
                elapsed += start.elapsed();
            }

            elapsed / operation_count as u32
        });
    });
}

pub fn bench_get<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize, hit: bool)
where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity), map_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (map, queries) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            (M::build(&generator.map_base_entries::<V>(), node_capacity), queries)
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for key in black_box(queries.as_slice()) {
                    black_box(map.get(black_box(key)));
                }
                elapsed += start.elapsed();
            }

            elapsed / queries.len() as u32
        });
    });
}

pub fn bench_remove<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize)
where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity), map_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, keys) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            (generator.map_base_entries::<V>(), generator.hit_keys())
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut map = M::build(base_entries, node_capacity);

                let start = Instant::now();
                for key in black_box(keys.as_slice()) {
                    black_box(map.remove(black_box(key)));
                }
                elapsed += start.elapsed();
            }

            elapsed / keys.len() as u32
        });
    });
}

pub fn bench_traversal<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize)
where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity), map_size);
    let mut map = None;

    group.bench_function(id, move |b| {
        let map = map.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            M::build(&generator.map_base_entries::<V>(), node_capacity)
        });
        b.iter(|| black_box(map.traversal_checksum()));
    });
}

pub fn bench_range<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize)
where
    V: BenchMapValue,
    M: MapImplementation<V>,
{
    let start = map_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let id = BenchmarkId::new(M::benchmark_id(node_capacity), map_size);
    let mut map = None;

    group.bench_function(id, move |b| {
        let map = map.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            M::build(&generator.map_base_entries::<V>(), node_capacity)
        });
        b.iter(|| black_box(map.range_checksum(start, end)));
    });
}
