use crate::value_generator::{BenchValue, ValueGenerator, RANGE_LEN};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId};
use std::time::{Duration, Instant};

pub type IndexSet<T> = indexset::BTreeSet<T>;
pub type StdSet<T> = std::collections::BTreeSet<T>;

pub trait SetImplementation<T>: Sized + 'static
where
    T: BenchValue,
{
    fn build(values: &[T], node_capacity: usize) -> Self;
    fn insert(&mut self, value: T) -> bool;
    fn contains(&self, key: &u64) -> bool;
    fn remove(&mut self, key: &u64) -> bool;
    fn traversal_checksum(&self) -> u64;
    fn range_checksum(&self, start: u64, end: u64) -> u64;
    fn benchmark_id(node_capacity: usize) -> String;
}

impl<T: BenchValue> SetImplementation<T> for IndexSet<T> {
    fn build(values: &[T], node_capacity: usize) -> Self {
        let mut set = Self::with_maximum_node_size(node_capacity);
        for value in values {
            set.insert(value.clone());
        }
        set
    }

    fn insert(&mut self, value: T) -> bool {
        self.insert(value)
    }

    fn contains(&self, key: &u64) -> bool {
        self.contains(key)
    }

    fn remove(&mut self, key: &u64) -> bool {
        self.remove(key)
    }

    fn traversal_checksum(&self) -> u64 {
        self.iter()
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end)
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn benchmark_id(node_capacity: usize) -> String {
        format!("indexset_cap_{node_capacity}")
    }
}

impl<T: BenchValue> SetImplementation<T> for StdSet<T> {
    fn build(values: &[T], _node_capacity: usize) -> Self {
        values.iter().cloned().collect()
    }

    fn insert(&mut self, value: T) -> bool {
        self.insert(value)
    }

    fn contains(&self, key: &u64) -> bool {
        self.contains(key)
    }

    fn remove(&mut self, key: &u64) -> bool {
        self.remove(key)
    }

    fn traversal_checksum(&self) -> u64 {
        self.iter()
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end)
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn benchmark_id(_node_capacity: usize) -> String {
        "std".to_owned()
    }
}

pub fn bench_insert_one<T, S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    make_insertions: F,
) where
    T: BenchValue,
    S: SetImplementation<T>,
    F: Fn(&ValueGenerator) -> Vec<T> + 'static,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity), set_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_values, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            (generator.base_values::<T>(), make_insertions(&generator))
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut insertion_batch = insertions.clone();
                let mut set = S::build(base_values, node_capacity);

                let start = Instant::now();
                for insertion in insertion_batch.drain(..) {
                    black_box(set.insert(black_box(insertion)));
                }
                elapsed += start.elapsed();
            }

            elapsed / insertions.len() as u32
        });
    });
}

pub fn bench_contains<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize, hit: bool)
where
    T: BenchValue,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity), set_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (set, queries) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            (S::build(&generator.base_values::<T>(), node_capacity), queries)
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for key in black_box(queries.as_slice()) {
                    black_box(set.contains(key));
                }
                elapsed += start.elapsed();
            }

            elapsed / queries.len() as u32
        });
    });
}

pub fn bench_remove<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize)
where
    T: BenchValue,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity), set_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_values, keys) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            (generator.base_values::<T>(), generator.hit_keys())
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut set = S::build(base_values, node_capacity);

                let start = Instant::now();
                for key in black_box(keys.as_slice()) {
                    black_box(set.remove(black_box(key)));
                }
                elapsed += start.elapsed();
            }

            elapsed / keys.len() as u32
        });
    });
}

pub fn bench_get_index<T: BenchValue>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize) {
    let id = BenchmarkId::new(IndexSet::<T>::benchmark_id(node_capacity), set_size);
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (set, indices) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            (
                IndexSet::<T>::build(&generator.base_values::<T>(), node_capacity),
                generator.random_indices(),
            )
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for index in black_box(indices.as_slice()) {
                    black_box(set.get_index(black_box(*index)));
                }
                elapsed += start.elapsed();
            }

            elapsed / indices.len() as u32
        });
    });
}

pub fn bench_traversal<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize)
where
    T: BenchValue,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity), set_size);
    let mut set = None;

    group.bench_function(id, move |b| {
        let set = set.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            S::build(&generator.base_values::<T>(), node_capacity)
        });
        b.iter(|| black_box(set.traversal_checksum()));
    });
}

pub fn bench_range<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize)
where
    T: BenchValue,
    S: SetImplementation<T>,
{
    let start = set_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let id = BenchmarkId::new(S::benchmark_id(node_capacity), set_size);
    let mut set = None;

    group.bench_function(id, move |b| {
        let set = set.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            S::build(&generator.base_values::<T>(), node_capacity)
        });
        b.iter(|| black_box(set.range_checksum(start, end)));
    });
}
