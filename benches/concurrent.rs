use criterion::{black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use crossbeam_skiplist::SkipSet;
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::core::multipair::OrdMultiPair;
use rand::{rng, rngs::StdRng, thread_rng, Rng, SeedableRng};
use scc::TreeIndex;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::thread;

#[derive(Clone)]
enum Op {
    Read(usize),
    Write(usize),
}

const NUM_READERS: usize = 30;
const NUM_WRITERS: usize = 10;
const NUM_THREADS: usize = NUM_READERS + NUM_WRITERS;
const OPERATIONS_PER_THREAD: usize = 100_000;
const TOTAL_OPERATIONS: usize = NUM_THREADS * OPERATIONS_PER_THREAD;
const MULTIMAP_REMOVE_ENTRIES: usize = 20_000;
const MULTIMAP_REMOVE_SEED: u64 = 42;

// fn generate_operations(write_ratio: f64) -> Vec<Vec<Op>> {
//     let mut rng = thread_rng();
//     let mut all_operations: Vec<Vec<Op>> =
//         vec![Vec::with_capacity(OPERATIONS_PER_THREAD); NUM_THREADS];

//     for i in 0..TOTAL_OPERATIONS {
//         let thread_index = i % NUM_THREADS;
//         let value = rng.gen_range(0..TOTAL_OPERATIONS);
//         let operation = if thread_index == NUM_READERS || rng.gen::<f64>() < write_ratio {
//             Op::Write(value)
//         } else {
//             Op::Read(value)
//         };
//         all_operations[thread_index].push(operation);
//     }

//     all_operations
// }

fn generate_operations(write_ratio: f64) -> Vec<Vec<Op>> {
    let mut rng = rng();
    let mut all_operations: Vec<Vec<Op>> = (0..NUM_THREADS)
        .map(|_| Vec::with_capacity(OPERATIONS_PER_THREAD))
        .collect();

    for (thread_idx, operations) in all_operations.iter_mut().enumerate() {
        let range_start = thread_idx * (TOTAL_OPERATIONS / NUM_THREADS);
        let range_end = (thread_idx + 1) * (TOTAL_OPERATIONS / NUM_THREADS);

        for _ in 0..OPERATIONS_PER_THREAD {
            let value = rng.random_range(range_start..range_end);
            let operation = if thread_idx < NUM_WRITERS || rng.random::<f64>() < write_ratio {
                Op::Write(value)
            } else {
                Op::Read(value)
            };
            operations.push(operation);
        }
    }
    all_operations
}

fn concurrent_operations<T: Send + Sync + 'static>(
    set: Arc<T>,
    operations: Vec<Op>,
    read_op: impl Fn(&T, usize) + Send + Sync + 'static,
    write_op: impl Fn(&T, usize) + Send + Sync + 'static,
) {
    for op in operations {
        match op {
            Op::Read(value) => read_op(&set, value),
            Op::Write(value) => write_op(&set, value),
        }
    }
}

fn bench_btreeset_with_ratio(c: &mut Criterion, write_ratio: f64) {
    let operations = Arc::new(generate_operations(write_ratio));

    let mut group = c.benchmark_group(format!("Write Ratio: {:.2}", write_ratio));
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function(BenchmarkId::new("scc::TreeIndex", write_ratio), |b| {
        b.iter(|| {
            let set = Arc::new(TreeIndex::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            black_box(set.contains(&item));
                        },
                        |set, item| {
                            let _ = set.insert(item, ());
                            black_box(());
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.bench_function(BenchmarkId::new("ConcurrentBTreeSet", write_ratio), |b| {
        b.iter(|| {
            let set: Arc<indexset::concurrent::set::BTreeSet<usize>> =
                Arc::new(indexset::concurrent::set::BTreeSet::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            set.contains(&item);
                        },
                        |set, item| {
                            set.insert(item);
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.bench_function(BenchmarkId::new("SkipSet", write_ratio), |b| {
        b.iter(|| {
            let set = Arc::new(SkipSet::new());
            let mut handles = vec![];

            for thread_ops in operations.iter() {
                let set = Arc::clone(&set);
                let thread_ops = thread_ops.clone();
                let handle = thread::spawn(move || {
                    concurrent_operations(
                        set,
                        thread_ops,
                        |set, item| {
                            set.contains(&item);
                        },
                        |set, item| {
                            set.insert(item);
                        },
                    );
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.join().unwrap();
            }
        });
    });

    group.finish();
}

fn bench_concurrent_btreeset(c: &mut Criterion) {
    let ratios = vec![0.01, 0.1, 0.3, 0.5];
    for ratio in ratios {
        bench_btreeset_with_ratio(c, ratio);
    }
}

fn removal_entries(values_per_key: RangeInclusive<usize>) -> Vec<(usize, usize)> {
    let mut rng = StdRng::seed_from_u64(MULTIMAP_REMOVE_SEED);
    let mut entries = Vec::with_capacity(MULTIMAP_REMOVE_ENTRIES);
    let mut key = 0;

    while entries.len() < MULTIMAP_REMOVE_ENTRIES {
        let value_count = rng.random_range(values_per_key.clone());
        entries.extend((0..value_count).map(|value| (key, value)));
        key += 1;
    }

    entries
}

fn build_random_multimap(entries: &[(usize, usize)]) -> BTreeMultiMap<usize, usize> {
    let map = BTreeMultiMap::<usize, usize>::new();
    for (key, value) in entries {
        map.insert(*key, *value);
    }

    map
}

fn build_ord_multimap(
    entries: &[(usize, usize)],
) -> BTreeMultiMap<
    usize,
    usize,
    Vec<OrdMultiPair<usize, usize>>,
    OrdMultiPair<usize, usize>,
> {
    let map = BTreeMultiMap::<
        usize,
        usize,
        Vec<OrdMultiPair<usize, usize>>,
        OrdMultiPair<usize, usize>,
    >::new();
    for (key, value) in entries {
        map.insert(*key, *value);
    }

    map
}

fn bench_multimap_removal(c: &mut Criterion) {
    let usual_entries = removal_entries(1..=3);
    let many_entries = removal_entries(1_000..=2_000);
    let usual_target = *usual_entries.last().unwrap();
    let many_target = *many_entries.last().unwrap();

    let mut group = c.benchmark_group("BTreeMultiMap remove pair");
    group.warm_up_time(std::time::Duration::from_millis(500));
    group.measurement_time(std::time::Duration::from_millis(500));

    group.bench_function(BenchmarkId::new("random", "1-3 values per key"), |b| {
        b.iter_batched_ref(
            || build_random_multimap(&usual_entries),
            |map| black_box(map.remove(&usual_target.0, &usual_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("ord", "1-3 values per key"), |b| {
        b.iter_batched_ref(
            || build_ord_multimap(&usual_entries),
            |map| black_box(map.remove(&usual_target.0, &usual_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("random", "1000-2000 values per key"), |b| {
        b.iter_batched_ref(
            || build_random_multimap(&many_entries),
            |map| black_box(map.remove(&many_target.0, &many_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.bench_function(BenchmarkId::new("ord", "1000-2000 values per key"), |b| {
        b.iter_batched_ref(
            || build_ord_multimap(&many_entries),
            |map| black_box(map.remove(&many_target.0, &many_target.1)),
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_concurrent_btreeset, bench_multimap_removal);
criterion_main!(benches);
