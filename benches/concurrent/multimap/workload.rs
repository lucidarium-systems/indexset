use super::dataset::{value_counts, BenchMultiMapValue, MultiMapDataset, MultiMapFanout};
use crate::value_generator::{QUERY_COUNT, RANGE_LEN, SEED};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

pub const INSERT_BATCH_COUNT: usize = 1_024;
pub const INSERT_ONE_BATCH_SIZE: usize = 128;
const POINT_OPERATION_COUNT: usize = 60_000;
const SPARSE_MIXED_OPERATION_COUNT: usize = 4_000;
const DENSE_OPERATION_COUNT: usize = 400;
const RANGE_QUERY_COUNT: usize = QUERY_COUNT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiMapInsertionKind {
    NewKey,
    ExistingKey,
}

impl MultiMapInsertionKind {
    pub fn id(self) -> &'static str {
        match self {
            Self::NewKey => "new_key",
            Self::ExistingKey => "existing_key",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiMapScenario {
    InsertNewKey,
    InsertExistingKey,
    GetHit,
    GetMiss,
    RemoveExactHit,
    Range128Keys,
    MixedReadHeavy,
    MixedBalanced,
}

impl MultiMapScenario {
    pub const ALL: [Self; 8] = [
        Self::InsertNewKey,
        Self::InsertExistingKey,
        Self::GetHit,
        Self::GetMiss,
        Self::RemoveExactHit,
        Self::Range128Keys,
        Self::MixedReadHeavy,
        Self::MixedBalanced,
    ];

    pub const CAPACITY_SWEEP: [Self; 3] = [Self::GetHit, Self::InsertExistingKey, Self::MixedBalanced];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertNewKey => "insert/new_key",
            Self::InsertExistingKey => "insert/existing_key",
            Self::GetHit => "get/hit",
            Self::GetMiss => "get/miss",
            Self::RemoveExactHit => "remove/exact_hit",
            Self::Range128Keys => "range/up_to_128_keys",
            Self::MixedReadHeavy => "mixed/random_read90",
            Self::MixedBalanced => "mixed/random_read50",
        }
    }

    pub fn operation_count(self, fanout: MultiMapFanout) -> usize {
        match self {
            Self::InsertNewKey | Self::InsertExistingKey => INSERT_BATCH_COUNT,
            Self::GetHit | Self::GetMiss => point_operation_count(fanout),
            Self::MixedReadHeavy | Self::MixedBalanced => mixed_operation_count(fanout),
            Self::RemoveExactHit | Self::Range128Keys => QUERY_COUNT,
        }
    }

    pub fn expected_successes(self, fanout: MultiMapFanout) -> usize {
        match self {
            Self::InsertNewKey | Self::InsertExistingKey => INSERT_BATCH_COUNT,
            Self::GetHit => point_operation_count(fanout),
            Self::GetMiss => 0,
            Self::RemoveExactHit | Self::Range128Keys => QUERY_COUNT,
            Self::MixedReadHeavy => mixed_successes(fanout, 18),
            Self::MixedBalanced => mixed_successes(fanout, 2),
        }
    }

    pub fn expected_len(self, pair_count: usize) -> usize {
        match self {
            Self::InsertNewKey | Self::InsertExistingKey => pair_count + INSERT_BATCH_COUNT,
            Self::RemoveExactHit => pair_count - QUERY_COUNT,
            Self::GetHit | Self::GetMiss | Self::Range128Keys | Self::MixedReadHeavy | Self::MixedBalanced => {
                pair_count
            }
        }
    }

    pub fn needs_fresh_map(self) -> bool {
        matches!(
            self,
            Self::InsertNewKey | Self::InsertExistingKey | Self::RemoveExactHit
        )
    }

    pub fn throughput_elements(self, pair_count: usize, fanout: MultiMapFanout) -> usize {
        match self {
            Self::GetHit => get_pair_visits(pair_count, fanout),
            Self::Range128Keys => range_pair_visits(pair_count, fanout),
            Self::InsertNewKey
            | Self::InsertExistingKey
            | Self::GetMiss
            | Self::RemoveExactHit
            | Self::MixedReadHeavy
            | Self::MixedBalanced => self.operation_count(fanout),
        }
    }
}

#[derive(Clone)]
pub enum MultiMapOperation<V> {
    Get {
        key: u64,
        expected: Option<(usize, u64)>,
    },
    Insert(u64, V),
    RemoveExact(u64, V),
    Range {
        start: u64,
        end: u64,
        expected: (usize, u64),
    },
}

pub fn insertions<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    amount: usize,
    kind: MultiMapInsertionKind,
) -> Vec<(u64, V)> {
    let mut insertions = match kind {
        MultiMapInsertionKind::NewKey => (0..amount)
            .map(|index| {
                let key = index as u64 * 2 + 1;
                (key, V::from_ordinal(key))
            })
            .collect::<Vec<_>>(),
        MultiMapInsertionKind::ExistingKey => {
            let key_count = dataset.key_count();
            (0..amount)
                .map(|index| {
                    let key_index = index % key_count;
                    let ordinal = dataset.value_counts[key_index] as u64 + (index / key_count) as u64;
                    (dataset.key(key_index), V::from_ordinal(ordinal))
                })
                .collect::<Vec<_>>()
        }
    };
    insertions.shuffle(&mut StdRng::seed_from_u64(
        SEED ^ match kind {
            MultiMapInsertionKind::NewKey => 0x1A5E_0001,
            MultiMapInsertionKind::ExistingKey => 0x1A5E_0002,
        },
    ));
    insertions
}

pub fn operations<V: BenchMultiMapValue>(
    scenario: MultiMapScenario,
    dataset: &MultiMapDataset<V>,
    fanout: MultiMapFanout,
    thread_count: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    match scenario {
        MultiMapScenario::InsertNewKey => shard_operations(
            insertions(dataset, INSERT_BATCH_COUNT, MultiMapInsertionKind::NewKey)
                .into_iter()
                .map(|(key, value)| MultiMapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MultiMapScenario::InsertExistingKey => shard_operations(
            insertions(dataset, INSERT_BATCH_COUNT, MultiMapInsertionKind::ExistingKey)
                .into_iter()
                .map(|(key, value)| MultiMapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MultiMapScenario::GetHit => point_queries(dataset, true, point_operation_count(fanout), thread_count),
        MultiMapScenario::GetMiss => point_queries(dataset, false, point_operation_count(fanout), thread_count),
        MultiMapScenario::RemoveExactHit => shard_operations(
            dataset
                .entries
                .iter()
                .take(QUERY_COUNT)
                .cloned()
                .map(|(key, value)| MultiMapOperation::RemoveExact(key, value))
                .collect(),
            thread_count,
        ),
        MultiMapScenario::Range128Keys => range_queries(dataset, thread_count),
        MultiMapScenario::MixedReadHeavy => mixed_operations(dataset, mixed_operation_count(fanout), thread_count, 18),
        MultiMapScenario::MixedBalanced => mixed_operations(dataset, mixed_operation_count(fanout), thread_count, 2),
    }
}

fn point_operation_count(fanout: MultiMapFanout) -> usize {
    match fanout {
        MultiMapFanout::Sparse => POINT_OPERATION_COUNT,
        MultiMapFanout::Dense => DENSE_OPERATION_COUNT,
    }
}

fn mixed_operation_count(fanout: MultiMapFanout) -> usize {
    match fanout {
        MultiMapFanout::Sparse => SPARSE_MIXED_OPERATION_COUNT,
        MultiMapFanout::Dense => DENSE_OPERATION_COUNT,
    }
}

fn mixed_successes(fanout: MultiMapFanout, reads_per_block: usize) -> usize {
    let block_count = mixed_operation_count(fanout) / (reads_per_block + 2);
    let read_count = block_count * reads_per_block;
    block_count * 2 + read_count * 4 / 5
}

fn point_queries<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    hit: bool,
    operation_count: usize,
    thread_count: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x6E70_0001);
    let operations = (0..operation_count)
        .map(|_| {
            let key_index = rng.random_range(0..dataset.key_count());
            let key = dataset.key(key_index) + u64::from(!hit);
            let expected = hit.then(|| dataset.bucket_result(key_index)).or(Some((0, 0)));
            MultiMapOperation::Get { key, expected }
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn range_queries<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    thread_count: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    let key_count = dataset.key_count();
    let range_key_count = RANGE_LEN.min(key_count);
    let count_prefix = prefix_sums(&dataset.value_counts, |value| value as u64);
    let checksum_prefix = prefix_sums(&dataset.bucket_checksums, |value| value);
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x6E70_0002);
    let operations = (0..RANGE_QUERY_COUNT)
        .map(|_| {
            let start_index = rng.random_range(0..=(key_count - range_key_count));
            let end_index = start_index + range_key_count;
            MultiMapOperation::Range {
                start: dataset.key(start_index),
                end: end_index as u64 * 2,
                expected: (
                    (count_prefix[end_index] - count_prefix[start_index]) as usize,
                    checksum_prefix[end_index].wrapping_sub(checksum_prefix[start_index]),
                ),
            }
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn mixed_operations<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    operation_count: usize,
    thread_count: usize,
    reads_per_block: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    let block_len = reads_per_block + 2;
    let block_count = operation_count / block_len;
    let key_count = dataset.key_count();
    let mutation_key_count = if key_count == 1 {
        1
    } else {
        block_count.min(key_count / 2).max(1)
    };
    let read_start = if key_count == 1 { 0 } else { mutation_key_count };
    let read_key_count = key_count - read_start;
    let mut shards = empty_shards(thread_count);
    let mut rng = StdRng::seed_from_u64(SEED ^ reads_per_block as u64 ^ 0x6E70_0003);
    let mut read_index = 0;

    for block in 0..block_count {
        let mutation_index = block % mutation_key_count;
        let mutation_key = dataset.key(mutation_index);
        let mutation_value = V::from_ordinal(0);
        let shard = mutation_index % thread_count;
        let mutation_position = rng.random_range(0..=reads_per_block);

        for position in 0..=reads_per_block {
            if position == mutation_position {
                shards[shard].push(MultiMapOperation::RemoveExact(mutation_key, mutation_value.clone()));
                shards[shard].push(MultiMapOperation::Insert(mutation_key, mutation_value.clone()));
            }
            if position < reads_per_block {
                let hit = read_index % 5 != 4;
                let key_index = read_start + rng.random_range(0..read_key_count);
                let key = dataset.key(key_index) + u64::from(!hit);
                let expected = if hit { None } else { Some((0, 0)) };
                let read_shard = read_index % thread_count;
                shards[read_shard].push(MultiMapOperation::Get { key, expected });
                read_index += 1;
            }
        }
    }

    shards
}

fn get_pair_visits(pair_count: usize, fanout: MultiMapFanout) -> usize {
    let value_counts = value_counts(pair_count, fanout);
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x6E70_0001);
    (0..point_operation_count(fanout))
        .map(|_| value_counts[rng.random_range(0..value_counts.len())])
        .sum()
}

fn range_pair_visits(pair_count: usize, fanout: MultiMapFanout) -> usize {
    let value_counts = value_counts(pair_count, fanout);
    let key_count = value_counts.len();
    let range_key_count = RANGE_LEN.min(key_count);
    let count_prefix = prefix_sums(&value_counts, |value| value as u64);
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x6E70_0002);
    (0..RANGE_QUERY_COUNT)
        .map(|_| {
            let start_index = rng.random_range(0..=(key_count - range_key_count));
            let end_index = start_index + range_key_count;
            (count_prefix[end_index] - count_prefix[start_index]) as usize
        })
        .sum()
}

fn prefix_sums<T>(values: &[T], value: impl Fn(T) -> u64) -> Vec<u64>
where
    T: Copy,
{
    let mut sums = Vec::with_capacity(values.len() + 1);
    sums.push(0_u64);
    for item in values.iter().copied() {
        sums.push(sums.last().copied().unwrap().wrapping_add(value(item)));
    }
    sums
}

fn shard_operations<T>(operations: Vec<T>, thread_count: usize) -> Vec<Vec<T>> {
    let mut shards = empty_shards(thread_count);
    for (index, operation) in operations.into_iter().enumerate() {
        shards[index % thread_count].push(operation);
    }
    shards
}

fn empty_shards<T>(thread_count: usize) -> Vec<Vec<T>> {
    (0..thread_count).map(|_| Vec::new()).collect()
}
