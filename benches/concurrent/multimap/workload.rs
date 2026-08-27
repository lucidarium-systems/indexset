use super::dataset::{BenchMultiMapValue, MultiMapDataset};
use crate::value_generator::{SEED, SINGLE_OPERATION_BATCH_SIZE};
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

pub const INSERT_BATCH_COUNT: usize = 1_024;
pub const MIXED_OPERATION_COUNT: usize = 10_000;
const MIXED_READ_HIT_PERCENT: usize = 90;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiMapInsertionKind {
    New,
    Existing,
}

impl MultiMapInsertionKind {
    pub fn id(self) -> &'static str {
        match self {
            Self::New => "new",
            Self::Existing => "existing",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiMapScenario {
    InsertBatchNew,
    InsertBatchExisting,
    MixedRead90Write10,
    MixedRead50Write50,
}

impl MultiMapScenario {
    pub const ALL: [Self; 4] = [
        Self::InsertBatchNew,
        Self::InsertBatchExisting,
        Self::MixedRead90Write10,
        Self::MixedRead50Write50,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertBatchNew => "insert_batch/new",
            Self::InsertBatchExisting => "insert_batch/existing",
            Self::MixedRead90Write10 => "mixed/read90_write10",
            Self::MixedRead50Write50 => "mixed/read50_write50",
        }
    }

    pub fn operation_count(self) -> usize {
        match self {
            Self::InsertBatchNew | Self::InsertBatchExisting => INSERT_BATCH_COUNT,
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => MIXED_OPERATION_COUNT,
        }
    }

    pub fn expected_successes(self) -> usize {
        match self {
            Self::InsertBatchNew | Self::InsertBatchExisting => INSERT_BATCH_COUNT,
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => {
                let (read_count, write_pair_count) = self.mixed_operation_counts();
                read_count * MIXED_READ_HIT_PERCENT / 100 + write_pair_count * 2
            }
        }
    }

    pub fn expected_len(self, pair_count: usize) -> usize {
        match self {
            Self::InsertBatchNew | Self::InsertBatchExisting => pair_count + INSERT_BATCH_COUNT,
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => pair_count,
        }
    }

    pub fn needs_fresh_map(self) -> bool {
        matches!(self, Self::InsertBatchNew | Self::InsertBatchExisting)
    }

    fn reads_per_block(self) -> usize {
        match self {
            Self::MixedRead90Write10 => 18,
            Self::MixedRead50Write50 => 2,
            Self::InsertBatchNew | Self::InsertBatchExisting => 0,
        }
    }

    fn mixed_operation_counts(self) -> (usize, usize) {
        let reads_per_block = self.reads_per_block();
        let block_count = MIXED_OPERATION_COUNT / (reads_per_block + 2);
        (block_count * reads_per_block, block_count)
    }

    pub fn validate_operations<V: BenchMultiMapValue>(self, shards: &[Vec<MultiMapOperation<V>>], thread_count: usize) {
        assert_eq!(shards.len(), thread_count);
        assert!(shards.iter().all(|shard| !shard.is_empty()));

        let actual = MultiMapWorkloadShape::from_shards(shards);
        let expected = match self {
            Self::InsertBatchNew | Self::InsertBatchExisting => MultiMapWorkloadShape {
                inserts: INSERT_BATCH_COUNT,
                ..MultiMapWorkloadShape::default()
            },
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => {
                let (gets, write_pair_count) = self.mixed_operation_counts();
                MultiMapWorkloadShape {
                    gets,
                    get_hits: gets * MIXED_READ_HIT_PERCENT / 100,
                    inserts: write_pair_count,
                    removes: write_pair_count,
                }
            }
        };

        assert_eq!(actual, expected);
        assert_eq!(actual.operation_count(), self.operation_count());
    }
}

#[derive(Clone)]
pub enum MultiMapOperation<V> {
    Get { key: u64, expected: Option<(usize, u64)> },
    Insert(u64, V),
    RemovePair(u64, V),
}

#[derive(Debug, Default, Eq, PartialEq)]
struct MultiMapWorkloadShape {
    gets: usize,
    get_hits: usize,
    inserts: usize,
    removes: usize,
}

impl MultiMapWorkloadShape {
    fn from_shards<V>(shards: &[Vec<MultiMapOperation<V>>]) -> Self {
        let mut shape = Self::default();

        for operation in shards.iter().flatten() {
            match operation {
                MultiMapOperation::Get { expected, .. } => {
                    shape.gets += 1;
                    shape.get_hits += usize::from(expected.is_none());
                }
                MultiMapOperation::Insert(_, _) => shape.inserts += 1,
                MultiMapOperation::RemovePair(_, _) => shape.removes += 1,
            }
        }

        shape
    }

    fn operation_count(&self) -> usize {
        self.gets + self.inserts + self.removes
    }
}

#[derive(Clone)]
pub struct MultiMapQuery {
    pub key: u64,
    pub expected: (usize, u64),
}

pub fn insertions<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    amount: usize,
    kind: MultiMapInsertionKind,
) -> Vec<(u64, V)> {
    let mut insertions = match kind {
        MultiMapInsertionKind::New => dataset
            .new_keys(amount)
            .into_iter()
            .map(|key| (key, V::from_ordinal(key)))
            .collect::<Vec<_>>(),
        MultiMapInsertionKind::Existing => {
            let key_count = dataset.key_count();
            let key_indices = shuffled_key_indices(key_count, 0x1A5E_1002);
            (0..amount)
                .map(|index| {
                    let key_index = key_indices[index % key_count];
                    let ordinal = dataset.value_counts[key_index] as u64 + (index / key_count) as u64;
                    (dataset.key(key_index), V::from_ordinal(ordinal))
                })
                .collect::<Vec<_>>()
        }
    };
    insertions.shuffle(&mut StdRng::seed_from_u64(
        SEED ^ match kind {
            MultiMapInsertionKind::New => 0x1A5E_0001,
            MultiMapInsertionKind::Existing => 0x1A5E_0002,
        },
    ));
    assert!(insertions
        .iter()
        .all(|(key, _)| { dataset.contains_key(*key) == matches!(kind, MultiMapInsertionKind::Existing) }));
    insertions
}

pub fn point_queries<V: BenchMultiMapValue>(dataset: &MultiMapDataset<V>, hit: bool) -> Vec<MultiMapQuery> {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x6E70_0001);
    (0..SINGLE_OPERATION_BATCH_SIZE)
        .map(|_| {
            let key_index = rng.random_range(0..dataset.key_count());
            MultiMapQuery {
                key: dataset.key(key_index) + u64::from(!hit),
                expected: if hit { dataset.bucket_result(key_index) } else { (0, 0) },
            }
        })
        .collect()
}

pub fn removal_pairs<V: BenchMultiMapValue>(dataset: &MultiMapDataset<V>, hit: bool) -> Vec<(u64, V)> {
    if hit {
        return dataset
            .entries
            .iter()
            .take(SINGLE_OPERATION_BATCH_SIZE)
            .cloned()
            .collect();
    }

    let key_count = dataset.key_count();
    let key_indices = shuffled_key_indices(key_count, 0x6E70_1002);
    (0..SINGLE_OPERATION_BATCH_SIZE)
        .map(|index| {
            let key_index = key_indices[index % key_count];
            let ordinal = dataset.value_counts[key_index] as u64 + (index / key_count) as u64;
            (dataset.key(key_index), V::from_ordinal(ordinal))
        })
        .collect()
}

pub fn operations<V: BenchMultiMapValue>(
    scenario: MultiMapScenario,
    dataset: &MultiMapDataset<V>,
    thread_count: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    let operations = match scenario {
        MultiMapScenario::InsertBatchNew => insertions(dataset, INSERT_BATCH_COUNT, MultiMapInsertionKind::New)
            .into_iter()
            .map(|(key, value)| MultiMapOperation::Insert(key, value))
            .collect(),
        MultiMapScenario::InsertBatchExisting => {
            insertions(dataset, INSERT_BATCH_COUNT, MultiMapInsertionKind::Existing)
                .into_iter()
                .map(|(key, value)| MultiMapOperation::Insert(key, value))
                .collect()
        }
        MultiMapScenario::MixedRead90Write10 | MultiMapScenario::MixedRead50Write50 => {
            return mixed_operations(dataset, thread_count, scenario.reads_per_block());
        }
    };
    shard_operations(operations, thread_count)
}

fn mixed_operations<V: BenchMultiMapValue>(
    dataset: &MultiMapDataset<V>,
    thread_count: usize,
    reads_per_block: usize,
) -> Vec<Vec<MultiMapOperation<V>>> {
    let block_count = MIXED_OPERATION_COUNT / (reads_per_block + 2);
    let key_count = dataset.key_count();
    assert!(key_count > thread_count);
    let mutation_key_count = block_count.min((key_count / 2).max(thread_count)).min(key_count - 1);
    assert!(mutation_key_count >= thread_count);
    let mut key_indices = shuffled_key_indices(key_count, reads_per_block as u64 ^ 0x6E70_1003);
    let read_key_indices = key_indices.split_off(mutation_key_count);
    let mutation_key_indices = key_indices;
    let mut shards = empty_shards(thread_count);
    let mut rng = StdRng::seed_from_u64(SEED ^ reads_per_block as u64 ^ 0x6E70_0003);
    let mut read_index = 0;

    for block in 0..block_count {
        let mutation_index = block % mutation_key_indices.len();
        let mutation_key = dataset.key(mutation_key_indices[mutation_index]);
        let mutation_value = V::from_ordinal(0);
        let shard = mutation_index % thread_count;
        let mutation_position = rng.random_range(0..=reads_per_block);

        for position in 0..=reads_per_block {
            if position == mutation_position {
                shards[shard].push(MultiMapOperation::RemovePair(mutation_key, mutation_value.clone()));
                shards[shard].push(MultiMapOperation::Insert(mutation_key, mutation_value.clone()));
            }
            if position < reads_per_block {
                let hit = read_index % 10 != 9;
                let key_index = read_key_indices[rng.random_range(0..read_key_indices.len())];
                let key = dataset.key(key_index) + u64::from(!hit);
                let expected = (!hit).then_some((0, 0));
                shards[shard].push(MultiMapOperation::Get { key, expected });
                read_index += 1;
            }
        }
    }

    shards
}

fn shuffled_key_indices(key_count: usize, seed_salt: u64) -> Vec<usize> {
    let mut key_indices = (0..key_count).collect::<Vec<_>>();
    key_indices.shuffle(&mut StdRng::seed_from_u64(SEED ^ seed_salt));
    key_indices
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
