use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::borrow::Borrow;
use std::cmp::Ordering;
use std::fmt::Debug;

pub const DEFAULT_NODE_CAPACITY: usize = 1_024;
pub const NODE_CAPACITIES: [usize; 2] = [256, DEFAULT_NODE_CAPACITY];
pub const RANGE_LEN: usize = 128;
pub const SEED: u64 = 42;
pub const SET_SIZES: [usize; 1] = [100_000];
pub const SINGLE_OPERATION_BATCH_SIZE: usize = 512;

pub trait BenchValue: Borrow<u64> + Clone + Ord + 'static {
    const ID: &'static str;

    fn from_key(key: u64) -> Self;
    fn key(&self) -> u64;
}

pub trait BenchMapValue: Clone + Debug + Eq + 'static {
    const ID: &'static str;

    fn from_key(key: u64) -> Self;
    fn updated_from_key(key: u64) -> Self;
    fn checksum(&self) -> u64;
}

impl BenchMapValue for u64 {
    const ID: &'static str = "entry_16b";

    fn from_key(key: u64) -> Self {
        key
    }

    fn updated_from_key(key: u64) -> Self {
        !key
    }

    fn checksum(&self) -> u64 {
        *self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LargeMapValue([u64; 7]);

impl BenchMapValue for LargeMapValue {
    const ID: &'static str = "entry_64b";

    fn from_key(key: u64) -> Self {
        Self([key; 7])
    }

    fn updated_from_key(key: u64) -> Self {
        Self([!key; 7])
    }

    fn checksum(&self) -> u64 {
        self.0[0]
    }
}

#[derive(Clone, Copy)]
pub enum MapInsertionKind {
    New,
    Update,
}

impl BenchValue for u64 {
    const ID: &'static str = "u64";

    fn from_key(key: u64) -> Self {
        key
    }

    fn key(&self) -> u64 {
        *self
    }
}

#[derive(Clone, Debug)]
pub struct LargeRecord {
    key: u64,
    _payload: [u8; 56],
}

impl Borrow<u64> for LargeRecord {
    fn borrow(&self) -> &u64 {
        &self.key
    }
}

impl PartialEq for LargeRecord {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for LargeRecord {}

impl PartialOrd for LargeRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for LargeRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl BenchValue for LargeRecord {
    const ID: &'static str = "record_64b";

    fn from_key(key: u64) -> Self {
        Self {
            key,
            _payload: [key as u8; 56],
        }
    }

    fn key(&self) -> u64 {
        self.key
    }
}

pub struct ValueGenerator {
    set_size: usize,
    base_keys: Vec<u64>,
}

impl ValueGenerator {
    pub fn new(set_size: usize) -> Self {
        let mut base_keys = (0..set_size as u64).map(|key| key * 2).collect::<Vec<_>>();
        base_keys.shuffle(&mut StdRng::seed_from_u64(SEED));
        Self { set_size, base_keys }
    }

    pub fn base_values<T: BenchValue>(&self) -> Vec<T> {
        self.base_keys.iter().copied().map(T::from_key).collect()
    }

    pub fn regular_insertion_batch<T: BenchValue>(&self, amount: usize) -> Vec<T> {
        let mut keys = self.new_keys(amount);
        keys.shuffle(&mut StdRng::seed_from_u64(SEED));
        keys.into_iter().map(T::from_key).collect()
    }

    pub fn duplicate_heavy_insertion_batch<T: BenchValue>(&self, amount: usize) -> Vec<T> {
        let new_value_count = amount / 10;
        let existing_count = amount - new_value_count;
        let mut keys = self.new_keys(new_value_count);
        keys.extend((0..existing_count).map(|index| {
            let position = index * self.set_size / existing_count;
            position as u64 * 2
        }));
        keys.shuffle(&mut StdRng::seed_from_u64(SEED));
        keys.into_iter().map(T::from_key).collect()
    }

    pub fn hit_keys(&self) -> Vec<u64> {
        self.base_keys
            .iter()
            .take(SINGLE_OPERATION_BATCH_SIZE)
            .copied()
            .collect()
    }

    pub fn miss_keys(&self) -> Vec<u64> {
        self.base_keys
            .iter()
            .take(SINGLE_OPERATION_BATCH_SIZE)
            .map(|key| key + 1)
            .collect()
    }

    pub fn random_indices(&self) -> Vec<usize> {
        self.base_keys
            .iter()
            .take(SINGLE_OPERATION_BATCH_SIZE)
            .map(|key| (key / 2) as usize)
            .collect()
    }

    pub fn map_base_entries<V: BenchMapValue>(&self) -> Vec<(u64, V)> {
        self.base_keys
            .iter()
            .copied()
            .map(|key| (key, V::from_key(key)))
            .collect()
    }

    pub fn map_insertions<V: BenchMapValue>(&self, amount: usize, kind: MapInsertionKind) -> Vec<(u64, V)> {
        match kind {
            MapInsertionKind::New => {
                let mut keys = self.new_keys(amount);
                keys.shuffle(&mut StdRng::seed_from_u64(SEED));
                keys.into_iter().map(|key| (key, V::from_key(key))).collect()
            }
            MapInsertionKind::Update => self
                .base_keys
                .iter()
                .take(amount)
                .copied()
                .map(|key| (key, V::updated_from_key(key)))
                .collect(),
        }
    }

    fn new_keys(&self, count: usize) -> Vec<u64> {
        (0..count)
            .map(|index| {
                let position = if count <= self.set_size {
                    index * self.set_size / count
                } else {
                    index
                };
                position as u64 * 2 + 1
            })
            .collect()
    }
}
