use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::borrow::Borrow;
use std::cmp::Ordering;

pub const DEFAULT_NODE_CAPACITY: usize = 1_024;
pub const NODE_CAPACITIES: [usize; 4] = [64, 256, DEFAULT_NODE_CAPACITY, 4_096];
pub const QUERY_COUNT: usize = 512;
pub const RANGE_LEN: usize = 128;
pub const SEED: u64 = 42;
pub const SET_SIZES: [usize; 3] = [1_000, 100_000, 1_000_000];

pub trait BenchValue: Borrow<u64> + Clone + Ord + 'static {
    const ID: &'static str;

    fn from_key(key: u64) -> Self;
    fn key(&self) -> u64;
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
        self.base_keys.iter().take(QUERY_COUNT).copied().collect()
    }

    pub fn miss_keys(&self) -> Vec<u64> {
        self.base_keys.iter().take(QUERY_COUNT).map(|key| key + 1).collect()
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
