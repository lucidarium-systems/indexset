use crate::value_generator::SEED;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};
use std::fmt::Debug;

const KEY_STRIDE: u64 = 4_096;

pub trait BenchMultiMapValue: Clone + Debug + Eq + Ord + 'static {
    const ID: &'static str;

    fn from_ordinal(ordinal: u64) -> Self;
    fn checksum(&self) -> u64;
}

impl BenchMultiMapValue for u64 {
    const ID: &'static str = "v8b";

    fn from_ordinal(ordinal: u64) -> Self {
        ordinal
    }

    fn checksum(&self) -> u64 {
        *self
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LargeValue([u64; 7]);

impl BenchMultiMapValue for LargeValue {
    const ID: &'static str = "v56b";

    fn from_ordinal(ordinal: u64) -> Self {
        Self([ordinal; 7])
    }

    fn checksum(&self) -> u64 {
        self.0[0]
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MultiMapFanout {
    Sparse,
    Dense,
}

impl MultiMapFanout {
    pub const ALL: [Self; 2] = [Self::Sparse, Self::Dense];

    pub fn id(self) -> &'static str {
        match self {
            Self::Sparse => "fan1_3",
            Self::Dense => "fan1k_2k",
        }
    }

    fn value_count_bounds(self) -> (usize, usize) {
        match self {
            Self::Sparse => (1, 3),
            Self::Dense => (1_000, 2_000),
        }
    }

    fn seed_salt(self) -> u64 {
        match self {
            Self::Sparse => 0x5A11_0001,
            Self::Dense => 0xDE45_E001,
        }
    }
}

pub struct MultiMapDataset<V> {
    pub entries: Vec<(u64, V)>,
    pub value_counts: Vec<usize>,
    pub bucket_checksums: Vec<u64>,
}

impl<V> MultiMapDataset<V>
where
    V: BenchMultiMapValue,
{
    pub fn new(pair_count: usize, fanout: MultiMapFanout) -> Self {
        let value_counts = value_counts(pair_count, fanout);
        let (minimum, maximum) = fanout.value_count_bounds();
        assert_eq!(value_counts.iter().sum::<usize>(), pair_count);
        assert!(value_counts
            .iter()
            .all(|value_count| (minimum..=maximum).contains(value_count)));
        let mut entries = Vec::with_capacity(pair_count);
        let mut bucket_checksums = Vec::with_capacity(value_counts.len());

        for (key_index, value_count) in value_counts.iter().copied().enumerate() {
            let key = key_index as u64 * KEY_STRIDE;
            let mut checksum = 0_u64;
            for ordinal in 0..value_count as u64 {
                let value = V::from_ordinal(ordinal);
                checksum = checksum.wrapping_add(key).wrapping_add(value.checksum());
                entries.push((key, value));
            }
            bucket_checksums.push(checksum);
        }

        entries.shuffle(&mut StdRng::seed_from_u64(
            SEED ^ fanout.seed_salt() ^ pair_count as u64,
        ));

        Self {
            entries,
            value_counts,
            bucket_checksums,
        }
    }

    pub fn key_count(&self) -> usize {
        self.value_counts.len()
    }

    pub fn key(&self, key_index: usize) -> u64 {
        key_index as u64 * KEY_STRIDE
    }

    pub fn contains_key(&self, key: u64) -> bool {
        key % KEY_STRIDE == 0 && key / KEY_STRIDE < self.key_count() as u64
    }

    pub fn new_keys(&self, amount: usize) -> Vec<u64> {
        assert!(amount > 0);
        assert!(self.key_count() > 1);

        let gap_count = self.key_count() - 1;
        (0..amount)
            .map(|index| {
                let (gap_index, offset) = if amount <= gap_count {
                    (index * gap_count / amount, 1)
                } else {
                    (index % gap_count, index / gap_count + 1)
                };
                assert!((offset as u64) < KEY_STRIDE);
                self.key(gap_index) + offset as u64
            })
            .collect()
    }

    pub fn bucket_result(&self, key_index: usize) -> (usize, u64) {
        (self.value_counts[key_index], self.bucket_checksums[key_index])
    }
}

pub fn value_counts(pair_count: usize, fanout: MultiMapFanout) -> Vec<usize> {
    let mut rng = StdRng::seed_from_u64(SEED ^ fanout.seed_salt());
    let (minimum, maximum) = fanout.value_count_bounds();
    let mut counts = Vec::new();
    let mut remaining = pair_count;

    if remaining == 0 {
        return counts;
    }
    assert!(remaining >= minimum);

    while remaining > maximum {
        let value_count = rng.random_range(minimum..=maximum.min(remaining - minimum));
        counts.push(value_count);
        remaining -= value_count;
    }
    counts.push(remaining);

    counts
}
