use std::borrow::Borrow;
use std::fmt::Debug;

use core::cmp::Ordering;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::cdc::change::ChangeEvent;
use crate::concurrent::set::BTreeSet;
use crate::core::node::NodeLike;
use crate::core::pair::Pair;

use super::{MultiPairLike, MultiPairRemoveHelper};

#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[derive(Debug, Default, Clone, Hash)]
pub struct OrdMultiPair<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> OrdMultiPair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K: Ord, V: Ord> Eq for OrdMultiPair<K, V> {}

impl<K: Ord, V: Ord> PartialEq<Self> for OrdMultiPair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key) && self.value.eq(&other.value)
    }
}

impl<K: Ord, V: Ord> Ord for OrdMultiPair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key).then(self.value.cmp(&other.value))
    }
}

impl<K: Ord, V: Ord> PartialOrd for OrdMultiPair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> Borrow<K> for OrdMultiPair<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<K: Ord, V: Ord> MultiPairLike<K, V> for OrdMultiPair<K, V> {
    fn new(key: K, value: V) -> Self {
        Self::new(key, value)
    }

    fn key(&self) -> &K {
        &self.key
    }

    fn value(&self) -> &V {
        &self.value
    }
}

impl<K, V> From<Pair<K, V>> for OrdMultiPair<K, V> {
    fn from(pair: Pair<K, V>) -> Self {
        OrdMultiPair {
            key: pair.key,
            value: pair.value,
        }
    }
}

impl<K, V> From<OrdMultiPair<K, V>> for Pair<K, V> {
    fn from(pair: OrdMultiPair<K, V>) -> Self {
        Pair {
            key: pair.key,
            value: pair.value,
        }
    }
}

impl<K, V> From<OrdMultiPair<K, V>> for (K, V) {
    fn from(pair: OrdMultiPair<K, V>) -> Self {
        (pair.key, pair.value)
    }
}

impl<K, V> MultiPairRemoveHelper<K, V> for OrdMultiPair<K, V>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Ord + Clone + 'static,
{
    fn remove_cdc_from<Node>(
        set: &BTreeSet<Self, Node>,
        key: &K,
        value: &V,
    ) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        let pair_to_remove = OrdMultiPair::new(key.clone(), value.clone());
        let (res, evs) = set.remove_cdc(&pair_to_remove);

        (res.map(Into::into), evs)
    }
}
