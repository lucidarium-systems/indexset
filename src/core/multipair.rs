use crate::cdc::change::ChangeEvent;
use crate::concurrent::set::BTreeSet;
use crate::core::node::NodeLike;

pub mod ord;
pub mod random;

pub use ord::OrdMultiPair;
pub use random::RandomMultiPair;

pub type MultiPair<K, V> = RandomMultiPair<K, V>;

/// Common contract for key-value pairs stored by `BTreeMultiMap`.
///
/// Implementations must order entries by key first: entries with the same key
/// must form one contiguous range, while their representation may refine the
/// order within that range.
pub trait MultiPairLike<K, V>: Into<(K, V)> + Ord {
    fn new(key: K, value: V) -> Self;
    fn key(&self) -> &K;
    fn value(&self) -> &V;
}

/// Pair-specific exact-removal strategy used by `BTreeMultiMap`.
pub trait MultiPairRemoveHelper<K, V> {
    fn remove_from<Node>(set: &BTreeSet<Self, Node>, key: &K, value: &V) -> Option<(K, V)>
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static,
    {
        Self::remove_cdc_from(set, key, value).0
    }

    fn remove_cdc_from<Node>(
        set: &BTreeSet<Self, Node>,
        key: &K,
        value: &V,
    ) -> (Option<(K, V)>, Vec<ChangeEvent<Self>>)
    where
        Self: Ord + Clone + 'static,
        Node: NodeLike<Self> + Send + 'static;
}
