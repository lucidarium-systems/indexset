use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{borrow::Borrow, iter::FusedIterator, ops::{Bound, RangeBounds}};

use parking_lot::Mutex;

use crate::core::node::NodeLike;
use crate::{cdc::change::ChangeEvent, core::multipair::{MultiPair, MultiPairLike, MultiPairRemoveHelper}};

use super::set::BTreeSet;

#[derive(Debug)]
pub struct BTreeMultiMap<K, V, Node = Vec<MultiPair<K, V>>, M = MultiPair<K, V>>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    pub(crate) set: BTreeSet<M, Node>,
    marker: PhantomData<(K, V)>,
}

impl<K, V, Node, M> Default for BTreeMultiMap<K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    fn default() -> Self {
        Self {
            set: Default::default(),
            marker: PhantomData,
        }
    }
}

pub struct Iter<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    inner: super::set::Iter<'a, M, Node>,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V, Node, M> Iterator for Iter<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next() {
            return Some((entry.key(), entry.value()));
        }

        None
    }
}

impl<'a, K, V, Node, M> DoubleEndedIterator for Iter<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        if let Some(entry) = self.inner.next_back() {
            return Some((entry.key(), entry.value()));
        }

        None
    }
}

impl<'a, K, V, Node, M> FusedIterator for Iter<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
}

pub struct Range<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    inner: super::set::Range<'a, M, Node>,
    marker: PhantomData<(K, V)>,
}

impl<'a, K, V, Node, M> Iterator for Range<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|entry| (entry.key(), entry.value()))
    }
}

impl<'a, K, V, Node, M> DoubleEndedIterator for Range<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.inner.next_back().map(|entry| (entry.key(), entry.value()))
    }
}

impl<'a, K, V, Node, M> FusedIterator for Range<'a, K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
}

impl<K, V, Node, M> BTreeMultiMap<K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    /// Makes a new, empty, persistent `BTreeMultiMap`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    ///
    /// // entries can now be inserted into the empty map
    /// map.insert(1, "a");
    /// ```
    pub fn new() -> Self {
        Self {
            set: Default::default(),
            marker: PhantomData,
        }
    }
    /// Makes a new, empty `BTreeMultiMap` with the given maximum node size. Allocates one vec with
    /// the capacity set to be the specified node size.
    ///
    /// # Examples
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let map = BTreeMultiMap::<i32, i32>::with_maximum_node_size(128);
    pub fn with_maximum_node_size(node_capacity: usize) -> Self {
        Self {
            set: BTreeSet::with_maximum_node_size(node_capacity),
            marker: PhantomData,
        }
    }
    /// Adds full [`Node`] to this multiset. [`Node`] should be correct node with
    /// values sorted.
    #[cfg(feature = "cdc")]
    pub fn attach_multi_node(&self, node: Node) {
        self.set.attach_node(node)
    }
    /// Returns iterator over this multiset's [`Node`]'s.
    #[cfg(feature = "cdc")]
    pub fn iter_nodes(&self) -> impl Iterator<Item = Arc<Mutex<Node>>> + '_ {
        self.set.index.iter().map(|e| e.value().clone())
    }
    /// Returns `true` if the map contains at least one occurance of the specified key.
    ///
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form *must* match the ordering on the key type.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    /// map.insert(1, "a");
    /// map.insert(1, "b");
    /// assert_eq!(map.contains_key(&1), true);
    /// assert_eq!(map.contains_key(&2), false);
    /// ```
    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        M: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.set.contains(key)
    }
    fn _range<Q, R>(&self, range: R) -> Range<'_, K, V, Node, M>
    where
        M: Borrow<Q>,
        Q: Ord + ?Sized,
        R: RangeBounds<Q>,
    {
        Range {
            inner: super::set::BTreeSet::range(&self.set, range),
            marker: PhantomData,
        }
    }
    /// Constructs a double-ended iterator over all key value pairs with the given key in the map.
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    /// use indexset::BTreeSet;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    ///
    /// map.insert(1, "b");
    /// map.insert(1, "a");
    /// map.insert(2, "c");
    ///
    /// let all_with_key = map.get(&1).collect::<BTreeSet<_>>();
    /// assert_eq!(all_with_key.len(), 2);
    /// assert_eq!(all_with_key, vec![(&1, &"a"), (&1, &"b")].into_iter().collect::<BTreeSet<_>>());
    /// ```
    pub fn get(&self, key: &K) -> Range<'_, K, V, Node, M>
    where
        M: Borrow<K>,
    {
        self._range((Bound::Included(key), Bound::Included(key)))
    }
    /// Inserts a key-value pair into the multi map.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    /// assert_eq!(map.insert(37, "a"), None);
    /// assert_eq!(map.len() == 0, false);
    ///
    /// map.insert(37, "b");
    /// assert_eq!(map.insert(37, "c"), None);
    /// ```
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let new_entry = M::new(key, value);

        self.set
            .put_cdc(new_entry)
            .0
            .map(|pair| pair.into().1)
    }
    /// Inserts a key-value pair into the map and returns old value (if it was
    /// already in set) with [`ChangeEvent`]'s that describes this insert
    /// action.
    #[cfg(feature = "cdc")]
    pub fn insert_cdc(&self, key: K, value: V) -> (Option<V>, Vec<ChangeEvent<M>>) {
        let new_entry = M::new(key, value);

        let (old_value, cdc) = self.set.put_cdc(new_entry);

        (old_value.map(|pair| pair.into().1), cdc)
    }
    /// Removes some key from the map that matches the given key, returning the
    /// key and the value if the key was previously in the map.
    ///
    /// The key may be any borrowed form of the map's key type, but the ordering
    /// on the borrowed form *must* match the ordering on the key type.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let map = BTreeMultiMap::<usize, &str>::new();
    /// map.insert(1, "b");
    /// map.insert(1, "a");
    ///
    /// let first_removed = map.remove_some(&1).unwrap();
    /// let second_removed = map.remove_some(&1).unwrap();
    /// let removals = vec![first_removed, second_removed];
    ///
    /// assert!(removals.contains(&(1, "a")));
    /// assert!(removals.contains(&(1, "b")));
    /// ```
    pub fn remove_some<Q>(&self, key: &Q) -> Option<(K, V)>
    where
        M: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.set
            .remove(key)
            .map(Into::into)
    }
    /// Removes some key from the map that matches the given key, returning the
    /// key and the value if the key was previously in the map with
    /// [`ChangeEvent`]'s describing this `remove_some` action.
    #[cfg(feature = "cdc")]
    pub fn remove_some_cdc<Q>(&self, key: &Q) -> (Option<(K, V)>, Vec<ChangeEvent<M>>)
    where
        M: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let (old_value, cdc) = self.set.remove_cdc(key);

        (old_value.map(Into::into), cdc)
    }
    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut a = BTreeMultiMap::<usize, &str>::new();
    /// assert_eq!(a.len(), 0);
    /// a.insert(1, "a");
    /// assert_eq!(a.len(), 1);
    /// ```
    pub fn len(&self) -> usize {
        self.set.len()
    }
    /// Returns `true` if the multimap contains no elements.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut a = BTreeMultiMap::<usize, &str>::new();
    /// assert!(a.is_empty());
    /// a.insert(1, "a");
    /// assert!(!a.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.set.is_empty()
    }
    /// Returns the total number of allocated slots across all internal nodes.
    ///
    /// This represents the number of key-value pairs the multimap can hold
    /// without reallocating memory in its internal vectors.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut a = BTreeMultiMap::<usize, &str>::with_maximum_node_size(8);
    ///
    /// a.insert(1, "a");
    /// a.insert(1, "b");
    ///
    /// // Capacity remains unchanged until reallocation occurs
    /// assert_eq!(a.capacity(), 8);
    /// ```
    pub fn capacity(&self) -> usize {
        self.set.capacity()
    }
    /// Returns the total number of nodes.
    ///
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::map::BTreeMap;
    ///
    /// let mut a = BTreeMap::<usize, &str>::with_maximum_node_size(16);
    ///
    /// a.insert(1, "a");
    /// a.insert(2, "b");
    ///
    /// assert_eq!(a.node_count(), 1);
    /// ```
    pub fn node_count(&self) -> usize {
        self.set.node_count()
    }
    /// Gets an iterator over the entries of the map, sorted by key.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    /// map.insert(3, "c");
    /// map.insert(2, "b");
    /// map.insert(1, "a");
    ///
    /// for (key, value) in map.iter() {
    ///     println!("{key}: {value}");
    /// }
    ///
    /// let (first_key, first_value) = map.iter().next().unwrap();
    /// assert_eq!((*first_key, *first_value), (1, "a"));
    /// ```
    pub fn iter(&self) -> Iter<'_, K, V, Node, M> {
        Iter {
            inner: self.set.iter(),
            marker: PhantomData,
        }
    }
    /// Constructs a double-ended iterator over a sub-range of elements in the map.
    /// The simplest way is to use the range syntax `min..max`, thus `range(min..max)` will
    /// yield elements from min (inclusive) to max (exclusive).
    /// The range may also be entered as `(Bound<T>, Bound<T>)`, so for example
    /// `range((Excluded(4), Included(10)))` will yield a left-exclusive, right-inclusive
    /// range from 4 to 10.
    ///
    /// # Panics
    ///
    /// Panics if range `start > end`.
    /// Panics if range `start == end` and both bounds are `Excluded`.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    /// use std::ops::Bound::Included;
    ///
    /// let mut map = BTreeMultiMap::<usize, &str>::new();
    /// map.insert(3, "a");
    /// map.insert(5, "b");
    /// map.insert(8, "c");
    /// for (&key, &value) in map.range((Included(&4), Included(&8))) {
    ///     println!("{key}: {value}");
    /// }
    /// assert_eq!(Some((&5, &"b")), map.range(4..).next());
    /// ```
    pub fn range<R>(&self, range: R) -> Range<'_, K, V, Node, M>
    where
        M: Borrow<K>,
        R: RangeBounds<K>,
    {
        self._range(range)
    }
}

impl<K, V, Node, M> BTreeMultiMap<K, V, Node, M>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + 'static,
    M: MultiPairLike<K, V> + MultiPairRemoveHelper<K, V> + Debug + Clone + Send + 'static,
    Node: NodeLike<M> + Send + 'static,
{
    /// Removes a specific key-value pair from the map returning the key and the value if the key
    /// was previously in the map.
    ///
    /// # Examples
    ///
    /// Basic usage:
    ///
    /// ```
    /// use indexset::concurrent::multimap::BTreeMultiMap;
    ///
    /// let map = BTreeMultiMap::<usize, &str>::new();
    /// map.insert(1, "b");
    /// map.insert(1, "a");
    ///
    /// assert_eq!(map.remove(&1, &"a"), Some((1, "a")));
    /// assert_eq!(map.remove(&1, &"b"), Some((1, "b")));
    /// ```
    pub fn remove(&self, key: &K, value: &V) -> Option<(K, V)> {
        M::remove_from(&self.set, key, value)
    }

    /// Removes a specific key-value pair from the map returning the key and the
    /// value if the key was previously in the map with [`ChangeEvent`]'s
    /// describing this `remove_some` action.
    #[cfg(feature = "cdc")]
    pub fn remove_cdc(
        &self,
        key: &K,
        value: &V,
    ) -> (Option<(K, V)>, Vec<ChangeEvent<M>>) {
        M::remove_cdc_from(&self.set, key, value)
    }
}

#[cfg(test)]
mod tests {
    use super::BTreeMultiMap;
    use crate::core::multipair::{MultiPairLike, OrdMultiPair, RandomMultiPair};
    use crate::BTreeSet;
    use std::borrow::Borrow;
    use std::fmt::Debug;
    use std::ops::Bound::{Excluded, Unbounded};

    #[test]
    fn test_insert_works_as_expected() {
        let maximum_node_size = 3;
        let multi_map = BTreeMultiMap::<usize, &str>::with_maximum_node_size(maximum_node_size);

        multi_map.insert(1usize, "a");
        multi_map.insert(1usize, "b");
        multi_map.insert(2usize, "c");
        multi_map.insert(2usize, "d");
        multi_map.insert(3usize, "e");
        multi_map.insert(4usize, "f");
        multi_map.insert(4usize, "g");

        let expected_pairs = vec![
            (&1, &"b"),
            (&1, &"a"),
            (&2, &"d"),
            (&2, &"c"),
            (&3, &"e"),
            (&4, &"f"),
            (&4, &"g"),
        ]
        .into_iter()
        .collect::<BTreeSet<_>>();

        let all_pairs = multi_map.iter().collect::<BTreeSet<_>>();
        assert_eq!(all_pairs, expected_pairs);
    }

    #[test]
    fn test_insert_all_same_key_works_as_expected() {
        let maximum_node_size = 3;
        let map = BTreeMultiMap::<usize, &str>::with_maximum_node_size(maximum_node_size);

        map.insert(1usize, "a");
        map.insert(1usize, "b");
        map.insert(1usize, "c");
        map.insert(1usize, "d");
        map.insert(1usize, "e");
        map.insert(1usize, "f");

        let all_actual_pairs = map.iter().map(|(k, v)| (*k, *v)).collect::<BTreeSet<_>>();
        let all_expected_pairs = vec![(1, "f"), (1, "e"), (1, "d"), (1, "c"), (1, "b"), (1, "a")]
            .into_iter()
            .collect::<BTreeSet<_>>();
        assert_eq!(all_actual_pairs, all_expected_pairs);

        let all_ranged_pairs = map.range(1..2).map(|(k, v)| (*k, *v)).collect::<BTreeSet<_>>();
        assert_eq!(all_ranged_pairs, all_expected_pairs);
        assert!(map.range(1..1).next().is_none());
    }

    #[test]
    fn test_range_edge_cast() {
        let maximum_node_size = 3;
        let map = BTreeMultiMap::<usize, &str>::with_maximum_node_size(maximum_node_size);

        map.insert(1usize, "a");
        map.insert(1usize, "b");
        map.insert(2usize, "c");
        map.insert(2usize, "d");
        map.insert(3usize, "e");
        map.insert(4usize, "f");
        map.insert(4usize, "g");

        let mid_range = map.range(2..3).collect::<BTreeSet<_>>();
        assert_eq!(
            mid_range,
            vec![(&2, &"c"), (&2, &"d"),].into_iter().collect::<BTreeSet<_>>()
        );
    }

    fn assert_range_works_as_expected<M>()
    where
        M: MultiPairLike<usize, &'static str> + Borrow<usize> + Debug + Clone + Send + 'static,
    {
        let maximum_node_size = 3;
        let map = BTreeMultiMap::<usize, &'static str, Vec<M>, M>::with_maximum_node_size(
            maximum_node_size,
        );

        map.insert(1usize, "a");
        map.insert(1usize, "b");
        map.insert(2usize, "c");
        map.insert(2usize, "d");
        map.insert(3usize, "e");
        map.insert(4usize, "f");
        map.insert(4usize, "g");

        let truly_all_pairs = map.iter().collect::<BTreeSet<_>>();
        let all_pairs = map.range(..).collect::<BTreeSet<_>>();
        assert_eq!(all_pairs, truly_all_pairs);

        let mid_range = map.range(2..3).collect::<BTreeSet<_>>();
        assert_eq!(
            mid_range,
            vec![(&2, &"c"), (&2, &"d"),].into_iter().collect::<BTreeSet<_>>()
        );

        let reverse_range = map.range(1..4).rev().collect::<BTreeSet<_>>();
        assert_eq!(
            reverse_range,
            vec![(&3, &"e"), (&2, &"d"), (&2, &"c"), (&1, &"b"), (&1, &"a"),]
                .into_iter()
            .collect::<BTreeSet<_>>()
        );

        let empty_range = map.range(5..).collect::<BTreeSet<_>>();
        assert_eq!(empty_range, vec![].into_iter().collect::<BTreeSet<_>>());
    }

    #[test]
    fn test_range_works_as_expected() {
        assert_range_works_as_expected::<RandomMultiPair<usize, &'static str>>();
        assert_range_works_as_expected::<OrdMultiPair<usize, &'static str>>();
    }

    fn assert_range_excludes_values_at_bounds<M>()
    where
        M: MultiPairLike<usize, &'static str> + Borrow<usize> + Debug + Clone + Send + 'static,
    {
        let map = BTreeMultiMap::<usize, &'static str, Vec<M>, M>::with_maximum_node_size(10);

        map.insert(1usize, "a");
        map.insert(1usize, "b");
        map.insert(2usize, "c");
        map.insert(2usize, "d");
        map.insert(3usize, "e");
        map.insert(3usize, "f");

        assert_eq!(
            map.range((Excluded(&1), Unbounded)).collect::<BTreeSet<_>>(),
            vec![(&2, &"c"), (&2, &"d"), (&3, &"e"), (&3, &"f")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        );
        assert_eq!(
            map.range((Unbounded, Excluded(&3))).collect::<BTreeSet<_>>(),
            vec![(&1, &"a"), (&1, &"b"), (&2, &"c"), (&2, &"d")]
                .into_iter()
                .collect::<BTreeSet<_>>(),
        );
    }

    #[test]
    fn test_range_excludes_all_values_at_bounds() {
        assert_range_excludes_values_at_bounds::<RandomMultiPair<usize, &'static str>>();
        assert_range_excludes_values_at_bounds::<OrdMultiPair<usize, &'static str>>();
    }

    fn assert_get_works_as_expected<M>()
    where
        M: MultiPairLike<usize, &'static str> + Borrow<usize> + Debug + Clone + Send + 'static,
    {
        let maximum_node_size = 10;
        let map = BTreeMultiMap::<usize, &'static str, Vec<M>, M>::with_maximum_node_size(
            maximum_node_size,
        );

        map.insert(1usize, "a");
        map.insert(1usize, "b");
        map.insert(2usize, "c");
        map.insert(2usize, "d");
        map.insert(3usize, "e");
        map.insert(4usize, "f");
        map.insert(4usize, "g");

        let range = map.get(&1).collect::<BTreeSet<_>>();

        assert_eq!(
            range,
            vec![(&1, &"b"), (&1, &"a"),].into_iter().collect::<BTreeSet<_>>()
        );

        let range = map.get(&2).collect::<BTreeSet<_>>();
        assert_eq!(
            range,
            vec![(&2, &"d"), (&2, &"c"),].into_iter().collect::<BTreeSet<_>>()
        );

        let range = map.get(&3).collect::<BTreeSet<_>>();
        assert_eq!(range, vec![(&3, &"e"),].into_iter().collect::<BTreeSet<_>>());

        let range = map.get(&4).collect::<BTreeSet<_>>();
        assert_eq!(
            range,
            vec![(&4, &"g"), (&4, &"f"),].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn test_get_works_as_expected() {
        assert_get_works_as_expected::<RandomMultiPair<usize, &'static str>>();
        assert_get_works_as_expected::<OrdMultiPair<usize, &'static str>>();
    }

    #[test]
    fn test_get_works_as_expected_at_big_amounts() {
        let maximum_node_size = 100;
        let map = BTreeMultiMap::<String, usize>::with_maximum_node_size(maximum_node_size);

        for i in 1..2000 {
            map.insert(format!("ValueNum{}", i), i);
        }

        for i in 1..2000 {
            let range = map.get(&format!("ValueNum{}", i)).collect::<BTreeSet<_>>();
            assert_eq!(
                range,
                vec![(&format!("ValueNum{}", i), &i),]
                    .into_iter()
                    .collect::<BTreeSet<_>>()
            );
        }
    }
}
