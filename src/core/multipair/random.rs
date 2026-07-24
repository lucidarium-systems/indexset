use std::borrow::Borrow;
use std::fmt::Debug;
use std::ops::Bound;

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
pub struct RandomMultiPair<K, V> {
    pub key: K,
    pub value: V,
    pub discriminator: u64,
}

impl<K, V> RandomMultiPair<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self { key, value, discriminator: fastrand::u64(..) }
    }

}

impl<K: Ord, V: PartialEq> Eq for RandomMultiPair<K, V> {}

impl<K: Ord, V: PartialEq> PartialEq<Self> for RandomMultiPair<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.key.eq(&other.key) && self.value.eq(&other.value)
    }
}

impl<K: Ord, V: PartialEq> Ord for RandomMultiPair<K, V> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal if self.value.eq(&other.value) => Ordering::Equal,
            Ordering::Equal => self.discriminator.cmp(&other.discriminator),
            ord => ord,
        }
    }
}

impl<K: Ord, V: PartialEq> PartialOrd for RandomMultiPair<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K, V> Borrow<K> for RandomMultiPair<K, V> {
    fn borrow(&self) -> &K {
        &self.key
    }
}

impl<K: Ord, V: PartialEq> MultiPairLike<K, V> for RandomMultiPair<K, V> {
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

impl<K, V> From<Pair<K, V>> for RandomMultiPair<K, V> {
    fn from(pair: Pair<K, V>) -> Self {
        RandomMultiPair {
            key: pair.key,
            value: pair.value,
            discriminator: fastrand::u64(..),
        }
    }
}

impl<K, V> From<RandomMultiPair<K, V>> for Pair<K, V> {
    fn from(pair: RandomMultiPair<K, V>) -> Self {
        Pair {
            key: pair.key,
            value: pair.value,
        }
    }
}

impl<K, V> From<RandomMultiPair<K, V>> for (K, V) {
    fn from(pair: RandomMultiPair<K, V>) -> Self {
        (pair.key, pair.value)
    }
}

impl<K, V> MultiPairRemoveHelper<K, V> for RandomMultiPair<K, V>
where
    K: Debug + Send + Ord + Clone + 'static,
    V: Debug + Send + Clone + PartialEq + 'static,
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
        let pair_to_remove = set
            .range::<K, _>((Bound::Included(key), Bound::Included(key)))
            .find(|pair| pair.key == *key && pair.value == *value)
            .cloned();

        if let Some(pair_to_remove) = pair_to_remove {
            let (res, evs) = set.remove_cdc(&pair_to_remove);
            return (res.map(Into::into), evs);
        }

        (None, vec![])
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::node::NodeLike;
    use std::ops::Bound::*;

    #[test]
    fn borrow_test() {
        let pair = RandomMultiPair::new(1usize, 2usize);
        assert_eq!(<RandomMultiPair<usize, usize> as Borrow<usize>>::borrow(&pair), &1usize);
    }

    #[test]
    fn eq_test() {
        let pair_one = RandomMultiPair::new(1usize, 2usize);
        let pair_two = RandomMultiPair::new(1usize, 3usize);
        assert_ne!(pair_one, pair_two);
    }

    #[test]
    fn node_like() {
        let mut vec = Vec::new();
        let p1 = RandomMultiPair::new(1, "a");
        let p2 = RandomMultiPair::new(1, "b");
        let p3 = RandomMultiPair::new(2, "c");
     
        NodeLike::insert(&mut vec, p1.clone());
        NodeLike::insert(&mut vec, p2.clone());
        assert_eq!(vec.len(), 2); 

        NodeLike::insert(&mut vec, p1.clone());
        assert_eq!(vec.len(), 2);
     
        NodeLike::insert(&mut vec, p3.clone());
        assert_eq!(vec.len(), 3);
     }

     #[test]
     fn range_bounds() {
        let mut vec = Vec::new();
    
        let p1a = RandomMultiPair::new(1, "a");
        let p1b = RandomMultiPair::new(1, "b");
        let p1c = RandomMultiPair::new(1, "c");
        let p2a = RandomMultiPair::new(2, "a");
        let p2b = RandomMultiPair::new(2, "b");
        let p3a = RandomMultiPair::new(3, "a");
        let p3b = RandomMultiPair::new(3, "b");
        let p3c = RandomMultiPair::new(3, "c");
        let p3d = RandomMultiPair::new(3, "d");
        let p4a = RandomMultiPair::new(4, "a");

        NodeLike::insert(&mut vec, p4a.clone());
        NodeLike::insert(&mut vec, p1a.clone());
        NodeLike::insert(&mut vec, p1c.clone());
        NodeLike::insert(&mut vec, p1b.clone());
        NodeLike::insert(&mut vec, p2b.clone());
        NodeLike::insert(&mut vec, p2a.clone());
        NodeLike::insert(&mut vec, p3a.clone());
        NodeLike::insert(&mut vec, p3b.clone());
        NodeLike::insert(&mut vec, p3d.clone());
        NodeLike::insert(&mut vec, p3c.clone());
        assert_eq!(vec.len(), 10);

        let start_1 = vec.rank(Included(&1), true).map_or(0, |rank| rank + 1);
        let end_1 = vec.rank(Excluded(&1), true).unwrap();
        let range_1 = &vec[start_1..=end_1];
        assert_eq!(range_1.len(), 3);
        assert!(range_1.contains(&p1a));
        assert!(range_1.contains(&p1b));
        assert!(range_1.contains(&p1c));

        let end_2 = vec.rank(Excluded(&2), true).unwrap();
        let range_2 = &vec[start_1..=end_2];
        assert_eq!(range_2.len(), 5);
        assert!(range_2.contains(&p1a));
        assert!(range_2.contains(&p1b));
        assert!(range_2.contains(&p1c));
        assert!(range_2.contains(&p2a));
        assert!(range_2.contains(&p2b));
        assert_ne!(range_2.contains(&p3a), true);

        let start_3 = vec.rank(Included(&3), true).unwrap() + 1;
        let end_3 = vec.rank(Excluded(&3), true).unwrap();
        let range_3 = &vec[start_3..=end_3];
        assert_eq!(range_3.len(), 4);
        assert!(range_3.contains(&p3a));
        assert!(range_3.contains(&p3b));
        assert!(range_3.contains(&p3c));
        assert!(range_3.contains(&p3d));

        let start_4 = vec.rank(Included(&4), true).unwrap() + 1;
        let end_4 = vec.rank(Excluded(&4), true).unwrap();
        let range_4 = &vec[start_4..=end_4];
        assert_eq!(range_4.len(), 1);
        assert!(range_4.contains(&p4a));
     }
}
