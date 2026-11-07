use std::fmt::Debug;
use std::sync::Arc;

use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::cdc::change::ChangeEventUnassigned;
use crate::core::node::NodeLike;

type OldVersion<Node> = Arc<Mutex<Node>>;
type CurrentVersion<Node> = Arc<Mutex<Node>>;

pub enum Operation<T: Send + Ord, Node: NodeLike<T>> {
    Split(OldVersion<Node>, T, T),
    UpdateMax(CurrentVersion<Node>, T),
    MakeUnreachable(CurrentVersion<Node>, T),
}

impl<T, Node> Operation<T, Node>
where
    T: Debug + Ord + Send + Clone + 'static,
    Node: NodeLike<T> + Send + 'static,
{
    pub fn commit(
        self,
        index: &SkipMap<T, Arc<Mutex<Node>>>,
        node_capacity: usize,
    ) -> Result<(Option<T>, Vec<ChangeEventUnassigned<T>>), ()> {
        match self {
            Operation::Split(old_node, old_max, value) => {
                let mut guard = old_node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry.value(), &old_node) {
                        // The node may have changed since the split was decided, in which case the
                        // insert is retried from the start.
                        if guard.contains(&value) || !guard.need_to_split(node_capacity, &value) {
                            return Err(());
                        }

                        let mut cdc = vec![];
                        #[cfg(feature = "cdc")]
                        let max_value = guard.max().expect("node should be non empty if split").clone();
                        entry.remove();
                        let mut new_vec = guard.halve();

                        #[cfg(feature = "cdc")]
                        {
                            let node_split = ChangeEventUnassigned::SplitNode {
                                max_value,
                                split_index: guard.len(),
                            };
                            cdc.push(node_split);
                        }

                        let mut old_value: Option<T> = None;
                        let mut insert_attempted = false;
                        if let Some(max) = guard.max().cloned() {
                            if max >= value {
                                let (inserted, idx) = NodeLike::insert(&mut *guard, value.clone());
                                insert_attempted = true;
                                if !inserted {
                                    old_value = NodeLike::replace(&mut *guard, idx, value.clone());
                                    #[cfg(feature = "cdc")]
                                    {
                                        let value_insertion = ChangeEventUnassigned::RemoveAt {
                                            max_value: max.clone(),
                                            index: idx,
                                            value: old_value.clone().unwrap(),
                                        };
                                        cdc.push(value_insertion);
                                    }
                                }
                                #[cfg(feature = "cdc")]
                                {
                                    let value_insertion = ChangeEventUnassigned::InsertAt {
                                        max_value: max.clone(),
                                        index: idx,
                                        value: value.clone(),
                                    };
                                    cdc.push(value_insertion);
                                }
                            }

                            index.insert(max, old_node.clone());
                        }

                        if let Some(mut max) = new_vec.max().cloned() {
                            if !insert_attempted {
                                let (inserted, idx) = NodeLike::insert(&mut new_vec, value.clone());
                                let old_max = max.clone();
                                if inserted {
                                    if value > max {
                                        max = value.clone()
                                    }
                                } else {
                                    old_value = NodeLike::replace(&mut new_vec, idx, value.clone());
                                    #[cfg(feature = "cdc")]
                                    {
                                        let value_insertion = ChangeEventUnassigned::RemoveAt {
                                            max_value: old_max.clone(),
                                            index: idx,
                                            value: old_value.clone().unwrap(),
                                        };
                                        cdc.push(value_insertion);
                                    }
                                }
                                #[cfg(feature = "cdc")]
                                {
                                    let value_insertion = ChangeEventUnassigned::InsertAt {
                                        max_value: old_max.clone(),
                                        index: idx,
                                        value: value.clone(),
                                    };
                                    cdc.push(value_insertion);
                                }
                            }
                            let new_node = Arc::new(Mutex::new(new_vec));

                            index.insert(max, new_node);
                        }

                        return Ok((old_value, cdc));
                    }
                }

                Err(())
            }
            Operation::UpdateMax(node, old_max) => {
                let guard = node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry.value(), &node) {
                        // Concurrent removals may have emptied the node while this operation was
                        // pending, and they look it up under an outdated max, so it is removed here.
                        let Some(new_max) = guard.max() else {
                            let mut cdc = vec![];

                            #[cfg(feature = "cdc")]
                            {
                                let node_removal = ChangeEventUnassigned::RemoveNode {
                                    max_value: old_max.clone(),
                                };
                                cdc.push(node_removal);
                            }
                            index.remove(&old_max);

                            return Ok((None, cdc));
                        };
                        let cdc = vec![];
                        return Ok(match new_max.cmp(&old_max) {
                            std::cmp::Ordering::Equal => (None, cdc),
                            std::cmp::Ordering::Greater => {
                                index.remove(&old_max);
                                index.insert(new_max.clone(), node.clone());

                                (None, cdc)
                            }
                            std::cmp::Ordering::Less => {
                                index.remove(&old_max);
                                index.insert(new_max.clone(), node.clone());

                                (None, cdc)
                            }
                        });
                    }
                }

                Err(())
            }
            Operation::MakeUnreachable(node, old_max) => {
                let guard = node.lock_arc();
                if let Some(entry) = index.get(&old_max) {
                    if Arc::ptr_eq(entry.value(), &node) {
                        // Other threads may have inserted into the node after it was emptied, so it
                        // is kept and only moved under its new max.
                        if let Some(new_max) = guard.max() {
                            if *new_max != old_max {
                                index.remove(&old_max);
                                index.insert(new_max.clone(), node.clone());
                            }

                            return Ok((None, vec![]));
                        }

                        let mut cdc = vec![];

                        #[cfg(feature = "cdc")]
                        {
                            let node_removal = ChangeEventUnassigned::RemoveNode {
                                max_value: old_max.clone(),
                            };
                            cdc.push(node_removal);
                        }
                        index.remove(&old_max);

                        return Ok((None, cdc));
                    }
                }

                Err(())
            }
        }
    }
}
