use std::{cmp::max, collections::HashSet, hash::Hash};

pub trait StateCRDT<V, O>: Clone + Send + Sync {
    fn value(&self) -> V;
    fn merge(&mut self, other: &Self);
    fn update(&mut self, operation: O);
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Max(u64);

impl Max {
    pub fn new() -> Self {
        Self(0)
    }
}

impl StateCRDT<u64, u64> for Max {
    fn value(&self) -> u64 {
        self.0
    }

    fn merge(&mut self, other: &Max) {
        self.0 = max(self.0, other.0);
    }

    fn update(&mut self, operation: u64) {
        self.0 = max(self.0, operation);
    }
}

#[derive(Clone, Debug)]
pub struct GCounter {
    index: usize,
    values: Vec<usize>,
}

impl GCounter {
    pub fn new(index: usize, size: usize) -> Self {
        Self {
            index,
            values: vec![0; size],
        }
    }
}

impl StateCRDT<usize, usize> for GCounter {
    fn value(&self) -> usize {
        self.values.iter().sum()
    }

    fn merge(&mut self, other: &Self) {
        let new_values: Vec<usize> = self
            .values
            .iter()
            .zip(&other.values)
            .map(|(first, second)| max(*first, *second))
            .collect();
        self.values = new_values;
    }

    fn update(&mut self, amount: usize) {
        self.values[self.index] += amount;
    }
}

#[derive(Clone, Debug)]
pub struct PNCounter {
    positive: GCounter,
    negative: GCounter,
}

impl PNCounter {
    pub fn new(index: usize, size: usize) -> Self {
        Self {
            positive: GCounter::new(index, size),
            negative: GCounter::new(index, size),
        }
    }
}

impl StateCRDT<isize, isize> for PNCounter {
    fn value(&self) -> isize {
        let positive = self.positive.value();
        let negative = self.negative.value();
        if positive >= negative {
            (positive - negative) as isize
        } else {
            -((negative - positive) as isize)
        }
    }

    fn merge(&mut self, other: &Self) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
    }

    fn update(&mut self, operation: isize) {
        if operation > 0 {
            self.positive.update(operation as usize);
        } else if operation < 0 {
            self.negative.update(-operation as usize);
        }
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct GSet<T>(HashSet<T>)
where
    T: PartialEq + Eq + Hash;

impl<T> GSet<T>
where
    T: PartialEq + Eq + Hash,
{
    pub fn new() -> Self {
        Self(HashSet::new())
    }
}

impl<T> StateCRDT<HashSet<T>, T> for GSet<T>
where
    T: Clone + Send + Sync + Eq + PartialEq + Hash,
{
    fn value(&self) -> HashSet<T> {
        self.0.clone()
    }

    fn merge(&mut self, other: &GSet<T>) {
        let new = self.0.union(&other.0).map(|it| it.clone()).collect();
        self.0 = new;
    }

    fn update(&mut self, operation: T) {
        self.0.insert(operation);
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TwoPSet<T>
where
    T: PartialEq + Eq + Hash,
{
    added: GSet<T>,
    removed: GSet<T>,
}

impl<T> TwoPSet<T>
where
    T: PartialEq + Eq + Hash,
{
    pub fn new() -> Self {
        Self {
            added: GSet::new(),
            removed: GSet::new(),
        }
    }
}

pub enum TwoPSetOperation<T> {
    Add(T),
    Remove(T),
}

impl<T> StateCRDT<HashSet<T>, TwoPSetOperation<T>> for TwoPSet<T>
where
    T: Eq + PartialEq + Hash + Clone + Send + Sync,
{
    fn value(&self) -> HashSet<T> {
        self.added
            .0
            .difference(&self.removed.0)
            .map(|it| it.clone())
            .collect()
    }

    fn merge(&mut self, other: &Self) {
        self.added.merge(&other.added);
        self.removed.merge(&other.removed);
    }

    fn update(&mut self, operation: TwoPSetOperation<T>) {
        match operation {
            TwoPSetOperation::Add(value) => self.added.0.insert(value),
            TwoPSetOperation::Remove(value) => self.removed.0.insert(value),
        };
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn max_merge() {
        let mut max = Max(5);
        max.merge(&Max(8));
        assert_eq!(Max(8), max);
    }

    #[test]
    fn max_update() {
        let mut max = Max(4);
        max.update(6);
        assert_eq!(max, Max(6));
    }

    #[test]
    fn max_commutativity() {
        let mut max1 = Max(5);
        max1.merge(&Max(12));
        let mut max2 = Max(12);
        max2.merge(&Max(5));
        assert_eq!(max1, max2);
    }

    #[test]
    fn max_idempotence() {
        let max1 = Max(5);
        let mut result = max1.clone();
        result.merge(&max1);
        assert_eq!(max1, result);
    }

    #[test]
    fn max_associativity() {
        let max1 = Max(5);
        let max2 = Max(12);
        let max3 = Max(3);

        let mut result1 = max1.clone();
        result1.merge(&max2);
        result1.merge(&max3);

        let mut result2 = max2.clone();
        result2.merge(&max3);
        result2.merge(&max1);

        assert_eq!(result1, result2);
    }

    #[test]
    fn max_increasing() {
        let mut max1 = Max(5);
        let mut max2 = max1.clone();
        max2.update(4);
        max1.merge(&max2);

        assert_eq!(max1, max2);
    }

    #[test]
    fn increment_gcounter() {
        let mut gcounter = GCounter {
            index: 1,
            values: vec![3, 6, 2],
        };
        gcounter.update(1);
        assert_eq!(gcounter.values, vec![3, 7, 2]);
    }

    #[test]
    fn merge_gcounter() {
        let mut gcounter = GCounter {
            index: 1,
            values: vec![0, 1, 3],
        };
        gcounter.merge(&GCounter {
            index: 0,
            values: vec![2, 1, 2],
        });
        assert_eq!(gcounter.values, vec![2, 1, 3]);
    }

    #[test]
    fn update_pncounter() {
        let mut counter = PNCounter::new(0, 2);
        counter.update(20);
        counter.update(-30);
        assert_eq!(counter.value(), -10);
    }

    #[test]
    fn merge_pncounter() {
        let mut counter1 = PNCounter {
            positive: GCounter {
                index: 0,
                values: vec![1, 0, 0],
            },
            negative: GCounter {
                index: 0,
                values: vec![1, 0, 0],
            },
        };
        let counter2 = PNCounter {
            positive: GCounter {
                index: 1,
                values: vec![0, 1, 0],
            },
            negative: GCounter {
                index: 1,
                values: vec![0, 1, 1],
            },
        };

        counter1.merge(&counter2);

        assert_eq!(counter1.positive.values, vec![1, 1, 0]);
        assert_eq!(counter1.negative.values, vec![1, 1, 1]);
    }

    #[test]
    fn update_gset() {
        let mut gset = GSet::new();
        gset.update("test".to_string());

        assert_eq!(gset, GSet(HashSet::from(["test".to_string()])))
    }

    #[test]
    fn merge_gset() {
        let mut gset1 = GSet(HashSet::from([1, 2, 3]));
        let gset2 = GSet(HashSet::from([3, 4, 5]));
        gset1.merge(&gset2);
        assert_eq!(gset1, GSet(HashSet::from([1, 2, 3, 4, 5])));
    }

    #[test]
    fn add_twopset() {
        let mut set = TwoPSet::new();
        set.update(TwoPSetOperation::Add(1));
        assert_eq!(HashSet::from([1]), set.value());
    }
    #[test]
    fn remove_twopset() {
        let mut set = TwoPSet::new();
        set.update(TwoPSetOperation::Add(1));
        set.update(TwoPSetOperation::Add(2));
        set.update(TwoPSetOperation::Remove(1));
        assert_eq!(HashSet::from([2]), set.value());
    }
}
