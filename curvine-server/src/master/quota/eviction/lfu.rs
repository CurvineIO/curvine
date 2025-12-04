use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::sync::Arc;

pub struct LFUCache<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    items: HashMap<Arc<K>, usize>,
    frequency_map: HashMap<usize, VecDeque<Arc<K>>>,
    min_frequency: usize,
    capacity: Option<usize>,
}

impl<K> LFUCache<K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            items: HashMap::new(),
            frequency_map: HashMap::new(),
            min_frequency: 0,
            capacity: Some(capacity),
        }
    }

    pub fn unbounded() -> Self {
        Self {
            items: HashMap::new(),
            frequency_map: HashMap::new(),
            min_frequency: 0,
            capacity: None,
        }
    }

    pub fn put(&mut self, key: K) {
        let key_arc = Arc::new(key);

        if self.items.contains_key(&key_arc) {
            self.increment_frequency(&key_arc);
        } else {
            if let Some(cap) = self.capacity {
                if self.items.len() >= cap {
                    self.evict();
                }
            }

            self.items.insert(key_arc.clone(), 1);

            self.frequency_map.entry(1).or_default().push_back(key_arc);

            self.min_frequency = 1;
        }
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let key_arc = Arc::new(key.clone());
        self.items.contains_key(&key_arc)
    }

    pub fn remove(&mut self, key: K) -> bool {
        let key_arc = Arc::new(key);

        if let Some(frequency) = self.items.remove(&key_arc) {
            if let Some(keys) = self.frequency_map.get_mut(&frequency) {
                if let Some(pos) = keys.iter().position(|k| k.as_ref() == key_arc.as_ref()) {
                    keys.remove(pos);
                }

                if keys.is_empty() {
                    self.frequency_map.remove(&frequency);
                }

                if frequency == self.min_frequency {
                    self.update_min_frequency();
                }
            }

            true
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn capacity(&self) -> Option<usize> {
        self.capacity
    }

    pub fn iter(&self) -> LFUIterator<K> {
        LFUIterator::new(self)
    }

    fn increment_frequency(&mut self, key: &Arc<K>) {
        if let Some(old_freq) = self.items.get_mut(key) {
            let new_freq = *old_freq + 1;

            if let Some(keys) = self.frequency_map.get_mut(old_freq) {
                if let Some(pos) = keys.iter().position(|k| k.as_ref() == key.as_ref()) {
                    keys.remove(pos);
                }

                if keys.is_empty() {
                    self.frequency_map.remove(old_freq);

                    if *old_freq == self.min_frequency {
                        self.min_frequency = new_freq;
                    }
                }
            }

            *old_freq = new_freq;
            self.frequency_map
                .entry(new_freq)
                .or_default()
                .push_back(key.clone());
        }
    }

    fn update_min_frequency(&mut self) {
        self.min_frequency = self.frequency_map.keys().min().copied().unwrap_or(0);
    }

    fn evict(&mut self) {
        if let Some(keys) = self.frequency_map.get_mut(&self.min_frequency) {
            if let Some(key_to_evict) = keys.pop_front() {
                self.items.remove(&key_to_evict);

                if keys.is_empty() {
                    self.frequency_map.remove(&self.min_frequency);
                    self.update_min_frequency();
                }
            }
        }
    }
}

pub struct LFUIterator<'a, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    cache: &'a LFUCache<K>,
    sorted_frequencies: Vec<usize>,
    current_freq_index: usize,
    current_index: usize,
}

impl<'a, K> LFUIterator<'a, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn new(cache: &'a LFUCache<K>) -> Self {
        let mut sorted_frequencies: Vec<usize> = cache.frequency_map.keys().copied().collect();
        sorted_frequencies.sort_unstable();

        Self {
            cache,
            sorted_frequencies,
            current_freq_index: 0,
            current_index: 0,
        }
    }
}

impl<'a, K> Iterator for LFUIterator<'a, K>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Item = &'a K;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_freq_index < self.sorted_frequencies.len() {
            let current_freq = self.sorted_frequencies[self.current_freq_index];

            if let Some(keys) = self.cache.frequency_map.get(&current_freq) {
                while self.current_index < keys.len() {
                    let key = &keys[self.current_index];
                    self.current_index += 1;

                    if self.cache.items.contains_key(key) {
                        return Some(key.as_ref());
                    }
                }
            }

            self.current_freq_index += 1;
            self.current_index = 0;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cache() {
        let cache: LFUCache<i32> = LFUCache::new(3);
        assert_eq!(cache.capacity(), Some(3));
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_unbounded_cache() {
        let cache: LFUCache<i32> = LFUCache::unbounded();
        assert_eq!(cache.capacity(), None);
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_put_and_contains_key() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        assert!(cache.contains_key(&1));
        assert_eq!(cache.len(), 1);

        cache.put(2);
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert_eq!(cache.len(), 2);

        cache.put(3);
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&3));
        assert_eq!(cache.len(), 3);
    }

    #[test]
    fn test_duplicate_put_increases_frequency() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        cache.put(1);
        cache.put(1);

        assert!(cache.contains_key(&1));
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_eviction_when_full() {
        let mut cache = LFUCache::new(2);

        cache.put(1);
        cache.put(2);
        assert_eq!(cache.len(), 2);

        cache.put(1);

        cache.put(3);
        assert_eq!(cache.len(), 2);
        assert!(cache.contains_key(&1));
        assert!(!cache.contains_key(&2));
        assert!(cache.contains_key(&3));
    }

    #[test]
    fn test_lru_within_same_frequency() {
        let mut cache = LFUCache::new(2);

        cache.put(1);
        cache.put(2);

        cache.put(3);
        assert!(!cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&3));
    }

    #[test]
    fn test_remove() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        cache.put(2);
        cache.put(3);

        assert!(cache.remove(1));
        assert!(!cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&3));
        assert_eq!(cache.len(), 2);

        assert!(!cache.remove(1));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_complex_eviction_scenario() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        cache.put(2);
        cache.put(3);

        cache.put(1);
        cache.put(2);

        cache.put(4);
        assert!(!cache.contains_key(&3));
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&4));
    }

    #[test]
    fn test_iterator() {
        let mut cache = LFUCache::new(5);

        cache.put(3);
        cache.put(1);
        cache.put(1);
        cache.put(2);
        cache.put(2);
        cache.put(2);

        let items: Vec<&i32> = cache.iter().collect();
        assert_eq!(items.len(), 3);
        assert!(items.contains(&&1));
        assert!(items.contains(&&2));
        assert!(items.contains(&&3));
    }

    #[test]
    fn test_iterator_empty_cache() {
        let cache: LFUCache<i32> = LFUCache::new(3);
        let items: Vec<&i32> = cache.iter().collect();
        assert_eq!(items.len(), 0);
    }

    #[test]
    fn test_unbounded_cache_no_eviction() {
        let mut cache = LFUCache::<i32>::unbounded();

        for i in 0..1000 {
            cache.put(i);
        }

        assert_eq!(cache.len(), 1000);
        for i in 0..1000 {
            assert!(cache.contains_key(&i));
        }
    }

    #[test]
    fn test_string_keys() {
        let mut cache = LFUCache::new(3);

        cache.put("hello".to_string());
        cache.put("world".to_string());
        cache.put("hello".to_string());

        assert!(cache.contains_key(&"hello".to_string()));
        assert!(cache.contains_key(&"world".to_string()));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_remove_all_items() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        cache.put(2);
        cache.put(3);

        cache.remove(1);
        cache.remove(2);
        cache.remove(3);

        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_frequent_access_pattern() {
        let mut cache = LFUCache::new(3);

        cache.put(1);
        cache.put(2);
        cache.put(3);

        for _ in 0..10 {
            cache.put(1);
        }

        for _ in 0..3 {
            cache.put(2);
        }

        cache.put(4);
        assert!(!cache.contains_key(&3));
        assert!(cache.contains_key(&1));
        assert!(cache.contains_key(&2));
        assert!(cache.contains_key(&4));
    }

    #[test]
    fn test_edge_case_capacity_one() {
        let mut cache = LFUCache::new(1);

        cache.put(1);
        assert!(cache.contains_key(&1));

        cache.put(2);
        assert!(!cache.contains_key(&1));
        assert!(cache.contains_key(&2));

        cache.put(2);
        cache.put(3);
        assert!(!cache.contains_key(&2));
        assert!(cache.contains_key(&3));
    }
}
