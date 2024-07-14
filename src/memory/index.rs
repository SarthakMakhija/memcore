use std::collections::HashMap;

pub(crate) struct Index {
    marker_by_key: HashMap<Vec<u8>, IndexMarker>,
}

pub(crate) struct IndexMarker {
    pub(crate) segment_index: usize,
    pub(crate) segment_position: usize,
    pub(crate) key_value_size: usize,
}

impl IndexMarker {
    pub(crate) fn new(segment_index: usize, segment_position: usize, key_value_size: usize) -> Self {
        IndexMarker {
            segment_index,
            segment_position,
            key_value_size,
        }
    }
}

impl Index {
    pub(crate) fn new() -> Self {
        Index {
            marker_by_key: HashMap::new()
        }
    }

    pub(crate) fn insert(&mut self, key: Vec<u8>, value: IndexMarker) {
        self.marker_by_key.insert(key, value);
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&IndexMarker> {
        self.marker_by_key.get(key)
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::index::{Index, IndexMarker};

    #[test]
    fn should_not_find_the_key_in_index() {
        let index = Index::new();
        let optional_marker = index.get(b"non-existing");

        assert_eq!(true, optional_marker.is_none());
    }

    #[test]
    fn should_find_the_key_in_index() {
        let mut index = Index::new();
        index.insert(Vec::from(b"raft"), IndexMarker::new(0, 16, 100));

        let optional_marker = index.get(b"raft");
        assert_eq!(true, optional_marker.is_some());
        assert_eq!(0, optional_marker.unwrap().segment_index);
        assert_eq!(16, optional_marker.unwrap().segment_position);
        assert_eq!(100, optional_marker.unwrap().key_value_size);
    }
}