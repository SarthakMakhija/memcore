use std::io::Error;

use bytes::BytesMut;

use crate::memory::key_value::KeyValue;
use crate::memory::index::{Index, IndexMarker};
use crate::memory::options::LogOptions;
use crate::memory::segment::Segment;

pub(crate) struct Log {
    segments: Vec<Segment>,
    index: Index,
    segment_tail: usize,
}

impl Log {
    pub(crate) fn new(options: LogOptions) -> Self {
        Log {
            segments: (1..=options.number_of_segments()).map(|_| Segment::new(options.segment_size())).collect(),
            segment_tail: 0,
            index: Index::new(),
        }
    }

    pub(crate) fn try_append(&mut self, key_value: KeyValue) -> bool {
        let encoded = key_value.encode();
        let appended = self.try_append_to_segment(&encoded);
        if let Some(segment_position) = appended {
            self.index.insert(
                key_value.key(),
                IndexMarker::new(self.segment_tail, segment_position, encoded.iter().len()),
            );
            return true;
        }
        return false;
    }

    pub(crate) fn try_get(&self, key: &[u8]) -> Option<Result<KeyValue, Error>> {
        self.index
            .get(key)
            .map(|index_marker| self.
                segments[index_marker.segment_index].
                get(index_marker.segment_position, index_marker.key_value_size))
            .map(|raw_value| BytesMut::from(raw_value))
            .map(|bytes| KeyValue::decode_from(bytes))
    }

    fn try_append_to_segment(&mut self, encoded: &BytesMut) -> Option<usize> {
        let appended = self.segments[self.segment_tail].try_append(&encoded);
        if let Some(segment_position) = appended {
            return Some(segment_position);
        }
        if self.segment_tail == self.segments.len() - 1 {
            return None;
        }
        self.segment_tail += 1;
        return self.segments[self.segment_tail].try_append(&encoded);
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::key_value::KeyValue;
    use crate::memory::log::Log;
    use crate::memory::options::LogOptions;

    #[test]
    fn should_append_to_the_log() {
        let log_size_bytes = 64;
        let segment_size_bytes = 64;

        let mut log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        assert_eq!(true, log.try_append(KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"))));
    }

    #[test]
    fn should_not_append_to_the_log() {
        let log_size_bytes = 32;
        let segment_size_bytes = 32;

        let mut log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        assert_eq!(true, log.try_append(KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"))));
        assert_eq!(false, log.try_append(KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"))));
    }

    #[test]
    fn get_from_log() {
        let log_size_bytes = 32;
        let segment_size_bytes = 32;

        let mut log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        assert_eq!(true, log.try_append(KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"))));

        let key_value = log.try_get(b"raft").unwrap().unwrap();
        assert_eq!(b"consensus", key_value.value());
    }
}