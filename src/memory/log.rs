use crate::key_value::KeyValue;
use crate::memory::options::LogOptions;
use crate::memory::segment::Segment;

pub(crate) struct Log {
    segments: Vec<Segment>,
    segment_tail: usize,
}

impl Log {
    pub(crate) fn new(options: LogOptions) -> Self {
        Log {
            segments: (1..=options.number_of_segments()).map(|_| Segment::new(options.segment_size())).collect(),
            segment_tail: 0,
        }
    }

    pub(crate) fn try_append(&mut self, key_value: KeyValue) -> bool {
        let encoded = key_value.encode();
        if self.segments[self.segment_tail].try_append(&encoded) {
            return true;
        }
        if self.segment_tail == self.segments.len() - 1 {
            return false;
        }
        self.segment_tail += 1;
        return self.segments[self.segment_tail].try_append(&encoded)
    }
}

#[cfg(test)]
mod tests {
    use crate::key_value::KeyValue;
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
}