pub(crate) struct LogOptions {
    log_size_bytes: usize,
    segment_size_bytes: usize,
}

impl LogOptions {
    pub(crate) fn new(log_size_bytes: usize, segment_size_bytes: usize) -> Self {
        assert!(log_size_bytes >= segment_size_bytes);
        LogOptions {
            log_size_bytes,
            segment_size_bytes,
        }
    }

    pub(crate) fn number_of_segments(&self) -> usize {
        if self.log_size_bytes % self.segment_size_bytes != 0 {
            return (self.log_size_bytes / self.segment_size_bytes) + 1;
        }
        return self.log_size_bytes / self.segment_size_bytes;
    }

    pub(crate) fn segment_size(&self) -> usize {
        self.segment_size_bytes
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::options::LogOptions;

    #[test]
    fn number_of_segments_1() {
        let log_options = LogOptions::new(100, 10);
        assert_eq!(10, log_options.number_of_segments());
    }

    #[test]
    fn number_of_segments_2() {
        let log_options = LogOptions::new(50, 3);
        assert_eq!(17, log_options.number_of_segments());
    }
}