pub(crate) struct Segment {
    buffer: Vec<u8>,
    available_capacity: usize,
}

impl Segment {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0);
        Segment {
            buffer: Vec::with_capacity(capacity),
            available_capacity: capacity,
        }
    }

    pub(crate) fn try_append(&mut self, slice: &[u8]) -> (usize, bool) {
        if self.available_capacity >= slice.len() {
            let index = self.buffer.len();
            self.buffer.extend_from_slice(slice);
            self.available_capacity -= slice.len();
            return (index, true);
        }
        return (0, false);
    }

    pub(crate) fn get(&self, index: usize, size: usize) -> &[u8] {
        assert!(size > 0);
        assert!(self.buffer.len() >= (index + size - 1));

        return &self.buffer[index..index + size];
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.available_capacity == self.buffer.capacity()
    }

    pub(crate) fn is_full(&self) -> bool {
        self.available_capacity <= 0
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::segment::Segment;

    #[test]
    fn is_empty() {
        let segment = Segment::new(16);
        assert_eq!(true, segment.is_empty());
    }

    #[test]
    fn is_not_empty() {
        let mut segment = Segment::new(32);
        let data = b"thread-per-core-1";

        assert_eq!(true, segment.try_append(data).1);
        assert_eq!(false, segment.is_empty());
    }

    #[test]
    fn is_full() {
        let mut segment = Segment::new(16);
        let data = b"thread-per-core1";

        assert_eq!(true, segment.try_append(data).1);
        assert_eq!(true, segment.is_full());
    }

    #[test]
    fn is_not_full() {
        let mut segment = Segment::new(32);
        let data = b"thread-per-core-1";

        assert_eq!(true, segment.try_append(data).1);
        assert_eq!(false, segment.is_full());
    }

    #[test]
    fn should_append_to_segment() {
        let mut segment = Segment::new(16);
        let data = b"thread-per-core";

        assert_eq!(true, segment.try_append(data).1);
    }

    #[test]
    fn should_append_to_segment_multiple_times() {
        let mut segment = Segment::new(32);
        let data = b"thread-per-core-1";

        let (append_index, ok) = segment.try_append(data);
        assert_eq!(0, append_index);
        assert_eq!(true, ok);

        let data = b"thread-per-core";
        let (append_index, ok) = segment.try_append(data);
        assert_eq!(17, append_index);
        assert_eq!(true, ok);
    }

    #[test]
    fn should_not_append_to_segment_given_segment_does_not_have_enough_capacity() {
        let mut segment = Segment::new(16);
        let data = b"thread-per-core";

        assert_eq!(true, segment.try_append(data).1);
        assert_eq!(false, segment.try_append(data).1);
    }

    #[test]
    fn should_get_from_segment() {
        let mut segment = Segment::new(16);
        let data = b"thread-per-core";

        assert_eq!(true, segment.try_append(data).1);

        let retrieved = segment.get(0, data.len());
        assert_eq!(data, retrieved);
    }

    #[test]
    fn should_get_from_segment_given_size_of_data_to_be_retrieved_is_equal_to_the_stored_data() {
        let mut segment = Segment::new(16);
        let data = b"memcore";

        assert_eq!(true, segment.try_append(data).1);

        let retrieved = segment.get(0, 7);
        assert_eq!(data, retrieved);
    }

    #[test]
    fn should_get_from_segment_given_size_of_data_to_be_retrieved_is_less_than_the_stored_data() {
        let mut segment = Segment::new(16);
        let data = b"memcore";

        assert_eq!(true, segment.try_append(data).1);

        let retrieved = segment.get(0, 3);
        assert_eq!(b"mem", retrieved);
    }

    #[test]
    #[should_panic]
    fn should_panic_given_insufficient_buffer() {
        let mut segment = Segment::new(16);
        let data = b"memcore";

        assert_eq!(true, segment.try_append(data).1);

        let _ = segment.get(0, 9);
    }
}