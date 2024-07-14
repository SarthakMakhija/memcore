use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

pub(crate) struct SPSCQueue<T> {
    head: CachePadded<AtomicUsize>,
    tail: CachePadded<AtomicUsize>,
    elements: Vec<T>,
}

impl<T> SPSCQueue<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        SPSCQueue {
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
            elements: Vec::with_capacity(capacity),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
    }

    pub(crate) fn try_enqueue(&mut self, element: T) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let mut next_tail = tail + 1;
        if next_tail == self.elements.capacity() + 1 {
            next_tail = 0;
        }
        if next_tail == self.head.load(Ordering::Acquire) {
            return false;
        }
        self.elements.insert(tail, element);
        self.tail.store(next_tail, Ordering::Release);
        return true;
    }

    pub(crate) fn try_get_front(&self) -> Option<&T> {
        let head = self.head.load(Ordering::Relaxed);
        if self.tail.load(Ordering::Acquire) == head {
            return None;
        }
        return Some(&self.elements[head]);
    }

    pub(crate) fn pop(&self) {
        let head = self.head.load(Ordering::Relaxed);
        let mut next_head = head + 1;
        if next_head == self.elements.capacity() {
            next_head = 0;
        }
        self.head.store(next_head, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use crate::queue::spsc::SPSCQueue;

    #[test]
    fn is_empty_queue() {
        let queue: SPSCQueue<usize> = SPSCQueue::new(2);
        assert_eq!(true, queue.is_empty());
    }

    #[test]
    fn try_enqueue_and_get_front() {
        let mut queue = SPSCQueue::new(2);
        assert_eq!(true, queue.try_enqueue(10));
        assert_eq!(true, queue.try_enqueue(20));

        assert_eq!(&10, queue.try_get_front().unwrap());
        queue.pop();

        assert_eq!(&20, queue.try_get_front().unwrap());
        queue.pop();
    }

    #[test]
    fn can_not_enqueue_in_a_full_queue() {
        let mut queue = SPSCQueue::new(2);
        assert_eq!(true, queue.try_enqueue(10));
        assert_eq!(true, queue.try_enqueue(20));
        assert_eq!(false, queue.try_enqueue(30));
    }

    #[test]
    fn can_not_get_front_from_an_empty_queue() {
        let queue: SPSCQueue<usize> = SPSCQueue::new(2);

        assert_eq!(None, queue.try_get_front());
    }

    #[test]
    fn pop_in_a_queue() {
        let mut queue = SPSCQueue::new(2);
        assert_eq!(true, queue.try_enqueue(10));
        assert_eq!(true, queue.try_enqueue(20));

        queue.pop();
        assert_eq!(1, queue.head.load(Ordering::SeqCst));

        queue.pop();
        assert_eq!(0, queue.head.load(Ordering::SeqCst));
    }
}