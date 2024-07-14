use std::hash::{Hash, Hasher};
use std::io::{Error, Read};

use bytes::{Buf, BufMut, BytesMut};
use fasthash::{FastHasher, MurmurHasher};

pub(crate) struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KeyValue {
    pub(crate) fn new(key: Vec<u8>, value: Vec<u8>) -> Self {
        assert!(key.len() > 0);
        assert!(value.len() > 0);
        KeyValue { key, value }
    }

    pub(crate) fn encode(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u16_le(self.key.len() as u16);
        buffer.put_u16_le(self.value.len() as u16);
        buffer.put_slice(&self.key);
        buffer.put_slice(&self.value);
        return buffer;
    }

    pub(crate) fn decode_from(mut buffer: BytesMut) -> Result<KeyValue, Error> {
        let key_length = buffer.get_u16_le();
        let value_length = buffer.get_u16_le();

        let mut buffer_reader = buffer.reader();

        let mut key = Vec::with_capacity(key_length as usize);
        key.resize(key_length as usize, 0);
        buffer_reader.read_exact(&mut key)?;

        let mut value = Vec::with_capacity(value_length as usize);
        value.resize(value_length as usize, 0);
        buffer_reader.read_exact(&mut value)?;

        return Ok(KeyValue::new(key, value));
    }

    pub(crate) fn hash_of(&self) -> u64 {
        let mut hasher: MurmurHasher = MurmurHasher::new();
        self.key.hash(&mut hasher);
        hasher.finish()
    }

    pub(crate) fn key(&self) -> Vec<u8> {
        return self.key.clone()
    }

    pub(crate) fn value(&self) -> &[u8] {
        return &self.value
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::key_value::KeyValue;

    #[test]
    fn encodes_and_decodes_key_value() {
        let key_value = KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"));
        let encoded = key_value.encode();

        let decoded = KeyValue::decode_from(encoded).expect("Failed to decode the key_value");
        assert_eq!(b"raft", &decoded.key[..]);
        assert_eq!(b"consensus", &decoded.value[..]);
    }

    #[test]
    fn get_the_hash_of_the_key() {
        let key_value = KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"));
        assert!(key_value.hash_of() > 0);
    }
}