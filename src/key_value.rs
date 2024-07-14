use std::io::{Error, Read};

use bytes::{Buf, BufMut, BytesMut};

pub(crate) struct KeyValue {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KeyValue {
    pub fn new(key: Vec<u8>, value: Vec<u8>) -> KeyValue {
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
}

#[cfg(test)]
mod tests {
    use crate::key_value::KeyValue;

    #[test]
    fn encodes_and_decodes_key_value() {
        let key_value = KeyValue::new(Vec::from(b"raft"), Vec::from(b"consensus"));
        let encoded = key_value.encode();

        let decoded = KeyValue::decode_from(encoded).expect("Failed to decode the key_value");
        assert_eq!(b"raft", &decoded.key[..]);
        assert_eq!(b"consensus", &decoded.value[..]);
    }
}