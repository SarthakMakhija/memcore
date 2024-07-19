use std::io::{Error, Read};

use bytes::{Buf, BufMut, BytesMut};

use crate::memory::key_value::KeyValue;

#[derive(Copy, Clone, PartialEq, Debug)]
pub(crate) enum CommandType {
    Get = 1,
    Put = 2,
    Update = 3,
}
pub(crate) struct Command {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Option<Vec<u8>>,
    pub(crate) command_type: CommandType,
}
pub(crate) enum CommandResponse {
    Put(bool),
    Update(bool),
    Get(Option<Result<KeyValue, Error>>),
}

impl From<u8> for CommandType {
    fn from(value: u8) -> Self {
        match value {
            1 => CommandType::Get,
            2 => CommandType::Put,
            3 => CommandType::Update,
            _ => panic!("Unknown command type")
        }
    }
}

impl Command {
    pub(crate) fn get(key: Vec<u8>) -> Self {
        Command {
            key,
            value: None,
            command_type: CommandType::Get,
        }
    }

    pub(crate) fn put(key: Vec<u8>, value: Vec<u8>) -> Self {
        Command {
            key,
            value: Some(value),
            command_type: CommandType::Put,
        }
    }
    pub(crate) fn update(key: Vec<u8>, value: Vec<u8>) -> Self {
        Command {
            key,
            value: Some(value),
            command_type: CommandType::Update,
        }
    }

    pub(crate) fn encode(&self) -> BytesMut {
        let mut buffer = BytesMut::new();
        buffer.put_u16_le(self.key.len() as u16);
        buffer.put_u16_le(self.value.as_ref().map_or(0, |value| value.len()) as u16);
        buffer.put_u8(self.command_type as u8);
        buffer.put_slice(&self.key);
        buffer.put_slice(self.value.as_ref().map_or(&Vec::new(), |value| value));
        buffer
    }

    pub(crate) fn decode_from(mut buffer: BytesMut) -> Result<Self, Error> {
        let key_length = buffer.get_u16_le();
        let value_length = buffer.get_u16_le();
        let command_type = buffer.get_u8();

        let mut buffer_reader = buffer.reader();

        let mut key = Vec::with_capacity(key_length as usize);
        key.resize(key_length as usize, 0);
        buffer_reader.read_exact(&mut key)?;

        let mut value = Vec::with_capacity(value_length as usize);
        value.resize(value_length as usize, 0);
        buffer_reader.read_exact(&mut value)?;

        Ok(
            Command {
                key,
                value: if value.is_empty() { None } else { Some(value) },
                command_type: CommandType::from(command_type),
            }
        )
    }
}

impl CommandResponse {
    pub(crate) fn is_put_response(&self) -> bool {
        if let CommandResponse::Put(_) = self {
            return true;
        }
        false
    }

    pub(crate) fn put_response(&self) -> bool {
        if let CommandResponse::Put(response) = self {
            return *response;
        }
        false
    }

    pub(crate) fn is_update_response(&self) -> bool {
        if let CommandResponse::Update(_) = self {
            return true;
        }
        false
    }

    pub(crate) fn update_response(&self) -> bool {
        if let CommandResponse::Update(response) = self {
            return *response;
        }
        false
    }

    pub(crate) fn is_get_response(&self) -> bool {
        if let CommandResponse::Get(_) = self {
            return true;
        }
        false
    }

    pub(crate) fn get_response(self) -> Option<Result<KeyValue, Error>> {
        if let CommandResponse::Get(response) = self {
            return response;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::command::{Command, CommandType};

    #[test]
    fn encodes_and_decodes_a_get_command() {
        let get = Command::get(Vec::from(b"raft"));
        let encoded = get.encode();

        let decoded_result = Command::decode_from(encoded);
        assert_eq!(true, decoded_result.is_ok());

        let decoded = decoded_result.unwrap();
        assert_eq!(CommandType::Get, decoded.command_type);
        assert_eq!(Vec::from(b"raft"), decoded.key);
        assert_eq!(true, decoded.value.is_none());
    }

    #[test]
    fn encodes_and_decodes_a_put_command() {
        let put = Command::put(Vec::from(b"raft"), Vec::from(b"consensus"));
        let encoded = put.encode();

        let decoded_result = Command::decode_from(encoded);
        assert_eq!(true, decoded_result.is_ok());

        let decoded = decoded_result.unwrap();
        assert_eq!(CommandType::Put, decoded.command_type);
        assert_eq!(Vec::from(b"raft"), decoded.key);
        assert_eq!(Vec::from(b"consensus"), decoded.value.unwrap());
    }

    #[test]
    fn encodes_and_decodes_an_update_command() {
        let update = Command::update(Vec::from(b"raft"), Vec::from(b"consensus"));
        let encoded = update.encode();

        let decoded_result = Command::decode_from(encoded);
        assert_eq!(true, decoded_result.is_ok());

        let decoded = decoded_result.unwrap();
        assert_eq!(CommandType::Update, decoded.command_type);
        assert_eq!(Vec::from(b"raft"), decoded.key);
        assert_eq!(Vec::from(b"consensus"), decoded.value.unwrap());
    }
}