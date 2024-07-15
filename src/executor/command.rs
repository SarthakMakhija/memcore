use std::io::Error;

use crate::memory::key_value::KeyValue;

pub(crate) enum CommandType {
    Get,
    Put,
    Update,
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
            return response
        }
        None
    }
}