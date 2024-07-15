use crate::executor::command::{Command, CommandResponse, CommandType};
use crate::memory::key_value::KeyValue;
use crate::memory::log::Log;

pub(crate) struct CommandExecutor {
    log: Log,
}

impl CommandExecutor {
    pub(crate) fn new(log: Log) -> Self {
        CommandExecutor {
            log
        }
    }

    pub(crate) fn execute(&mut self, command: Command) -> CommandResponse {
        match command.command_type {
            CommandType::Get =>
                CommandResponse::Get(self.log.try_get(&command.key)),
            CommandType::Put =>
                CommandResponse::Put(self.log.try_append(KeyValue::new(command.key, command.value.unwrap()))),
            CommandType::Update =>
                CommandResponse::Update(self.log.try_append(KeyValue::new(command.key, command.value.unwrap()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::executor::command::Command;
    use crate::executor::command_executor::CommandExecutor;
    use crate::memory::log::Log;
    use crate::memory::options::LogOptions;

    #[test]
    fn should_execute_put_command_successfully() {
        let log_size_bytes = 64;
        let segment_size_bytes = 64;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);

        let command_response = executor.execute(Command::put(Vec::from(b"raft"), Vec::from(b"consensus")));
        assert_eq!(true, command_response.is_put_response());
        assert_eq!(true, command_response.put_response());
    }

    #[test]
    fn should_not_execute_put_command_successfully_given_segment_does_not_have_sufficient_space() {
        let log_size_bytes = 16;
        let segment_size_bytes = 16;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);

        let command_response = executor.execute(Command::put(Vec::from(b"raft"), Vec::from(b"consensus")));
        assert_eq!(true, command_response.is_put_response());
        assert_eq!(false, command_response.put_response());
    }

    #[test]
    fn should_execute_update_command_successfully() {
        let log_size_bytes = 64;
        let segment_size_bytes = 64;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);

        let command_response = executor.execute(Command::update(Vec::from(b"raft"), Vec::from(b"consensus")));
        assert_eq!(true, command_response.is_update_response());
        assert_eq!(true, command_response.update_response());
    }

    #[test]
    fn should_not_execute_update_command_successfully_given_segment_does_not_have_sufficient_space() {
        let log_size_bytes = 16;
        let segment_size_bytes = 16;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);

        let command_response = executor.execute(Command::update(Vec::from(b"raft"), Vec::from(b"consensus")));
        assert_eq!(true, command_response.is_update_response());
        assert_eq!(false, command_response.update_response());
    }

    #[test]
    fn should_execute_get_command_successfully_and_get_the_value_of_the_key() {
        let log_size_bytes = 64;
        let segment_size_bytes = 64;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);

        let command_response = executor.execute(Command::put(Vec::from(b"raft"), Vec::from(b"consensus")));
        assert_eq!(true, command_response.is_put_response());
        assert_eq!(true, command_response.put_response());

        let command_response = executor.execute(Command::get(Vec::from(b"raft")));
        assert_eq!(true, command_response.is_get_response());
        assert_eq!(Vec::from(b"consensus"), command_response.get_response().unwrap().unwrap().value());
    }

    #[test]
    fn should_execute_get_command_successfully_but_not_get_the_value_of_the_key() {
        let log_size_bytes = 64;
        let segment_size_bytes = 64;

        let log = Log::new(LogOptions::new(log_size_bytes, segment_size_bytes));
        let mut executor = CommandExecutor::new(log);
        
        let command_response = executor.execute(Command::get(Vec::from(b"raft")));
        assert_eq!(true, command_response.is_get_response());
        assert_eq!(true, command_response.get_response().is_none());
    }
}