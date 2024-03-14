use std::{
    str,
    time::{Duration, SystemTime},
};
static CRLF: &str = "\r\n";
use anyhow::{Error, Result};

use crate::Command;

pub fn parse_command(cmd_vec: Vec<String>) -> Result<Command> {
    match cmd_vec[0].to_uppercase().as_str() {
        "PING" => Ok(Command::Ping),
        "ECHO" => {
            let arg = cmd_vec.get(1);
            let mut args = String::new();
            if let Some(arg) = arg {
                args.push_str(arg);
            }
            let len = args.len();
            args.insert_str(0, format!("${}{}", len, CRLF).as_str());
            args.push_str(CRLF);
            Ok(Command::Echo(args.clone()))
        }
        "REPLCONF" => {
            if let Some(arg) = cmd_vec.get(1) {
                match arg.as_str() {
                    "getack" => Ok(Command::ReplConfAck),
                    _ => Ok(Command::ReplConf(arg.clone())),
                }
            } else {
                Ok(Command::ReplConf("".to_string()))
            }
        }
        "PSYNC" => {
            let mut args = Vec::new();
            for arg in cmd_vec.iter().skip(1) {
                args.push(arg.clone());
            }
            Ok(Command::Psync(args))
        }
        "GET" => {
            let key = cmd_vec.get(1);
            println!("Key is: {:?}", key);
            let mut key_str = String::new();
            if let Some(key) = key {
                key_str.push_str(key);
            }
            Ok(Command::Get(key_str.clone()))
        }
        "SET" => {
            let key = cmd_vec.get(1);
            let value = cmd_vec.get(2);
            let mut key_str = String::new();
            let mut value_str = String::new();
            if let Some(key) = key {
                key_str.push_str(key);
            }
            if let Some(value) = value {
                value_str.push_str(value);
            }
            if let Some(arg) = cmd_vec.get(3) {
                match arg.as_str() {
                    "px" | "ex" => {
                        if let Some(expiry) = cmd_vec.get(4) {
                            match expiry.parse::<u64>() {
                                Ok(duration) => Ok(Command::Set(
                                    key_str.clone(),
                                    value_str.clone(),
                                    Some(SystemTime::now() + Duration::from_millis(duration)),
                                )),
                                Err(_) => Err(Error::msg("Invalid expiry duration")),
                            }
                        } else {
                            Err(Error::msg("Invalid expiry duration"))
                        }
                    }
                    _ => Err(Error::msg("Invalid argument")),
                }
            } else {
                Ok(Command::Set(key_str.clone(), value_str.clone(), None))
            }
        }
        "CONFIG" => {
            if let Some(sub_command) = cmd_vec.get(1) {
                match sub_command.as_str() {
                    "get" => {
                        let key = cmd_vec.get(2);
                        let mut key_str = String::new();
                        if let Some(key) = key {
                            key_str.push_str(key);
                        }
                        Ok(Command::GetConfig(key_str.clone()))
                    }
                    _ => Err(Error::msg("Only supports CONFIG GET command")),
                }
            } else {
                Err(Error::msg("Invalid command"))
            }
        }
        "KEYS" => {
            let pattern = cmd_vec.get(1);
            let mut pattern_str = String::new();
            if let Some(pattern) = pattern {
                if pattern == "" {
                    return Ok(Command::Keys("*".to_string()));
                }
                pattern_str.push_str(pattern);
            }
            Ok(Command::Keys(pattern_str.clone()))
        }
        "INFO" => {
            let arg = cmd_vec.get(1);
            let mut arg_str = String::new();
            if let Some(arg) = arg {
                arg_str.push_str(arg);
            }
            Ok(Command::Info(arg_str.clone()))
        }
        _ => Err(Error::msg("Command not supported")),
    }
}

pub async fn process_buff(buff: &[u8]) -> Result<Vec<Vec<String>>> {
    let resp_message = std::str::from_utf8(buff)?;
    let resp_arrays = resp_message.split('*').collect::<Vec<_>>();
    let mut commands: Vec<Vec<String>> = Vec::new();
    for resp_array in resp_arrays.iter() {
        if resp_array.is_empty() {
            continue;
        }
        let resp_words = resp_array.split('$').collect::<Vec<_>>();
        let mut command: Vec<String> = Vec::new();
        for resp_word in resp_words.iter().skip(1) {
            let words = resp_word
                .split("\r\n")
                .map(|s| s.to_owned())
                .collect::<Vec<_>>();
            command.push(words.get(1).unwrap().clone());
        }
        commands.push(command);
    }
    Ok(commands)
}
