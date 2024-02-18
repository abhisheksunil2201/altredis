use std::time::{Duration, SystemTime};
static CRLF: &str = "\r\n";
use crate::Command;

#[derive(Debug, PartialOrd, PartialEq, Clone)]
pub enum Data {
    Array(Vec<Data>),
    BulkStringValue(String),
}

fn parse_array(data: &str) -> Result<(Data, &str), &str> {
    if let Some((num_elements, mut rest)) = data.split_once(CRLF) {
        let num_elements = num_elements[1..]
            .parse::<usize>()
            .map_err(|_| "Error parsing usize value.")?;
        let mut ret_data: Vec<Data> = Vec::with_capacity(num_elements);
        for _ in 0..num_elements {
            let (data, new_rest) = parse_val(rest)?;
            rest = new_rest;
            ret_data.push(data);
        }
        Ok((Data::Array(ret_data), rest))
    } else {
        Err("Can't parse array.")
    }
}
pub fn parse_string(data: &str) -> Result<(Data, &str), &str> {
    let data = data.split_once(CRLF);
    if let Some((len, rest)) = data {
        let len = len[1..]
            .parse::<usize>()
            .map_err(|_e| "Error parsing usize value.")?;
        let (val, rest) = rest.split_once(CRLF).ok_or("Invalid string format.")?;
        if len != val.len() {
            return Err("String legths don't match");
        }
        Ok((Data::BulkStringValue(val.to_string()), rest))
    } else {
        Err("Invalid string format.")
    }
}
fn parse_val(data: &str) -> Result<(Data, &str), &str> {
    match data.chars().next() {
        Some('*') => parse_array(data),
        Some('$') => parse_string(data),
        _ => Err("Unknown format"),
    }
}
pub fn parse_command(data: &str) -> Result<Command, &str> {
    let (data, _) = parse_val(data)?;
    if let Data::Array(cmd_vec) = data {
        if let Data::BulkStringValue(cmd) = cmd_vec.first().ok_or("Invalid command format.")? {
            match cmd.to_uppercase().as_str() {
                "PING" => Ok(Command::Ping),
                "ECHO" => {
                    let arg = cmd_vec.get(1);
                    let mut args = String::new();
                    if let Some(Data::BulkStringValue(arg)) = arg {
                        args.push_str(arg);
                    }
                    let len = args.len();
                    args.insert_str(0, format!("${}{}", len, CRLF).as_str());
                    args.push_str(CRLF);
                    Ok(Command::Echo(args.clone()))
                }
                "REPLCONF" => Ok(Command::ReplConf),
                "PSYNC" => {
                    let mut args = Vec::new();
                    for arg in cmd_vec.iter().skip(1) {
                        if let Data::BulkStringValue(arg) = arg {
                            args.push(arg.clone());
                        }
                    }
                    Ok(Command::Psync(args))
                }
                "GET" => {
                    let key = cmd_vec.get(1);
                    let mut key_str = String::new();
                    if let Some(Data::BulkStringValue(key)) = key {
                        key_str.push_str(key);
                    }
                    Ok(Command::Get(key_str.clone()))
                }
                "SET" => {
                    let key = cmd_vec.get(1);
                    let value = cmd_vec.get(2);
                    let mut key_str = String::new();
                    let mut value_str = String::new();
                    if let Some(Data::BulkStringValue(key)) = key {
                        key_str.push_str(key);
                    }
                    if let Some(Data::BulkStringValue(value)) = value {
                        value_str.push_str(value);
                    }
                    if let Some(Data::BulkStringValue(arg)) = cmd_vec.get(3) {
                        match arg.as_str() {
                            "px" | "ex" => {
                                if let Some(Data::BulkStringValue(expiry)) = cmd_vec.get(4) {
                                    match expiry.parse::<u64>() {
                                        Ok(duration) => Ok(Command::Set(
                                            key_str.clone(),
                                            value_str.clone(),
                                            Some(
                                                SystemTime::now() + Duration::from_millis(duration),
                                            ),
                                        )),
                                        Err(_) => return Err("Invalid expiry duration"),
                                    }
                                } else {
                                    Err("Invalid expiry duration")
                                }
                            }
                            _ => Err("Invalid argument."),
                        }
                    } else {
                        Ok(Command::Set(key_str.clone(), value_str.clone(), None))
                    }
                }
                "CONFIG" => {
                    if let Some(Data::BulkStringValue(sub_command)) = cmd_vec.get(1) {
                        match sub_command.as_str() {
                            "get" => {
                                let key = cmd_vec.get(2);
                                let mut key_str = String::new();
                                if let Some(Data::BulkStringValue(key)) = key {
                                    key_str.push_str(key);
                                }
                                Ok(Command::GetConfig(key_str.clone()))
                            }
                            _ => Err("Only supports config GET command"),
                        }
                    } else {
                        Err("Invalid Command")
                    }
                }
                "KEYS" => {
                    let pattern = cmd_vec.get(1);
                    let mut pattern_str = String::new();
                    if let Some(Data::BulkStringValue(pattern)) = pattern {
                        pattern_str.push_str(pattern);
                    }
                    Ok(Command::Keys(pattern_str.clone()))
                }
                "INFO" => {
                    let arg = cmd_vec.get(1);
                    let mut arg_str = String::new();
                    if let Some(Data::BulkStringValue(arg)) = arg {
                        arg_str.push_str(arg);
                    }
                    Ok(Command::Info(arg_str.clone()))
                }
                _ => Err("Command not supported."),
            }
        } else {
            Err("Invalid command format.")
        }
    } else {
        Err("Invalid command.")
    }
}
