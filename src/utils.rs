use bytes::buf::Writer;
use std::{io::Write, str};

pub const EMPTY_RDB_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

pub fn get_bulk_string(buffer: &mut Writer<Vec<u8>>, string: &[u8]) -> tokio::io::Result<()> {
    let length_str = string.len().to_string();
    buffer.write_all(
        format!("${}\r\n{}\r\n", length_str, String::from_utf8_lossy(string)).as_bytes(),
    )?;
    Ok(())
}

pub fn get_array(buffer: &mut Writer<Vec<u8>>, array: Vec<Vec<u8>>) -> tokio::io::Result<()> {
    let length_str = array.len().to_string();
    buffer.write_all(format!("*{}\r\n", length_str).as_bytes())?;
    for element in array {
        get_bulk_string(buffer, &element)?;
    }
    Ok(())
}
pub fn build_resp_string(text: &str) -> Vec<u8> {
    if text.is_empty() {
        return "$-1\r\n".as_bytes().to_vec();
    }
    let string = format!("${}\r\n{}\r\n", text.len(), text);
    string.as_bytes().to_vec()
}
pub fn build_resp_array(command: &str) -> Vec<u8> {
    let split = command.split(' ').collect::<Vec<_>>();
    let resp_strings = split
        .iter()
        .map(|text| build_resp_string(text))
        .collect::<Vec<_>>();
    let res = resp_strings.concat();
    let mut string = format!("*{}\r\n", resp_strings.len()).as_bytes().to_vec();
    string.extend_from_slice(&res);
    string
}
