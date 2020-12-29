use crc::{crc32, Hasher32};
use std::convert::TryInto;
use std::io::ErrorKind;

// TODO: parameterize them
const LEN_SIZE: usize = 4;
pub const LEN_CRC: usize = 4;
const LEN_REDUNDANCY: usize = LEN_SIZE + LEN_CRC;

/*
 * Common data format:
 * | Size (4B) | Data | CRC (4B) |
 */

pub fn format_data_with_crc(key: &Vec<u8>, value: &Vec<u8>) -> Vec<u8> {
    let data_size = get_data_size(key.len(), value.len());
    let mut data: Vec<u8> = Vec::with_capacity(data_size);

    data.extend(&(key.len() as u32).to_le_bytes());
    data.extend(key);
    data.extend(&calc_crc(key).to_le_bytes());

    data.extend(&(value.len() as u32).to_le_bytes());
    data.extend(value);
    data.extend(&calc_crc(value).to_le_bytes());

    data
}

pub fn get_key_offset(key_size: usize) -> (usize, usize) {
    let start = LEN_SIZE;
    let end = LEN_SIZE + key_size;

    (start, end)
}

pub fn get_value_offset(key_size: usize, value_size: usize) -> (usize, usize) {
    let start = key_size + LEN_REDUNDANCY + LEN_SIZE;
    let end = start + value_size;

    (start, end)
}

pub fn get_data_size(key_size: usize, value_size: usize) -> usize {
    key_size + value_size + LEN_REDUNDANCY * 2
}

pub fn get_bound_offset(key_size: usize) -> usize {
    key_size + LEN_REDUNDANCY
}

pub fn calc_crc(data: &Vec<u8>) -> u32 {
    let mut digest = crc32::Digest::new(crc32::IEEE);
    digest.write(data);

    digest.sum32()
}

pub fn check_kv_crc(bytes: &[u8]) -> Result<(), std::io::Error> {
    let len = bytes.len();
    let crc = u32::from_le_bytes(bytes[(len - LEN_CRC)..len].try_into().unwrap());
    let size = u32::from_le_bytes(bytes[0..LEN_SIZE].try_into().unwrap());
    let data = bytes[LEN_SIZE..(LEN_SIZE + size as usize)].to_vec();

    if calc_crc(&data) == crc {
        Ok(())
    } else {
        // TODO: replace with an amphis error
        Err(std::io::Error::new(ErrorKind::Other, "CRC check failed!"))
    }
}
