use anyhow::{anyhow, Result};
use std::{fs::{OpenOptions, File}, u128, io::{Write, Read}};
use xattr;
use serde::{Deserialize, Serialize};
use std::str::{from_utf8, from_utf8_unchecked};

pub mod record;
pub struct Extent {
    is_sealed : bool,
    commmit_length : u64,
    id : u64,
    file_name : String,
    last_revision: u64,
}


const EXTENT_MAGIC_NUMBER : &str = "EXTENTXX";
const XATTR_META : &str = "user.EXTENTMETA";
const XATTR_SEAL : &str = "user.XATTRSEAL";
const XATTR_REV : &str = "user.REV";


pub fn create_extent(file_name : &str, id :u64) -> Result<Extent> {
    let mut file = File::create(file_name)?;

    let header = new_extent_head(id);
    let value = serde_json::to_vec(&header)?;
    xattr::set(file_name, XATTR_META, &value)?;
    file.sync_all()?;


    Ok(Extent {
        is_sealed : false,
        commmit_length : 0,
        id : id,
        file_name : file_name.to_string(),
        last_revision : 0,
    })
}


#[derive(Serialize, Deserialize, Debug)]
struct ExtentHeader {
    magic_number : String,
    id : u64,
}


fn new_extent_head(id:u64) -> ExtentHeader {
    ExtentHeader {
        magic_number : EXTENT_MAGIC_NUMBER.to_string(),
        id : id,
    }
}
fn read_extent_header(filename :&str) -> Result<ExtentHeader> {
    let value :Option<Vec<u8>> = xattr::get(filename, XATTR_META)?;
    let value = value.ok_or(anyhow!("no meta data"))?;
    let header:ExtentHeader = serde_json::from_slice(&value)?;
    if header.magic_number != EXTENT_MAGIC_NUMBER.to_string() {
        return Err(anyhow!("meta data is not correct"));
    }
    Ok(header)
}

pub fn open_extent(file_name : &str) -> Result<Extent> {
    //read attr XATTR_SEAL
    let is_seal :bool = match xattr::get(file_name, XATTR_SEAL)? {
        Some(value) => {
            if value.as_slice() == b"true" {
                true
            } else {
                false
            }
        },
        None => {
            false
        }
    };


    if is_seal {
        let f = File::open(file_name)?;
        let stat = f.metadata()?;

        if stat.len() > (4 << 30) {
            return Err(anyhow!("file size too large , {:?}", stat.len()));
        }

        let header = read_extent_header(file_name)?;

        return Ok(Extent {
            is_sealed : true,
            commmit_length : stat.len(),
            id : header.id,
            file_name : file_name.to_string(),
            last_revision : 0,
        });
    }


    let mut f = OpenOptions::new().read(true).append(true).open(file_name)?;

    let stat = f.metadata()?;

    if stat.len() > (4 << 30) {
        return Err(anyhow!("file size too large , {:?}", stat.len()));
    }

    let header = read_extent_header(file_name)?;


    let mut rev :u64 = 0;
    match xattr::get(file_name, XATTR_REV)? {
        Some(hex) => {
            rev = u64::from_str_radix(from_utf8(&hex)?, 10)?;
        },
        None => {
            rev = 0;
        }
    }

    Ok(Extent {
        is_sealed : false,
        commmit_length : stat.len(),
        id : header.id,
        file_name : file_name.to_string(),
        last_revision: rev, 
    })
    
}  

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn it_works() {
        let extent = create_extent("test.txt", 0).unwrap();
        assert_eq!(extent.id, 0);
        assert_eq!(extent.file_name, "test.txt");
        assert_eq!(extent.is_sealed, false);
        assert_eq!(extent.commmit_length, 0);

        let ex = open_extent("test.txt").unwrap();

        assert_eq!(ex.id, 0);
        std::fs::remove_file("test.txt").unwrap();
    }
}

