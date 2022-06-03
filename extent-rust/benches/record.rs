#![feature(test)]
extern crate test;
use std::{io::Write, hash::Hasher};
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;

use extent_rust::extent::record::LogWriter;


#[bench]
fn bench_crc(b : &mut test::Bencher) {

    let rand_string: String = thread_rng()
    .sample_iter(&Alphanumeric)
    .take(1 << 20)
    .map(char::from)
    .collect();


    let mut digest = crc32fast::Hasher::new();
    b.iter(||{
        digest.reset();
        digest.write(rand_string.as_bytes());
        digest.finish();
    })
}



#[bench]
fn bench_log_writer(b: &mut test::Bencher) {
    let mut discard = Discard{};
    let mut writer = LogWriter::new(discard);
    let rand_string: String = thread_rng()
    .sample_iter(&Alphanumeric)
    .take(1 << 20)
    .map(char::from)
    .collect();
    b.iter(|| {
        writer.add_record(rand_string.as_bytes()).unwrap();
    });
}


pub struct Discard {}

impl Write for Discard  {
    fn write(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}