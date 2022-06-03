use extent_rust::extent::open_extent;

fn main() {
    open_extent("../extent/test.extent").unwrap();
    println!("Hello, world!");
}