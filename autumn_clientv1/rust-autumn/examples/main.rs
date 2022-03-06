use rust_autumn::AutumnLib;
use tokio::task;

#[tokio::main]
async fn main(){
    let urls :Vec<&str> = vec!["127.0.0.1:2379"];
    let lib =  AutumnLib::connect(urls).await.unwrap();

    match lib.put("x", vec![0, 2, 4, 6]).await {
        Ok(_) => println!("ok"),
        Err(e) => println!("{}", e),
    }

    match lib.get("x").await {
        Ok(x) => println!("{:#?}", x),
        Err(e) => println!("{}", e),
    }
    match lib.range("", "", 100).await {
        Ok(x) => println!("{:#?}", x),
        Err(e) => println!("{}", e),
    }

    match lib.head("x").await {
        Ok(x) => println!("{:#?}", x),
        Err(e) => println!("{}", e),
    }
    
    match lib.delete("x").await {
        Ok(x) => println!("{:#?}", x),
        Err(e) => println!("{}", e),
    }
}
