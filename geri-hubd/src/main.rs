// Testler Claude tarafından yazılmıştır.

mod api;
mod msg;
mod quic;
mod store;
mod tcp;
mod ui;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    if let Some(command) = args.next() {
        if command != "verify-backup" {
            return Err("usage: geri-hubd [verify-backup PATH]".into());
        }
        let path = args.next().ok_or("verify-backup requires a directory")?;
        if args.next().is_some() {
            return Err("usage: geri-hubd verify-backup PATH".into());
        }
        store::Store::verify_backup(std::path::Path::new(&path))?;
        println!("Backup verified: {}", std::path::Path::new(&path).display());
        return Ok(());
    }
    api::serve().await
}
