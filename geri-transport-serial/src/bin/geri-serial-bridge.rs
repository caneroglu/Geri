use geri_transport_serial::{DEFAULT_MAX_FRAME, SerialStream, bridge};
use geri_transport_ws::WsTransport;

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        eprintln!("{name} gerekli");
        std::process::exit(2)
    })
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let path = env("GERI_SERIAL");
    let baud = std::env::var("GERI_BAUD")
        .ok()
        .and_then(|b| b.parse().ok())
        .unwrap_or(115_200);
    let max = std::env::var("GERI_MAX_FRAME")
        .ok()
        .and_then(|b| b.parse().ok())
        .unwrap_or(DEFAULT_MAX_FRAME);
    let hub = match WsTransport::new(&env("GERI_HUB"), env("GERI_TOKEN"), env("GERI_AGENT")) {
        Ok(hub) => hub,
        Err(e) => {
            eprintln!("GERI_HUB: {e}");
            std::process::exit(2)
        }
    };
    let serial = match SerialStream::open(&tokio_serial::new(&path, baud)) {
        Ok(port) => port,
        Err(e) => {
            eprintln!("{path}: {e}");
            std::process::exit(1)
        }
    };
    println!("köprü: {path} @ {baud} ↔ {}", hub.url());
    let result = bridge::run(serial, hub, max, |event| match event {
        bridge::Event::Connected => println!("hub'a bağlandı"),
        bridge::Event::ConnectFailed(e) => eprintln!("hub'a bağlanılamadı: {e}"),
        bridge::Event::Lost(e) => eprintln!("hub bağlantısı koptu: {e}"),
        bridge::Event::Dropped(n) => eprintln!("seri hatta atılan çerçeve: {n}"),
    })
    .await;
    if let Err(e) = result {
        eprintln!("seri hat: {e}");
        std::process::exit(1);
    }
}
