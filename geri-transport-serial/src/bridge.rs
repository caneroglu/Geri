use crate::{Error, SerialTransport};
use geri_adapter_sdk::Transport;
use geri_binding_serial::{Codec, Item};
use geri_proto::v4::msg::ToHub;
use geri_transport_ws::WsTransport;
use tokio::io::{AsyncRead, AsyncWrite};

#[derive(Debug)]
pub enum Event {
    Connected,
    ConnectFailed(geri_transport_ws::Error),
    Lost(geri_transport_ws::Error),
    Dropped(u64),
}

pub async fn run<IO>(
    serial: IO,
    mut hub: WsTransport,
    max_frame: usize,
    mut on: impl FnMut(Event),
) -> Result<(), Error>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let mut line = SerialTransport::new(serial).with_max_frame(max_frame);
    let mut live = false;
    let mut dropped = 0;
    loop {
        tokio::select! {
            item = line.next_item() => {
                let item = item?;
                if line.dropped() != dropped {
                    dropped = line.dropped();
                    on(Event::Dropped(dropped));
                }
                match item {
                    Item::Reset => match hub.connect().await {
                        Ok(()) => {
                            live = true;
                            on(Event::Connected);
                        }
                        Err(e) => {
                            live = false;
                            on(Event::ConnectFailed(e));
                            line.write_reset().await?;
                        }
                    },
                    Item::Frame(payload) => {
                        if !live {
                            line.write_reset().await?;
                            continue;
                        }
                        let Ok(frame) = geri_binding_serial::decode::<ToHub>(&payload) else {
                            continue;
                        };
                        if let Some(codec) = Codec::of(&payload) {
                            line.set_codec(codec);
                        }
                        if let Err(e) = hub.send(frame).await {
                            live = false;
                            on(Event::Lost(e));
                            line.write_reset().await?;
                        }
                    }
                }
            }
            frame = hub.recv(), if live => match frame {
                Ok(frame) => line.write_frame(&frame).await?,
                Err(e) => {
                    live = false;
                    on(Event::Lost(e));
                    line.write_reset().await?;
                }
            },
        }
    }
}
