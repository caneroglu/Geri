use geri_proto::v4::msg::{Frame, ToHub, ToSource};

#[allow(async_fn_in_trait)]
pub trait Transport {
    type Error: core::fmt::Debug + core::fmt::Display;

    fn pushes(&self) -> bool;

    /// İptal edilen I/O'nun yarım çerçevesini bırakır. Akış transport'ları
    /// bağlantıyı kapatmalı; sonraki `connect` temiz bir oturum açmalıdır.
    fn disconnect(&mut self) {}

    async fn connect(&mut self) -> Result<(), Self::Error>;

    async fn send(&mut self, frame: Frame<ToHub>) -> Result<(), Self::Error>;

    async fn recv(&mut self) -> Result<Frame<ToSource>, Self::Error>;
}
