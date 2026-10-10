# Géri 🐺

> 🐺 ***Geri**, Odin'in iki kurdundan biri. Adı "obur" demek, sofraya ne konsa yer.*

[![Lisans](https://img.shields.io/badge/lisans-MIT%20veya%20Apache--2.0-blue.svg)](#lisans)
[![Rust](https://img.shields.io/badge/rust-1.85%2B-orange.svg?logo=rust)](https://blog.rust-lang.org/2025/02/20/Rust-1.85.0.html)
[![no_std](https://img.shields.io/badge/no__std-uyumlu-success.svg)](geri-proto)
[![Son commit](https://img.shields.io/github/last-commit/caneroglu/Geri/main.svg)](https://github.com/caneroglu/Geri/commits/main)
[![Kişisel proje](https://img.shields.io/badge/kişisel-proje-lightgrey.svg)](#durum)

**Türkçe** · [English](README.en.md)

Masamda bir sürü küçük kart var: router, ESP32, bir de 20 KB RAM'li minicik bir Blue Pill.
Hepsi bana bir şeyler söylesin, ben de onlara bir şeyler söyleyebileyim istedim.
Elektrik gitse, internet kopsa bile hiçbir şey kaybolmasın.
Bunu yapan, üstelik bu kartlara sığan bir şey bulamadım. Ben de oturdum yazdım.

Kendim için yaptım, benim işimi görüyor.

![Géri demo](demo/demo.gif)

## Ne yapıyor?

Düşün ki bir cihazın var, sıcaklık ölçüyor, bir de rölesi var.

- Cihaz Géri'ye bağlanınca kendini tanıtıyor: "Ben şuyum, sıcaklık ölçüyorum, bana *aç/kapa* diyebilirsin."
- Géri bunu duyunca kendiliğinden bir panel çiziyor. Sıcaklık orada görünüyor, röle için bir düğme çıkıyor. Senin bir şey kodlaman gerekmiyor.
- Cihazın gönderdiği her şey saklanıyor. Sonradan "ne oldu, ne zaman oldu?" diye bakabiliyorsun.
- Düğmeye basınca komut cihaza anında gidiyor.

Géri'yi ister kendi bilgisayarında tek bir dosya olarak çalıştırırsın (`geri-hubd`), ister Cloudflare'e koyarsın.
Benimki Cloudflare'de duruyor: hosting'e para vermek istemedim, bedava paketleri de benim düzeneğe yetecek kadar cömert.
[workers-rs](https://github.com/cloudflare/workers-rs) sayesinde orada da aynı Rust kodu çalışıyor, ayrıca bir şey yazmadım.
(Bir kere günlük bedava kotayı doldurdum, sonra kodu düzelttim 🙂) Cloudflare tarafının kodu henüz açık değil.

## Kurallar

Bunlar hiç değişmez. Gerisi (hangi yoldan gittiği, şifreli olup olmadığı falan) senin bileceğin iş.

1. **Cihaz kendini tanıtır.** Géri'nin onu önceden bilmesi gerekmez. Takarsın, "selam ben buyum" der.
2. **Géri "aldım" dediyse diskte.** "Herhalde aldım" yok, "bellekte bir yerdeydi" yok.
3. **Her mesaj sıradaki numarayı alır.** Kayıp yok, çift kayıt yok. Grafiklerin sana yalan söylemez.
4. **Bir komut en fazla bir kez çalışır.** Röleyi iki kere tetikleyip evi yakmaz.
5. **Bağlantıyı hep cihaz açar.** Modemde port açmakla uğraşmazsın.
6. **Géri dürter, ama cihaz hep gelip sorar.** Dürtme yolda kaybolsa da bir sonraki soruda yakalanır.
7. **Yol fark etmez.** Kablo, Wi-Fi, internet: mesajlar aynı, sadece yol değişir.
8. **Varsayılan hep sağlam olan.** Gevşetmek mi istiyorsun (mesela önemsiz ölçümlerde sıra)? Olur, ama bilerek.

## Nasıl konuşuyor?

Ne varsa onunla: internetten, Wi-Fi'dan, hatta düz bir kabloyla.
Ağı olmayan bir kart, kabloyla yanındaki bir karta bağlanıyor, o da mesajı Géri'ye taşıyor.
Hepsinde mesajlar aynı, sadece yol değişiyor. İstersen trafik şifreli gidiyor.

İşin en sade hali bu kadar: elinde bir `curl` varsa o da cihaz sayılır.

## Küçük kartlar ne durumda?

Bütün dert bu zaten, o yüzden açık konuşayım:

- Router'ımda her gün çalışıyor, ~4.6 MB bellekle. Yanında koştuğum basit bir dashboard 20 MB'tan fazla yiyordu.
- ESP32 gibi kartlara rahat sığıyor.
- **Blue Pill'e henüz sığmıyor.** Bugünkü hali ~110 KB, kartta 64 KB yer var. Onu küçültmek sıradaki iş.

## Dene

Bilgisayarında bir Géri aç:

```sh
export GERI_AGENTS='[
  {"id":"sat","token":"a-long-random-agent-token","sources":["nasa-sat-01"]},
  {"id":"me","token":"a-long-random-operator-token","role":"operator"}
]'
cargo run --release -p geri-hubd   # http://127.0.0.1:8787
```

Başka bir terminalden bir "uydu" gibi davran:

```sh
H=http://127.0.0.1:8787; AUTH='Authorization: Bearer a-long-random-agent-token'
curl -X PUT $H/v4/sources/nasa-sat-01 -H "$AUTH" -H 'content-type: application/json' -d @demo/source.json
curl $H/v4/batch -H "$AUTH" -H 'content-type: application/json' -d @demo/1.json
```

Sonra <http://127.0.0.1:8787/> adresini aç, `a-long-random-operator-token` ile giriş yap. Panel orada.

Rust'la bir Linux cihazı yazmak istersen:

```rust
let (geri, mut events) = geri_sdk_linux::builder("https://hub.example", "", "router")
    .key("/etc/geri/key")              // kimliğim bu anahtar
    .source(declaration)               // ben kimim, neler yapabilirim
    .outbox("/etc/geri/outbox.sqlite") // internet koparsa burada beklesin
    .start()?;

geri.state("router", "cpu", json!({ "t": "gauge", "v": 12.5, "max": 100, "unit": "%" }))?;
// gelen komutlar events'ten düşer: yap, sonra geri.result(...) ile "yaptım" de
```

## Meraklısına

- Diğerleriyle (MQTT, Zenoh, LwM2M…) kıyas ve ölçtüğüm rakamlar: [COMPARISON.md](COMPARISON.md) (İngilizce)
- Parçalar ayrı crate'ler halinde: `geri-proto` (dil), `geri-core` (kurallar), `geri-sdk-linux` (cihaz tarafı),
  `geri-hubd` (merkez), `geri-view` (panel), bir de taşıma ve depolama parçaları (`geri-transport-*`, `geri-store-*`).
- crates.io'da şimdilik sadece `geri-proto` var. Gerisi dokümanı yazınca gelecek.

## Durum

Evde her gün çalışıyor. Sırada Blue Pill'i sığdırmak ve küçük kartlar için hazır paketler var.

Kişisel proje. Issue açabilirsin, bakarım ama ne zaman bakarım bilemem 🙂

## Lisans

MIT ya da Apache-2.0, hangisi işine gelirse ([LICENSE-MIT](LICENSE-MIT), [LICENSE-APACHE](LICENSE-APACHE)).
Panelin kullandığı dış dosyaların kendi lisansları var: [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
