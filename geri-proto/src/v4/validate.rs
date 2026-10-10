//! Hub ve SDK için ortak, I/O içermeyen v4 giriş doğrulaması.

use super::{BlobRef, MAX_INLINE_BYTES, Record, SourceDeclaration, valid_id};
use alloc::collections::BTreeSet;
use serde_json::Value;

/// İlan ve entity etiketinin bayt sınırı.
pub const MAX_LABEL: usize = 128;
/// JSON olarak kodlanmış indeks verisinin bayt sınırı.
pub const MAX_INDEXED_BYTES: usize = 16 * 1024;

/// Geçersiz alanı gösterir; depolama ve yetki kararlarını içermez.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    #[error("invalid identifier")]
    /// Kimlik biçimi geçersiz.
    Id,
    #[error("content_type must be 1..=128 ASCII bytes")]
    /// İçerik türü boş, uzun veya ASCII dışında.
    ContentType,
    #[error("exactly one of data and blob is required")]
    /// Data ve blob alanlarından tam olarak biri bulunmalı.
    PayloadCount,
    #[error("inline data requires an application/json content_type")]
    /// Inline verinin içerik türü JSON olmalı.
    InlineContentType,
    #[error("inline data exceeds 64 KiB or cannot be serialized")]
    /// Inline JSON boyutu sınırı aşıyor veya kodlanamıyor.
    InlineSize,
    #[error("indexed data exceeds 16 KiB or cannot be serialized")]
    /// İndeks JSON boyutu sınırı aşıyor veya kodlanamıyor.
    IndexedSize,
    #[error("blob needs a positive size and a 64-character lowercase SHA-256 hash")]
    /// Blob boyutu veya SHA-256 biçimi geçersiz.
    Blob,
    #[error("sequence exceeds the exact cross-store limit")]
    /// Sıra, bütün desteklenen depolarda tam temsil edilemiyor.
    Sequence,
    #[error("source label exceeds 128 bytes")]
    /// Kaynak etiketi çok uzun.
    SourceLabel,
    #[error("source kind exceeds 64 bytes")]
    /// Kaynak türü çok uzun.
    SourceKind,
    #[error("source has more than 128 entities")]
    /// Entity sayısı sınırı aşıyor.
    EntityCount,
    #[error("entity belongs to another source")]
    /// Entity başka kaynağa ait.
    EntitySource,
    #[error("entity label exceeds 128 bytes")]
    /// Entity etiketi çok uzun.
    EntityLabel,
    #[error("entity kind exceeds 64 bytes")]
    /// Entity türü çok uzun.
    EntityKind,
    #[error("entity group must be 1..=128 bytes")]
    /// Entity grubu boş veya çok uzun.
    EntityGroup,
    #[error("duplicate entity identifier")]
    /// Entity kimliği ilanda tekrarlanıyor.
    DuplicateEntity,
    #[error("invalid control shape")]
    /// Kontrol biçimi geçersiz.
    Control,
}

/// Sequence ayırmadan Draft alanlarını da doğrulayabilen ortak payload yolu.
pub fn payload(
    content_type: &str,
    data: Option<&Value>,
    indexed: Option<&Value>,
    blob: Option<&BlobRef>,
) -> Result<(), Invalid> {
    if content_type.is_empty() || content_type.len() > 128 || !content_type.is_ascii() {
        return Err(Invalid::ContentType);
    }
    if data.is_some() == blob.is_some() {
        return Err(Invalid::PayloadCount);
    }
    if let Some(data) = data {
        if !content_type.starts_with("application/json") {
            return Err(Invalid::InlineContentType);
        }
        if serde_json::to_vec(data).map_or(true, |v| v.len() > MAX_INLINE_BYTES) {
            return Err(Invalid::InlineSize);
        }
    }
    if indexed.is_some_and(|v| serde_json::to_vec(v).map_or(true, |v| v.len() > MAX_INDEXED_BYTES))
    {
        return Err(Invalid::IndexedSize);
    }
    if let Some(blob) = blob {
        if blob.size == 0
            || blob.sha256.len() != 64
            || !blob
                .sha256
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(Invalid::Blob);
        }
    }
    Ok(())
}

/// Depolanmış Record için aynı kuralları, kimliklerle birlikte uygular.
pub fn record(record: &Record) -> Result<(), Invalid> {
    if !valid_id(&record.source_id) || !valid_id(&record.entity_id) || !valid_id(&record.record_id)
    {
        return Err(Invalid::Id);
    }
    if record.sequence > super::MAX_SEQUENCE {
        return Err(Invalid::Sequence);
    }
    payload(
        &record.content_type,
        record.data.as_ref(),
        record.indexed.as_ref(),
        record.blob.as_ref(),
    )
}

/// İlanın tamamını doğrular; mevcut hub kabul kurallarını değiştirmez.
pub fn source(declaration: &SourceDeclaration) -> Result<(), Invalid> {
    if !valid_id(&declaration.source_id) {
        return Err(Invalid::Id);
    }
    if declaration.label.len() > MAX_LABEL {
        return Err(Invalid::SourceLabel);
    }
    if declaration.kind.len() > 64 {
        return Err(Invalid::SourceKind);
    }
    if declaration.entities.len() > 128 {
        return Err(Invalid::EntityCount);
    }
    let mut seen = BTreeSet::new();
    for entity in &declaration.entities {
        if entity.source_id != declaration.source_id {
            return Err(Invalid::EntitySource);
        }
        if !valid_id(&entity.entity_id) {
            return Err(Invalid::Id);
        }
        if entity.label.len() > MAX_LABEL {
            return Err(Invalid::EntityLabel);
        }
        if entity.kind.len() > 64 {
            return Err(Invalid::EntityKind);
        }
        if entity
            .group
            .as_ref()
            .is_some_and(|g| g.is_empty() || g.len() > 128)
        {
            return Err(Invalid::EntityGroup);
        }
        if !seen.insert(&entity.entity_id) {
            return Err(Invalid::DuplicateEntity);
        }
        if entity.control.as_ref().is_some_and(|c| !c.shape.is_valid()) {
            return Err(Invalid::Control);
        }
    }
    Ok(())
}
