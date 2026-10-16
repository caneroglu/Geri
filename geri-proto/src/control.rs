use alloc::{boxed::Box, collections::BTreeMap, string::String, vec::Vec};
use serde::{Deserialize, Serialize};

use crate::Value;

pub const SET: &str = "set";
pub const PRESS: &str = "press";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Control {
    pub shape: Shape,
    #[serde(default)]
    pub danger: Danger,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Shape {
    Trigger,
    Bool,
    Range {
        min: f64,
        max: f64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step: Option<f64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        unit: Option<String>,
    },
    Choice {
        options: Vec<String>,
    },
    Text {
        max: u32,
    },
    /// Çok satırlı metin; `lang` (ör. "toml") yalnızca görünümde sözdizimi vurgusu içindir.
    Code {
        max: u32,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lang: Option<String>,
    },
    Item {
        of: Box<Shape>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Danger {
    #[default]
    Safe,
    Risky,
    #[serde(other)]
    Destructive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    #[error("arguments have the wrong type for this control")]
    Type,
    #[error("argument is out of range, off-grid, not an option, or too long")]
    Range,
    #[error("this control's shape is unknown and cannot be commanded")]
    Unsupported,
}

impl Shape {
    pub fn action(&self) -> &'static str {
        match self {
            Self::Trigger => PRESS,
            Self::Item { of } => of.action(),
            _ => SET,
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            Self::Range { min, max, step, .. } => {
                min.is_finite()
                    && max.is_finite()
                    && min <= max
                    && step.is_none_or(|s| s.is_finite() && s > 0.0)
            }
            Self::Choice { options } => !options.is_empty(),
            Self::Text { max } => *max > 0,
            Self::Code { max, lang } => *max > 0 && lang.as_ref().is_none_or(|l| !l.is_empty()),
            Self::Item { of } => {
                !matches!(**of, Self::Item { .. } | Self::Unknown) && of.is_valid()
            }
            Self::Trigger | Self::Bool | Self::Unknown => true,
        }
    }

    pub fn accept(&self, args: &Value) -> Result<Value, Invalid> {
        match (self, args) {
            (Self::Trigger, Value::Null) => Ok(Value::Null),
            (Self::Bool, Value::Bool(b)) => Ok(Value::Bool(*b)),
            (Self::Range { min, max, step, .. }, v) => {
                let x = v.as_f64().ok_or(Invalid::Type)?;
                if !(x.is_finite() && *min <= x && x <= *max) {
                    return Err(Invalid::Range);
                }
                if let Some(step) = step {
                    let k = (x - min) / step;
                    if (k - round(k)).abs() > 1e-9 * k.abs().max(1.0) {
                        return Err(Invalid::Range);
                    }
                }
                Ok(number(x))
            }
            (Self::Choice { options }, Value::Text(t)) if options.contains(t) => {
                Ok(Value::Text(t.clone()))
            }
            (Self::Choice { .. }, Value::Text(_)) => Err(Invalid::Range),
            (Self::Text { max } | Self::Code { max, .. }, Value::Text(t))
                if t.chars().count() <= *max as usize =>
            {
                Ok(Value::Text(t.clone()))
            }
            (Self::Text { .. } | Self::Code { .. }, Value::Text(_)) => Err(Invalid::Range),
            (Self::Item { of }, Value::Map(m)) => {
                let Some(Value::Text(id)) = m.get("id") else {
                    return Err(Invalid::Type);
                };
                let value = of.accept(m.get("value").unwrap_or(&Value::Null))?;
                let mut out = BTreeMap::from([("id".into(), Value::Text(id.clone()))]);
                if value != Value::Null {
                    out.insert("value".into(), value);
                }
                Ok(Value::Map(out))
            }
            (Self::Unknown, _) => Err(Invalid::Unsupported),
            _ => Err(Invalid::Type),
        }
    }

    pub fn reached(&self, reported: &Value, desired: &Value) -> bool {
        match self {
            Self::Trigger => true,
            Self::Item { of } => {
                let id = desired.get("id");
                let want = desired.get("value").unwrap_or(&Value::Null);
                match reported {
                    Value::List(items) => items
                        .iter()
                        .find(|i| i.get("id").is_some() && i.get("id") == id)
                        .is_some_and(|i| of.reached(i.get("value").unwrap_or(&Value::Null), want)),
                    _ => false,
                }
            }
            _ => match (reported.as_f64(), desired.as_f64()) {
                (Some(a), Some(b)) => (a - b).abs() <= 1e-9 * a.abs().max(b.abs()).max(1.0),
                _ => reported == desired,
            },
        }
    }
}

fn round(x: f64) -> f64 {
    let t = x as i64 as f64;
    if (x - t).abs() >= 0.5 {
        t + x.signum()
    } else {
        t
    }
}

fn number(x: f64) -> Value {
    if x == x as i64 as f64 {
        Value::from(x as i64)
    } else {
        Value::Float(x)
    }
}

impl Control {
    pub fn new(shape: Shape, danger: Danger) -> Self {
        Self { shape, danger }
    }
}

impl Shape {
    pub fn range(min: f64, max: f64) -> Self {
        Self::Range {
            min,
            max,
            step: None,
            unit: None,
        }
    }

    pub fn choice<S: Into<String>>(options: impl IntoIterator<Item = S>) -> Self {
        Self::Choice {
            options: options.into_iter().map(Into::into).collect(),
        }
    }

    pub fn code(max: u32, lang: Option<&str>) -> Self {
        Self::Code {
            max,
            lang: lang.map(Into::into),
        }
    }

    pub fn item(of: Shape) -> Self {
        Self::Item { of: Box::new(of) }
    }
}
