use crate::error::Error;
use std::cmp::Ordering;
use std::collections::HashSet;

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ColumnType {
    BigInt,
    Varchar(usize),
    Vector(usize),
}

pub enum Literal {
    Integer(i64),
    Text(String),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub enum Cell {
    BigInt(i64),
    Text(String),
    Vector(Vec<f32>),
}

impl Cell {
    pub fn compare(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Self::BigInt(a), Self::BigInt(b)) => Some(a.cmp(b)),
            (Self::Text(a), Self::Text(b)) => Some(a.cmp(b)),
            _ => None,
        }
    }
}

impl ColumnType {
    pub fn convert(&self, literal: Literal) -> Result<Cell, Error> {
        match (self, literal) {
            (Self::BigInt, Literal::Integer(value)) => Ok(Cell::BigInt(value)),
            (Self::Varchar(limit), Literal::Text(value)) => {
                if value.chars().count() > *limit {
                    return Err(Error::ValueTooLong);
                }
                Ok(Cell::Text(value))
            }
            (Self::Vector(dimension), Literal::Text(value)) => {
                let numbers: Vec<serde_json::Number> =
                    serde_json::from_str(&value).map_err(|_| Error::InvalidValue)?;
                // Preserve the decimal lexeme to avoid rounding through float64.
                let vector: Vec<f32> = numbers
                    .iter()
                    .map(|number| number.to_string().parse().map_err(|_| Error::InvalidValue))
                    .collect::<Result<_, _>>()?;
                if vector.len() != *dimension {
                    return Err(Error::DimensionMismatch);
                }
                if vector.iter().any(|value| !value.is_finite()) {
                    return Err(Error::InvalidValue);
                }
                Ok(Cell::Vector(vector))
            }
            _ => Err(Error::InvalidValue),
        }
    }

    pub fn is_orderable(&self) -> bool {
        !matches!(self, Self::Vector(_))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Column {
    pub name: String,
    pub kind: ColumnType,
    pub primary: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Schema {
    pub columns: Vec<Column>,
    pub primary: usize,
    #[serde(default)]
    vector_indexes: std::collections::BTreeMap<String, usize>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Result<Self, Error> {
        if columns
            .iter()
            .filter(|column| matches!(column.kind, ColumnType::Vector(_)))
            .nth(1)
            .is_some()
        {
            return Err(Error::InvalidSchema);
        }

        let mut names = HashSet::new();
        let mut primary = None;
        for (index, column) in columns.iter().enumerate() {
            if !names.insert(column.name.to_ascii_lowercase()) {
                return Err(Error::InvalidSchema);
            }
            if column.primary {
                if column.kind != ColumnType::BigInt || primary.is_some() {
                    return Err(Error::InvalidSchema);
                }
                primary = Some(index);
            }
        }
        Ok(Self {
            columns,
            primary: primary.ok_or(Error::InvalidSchema)?,
            vector_indexes: Default::default(),
        })
    }

    pub fn add_vector_index(&mut self, name: &str, column: &str) -> Result<(), Error> {
        let column = self.column(column)?;
        if !matches!(self.columns[column].kind, ColumnType::Vector(_)) {
            return Err(Error::InvalidValue);
        }
        match self.vector_indexes.entry(name.to_ascii_lowercase()) {
            std::collections::btree_map::Entry::Occupied(_) => Err(Error::IndexExists),
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(column);
                Ok(())
            }
        }
    }

    pub fn column(&self, name: &str) -> Result<usize, Error> {
        self.columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(name))
            .ok_or(Error::UnknownColumn)
    }

    pub fn row(&self, values: Vec<Literal>) -> Result<Vec<Cell>, Error> {
        if values.len() != self.columns.len() {
            return Err(Error::InvalidValue);
        }
        self.columns
            .iter()
            .zip(values)
            .map(|(column, value)| column.kind.convert(value))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{Cell, ColumnType, Literal};

    #[test]
    fn parses_decimal_directly_to_float32_and_rejects_invalid_vectors() {
        let kind = ColumnType::Vector(1);
        let Cell::Vector(value) = kind
            .convert(Literal::Text("[1.0000000596046448]".into()))
            .unwrap()
        else {
            panic!("expected vector");
        };
        assert_eq!(value[0].to_bits(), 1.0_f32.to_bits() + 1);
        for text in [
            "[]",
            "[1,2]",
            "[NaN]",
            "[Infinity]",
            "[-Infinity]",
            "[3.5e38]",
        ] {
            assert!(kind.convert(Literal::Text(text.into())).is_err(), "{text}");
        }
    }
}
