use std::collections::HashMap;
use std::sync::OnceLock;

use anyhow::{Result, ensure};

use crate::heap_size::HeapSize;
use crate::schema::TableSchema;
use crate::value::{Row, Tup2, ZWeight};

static ENABLED: OnceLock<bool> = OnceLock::new();
const MAX_KEYS: usize = 100_000;
const MAX_CHARGED_BYTES: usize = 512 * 1024 * 1024;

pub(crate) fn enabled() -> bool {
    *ENABLED.get_or_init(|| std::env::var("STALL_REPRO_COMMIT_MEMBERSHIP").is_ok_and(|value| value == "1"))
}

struct Images {
    before: Option<Row>,
    after: Option<Row>,
}

#[derive(Default)]
struct Rows {
    images: HashMap<String, Images>,
}

impl Rows {
    fn push(&mut self, key: String, row: &Row, weight: ZWeight) {
        let images =
            self.images.entry(key).or_insert_with(|| Images { before: (weight < 0).then(|| row.clone()), after: None });
        images.after = (weight > 0).then(|| row.clone());
    }

    fn finish(self) -> Vec<Tup2<Row, ZWeight>> {
        let mut delta = Vec::new();
        for images in self.images.into_values() {
            if images.before == images.after {
                continue;
            }
            if let Some(before) = images.before {
                delta.push(Tup2(before, -1));
            }
            if let Some(after) = images.after {
                delta.push(Tup2(after, 1));
            }
        }
        delta
    }
}

#[derive(Default)]
pub(crate) struct Commit {
    tables: Vec<(TableSchema, Rows)>,
    keys: usize,
    charged_bytes: usize,
}

impl Commit {
    pub(crate) fn push(&mut self, schema: &TableSchema, delta: &[Tup2<Row, ZWeight>]) -> Result<()> {
        let index = match self.tables.iter().position(|(existing, _)| existing.table == schema.table) {
            Some(index) => index,
            None => {
                self.tables.push((schema.clone(), Rows::default()));
                self.tables.len() - 1
            }
        };
        let rows = &mut self.tables[index].1;
        for Tup2(row, weight) in delta {
            ensure!(*weight == -1 || *weight == 1, "scratch commit batch requires unit row images");
            let key = schema.key_string(row)?;
            let additional_key = usize::from(!rows.images.contains_key(&key));
            let charge = row.heap_bytes().saturating_mul(2).saturating_add(key.len()).saturating_add(512);
            ensure!(self.keys + additional_key <= MAX_KEYS, "scratch commit batch exceeded 100000 keys; no receipt");
            ensure!(
                self.charged_bytes.saturating_add(charge) <= MAX_CHARGED_BYTES,
                "scratch commit batch exceeded 512 MiB charged images; no receipt"
            );
            self.keys += additional_key;
            self.charged_bytes += charge;
            rows.push(key, row, *weight);
        }
        Ok(())
    }

    pub(crate) fn finish(self) -> Vec<(TableSchema, Vec<Tup2<Row, ZWeight>>)> {
        self.tables.into_iter().map(|(schema, rows)| (schema, rows.finish())).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn row(value: i64) -> Row {
        Row(vec![Value::Int(value)])
    }

    #[test]
    fn folds_repeated_updates_to_first_old_and_last_new() {
        let mut rows = Rows::default();
        for (value, weight) in [(1, -1), (2, 1), (2, -1), (3, 1)] {
            rows.push("key".into(), &row(value), weight);
        }
        assert_eq!(rows.finish(), vec![Tup2(row(1), -1), Tup2(row(3), 1)]);
    }

    #[test]
    fn insert_then_delete_has_no_commit_effect() {
        let mut rows = Rows::default();
        rows.push("key".into(), &row(1), 1);
        rows.push("key".into(), &row(1), -1);
        assert!(rows.finish().is_empty());
    }

    #[test]
    fn update_then_delete_keeps_original_old_image() {
        let mut rows = Rows::default();
        for (value, weight) in [(1, -1), (2, 1), (2, -1)] {
            rows.push("key".into(), &row(value), weight);
        }
        assert_eq!(rows.finish(), vec![Tup2(row(1), -1)]);
    }
}
