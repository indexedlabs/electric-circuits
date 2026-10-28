//! Characterization for the dbsp upgrade: production circuit types keep their ordering and
//! equality when archived and when searched in actual spill files. No database is needed.

use std::collections::BTreeMap;

use dbsp::algebra::F64;
use dbsp::circuit::{CircuitConfig, CircuitStorageConfig, StorageConfig, StorageOptions};
use dbsp::dynamic::{DowncastTrait, Erase, OrdRepr};
use dbsp::storage::file::{Deserializer, to_bytes};
use dbsp::trace::{Batch, BatchLocation, BatchReader as _, Cursor};
use dbsp::typed_batch::{BatchReader, FileIndexedZSet, FileZSet, IndexedZSetReader};
use dbsp::{DBData, Runtime};
use electric_circuits_engine::subq_circuit::PkKey;
use electric_circuits_engine::value::{Row, Tup2, Value};
use rkyv::Deserialize;

fn with_storage(test: impl FnOnce() + Clone + Send + 'static) {
    let dir = std::env::temp_dir().join(format!("dbsp-upgrade-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&dir).unwrap();
    let storage = CircuitStorageConfig::for_config(
        StorageConfig { path: dir.to_string_lossy().into_owned(), cache: Default::default() },
        StorageOptions { cache_mib: Some(8), ..StorageOptions::default() },
    )
    .unwrap();
    // FileZSet/FileIndexedZSet always write files, regardless of the spill thresholds or
    // background merge timing. Leave compression at the upstream default used in production.
    let result = Runtime::run(CircuitConfig::with_workers(1).with_storage(Some(storage)), move |_| test())
        .unwrap()
        .join();
    std::fs::remove_dir_all(&dir).unwrap();
    result.unwrap();
}

fn assert_archive_order<T: DBData>(values: &[T]) {
    for value in values {
        let bytes = to_bytes(value).unwrap();
        // SAFETY: to_bytes produced this aligned buffer from a T; it remains alive and
        // unmodified while its archived root is read.
        let archived = unsafe { rkyv::archived_root::<T>(&bytes) };
        let decoded: T = archived.deserialize(&mut Deserializer::default()).unwrap();
        assert_eq!(&decoded, value);
        for other in values {
            assert_eq!(archived.ord_cmp(other), decoded.cmp(other), "{value:?} vs {other:?}");
        }
    }
}

fn reopen<B: Batch>(batch: &B) -> B {
    assert_eq!(batch.location(), BatchLocation::Storage);
    assert!(!batch.is_empty());
    assert!(batch.approximate_byte_size() > 0);
    let file = batch.file_reader().expect("a spill file, not just runtime metadata");
    let path = file.path().to_string().into();
    B::from_path(&batch.factories(), &path).unwrap()
}

fn assert_spill_searches<K: DBData>(keys: &[K], queries: &[K]) {
    assert_archive_order(keys);
    let mut expected = BTreeMap::new();
    for key in keys {
        *expected.entry(key.clone()).or_insert(0i64) += 1;
    }
    let written = FileZSet::<K>::from_keys((), keys.iter().cloned().map(|key| Tup2(key, 1)).collect());
    let batch = FileZSet::<K>::from_inner(reopen(written.inner()));
    drop(written);
    let decoded: Vec<_> = batch.iter().map(|(key, (), weight)| (key, weight)).collect();
    assert_eq!(decoded, expected.iter().map(|(key, weight)| (key.clone(), *weight)).collect::<Vec<_>>());

    let mut cursor = batch.inner().cursor();
    for query in queries {
        cursor.rewind_keys();
        cursor.seek_key(query.erase());
        let expected_next = expected.range(query.clone()..).next();
        assert_eq!(cursor.key_valid(), expected_next.is_some(), "seek {query:?}");
        if let Some((key, weight)) = expected_next {
            // SAFETY: this cursor belongs to a FileZSet<K>, whose key factory is K.
            let found = unsafe { cursor.key().downcast::<K>() };
            assert_eq!(found, key, "seek {query:?}");
            assert_eq!(**cursor.weight(), *weight, "weight after seek {query:?}");
        }
    }
}

#[test]
fn spill_round_trips_and_batch_searches_cover_every_cell_type() {
    with_storage(|| {
        let cells = vec![
            Value::Null,
            Value::Int(i64::MIN),
            Value::Int(0),
            Value::Int(i64::MAX),
            Value::Text(String::new()),
            Value::Text("a".into()),
            Value::Text("a\0suffix".into()),
            Value::Text("é🦀".repeat(1024)),
            Value::Bool(false),
            Value::Bool(true),
            Value::Float(F64::new(-1.5)),
            Value::Float(F64::new(1.5)),
        ];
        let mut queries = cells.clone();
        queries.extend([Value::Int(-1), Value::Text("ab".into()), Value::Float(F64::new(f64::INFINITY))]);
        assert_spill_searches(&cells, &queries);

        // Row prefix ordering is used by membership seeks; include a shorter prefix and a
        // longer same-prefix row, as well as an absent node on either side of the stored node.
        let rows: Vec<_> = cells.iter().map(|cell| Row(vec![Value::Int(7), cell.clone()])).collect();
        let mut row_queries: Vec<_> = queries.iter().map(|cell| Row(vec![Value::Int(7), cell.clone()])).collect();
        row_queries.extend([
            Row(vec![]),
            Row(vec![Value::Int(6)]),
            Row(vec![Value::Int(7)]),
            Row(vec![Value::Int(7), Value::Null, Value::Null]),
            Row(vec![Value::Int(8)]),
        ]);
        assert_spill_searches(&rows, &row_queries);
        assert_archive_order(&row_queries);

        let keys = [
            PkKey { id: i64::MIN, pk: 0 },
            PkKey { id: 7, pk: 0 },
            PkKey { id: 7, pk: u32::MAX },
            PkKey { id: i64::MAX, pk: u32::MAX },
        ];
        let mut key_queries = keys.to_vec();
        key_queries.extend([PkKey { id: 7, pk: 1 }, PkKey { id: 8, pk: 0 }]);
        assert_spill_searches(&keys, &key_queries);

        // The contributor map also stores Value as a value (rather than as part of a key).
        let tuples: Vec<_> = cells
            .into_iter()
            .enumerate()
            .map(|(pk, value)| Tup2(Tup2(PkKey { id: 7, pk: pk as u32 }, value), 1))
            .collect();
        let written = FileIndexedZSet::<PkKey, Value>::from_tuples((), tuples.clone());
        let batch = FileIndexedZSet::<PkKey, Value>::from_inner(reopen(written.inner()));
        drop(written);
        let expected: Vec<_> = tuples.into_iter().map(|Tup2(Tup2(key, value), weight)| (key, value, weight)).collect();
        assert_eq!(batch.iter().collect::<Vec<_>>(), expected);
    });
}

#[test]
fn float_order_and_equality_survive_spill() {
    with_storage(|| {
        let float = |value| Value::Float(F64::new(value));
        // This is the ordered-float 3 contract: NaNs are one greatest equivalence class,
        // signed zeros are equal, and infinities bracket the finite values.
        let classes = [
            vec![f64::NEG_INFINITY],
            vec![-1.5],
            vec![-0.0, 0.0],
            vec![1.5],
            vec![f64::INFINITY],
            vec![f64::NAN, f64::from_bits(0xfff8_0000_0000_0001)],
        ];
        for (i, lhs) in classes.iter().enumerate() {
            for (j, rhs) in classes.iter().enumerate() {
                for &left in lhs {
                    for &right in rhs {
                        assert_eq!(float(left).cmp(&float(right)), i.cmp(&j));
                        assert_eq!(float(left) == float(right), i == j);
                    }
                }
            }
        }
        let cells: Vec<_> = classes.iter().flatten().copied().map(float).collect();
        assert_spill_searches(&cells, &cells);
        let rows: Vec<_> = cells.iter().map(|cell| Row(vec![Value::Int(7), cell.clone()])).collect();
        assert_spill_searches(&rows, &rows);

        // Equivalent values must cancel across a spill, including distinct NaN bit patterns.
        let positive = FileZSet::<Value>::from_keys((), vec![Tup2(float(-0.0), 1), Tup2(float(f64::NAN), 1)]);
        let negative = FileZSet::<Value>::from_keys(
            (),
            vec![Tup2(float(0.0), -1), Tup2(float(f64::from_bits(0xfff8_0000_0000_0001)), -1)],
        );
        let positive = FileZSet::<Value>::from_inner(reopen(positive.inner()));
        let negative = FileZSet::<Value>::from_inner(reopen(negative.inner()));
        assert!(positive.merge(&negative).is_empty());
    });
}
