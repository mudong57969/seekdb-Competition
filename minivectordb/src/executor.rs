use crate::error::Error;
use crate::parser::{self, Comparison, Expression, Ordering, Predicate, Projection, Statement};
use crate::schema::{Cell, ColumnType, Literal, Schema};
use crate::storage::{decode_record, write_record, Row, Storage, Table, Writer};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;

#[cfg_attr(test, derive(Default))]
pub struct Executor {
    storage: Storage,
}

impl Executor {
    pub fn open(path: PathBuf) -> std::io::Result<Self> {
        Ok(Self {
            storage: Storage::open(path)?,
        })
    }

    pub fn execute(&mut self, sql: &str) -> crate::protocol::ExecuteResult {
        self.storage
            .ensure_available()
            .and_then(|()| parser::parse(sql))
            .and_then(|statement| self.run(statement))
            .map_err(Error::response)
    }

    fn run(&mut self, statement: Statement) -> Result<Map<String, Value>, Error> {
        match statement {
            Statement::Begin => self.storage.begin()?,
            Statement::Commit => self.storage.commit()?,
            Statement::Rollback => self.storage.rollback()?,
            Statement::Select {
                table,
                projection,
                filters,
                order,
                limit,
            } => {
                return select(
                    &self.storage.table(&table)?,
                    projection,
                    filters,
                    order,
                    limit,
                );
            }
            statement => {
                return self
                    .storage
                    .write_statement(|storage| mutate(storage, statement))
            }
        }
        Ok(Map::new())
    }
}

fn mutate(storage: &Storage, statement: Statement) -> Result<Map<String, Value>, Error> {
    let count = match statement {
        Statement::Create(name, columns) => {
            storage.create(&name, &Schema::new(columns)?)?;
            return Ok(Map::new());
        }
        Statement::CreateVectorIndex {
            name,
            table,
            column,
        } => {
            let mut snapshot = storage.table(&table)?;
            snapshot.schema.add_vector_index(&name, &column)?;
            storage.rewrite(&table, &snapshot.schema, |writer| {
                for row in snapshot.scan()? {
                    write_record(writer, &row?)?;
                }
                Ok(())
            })?;
            return Ok(Map::new());
        }
        Statement::Insert(name, rows) => {
            let table = storage.table(&name)?;
            let mut inserts = BTreeMap::new();
            for values in rows {
                let row = table.schema.row(values)?;
                if inserts.insert(table.key(&row)?, row).is_some() {
                    return Err(Error::DuplicateKey);
                }
            }
            let count = inserts.len();
            storage.rewrite(&name, &table.schema, |writer| {
                merge(&table, writer, inserts, None)
            })?;
            count
        }
        Statement::Update(name, assignments, filters) => {
            let table = storage.table(&name)?;
            let mut columns = HashSet::new();
            let mut changes = Vec::new();
            for (name, value) in assignments {
                let index = table.schema.column(&name)?;
                if !columns.insert(index) {
                    return Err(Error::Syntax);
                }
                changes.push((index, table.schema.columns[index].kind.convert(value)?));
            }
            let filters = bind_filters(&table.schema, filters)?;
            if columns.contains(&table.schema.primary) {
                let mut moved = None;
                for row in table.scan()? {
                    let mut row = row?;
                    if matches(&row, &filters) {
                        if moved.is_some() {
                            return Err(Error::DuplicateKey);
                        }
                        let old = table.key(&row)?;
                        assign(&mut row, &changes);
                        moved = Some((old, row));
                    }
                }
                let Some((old, row)) = moved else {
                    return affected(0);
                };
                let inserts = BTreeMap::from([(table.key(&row)?, row)]);
                storage.rewrite(&name, &table.schema, |writer| {
                    merge(&table, writer, inserts, Some(old))
                })?;
                1
            } else {
                storage.rewrite(&name, &table.schema, |writer| {
                    let mut count = 0;
                    for row in table.scan()? {
                        let mut row = row?;
                        if matches(&row, &filters) {
                            assign(&mut row, &changes);
                            count += 1;
                        }
                        write_record(writer, &row)?;
                    }
                    Ok(count)
                })?
            }
        }
        Statement::Delete(name, filters) => {
            let table = storage.table(&name)?;
            let filters = bind_filters(&table.schema, filters)?;
            storage.rewrite(&name, &table.schema, |writer| {
                let mut count = 0;
                for row in table.scan()? {
                    let row = row?;
                    if matches(&row, &filters) {
                        count += 1;
                    } else {
                        write_record(writer, &row)?;
                    }
                }
                Ok(count)
            })?
        }
        _ => unreachable!("transaction controls and reads are handled by Executor"),
    };
    affected(count)
}

fn affected(count: usize) -> Result<Map<String, Value>, Error> {
    Ok(Map::from_iter([("affected_rows".into(), json!(count))]))
}

fn assign(row: &mut Row, changes: &[(usize, Cell)]) {
    for (index, value) in changes {
        row[*index] = value.clone();
    }
}

fn merge(
    table: &Table,
    writer: &mut Writer,
    inserts: BTreeMap<i64, Row>,
    removed: Option<i64>,
) -> Result<(), Error> {
    let mut inserts = inserts.into_iter().peekable();
    for row in table.scan()? {
        let row = row?;
        let key = table.key(&row)?;
        if Some(key) == removed {
            continue;
        }
        while let Some((_, row)) = inserts.next_if(|(new_key, _)| *new_key < key) {
            write_record(writer, &row)?;
        }
        if inserts.peek().is_some_and(|(new_key, _)| *new_key == key) {
            return Err(Error::DuplicateKey);
        }
        write_record(writer, &row)?;
    }
    for (_, row) in inserts {
        write_record(writer, &row)?;
    }
    Ok(())
}

struct BoundDistance {
    column: usize,
    query: Vec<f32>,
}

impl BoundDistance {
    fn bind(schema: &Schema, column: &str, query: Literal) -> Result<Self, Error> {
        let column = schema.column(column)?;
        let Cell::Vector(query) = schema.columns[column].kind.convert(query)? else {
            return Err(Error::InvalidValue);
        };
        Ok(Self { column, query })
    }

    fn value(&self, row: &Row) -> f64 {
        let Cell::Vector(vector) = &row[self.column] else {
            unreachable!("bound vector column");
        };
        let squared: f64 = vector
            .iter()
            .zip(&self.query)
            .map(|(&left, &right)| {
                let difference = f64::from(left) - f64::from(right);
                difference * difference
            })
            .sum();
        squared.sqrt()
    }
}

enum BoundExpr {
    Column(usize),
    Distance(BoundDistance),
}

impl BoundExpr {
    fn value(&self, row: &Row) -> Value {
        match self {
            Self::Column(index) => row[*index].json(),
            Self::Distance(distance) => json!(distance.value(row)),
        }
    }
}

enum SortKey<'a> {
    Column(usize),
    ProjectedDistance(&'a BoundDistance),
    Distance(BoundDistance),
}

enum SortValue {
    Column(Cell),
    Distance(f64),
}

impl SortValue {
    fn compare(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Column(a), Self::Column(b)) => a.compare(b).expect("bound scalar ordering"),
            (Self::Distance(a), Self::Distance(b)) => a.total_cmp(b),
            _ => unreachable!("bound sort key"),
        }
    }
}

struct Candidate {
    key: i64,
    values: Vec<Value>,
    sort: Vec<SortValue>,
    bytes: usize,
}

fn select(
    table: &Table,
    projection: Option<Vec<Projection>>,
    filters: Vec<Predicate>,
    order: Vec<Ordering>,
    limit: usize,
) -> Result<Map<String, Value>, Error> {
    let schema = &table.schema;
    let projection: Vec<(BoundExpr, String)> = match projection {
        None => schema
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| (BoundExpr::Column(index), column.name.clone()))
            .collect(),
        Some(projection) => projection
            .into_iter()
            .map(|Projection(expr, alias)| match expr {
                Expression::Column(name) => {
                    let index = schema.column(&name)?;
                    Ok((
                        BoundExpr::Column(index),
                        alias.unwrap_or_else(|| schema.columns[index].name.clone()),
                    ))
                }
                Expression::Distance(name, query) => Ok((
                    BoundExpr::Distance(BoundDistance::bind(schema, &name, query)?),
                    alias.unwrap_or_else(|| "L2_DISTANCE".into()),
                )),
            })
            .collect::<Result<_, Error>>()?,
    };
    let filters = bind_filters(schema, filters)?;
    let order: Vec<(SortKey<'_>, bool)> = order
        .into_iter()
        .map(|Ordering(expression, descending)| {
            let name = match expression {
                Expression::Distance(name, query) => {
                    return Ok((
                        SortKey::Distance(BoundDistance::bind(schema, &name, query)?),
                        descending,
                    ))
                }
                Expression::Column(name) => name,
            };
            let column = match projection
                .iter()
                .find(|(_, label)| label.eq_ignore_ascii_case(&name))
            {
                Some((BoundExpr::Column(index), _)) => *index,
                Some((BoundExpr::Distance(distance), _)) => {
                    return Ok((SortKey::ProjectedDistance(distance), descending))
                }
                None => schema.column(&name)?,
            };
            if !schema.columns[column].kind.is_orderable() {
                return Err(Error::Unsupported);
            }
            Ok((SortKey::Column(column), descending))
        })
        .collect::<Result<_, Error>>()?;
    let compare = |a: &Candidate, b: &Candidate| {
        a.sort
            .iter()
            .zip(&b.sort)
            .zip(&order)
            .map(|((a, b), (_, descending))| {
                let order = a.compare(b);
                if *descending {
                    order.reverse()
                } else {
                    order
                }
            })
            .find(|order| !order.is_eq())
            .unwrap_or_else(|| a.key.cmp(&b.key))
    };
    // Bound materialized results and sort keys. Disk scans themselves retain one row.
    const QUERY_BUDGET: usize = 64 * 1024 * 1024;
    let mut bytes = 0;
    let mut rows: Vec<Candidate> = Vec::new();
    if limit != 0 {
        let filter_before_decode = !filters.is_empty()
            && schema
                .columns
                .iter()
                .any(|column| matches!(column.kind, ColumnType::Vector(_)));
        let scan: Box<dyn Iterator<Item = Result<Row, Error>>> = if filter_before_decode {
            // When many rows pass, parsing the same row twice costs more than it saves.
            // Sampling only selects a scan strategy; all decoded rows are still filtered.
            const SAMPLE_ROWS: usize = 32;
            let mut sampled = 0;
            let mut accepted = 0;
            let mut prefilter = true;
            let filters = &filters;
            Box::new(table.scan_where(move |line| {
                if !prefilter {
                    return Ok(true);
                }
                let matches = matches_snapshot(line, filters)?;
                if sampled < SAMPLE_ROWS {
                    sampled += 1;
                    accepted += usize::from(matches);
                    if sampled == SAMPLE_ROWS && accepted > SAMPLE_ROWS / 4 {
                        prefilter = false;
                    }
                }
                Ok(matches)
            })?)
        } else {
            Box::new(table.scan()?)
        };
        for row in scan {
            let row = row?;
            if !matches(&row, &filters) {
                continue;
            }
            let sort = order
                .iter()
                .map(|(key, _)| match key {
                    SortKey::Column(index) => SortValue::Column(row[*index].clone()),
                    SortKey::ProjectedDistance(distance) => {
                        SortValue::Distance(distance.value(&row))
                    }
                    SortKey::Distance(distance) => SortValue::Distance(distance.value(&row)),
                })
                .collect::<Vec<_>>();
            let mut candidate = Candidate {
                key: table.key(&row)?,
                values: Vec::new(),
                sort,
                bytes: 0,
            };
            let insertion = if !order.is_empty() && limit != usize::MAX {
                let index = rows.partition_point(|row| compare(row, &candidate).is_le());
                if index >= limit {
                    continue;
                }
                Some(index)
            } else {
                None
            };
            candidate.values = projection
                .iter()
                .map(|(expr, _)| expr.value(&row))
                .collect();
            let size = 128
                + candidate.values.iter().map(value_memory).sum::<usize>()
                + candidate
                    .sort
                    .iter()
                    .map(|key| match key {
                        SortValue::Column(Cell::Text(text)) => 64 + text.len(),
                        _ => 64,
                    })
                    .sum::<usize>();
            candidate.bytes = size;
            if let Some(index) = insertion {
                if rows.len() == limit {
                    bytes -= rows.pop().expect("full limit").bytes;
                }
                rows.insert(index, candidate);
            } else {
                rows.push(candidate);
            }
            bytes += size;
            if bytes > QUERY_BUDGET {
                return Err(Error::QueryTooLarge);
            }
            if order.is_empty() && rows.len() == limit {
                break;
            }
        }
    }
    if !order.is_empty() && limit == usize::MAX {
        rows.sort_unstable_by(compare);
    }
    let columns: Vec<_> = projection.into_iter().map(|(_, label)| label).collect();
    let rows: Vec<_> = rows.into_iter().map(|candidate| candidate.values).collect();
    Ok(Map::from_iter([
        ("columns".into(), json!(columns)),
        (
            "rows".into(),
            Value::Array(rows.into_iter().map(Value::Array).collect()),
        ),
    ]))
}

fn value_memory(value: &Value) -> usize {
    64 + match value {
        Value::String(text) => text.len(),
        Value::Array(values) => values.iter().map(value_memory).sum(),
        _ => 0,
    }
}
fn bind_filters(
    schema: &Schema,
    filters: Vec<Predicate>,
) -> Result<Vec<(usize, Comparison, Cell)>, Error> {
    filters
        .into_iter()
        .map(|Predicate(name, op, value)| {
            let index = schema.column(&name)?;
            let kind = &schema.columns[index].kind;
            if !kind.is_orderable() {
                return Err(Error::Unsupported);
            }
            let value = match (kind, value) {
                (ColumnType::Varchar(_), Literal::Text(value)) => Cell::Text(value),
                (kind, value) => kind.convert(value)?,
            };
            Ok((index, op, value))
        })
        .collect()
}

fn matches_snapshot(line: &[u8], filters: &[(usize, Comparison, Cell)]) -> Result<bool, Error> {
    // Borrow raw cells so a rejected row never materializes its vector.
    let cells: Vec<&serde_json::value::RawValue> = decode_record(line)?;
    for (index, op, expected) in filters {
        let raw = cells.get(*index).ok_or(Error::Storage)?;
        let actual: Cell = decode_record(raw.get().as_bytes())?;
        if !matches_value(&actual, *op, expected) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn matches(row: &Row, filters: &[(usize, Comparison, Cell)]) -> bool {
    filters
        .iter()
        .all(|(index, op, value)| matches_value(&row[*index], *op, value))
}

fn matches_value(actual: &Cell, op: Comparison, expected: &Cell) -> bool {
    let order = actual.compare(expected).expect("bound scalar comparison");
    match op {
        Comparison::Equal => order.is_eq(),
        Comparison::Less => order.is_lt(),
        Comparison::LessEqual => order.is_le(),
        Comparison::Greater => order.is_gt(),
        Comparison::GreaterEqual => order.is_ge(),
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;
    use serde_json::{json, Value};

    fn run(db: &mut Executor, sql: &str) -> Value {
        Value::Object(db.execute(sql).unwrap())
    }

    #[test]
    fn crud_and_projection() {
        let mut db = Executor::default();
        run(
            &mut db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(4));",
        );
        assert_eq!(
            run(&mut db, "INSERT INTO t VALUES (1, '甲'), (2, '乙')")["affected_rows"],
            2
        );
        assert_eq!(
            run(&mut db, "UPDATE t SET name = '丙' WHERE id = 1")["affected_rows"],
            1
        );
        assert_eq!(
            run(&mut db, "SELECT name AS label FROM t WHERE id <= 1"),
            json!({"columns":["label"],"rows":[["丙"]]})
        );
        assert_eq!(
            run(&mut db, "DELETE FROM t WHERE id > 1")["affected_rows"],
            1
        );
        assert_eq!(
            run(&mut db, "SELECT * FROM t"),
            json!({"columns":["id","name"],"rows":[[1,"丙"]]})
        );
    }

    #[test]
    fn types_filters_and_order() {
        let mut db = Executor::default();
        run(
            &mut db,
            "create table T (id bigint primary key, name varchar(2), vec vector(2))",
        );
        run(&mut db, "INSERT INTO t VALUES (-9223372036854775808, '甲😀', '[1,2]'), (9007199254740993, '乙', '[3,4]')");
        assert_eq!(
            run(&mut db, "SELECT ID AS k, vec FROM T WHERE id >= -9223372036854775808 AND id < 9223372036854775807 ORDER BY k DESC LIMIT 1"),
            json!({"columns":["k","vec"],"rows":[[9007199254740993_i64,[3.0,4.0]]]})
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (3, '甲乙😀', '[0,0]')")
                .unwrap_err()
                .0,
            "value_too_long"
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (9223372036854775808, '甲', '[0,0]')")
                .unwrap_err()
                .0,
            "invalid_value"
        );
        assert_eq!(
            db.execute("INSERT INTO t VALUES (3, '甲', '[1e39,0]')")
                .unwrap_err()
                .0,
            "invalid_value"
        );
    }

    #[test]
    fn errors_leave_table_unchanged() {
        let mut db = Executor::default();
        assert!(db
            .execute("CREATE TABLE bad (id VARCHAR(2) PRIMARY KEY)")
            .is_err());
        run(
            &mut db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(2), vec VECTOR(2))",
        );
        run(&mut db, "INSERT INTO t VALUES (1, '甲', '[0,0]')");
        for (sql, expected) in [
            (
                "INSERT INTO t VALUES (2, '乙', '[0,0]'), (1, '丙', '[0,0]')",
                "duplicate_key",
            ),
            (
                "INSERT INTO t VALUES (2, '乙', '[0,0]'), (3, '丙', '[0]')",
                "dimension_mismatch",
            ),
            (
                "INSERT INTO t VALUES (2, '乙', '[0,0]'), (2, '丙', '[0,0]')",
                "duplicate_key",
            ),
            ("ALTER TABLE t ADD extra BIGINT", "unsupported_sql"),
            ("DELETE FROM t; SELECT id FROM t", "syntax_error"),
            ("UPDATE t SET absent = 1", "unknown_column"),
        ] {
            assert_eq!(db.execute(sql).unwrap_err().0, expected);
        }
        assert_eq!(run(&mut db, "SELECT id FROM t")["rows"], json!([[1]]));
    }

    #[test]
    fn primary_key_update_is_atomic() {
        let mut db = Executor::default();
        run(
            &mut db,
            "CREATE TABLE t (name VARCHAR(4), id BIGINT PRIMARY KEY)",
        );
        run(&mut db, "INSERT INTO t VALUES ('甲', 1), ('乙', 2)");
        assert_eq!(
            db.execute("UPDATE t SET id = 3").unwrap_err().0,
            "duplicate_key"
        );
        assert_eq!(run(&mut db, "SELECT id FROM t")["rows"], json!([[1], [2]]));
        run(&mut db, "UPDATE t SET id = 3 WHERE id = 1");
        assert_eq!(
            run(&mut db, "SELECT id FROM t ORDER BY id ASC")["rows"],
            json!([[2], [3]])
        );
    }

    #[test]
    fn varchar_filter_literal_is_not_a_stored_value() {
        let mut db = Executor::default();
        run(
            &mut db,
            "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(1))",
        );
        run(&mut db, "INSERT INTO t VALUES (1, '甲')");
        assert_eq!(
            run(&mut db, "SELECT id FROM t WHERE name = '甲乙'")["rows"],
            json!([])
        );
    }
}

#[cfg(test)]
mod transaction_tests {
    use super::Executor;
    use serde_json::json;

    #[test]
    fn reads_own_writes_and_failed_statement_preserves_prior_work() {
        let mut db = Executor::default();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(4))")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1,'old')").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET name = 'new' WHERE id = 1")
            .unwrap();
        assert!(db
            .execute("INSERT INTO t VALUES (2,'new'), (1,'dup')")
            .is_err());
        assert_eq!(
            db.execute("SELECT * FROM t").unwrap()["rows"],
            json!([[1, "new"]])
        );
        db.execute("COMMIT").unwrap();
        db.execute("BEGIN").unwrap();
        db.execute("DELETE FROM t").unwrap();
        db.execute("ROLLBACK").unwrap();
        assert_eq!(
            db.execute("SELECT * FROM t").unwrap()["rows"],
            json!([[1, "new"]])
        );
    }
}

#[cfg(test)]
mod vector_tests {
    use super::Executor;
    use serde_json::json;

    #[test]
    fn rejects_second_vector_column_without_creating_a_table() {
        for explicit in [false, true] {
            let mut db = Executor::default();
            if explicit {
                db.execute("BEGIN").unwrap();
            }
            assert_eq!(
                db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, a VECTOR(2), label VARCHAR(8), b VECTOR(3))")
                    .unwrap_err().0,
                "invalid_schema"
            );
            assert_eq!(
                db.execute("SELECT * FROM t").unwrap_err().0,
                "unknown_table"
            );
            db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, a VECTOR(2))")
                .unwrap();
            if explicit {
                db.execute("COMMIT").unwrap();
            }
            assert_eq!(
                db.execute("SELECT * FROM t").unwrap()["columns"],
                json!(["id", "a"])
            );
        }
    }

    #[test]
    fn filter_remains_active_after_sampling_switches_scan_strategy() {
        let mut db = Executor::default();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, bucket BIGINT, v VECTOR(1))")
            .unwrap();
        let rows = (0..40)
            .map(|id| format!("({id},{},'[{id}]')", usize::from(id >= 32)))
            .collect::<Vec<_>>()
            .join(",");
        db.execute(&format!("INSERT INTO t VALUES {rows}")).unwrap();
        let result = db
            .execute("SELECT id FROM t WHERE bucket = 0 ORDER BY L2_DISTANCE(v,'[39]') LIMIT 2")
            .unwrap();
        assert_eq!(result["rows"], json!([[31], [30]]));
    }

    #[test]
    fn filters_after_vector_preserve_visibility_and_scalar_precision() {
        let mut db = Executor::default();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VECTOR(2), label VARCHAR(4))")
            .unwrap();
        db.execute("INSERT INTO t VALUES (-9223372036854775808,'[1,0]','甲'), (9223372036854775807,'[0,0]','甲'), (0,'[2,0]','乙')").unwrap();
        let sql = "SELECT id FROM t WHERE label = '甲' AND id > -1 ORDER BY L2_DISTANCE(v,'[0,0]') LIMIT 1";
        assert_eq!(db.execute(sql).unwrap()["rows"], json!([[i64::MAX]]));
        db.execute("BEGIN").unwrap();
        db.execute("UPDATE t SET label = '乙' WHERE id = 9223372036854775807")
            .unwrap();
        assert_eq!(db.execute(sql).unwrap()["rows"], json!([]));
        db.execute("ROLLBACK").unwrap();
        assert_eq!(db.execute(sql).unwrap()["rows"], json!([[i64::MAX]]));
        assert_eq!(
            db.execute("SELECT id FROM t ORDER BY id LIMIT 0").unwrap()["rows"],
            json!([])
        );
    }

    #[test]
    fn direct_distance_sort_filters_and_breaks_ties_by_primary_key() {
        let mut db = Executor::default();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, category VARCHAR(8), v VECTOR(2))")
            .unwrap();
        db.execute(
            "INSERT INTO t VALUES (9,'keep','[1,0]'), (2,'keep','[-1,0]'), (1,'skip','[0,0]')",
        )
        .unwrap();
        let result = db.execute("SELECT id FROM t WHERE category = 'keep' ORDER BY L2_DISTANCE(v, '[0,0]') LIMIT 10").unwrap();
        assert_eq!(result["rows"], json!([[2], [9]]));
    }

    #[test]
    fn index_definition_rolls_back_and_requires_vector_column() {
        let mut db = Executor::default();
        db.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, v VECTOR(2))")
            .unwrap();
        assert!(db.execute("CREATE VECTOR INDEX vec ON t(id)").is_err());
        db.execute("BEGIN").unwrap();
        db.execute("CREATE VECTOR INDEX vec ON t(v)").unwrap();
        assert!(db.execute("CREATE VECTOR INDEX vec ON t(v)").is_err());
        db.execute("ROLLBACK").unwrap();
        db.execute("CREATE VECTOR INDEX vec ON t(v)").unwrap();
        assert!(db.execute("CREATE VECTOR INDEX VEC ON t(v)").is_err());
    }
}
