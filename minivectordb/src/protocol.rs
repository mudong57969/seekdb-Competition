use serde::Serialize;
use serde_json::{json, Map, Value};
use std::io::{self, BufRead, Write};

#[derive(Clone, Copy)]
pub enum SearchMode {
    Exact,
    Ann,
}

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum Output {
    Fields(Map<String, Value>),
    Query {
        columns: Vec<String>,
        rows: Vec<Vec<crate::executor::ProjectedValue>>,
    },
}

pub type ExecuteResult = Result<Output, (&'static str, &'static str)>;

#[derive(Serialize)]
struct Success<'a> {
    id: &'a Value,
    ok: bool,
    #[serde(flatten)]
    output: Output,
}

fn emit(output: &mut impl Write, response: &impl Serialize) -> io::Result<()> {
    serde_json::to_writer(&mut *output, response)?;
    output.write_all(b"\n")?;
    output.flush()
}

pub fn serve(
    mut input: impl BufRead,
    mut output: impl Write,
    mut execute: impl FnMut(&str, SearchMode) -> ExecuteResult,
) -> io::Result<()> {
    emit(&mut output, &json!({"type":"ready"}))?;

    let mut line = Vec::new();
    loop {
        line.clear();
        if input.read_until(b'\n', &mut line)? == 0 {
            return Ok(());
        }
        let request = match serde_json::from_slice::<Value>(&line) {
            Ok(request) => request,
            Err(error) => {
                eprintln!("Invalid JSON request: {error}");
                continue;
            }
        };
        let Some(id) = request.get("id") else {
            eprintln!("Missing request id");
            continue;
        };

        let result = match request.get("op").and_then(Value::as_str) {
            Some("shutdown") => {
                drop(execute);
                return emit(&mut output, &json!({"id":id,"ok":true}));
            }
            Some("execute") => {
                let mode = match request.get("search_mode") {
                    None => Some(SearchMode::Exact),
                    Some(value) if value == "exact" => Some(SearchMode::Exact),
                    Some(value) if value == "ann" => Some(SearchMode::Ann),
                    _ => None,
                };
                match (request.get("sql").and_then(Value::as_str), mode) {
                    (Some(sql), Some(mode)) => execute(sql, mode),
                    _ => Err((
                        "INVALID_REQUEST",
                        "Expected SQL text and search mode exact or ann",
                    )),
                }
            }
            Some(_) => Err(("UNSUPPORTED_OP", "Unsupported operation")),
            None => Err(("INVALID_REQUEST", "Expected an operation name")),
        };
        match result {
            Ok(output_fields) => emit(
                &mut output,
                &Success { id, ok: true, output: output_fields },
            )?,
            Err((code, message)) => emit(
                &mut output,
                &json!({"id": id, "ok": false, "error": {"code": code, "message": message}}),
            )?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::serve;
    use serde_json::{json, Value};

    fn exchange(input: &[u8]) -> Vec<Value> {
        let mut output = Vec::new();
        serve(input, &mut output, crate::execute_sql).unwrap();
        std::str::from_utf8(&output)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    #[test]
    fn ready_and_shutdown() {
        assert_eq!(
            exchange(b"{\"id\":\"stop\",\"op\":\"shutdown\"}\n{\"id\":2,\"op\":\"shutdown\"}\n"),
            [json!({"type":"ready"}), json!({"id":"stop","ok":true})]
        );
    }

    #[test]
    fn execute_reports_unimplemented_and_preserves_large_id() {
        let responses = exchange(
            b"{\"id\":9007199254740993,\"op\":\"execute\",\"sql\":\"SELECT 1;\",\"search_mode\":\"ann\"}\n",
        );
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[1]["id"], json!(9007199254740993_u64));
        assert_eq!(responses[1]["ok"], false);
        assert_eq!(responses[1]["error"]["code"], "NOT_IMPLEMENTED");
    }

    #[test]
    fn invalid_input_does_not_interrupt_next_request() {
        let responses = exchange(
            b"\xff\n{\"id\":true,\"op\":\"execute\",\"sql\":\"SELECT 1;\",\"search_mode\":\"wrong\"}\n{\"id\":null,\"op\":\"shutdown\"}\n",
        );
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[1]["id"], true);
        assert_eq!(responses[1]["ok"], false);
        assert_eq!(responses[1]["error"]["code"], "INVALID_REQUEST");
        assert_eq!(responses[2], json!({"id":null,"ok":true}));
    }
}
