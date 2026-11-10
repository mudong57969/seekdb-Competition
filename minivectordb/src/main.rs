mod error;
mod executor;
mod parser;
mod protocol;
mod schema;
mod storage;

use std::io;
use std::path::PathBuf;

#[cfg(test)]
fn execute_sql(_sql: &str, _mode: protocol::SearchMode) -> protocol::ExecuteResult {
    Err(("NOT_IMPLEMENTED", "SQL execution is not implemented"))
}

fn main() -> io::Result<()> {
    let data_dir = std::env::var_os("VECTORDB_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/data"));

    let mut executor = executor::Executor::open(data_dir)?;
    protocol::serve(
        io::stdin().lock(),
        io::stdout().lock(),
        move |sql, _mode| executor.execute(sql),
    )
}
