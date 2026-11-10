#[derive(Debug)]
pub enum Error {
    Syntax,
    Unsupported,
    InvalidSchema,
    TableExists,
    IndexExists,
    UnknownTable,
    UnknownColumn,
    DuplicateKey,
    InvalidValue,
    ValueTooLong,
    DimensionMismatch,
    Storage,
    RecoveryRequired,
    TransactionState,
    QueryTooLarge,
}

impl Error {
    pub fn response(self) -> (&'static str, &'static str) {
        match self {
            Self::Storage => ("storage_error", "Storage operation failed"),
            Self::RecoveryRequired => ("recovery_required", "Restart to complete recovery"),
            Self::TransactionState => ("transaction_state", "Invalid transaction state"),
            Self::QueryTooLarge => (
                "query_too_large",
                "Query exceeded the in-memory result or sort budget",
            ),
            Self::Syntax => ("syntax_error", "Invalid SQL syntax"),
            Self::Unsupported => ("unsupported_sql", "Unsupported SQL"),
            Self::InvalidSchema => (
                "invalid_schema",
                "Expected unique columns, one BIGINT primary key and at most one VECTOR column",
            ),
            Self::IndexExists => ("index_exists", "Index already exists on this table"),
            Self::TableExists => ("table_exists", "Table already exists"),
            Self::UnknownTable => ("unknown_table", "Table does not exist"),
            Self::UnknownColumn => ("unknown_column", "Column does not exist"),
            Self::DuplicateKey => ("duplicate_key", "Duplicate primary key"),
            Self::InvalidValue => (
                "invalid_value",
                "Value does not match the column type or range",
            ),
            Self::ValueTooLong => ("value_too_long", "VARCHAR character limit exceeded"),
            Self::DimensionMismatch => ("dimension_mismatch", "Incorrect vector dimension"),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        eprintln!("Storage I/O failed: {error}");
        Self::Storage
    }
}
