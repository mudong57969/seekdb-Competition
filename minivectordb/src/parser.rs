use crate::error::Error;
use crate::schema::{Column, ColumnType, Literal};

#[derive(Clone, Copy, PartialEq)]
pub enum Comparison {
    Equal,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
}

pub struct Predicate(pub String, pub Comparison, pub Literal);
pub enum Expression {
    Column(String),
    Distance(String, Literal),
}
pub struct Projection(pub Expression, pub Option<String>);
pub struct Ordering(pub Expression, pub bool);

pub enum Statement {
    Begin,
    Commit,
    Rollback,
    Create(String, Vec<Column>),
    CreateVectorIndex {
        name: String,
        table: String,
        column: String,
    },
    Insert(String, Vec<Vec<Literal>>),
    Select {
        table: String,
        projection: Option<Vec<Projection>>,
        filters: Vec<Predicate>,
        order: Vec<Ordering>,
        limit: usize,
    },
    Update(String, Vec<(String, Literal)>, Vec<Predicate>),
    Delete(String, Vec<Predicate>),
}

#[derive(PartialEq)]
enum Token {
    Word(String),
    Number(String),
    Text(String),
    Symbol(char),
    Compare(Comparison),
}

fn tokenize(sql: &str) -> Result<Vec<Token>, Error> {
    let mut chars = sql.chars().peekable();
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        let token = match ch {
            ch if ch.is_whitespace() => continue,
            ch if ch.is_alphabetic() || ch == '_' => {
                let mut word = String::from(ch);
                while let Some(next) = chars.next_if(|c| c.is_alphanumeric() || *c == '_') {
                    word.push(next);
                }
                Token::Word(word)
            }
            '0'..='9' | '-' | '+' => {
                let mut number = String::from(ch);
                while let Some(next) = chars.next_if(char::is_ascii_digit) {
                    number.push(next);
                }
                Token::Number(number)
            }
            '\'' => {
                let mut text = String::new();
                loop {
                    match chars.next().ok_or(Error::Syntax)? {
                        '\'' if chars.next_if_eq(&'\'').is_some() => text.push('\''),
                        '\'' => break,
                        value => text.push(value),
                    }
                }
                Token::Text(text)
            }
            '(' | ')' | ',' | '*' | ';' => Token::Symbol(ch),
            '=' => Token::Compare(Comparison::Equal),
            '<' if chars.next_if_eq(&'=').is_some() => Token::Compare(Comparison::LessEqual),
            '>' if chars.next_if_eq(&'=').is_some() => Token::Compare(Comparison::GreaterEqual),
            '<' => Token::Compare(Comparison::Less),
            '>' => Token::Compare(Comparison::Greater),
            _ => return Err(Error::Syntax),
        };
        tokens.push(token);
    }
    Ok(tokens)
}

pub fn parse(sql: &str) -> Result<Statement, Error> {
    let mut parser = Parser(tokenize(sql)?.into_iter().peekable());
    let statement = parser.statement()?;
    parser.take_symbol(';');
    if parser.0.peek().is_some() {
        return Err(Error::Syntax);
    }
    Ok(statement)
}

struct Parser(std::iter::Peekable<std::vec::IntoIter<Token>>);

impl Parser {
    fn take_word(&mut self, word: &str) -> bool {
        self.0
            .next_if(
                |token| matches!(token, Token::Word(value) if value.eq_ignore_ascii_case(word)),
            )
            .is_some()
    }

    fn word(&mut self, word: &str) -> Result<(), Error> {
        self.take_word(word).then_some(()).ok_or(Error::Syntax)
    }

    fn take_symbol(&mut self, symbol: char) -> bool {
        self.0.next_if_eq(&Token::Symbol(symbol)).is_some()
    }

    fn symbol(&mut self, symbol: char) -> Result<(), Error> {
        self.take_symbol(symbol).then_some(()).ok_or(Error::Syntax)
    }

    fn ident(&mut self) -> Result<String, Error> {
        match self.0.next() {
            Some(Token::Word(value)) => Ok(value),
            _ => Err(Error::Syntax),
        }
    }

    fn size(&mut self) -> Result<usize, Error> {
        match self.0.next() {
            Some(Token::Number(value)) => value.parse().map_err(|_| Error::InvalidValue),
            _ => Err(Error::Syntax),
        }
    }

    fn literal(&mut self) -> Result<Literal, Error> {
        match self.0.next() {
            Some(Token::Number(value)) => value
                .parse()
                .map(Literal::Integer)
                .map_err(|_| Error::InvalidValue),
            Some(Token::Text(value)) => Ok(Literal::Text(value)),
            _ => Err(Error::Syntax),
        }
    }

    fn comparison(&mut self) -> Result<Comparison, Error> {
        match self.0.next() {
            Some(Token::Compare(value)) => Ok(value),
            _ => Err(Error::Syntax),
        }
    }

    fn list<T>(
        &mut self,
        mut item: impl FnMut(&mut Self) -> Result<T, Error>,
    ) -> Result<Vec<T>, Error> {
        let mut values = vec![item(self)?];
        while self.take_symbol(',') {
            values.push(item(self)?);
        }
        Ok(values)
    }

    fn parenthesized<T>(
        &mut self,
        parse: impl FnOnce(&mut Self) -> Result<T, Error>,
    ) -> Result<T, Error> {
        self.symbol('(')?;
        let value = parse(self)?;
        self.symbol(')')?;
        Ok(value)
    }

    fn column(&mut self) -> Result<Column, Error> {
        let name = self.ident()?;
        let kind = if self.take_word("BIGINT") {
            ColumnType::BigInt
        } else {
            let vector = if self.take_word("VECTOR") {
                true
            } else if self.take_word("VARCHAR") {
                false
            } else {
                return Err(Error::Unsupported);
            };
            let size = self.parenthesized(Self::size)?;
            if size == 0 {
                return Err(Error::InvalidSchema);
            }
            if vector {
                ColumnType::Vector(size)
            } else {
                ColumnType::Varchar(size)
            }
        };
        let primary = self.take_word("PRIMARY");
        if primary {
            self.word("KEY")?;
        }
        Ok(Column {
            name,
            kind,
            primary,
        })
    }

    fn filters(&mut self) -> Result<Vec<Predicate>, Error> {
        let mut filters = Vec::new();
        if self.take_word("WHERE") {
            loop {
                filters.push(Predicate(
                    self.ident()?,
                    self.comparison()?,
                    self.literal()?,
                ));
                if !self.take_word("AND") {
                    break;
                }
            }
        }
        Ok(filters)
    }

    fn expression(&mut self) -> Result<Expression, Error> {
        let name = self.ident()?;
        if name.eq_ignore_ascii_case("L2_DISTANCE")
            && matches!(self.0.peek(), Some(Token::Symbol('(')))
        {
            self.parenthesized(|p| {
                let column = p.ident()?;
                p.symbol(',')?;
                Ok(Expression::Distance(column, p.literal()?))
            })
        } else {
            Ok(Expression::Column(name))
        }
    }

    fn statement(&mut self) -> Result<Statement, Error> {
        if self.take_word("BEGIN") {
            return Ok(Statement::Begin);
        }
        if self.take_word("COMMIT") {
            return Ok(Statement::Commit);
        }
        if self.take_word("ROLLBACK") {
            return Ok(Statement::Rollback);
        }
        if self.take_word("CREATE") {
            if self.take_word("VECTOR") {
                self.word("INDEX")?;
                let name = self.ident()?;
                self.word("ON")?;
                let table = self.ident()?;
                let column = self.parenthesized(Self::ident)?;
                return Ok(Statement::CreateVectorIndex {
                    name,
                    table,
                    column,
                });
            }
            if !self.take_word("TABLE") {
                return Err(Error::Unsupported);
            }
            let table = self.ident()?;
            let columns = self.parenthesized(|p| p.list(Self::column))?;
            return Ok(Statement::Create(table, columns));
        }
        if self.take_word("INSERT") {
            self.word("INTO")?;
            let table = self.ident()?;
            self.word("VALUES")?;
            let rows = self.list(|p| p.parenthesized(|p| p.list(Self::literal)))?;
            return Ok(Statement::Insert(table, rows));
        }
        if self.take_word("SELECT") {
            let projection = if self.take_symbol('*') {
                None
            } else {
                Some(self.list(|p| {
                    let expression = p.expression()?;
                    let alias = if p.take_word("AS") {
                        Some(p.ident()?)
                    } else {
                        None
                    };
                    Ok(Projection(expression, alias))
                })?)
            };
            self.word("FROM")?;
            let table = self.ident()?;
            let filters = self.filters()?;
            let order = if self.take_word("ORDER") {
                self.word("BY")?;
                self.list(|p| {
                    let expression = p.expression()?;
                    let descending = p.take_word("DESC");
                    if !descending {
                        p.take_word("ASC");
                    }
                    Ok(Ordering(expression, descending))
                })?
            } else {
                Vec::new()
            };
            let limit = if self.take_word("LIMIT") {
                self.size()?
            } else {
                usize::MAX
            };
            return Ok(Statement::Select {
                table,
                projection,
                filters,
                order,
                limit,
            });
        }
        if self.take_word("UPDATE") {
            let table = self.ident()?;
            self.word("SET")?;
            let assignments = self.list(|p| {
                let column = p.ident()?;
                if p.comparison()? != Comparison::Equal {
                    return Err(Error::Syntax);
                }
                Ok((column, p.literal()?))
            })?;
            return Ok(Statement::Update(table, assignments, self.filters()?));
        }
        if self.take_word("DELETE") {
            self.word("FROM")?;
            return Ok(Statement::Delete(self.ident()?, self.filters()?));
        }
        Err(Error::Unsupported)
    }
}
