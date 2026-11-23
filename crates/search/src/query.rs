//! Lexer and typed syntax for the RFC search DSL.

use rfc_core::RfcNumber;
use std::{fmt, ops::Range};

const QUERY_BYTES: usize = 8192;
const LITERAL_BYTES: usize = 4096;
const NODES: usize = 128;
const DEPTH: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Field {
    Fulltext,
    Title,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Match {
    Exact,
    Token,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Family {
    References,
    Normative,
    Informative,
    Updates,
    Obsoletes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Outgoing,
    Incoming,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Text(Field, Match, String),
    Relation(Family, Direction, RfcNumber),
    Obsoleted(bool),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Predicate(Predicate, Range<usize>),
    Not(Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryError {
    pub code: &'static str,
    pub message: &'static str,
    pub span: Range<usize>,
}

impl QueryError {
    pub(crate) fn new(code: &'static str, message: &'static str, span: Range<usize>) -> Self {
        Self {
            code,
            message,
            span,
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {} (query bytes {}..{})",
            self.code, self.message, self.span.start, self.span.end
        )
    }
}

impl std::error::Error for QueryError {}

#[derive(Debug, PartialEq)]
enum Token<'a> {
    Word(&'a str),
    Literal(Match, String),
    DoubleColon,
    Colon,
    Open,
    Close,
    Not,
    End,
}

const SYNTAX: &str = "invalid_query_syntax";
const COMPLEX: &str = "query_too_complex";

fn lex(input: &str) -> Result<Vec<(Token<'_>, Range<usize>)>, QueryError> {
    let mut tokens = Vec::new();
    let mut chars = input.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        let token = match ch {
            ch if ch.is_whitespace() => continue,
            ':' if chars.next_if(|&(_, c)| c == ':').is_some() => Token::DoubleColon,
            ':' => Token::Colon,
            '(' => Token::Open,
            ')' => Token::Close,
            '!' => Token::Not,
            '\'' | '"' => {
                let mut value = String::new();
                loop {
                    match chars.next() {
                        Some((_, end)) if end == ch => break,
                        Some((escape, '\\')) => {
                            let Some((offset, escaped)) = chars.next() else {
                                return Err(QueryError::new(
                                    SYNTAX,
                                    "Unterminated escape",
                                    escape..input.len(),
                                ));
                            };
                            value.push(match escaped {
                                c if c == ch => c,
                                '\\' => '\\',
                                'n' => '\n',
                                'r' => '\r',
                                't' => '\t',
                                _ => {
                                    return Err(QueryError::new(
                                        SYNTAX,
                                        "Unknown literal escape",
                                        escape..offset + escaped.len_utf8(),
                                    ));
                                }
                            });
                        }
                        Some((_, c)) => value.push(c),
                        None => {
                            return Err(QueryError::new(
                                SYNTAX,
                                "Unterminated quoted literal",
                                start..input.len(),
                            ));
                        }
                    }
                }
                let end = chars.peek().map_or(input.len(), |&(offset, _)| offset);
                if value.is_empty() {
                    return Err(QueryError::new(
                        SYNTAX,
                        "A literal cannot be empty",
                        start..end,
                    ));
                }
                if value.len() > LITERAL_BYTES {
                    return Err(QueryError::new(
                        COMPLEX,
                        "Decoded literal exceeds 4096 bytes",
                        start..end,
                    ));
                }
                let mode = if ch == '"' {
                    Match::Exact
                } else {
                    Match::Token
                };
                Token::Literal(mode, value)
            }
            ch if ch.is_ascii_alphanumeric() || ch == '_' => {
                while chars
                    .next_if(|&(_, c)| c.is_ascii_alphanumeric() || c == '_')
                    .is_some()
                {}
                let end = chars.peek().map_or(input.len(), |&(offset, _)| offset);
                Token::Word(&input[start..end])
            }
            _ => {
                return Err(QueryError::new(
                    SYNTAX,
                    "Unexpected query character",
                    start..start + ch.len_utf8(),
                ));
            }
        };
        let end = chars.peek().map_or(input.len(), |&(offset, _)| offset);
        tokens.push((token, start..end));
    }
    tokens.push((Token::End, input.len()..input.len()));
    Ok(tokens)
}

/// Parse one complete search expression.
pub fn parse(input: &str) -> Result<Expr, QueryError> {
    if input.len() > QUERY_BYTES {
        return Err(QueryError::new(
            COMPLEX,
            "Query exceeds 8192 bytes",
            0..input.len(),
        ));
    }
    if let Some(offset) = input.find('\0') {
        return Err(QueryError::new(
            SYNTAX,
            "NUL is forbidden",
            offset..offset + 1,
        ));
    }
    let mut parser = Parser {
        tokens: lex(input)?.into_iter().peekable(),
        nodes: 0,
    };
    let expr = parser.or(0)?;
    parser.expect(Token::End, "Expected AND, OR, or end of query")?;
    Ok(expr)
}

/// Validate UTF-8 at byte-oriented API boundaries before parsing.
pub fn parse_bytes(input: &[u8]) -> Result<Expr, QueryError> {
    if input.len() > QUERY_BYTES {
        return Err(QueryError::new(
            COMPLEX,
            "Query exceeds 8192 bytes",
            0..input.len(),
        ));
    }
    let input = std::str::from_utf8(input).map_err(|error| {
        let start = error.valid_up_to();
        QueryError::new(
            SYNTAX,
            "Query must be UTF-8",
            start..error.error_len().map_or(input.len(), |len| start + len),
        )
    })?;
    parse(input)
}

struct Parser<'a> {
    tokens: std::iter::Peekable<std::vec::IntoIter<(Token<'a>, Range<usize>)>>,
    nodes: usize,
}

impl<'a> Parser<'a> {
    fn peek(&mut self) -> &(Token<'a>, Range<usize>) {
        self.tokens.peek().expect("the lexer emits EOF")
    }

    fn take(&mut self) -> (Token<'a>, Range<usize>) {
        self.tokens.next().expect("the lexer emits EOF")
    }

    fn expect(
        &mut self,
        token: Token<'_>,
        message: &'static str,
    ) -> Result<Range<usize>, QueryError> {
        let (actual, span) = self.take();
        if actual == token {
            Ok(span)
        } else {
            Err(QueryError::new(SYNTAX, message, span))
        }
    }

    fn node(&mut self, span: Range<usize>) -> Result<(), QueryError> {
        self.nodes += 1;
        if self.nodes > NODES {
            return Err(QueryError::new(
                COMPLEX,
                "Query exceeds 128 syntax nodes",
                span,
            ));
        }
        Ok(())
    }

    fn or(&mut self, depth: usize) -> Result<Expr, QueryError> {
        let first = self.and(depth)?;
        if self.peek().0 != Token::Word("OR") {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.peek().0 == Token::Word("OR") {
            let (_, span) = self.take();
            self.node(span)?;
            children.push(self.and(depth)?);
        }
        Ok(Expr::Or(children))
    }

    fn and(&mut self, depth: usize) -> Result<Expr, QueryError> {
        let first = self.unary(depth)?;
        if self.peek().0 != Token::Word("AND") {
            return Ok(first);
        }
        let mut children = vec![first];
        while self.peek().0 == Token::Word("AND") {
            let (_, span) = self.take();
            self.node(span)?;
            children.push(self.unary(depth)?);
        }
        Ok(Expr::And(children))
    }

    fn unary(&mut self, depth: usize) -> Result<Expr, QueryError> {
        if depth > DEPTH {
            return Err(QueryError::new(
                COMPLEX,
                "Query exceeds 16 levels of nesting",
                self.peek().1.clone(),
            ));
        }
        match self.peek().0 {
            Token::Not => {
                let (_, span) = self.take();
                self.node(span)?;
                Ok(Expr::Not(Box::new(self.unary(depth + 1)?)))
            }
            Token::Word("NOT") => {
                let (_, span) = self.take();
                self.node(span)?;
                self.expect(Token::Open, "NOT requires a parenthesized expression")?;
                Ok(Expr::Not(Box::new(self.group(depth)?)))
            }
            Token::Open => {
                self.take();
                self.group(depth)
            }
            _ => self.predicate(),
        }
    }

    fn group(&mut self, depth: usize) -> Result<Expr, QueryError> {
        let inner = self.or(depth + 1)?;
        self.expect(Token::Close, "Expected a closing parenthesis")?;
        Ok(inner)
    }

    fn predicate(&mut self) -> Result<Expr, QueryError> {
        let (prefix, start) = self.take();
        match prefix {
            Token::Word("in" | "relation" | "is" | "depend") => {}
            Token::Word(_) if self.peek().0 == Token::DoubleColon => {
                return Err(QueryError::new(
                    "unknown_query_field",
                    "Unknown predicate namespace",
                    start,
                ));
            }
            _ => {
                return Err(QueryError::new(
                    SYNTAX,
                    "Expected a search predicate",
                    start,
                ));
            }
        }
        self.node(start.clone())?;
        self.expect(
            Token::DoubleColon,
            "Expected :: after the predicate namespace",
        )?;
        let (name, span) = self.take();
        let Token::Word(name) = name else {
            return Err(QueryError::new(SYNTAX, "Expected a predicate name", span));
        };
        let predicate = match prefix {
            Token::Word("in") => {
                let field = match name {
                    "title" => Field::Title,
                    "fulltext" => Field::Fulltext,
                    _ => {
                        return Err(QueryError::new(
                            "unknown_query_field",
                            "Use title or fulltext",
                            span,
                        ));
                    }
                };
                self.expect(Token::Colon, "Expected : after the field")?;
                let (value, span) = self.take();
                let Token::Literal(mode, value) = value else {
                    return Err(QueryError::new(SYNTAX, "Expected a quoted literal", span));
                };
                return Ok(Expr::Predicate(
                    Predicate::Text(field, mode, value),
                    start.start..span.end,
                ));
            }
            Token::Word("relation") => {
                let (family, direction) = relation(name).ok_or_else(|| {
                    QueryError::new("unknown_relation", "Unknown relationship name", span)
                })?;
                Some((family, direction))
            }
            Token::Word("depend") if name == "id" => {
                Some((Family::References, Direction::Outgoing))
            }
            Token::Word("is") if name == "obsoleted" => None,
            _ => {
                return Err(QueryError::new(
                    "unknown_query_field",
                    "Unknown predicate name",
                    span,
                ));
            }
        };
        self.expect(Token::Colon, "Expected : before the predicate value")?;
        let (value, span) = self.take();
        let leaf = match predicate {
            Some((family, direction)) => {
                let number = match value {
                    Token::Word(raw) if raw.bytes().all(|byte| byte.is_ascii_digit()) => {
                        raw.parse::<i32>().ok().and_then(RfcNumber::new)
                    }
                    _ => None,
                }
                .ok_or_else(|| {
                    QueryError::new(
                        SYNTAX,
                        "Expected a positive RFC number up to 2147483647",
                        span.clone(),
                    )
                })?;
                Predicate::Relation(family, direction, number)
            }
            None => match value {
                Token::Word("true") => Predicate::Obsoleted(true),
                Token::Word("false") => Predicate::Obsoleted(false),
                _ => return Err(QueryError::new(SYNTAX, "Expected true or false", span)),
            },
        };
        Ok(Expr::Predicate(leaf, start.start..span.end))
    }
}

fn relation(name: &str) -> Option<(Family, Direction)> {
    use Direction::{Incoming, Outgoing};
    use Family::{Informative, Normative, Obsoletes, References, Updates};
    Some(match name {
        "references" => (References, Outgoing),
        "normative_reference" => (Normative, Outgoing),
        "informative_reference" => (Informative, Outgoing),
        "updates" => (Updates, Outgoing),
        "obsoletes" => (Obsoletes, Outgoing),
        "referenced_by" => (References, Incoming),
        "normative_referenced_by" => (Normative, Incoming),
        "informative_referenced_by" => (Informative, Incoming),
        "updated_by" => (Updates, Incoming),
        "obsoleted_by" => (Obsoletes, Incoming),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_precedence_alias_and_escapes() {
        let result = parse(
            r#"in :: title : "É\n\"\\%" OR depend::id:02119 AND NOT(in::fulltext:'can\'t "TLS"')"#,
        )
        .unwrap();
        let Expr::Or(parts) = result else {
            panic!("expected OR")
        };
        assert!(
            matches!(&parts[0], Expr::Predicate(Predicate::Text(Field::Title, Match::Exact, value), _) if value == "É\n\"\\%")
        );
        let Expr::And(parts) = &parts[1] else {
            panic!("expected AND")
        };
        assert!(
            matches!(&parts[0], Expr::Predicate(Predicate::Relation(Family::References, Direction::Outgoing, id), _) if id.get() == 2119)
        );
        assert!(
            matches!(&parts[1], Expr::Not(child) if matches!(child.as_ref(), Expr::Predicate(Predicate::Text(Field::Fulltext, Match::Token, value), _) if value == "can't \"TLS\""))
        );
        assert!(parse("!!is::obsoleted:false").is_ok());
        assert!(parse("relation::references:2147483647").is_ok());
    }

    #[test]
    fn rejects_invalid_syntax_with_byte_spans() {
        for (query, code) in [
            ("", SYNTAX),
            ("in::title:'TLS", SYNTAX),
            ("int::fulltext", "unknown_query_field"),
            ("in::name:'TLS'", "unknown_query_field"),
            ("relation::reference:1", "unknown_relation"),
            ("relation::updates:2147483648", SYNTAX),
            ("relation::updates:0", SYNTAX),
            ("NOT is::obsoleted:false", SYNTAX),
            ("is::obsoleted:False", SYNTAX),
            ("in::title:''", SYNTAX),
            ("in::title:'TLS' in::title:'HTTP'", SYNTAX),
            ("(in::title:'TLS'", SYNTAX),
            ("in::title:'TLS\0'", SYNTAX),
        ] {
            assert_eq!(parse(query).unwrap_err().code, code, "{query}");
        }
        let query = "in::title:'é\\q'";
        assert_eq!(parse(query).unwrap_err().span, 13..15);
        assert_eq!(parse_bytes(&[b'a', 0xff]).unwrap_err().span, 1..2);
    }

    #[test]
    fn rejects_excessive_complexity() {
        let leaf = "is::obsoleted:false";
        assert!(parse(&format!("{}{leaf}{}", "(".repeat(16), ")".repeat(16))).is_ok());
        assert_eq!(
            parse(&format!("{}{leaf}{}", "(".repeat(17), ")".repeat(17)))
                .unwrap_err()
                .code,
            COMPLEX
        );
        assert_eq!(parse(&[leaf; 65].join(" OR ")).unwrap_err().code, COMPLEX);
        assert_eq!(
            parse(&format!("in::title:'{}'", "é".repeat(2049)))
                .unwrap_err()
                .code,
            COMPLEX
        );
        assert_eq!(parse(&" ".repeat(8193)).unwrap_err().code, COMPLEX);
    }

    #[test]
    fn seeded_malformed_input_keeps_spans_in_bounds() {
        let alphabet = ["a", "é", " ", "(", ")", "!", ":", "\\", "'", "\"", "\0"];
        let mut state = 31_u64;
        for count in 0..3000 {
            let input: String = (0..count % 87)
                .map(|_| {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    alphabet[((state >> 32) as usize) % alphabet.len()]
                })
                .collect();
            if let Err(error) = parse(&input) {
                assert!(error.span.start <= error.span.end && error.span.end <= input.len());
                assert!(input.is_char_boundary(error.span.start));
                assert!(input.is_char_boundary(error.span.end));
            }
        }
    }
}
