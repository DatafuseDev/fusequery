// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cell::RefCell;
use std::cmp::Ordering;
use std::fmt::Write;
use std::num::IntErrorKind;
use std::num::ParseIntError;

use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::parser::common::transform_span;
use crate::parser::input::Input;
use crate::parser::token::*;
use crate::span::pretty_print_error;
use crate::Range;

const MAX_DISPLAY_ERROR_COUNT: usize = 60;

/// This error type accumulates errors and their position when backtracking
/// through a parse tree. This take a deepest error at `alt` combinator.
#[derive(Clone, Debug)]
pub struct Error<'a> {
    /// The span of the next token of the last valid one when encountering an error.
    pub span: Range,
    /// List of errors tried in various branches that consumed
    /// the same (farthest) length of input.
    pub errors: Vec<ErrorKind>,
    /// The backtrace stack of the error.
    pub contexts: Vec<(Range, &'static str)>,
    /// The extra backtrace of error in optional branches.
    pub backtrace: &'a Backtrace,
}

/// ErrorKind is the error type returned from parser.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Error generated by `match_token` function
    ExpectToken(TokenKind),
    /// Error generated by `match_text` function
    ExpectText(&'static str),
    /// Plain text description of an error
    Other(&'static str),
}

/// Record the farthest position in the input before encountering an error.
///
/// This is similar to the `Error`, but the information will not get lost
/// even the error is from a optional branch.
#[derive(Debug, Clone, Default)]
pub struct Backtrace {
    inner: RefCell<Option<BacktraceInner>>,
}

impl Backtrace {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&self) {
        self.inner.replace(None);
    }

    /// Restore the backtrace to a previous state.
    ///
    /// This is useful when the furthest-reached error reporting strategy is undesirable,
    /// particularly when the furthest path is reached but considered invalid.
    pub fn restore(&self, other: Backtrace) {
        *self.inner.borrow_mut() = other.inner.into_inner();
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BacktraceInner {
    /// The span of the next token of the last valid one when encountering an error.
    span: Range,
    /// List of errors tried in various branches that consumed
    /// the same (farthest) length of input.
    errors: Vec<ErrorKind>,
}

impl<'a> nom::error::ParseError<Input<'a>> for Error<'a> {
    fn from_error_kind(i: Input<'a>, _: nom::error::ErrorKind) -> Self {
        Error {
            span: transform_span(&i[..1]).unwrap(),
            errors: vec![],
            contexts: vec![],
            backtrace: i.backtrace,
        }
    }

    fn append(_: Input<'a>, _: nom::error::ErrorKind, other: Self) -> Self {
        other
    }

    fn from_char(_: Input<'a>, _: char) -> Self {
        unreachable!()
    }

    fn or(mut self, mut other: Self) -> Self {
        match self.span.start.cmp(&other.span.start) {
            Ordering::Equal => {
                self.errors.append(&mut other.errors);
                self.contexts.clear();
                self
            }
            Ordering::Less => other,
            Ordering::Greater => self,
        }
    }
}

impl<'a> nom::error::ContextError<Input<'a>> for Error<'a> {
    fn add_context(input: Input<'a>, ctx: &'static str, mut other: Self) -> Self {
        other
            .contexts
            .push((transform_span(&input.tokens[..1]).unwrap(), ctx));
        other
    }
}

impl<'a> Error<'a> {
    pub fn from_error_kind(input: Input<'a>, kind: ErrorKind) -> Self {
        let mut inner = input.backtrace.inner.borrow_mut();
        if let Some(ref mut inner) = *inner {
            match input.tokens[0].span.start.cmp(&inner.span.start) {
                Ordering::Equal => {
                    inner.errors.push(kind);
                }
                Ordering::Less => (),
                Ordering::Greater => {
                    *inner = BacktraceInner {
                        span: transform_span(&input.tokens[..1]).unwrap(),
                        errors: vec![kind],
                    };
                }
            }
        } else {
            *inner = Some(BacktraceInner {
                span: transform_span(&input.tokens[..1]).unwrap(),
                errors: vec![kind],
            })
        }

        Error {
            span: transform_span(&input.tokens[..1]).unwrap(),
            errors: vec![kind],
            contexts: vec![],
            backtrace: input.backtrace,
        }
    }
}

impl From<fast_float::Error> for ErrorKind {
    fn from(_: fast_float::Error) -> Self {
        ErrorKind::Other("unable to parse float number")
    }
}

impl From<ParseIntError> for ErrorKind {
    fn from(err: ParseIntError) -> Self {
        let msg = match err.kind() {
            IntErrorKind::InvalidDigit => {
                "unable to parse number because it contains invalid characters"
            }
            IntErrorKind::PosOverflow => "unable to parse number because it positively overflowed",
            IntErrorKind::NegOverflow => "unable to parse number because it negatively overflowed",
            _ => "unable to parse number",
        };
        ErrorKind::Other(msg)
    }
}

pub fn display_parser_error(error: Error, source: &str) -> String {
    let inner = &*error.backtrace.inner.borrow();
    let inner = match inner {
        Some(inner) => inner,
        None => return String::new(),
    };
    let span_text = &source[std::ops::Range::from(inner.span)];

    let mut labels = vec![];

    // Plain text error has the highest priority. Only display it if exists.
    for (span, kind) in error
        .errors
        .iter()
        .map(|err| (error.span, err))
        .chain(inner.errors.iter().map(|err| (inner.span, err)))
    {
        if let ErrorKind::Other(msg) = kind {
            labels = vec![(span, msg.to_string())];
            break;
        }
    }

    // List all expected tokens in alternative branches.
    if labels.is_empty() {
        let mut expected_tokens = error
            .errors
            .iter()
            .chain(&inner.errors)
            .filter_map(|kind| match kind {
                ErrorKind::ExpectToken(EOI) => None,
                ErrorKind::ExpectToken(token) if token.is_keyword() => {
                    Some(format!("`{:?}`", token))
                }
                ErrorKind::ExpectToken(token) => Some(format!("<{:?}>", token)),
                ErrorKind::ExpectText(text) => Some(format!("`{}`", text)),
                _ => None,
            })
            .unique()
            .collect::<Vec<_>>();
        expected_tokens.sort_by_cached_key(|token| {
            OrderedFloat::from(-strsim::jaro_winkler(
                &token.to_lowercase(),
                &span_text.to_lowercase(),
            ))
        });

        let mut msg = if span_text.is_empty() {
            "unexpected end of input".to_string()
        } else {
            format!("unexpected `{span_text}`")
        };
        let mut iter = expected_tokens.iter().enumerate().peekable();
        while let Some((i, error)) = iter.next() {
            if i == MAX_DISPLAY_ERROR_COUNT {
                let more = expected_tokens
                    .len()
                    .saturating_sub(MAX_DISPLAY_ERROR_COUNT);
                write!(msg, ", or {} more ...", more).unwrap();
                break;
            } else if i == 0 {
                msg += ", expecting ";
            } else if iter.peek().is_none() && i == 1 {
                msg += " or ";
            } else if iter.peek().is_none() {
                msg += ", or ";
            } else {
                msg += ", ";
            }
            msg += error;
        }

        labels = vec![(inner.span, msg)];
    }

    // Append contexts as secondary labels.
    labels.extend(
        error
            .contexts
            .iter()
            .map(|(span, msg)| (*span, format!("while parsing {}", msg))),
    );

    pretty_print_error(source, labels)
}
