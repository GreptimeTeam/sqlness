// Copyright 2024 CeresDB Project Authors. Licensed under Apache-2.0.

use crate::error::Result;
use crate::interceptor::{Interceptor, InterceptorFactory, InterceptorRef};

pub struct SkipInterceptorFactory;

pub const PREFIX: &str = "SKIP";
pub const SKIP_MARKER_PREFIX: &str = "-- SQLNESS_SKIP:";

/// Skip interceptor that generates SKIP markers in result output.
///
/// Grammar:
/// ``` text
/// -- SQLNESS SKIP <reason>
/// ```
///
/// Example:
/// ``` sql
/// -- SQLNESS SKIP version 0.14.0 < required 0.15.0
/// SELECT * FROM new_feature_table;
/// ```
///
/// The query will not be executed, and the SKIP marker will be written to the result file.
#[derive(Debug)]
pub struct SkipInterceptor {
    reason: String,
}

impl Interceptor for SkipInterceptor {
    fn before_execute(&self, execute_query: &mut Vec<String>, _context: &mut crate::QueryContext) {
        execute_query.clear();
    }

    fn after_execute(&self, result: &mut String) {
        *result = format!("{} {}", SKIP_MARKER_PREFIX, self.reason);
    }
}

impl InterceptorFactory for SkipInterceptorFactory {
    fn try_new(&self, ctx: &str) -> Result<InterceptorRef> {
        Ok(Box::new(SkipInterceptor {
            reason: ctx.to_string(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryContext;

    #[test]
    fn test_skip_interceptor() {
        let interceptor = SkipInterceptorFactory
            .try_new("version 0.14.0 < required 0.15.0")
            .unwrap();

        let mut query = vec!["SELECT * FROM new_feature_table;".to_string()];
        let mut context = QueryContext::default();

        interceptor.before_execute(&mut query, &mut context);
        assert!(query.is_empty());

        let mut result = "some result".to_string();
        interceptor.after_execute(&mut result);
        assert_eq!(result, "-- SQLNESS_SKIP: version 0.14.0 < required 0.15.0");
    }
}
