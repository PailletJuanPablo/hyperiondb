use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;

use serde_json::Value;

use crate::handler::Expr;

use super::HyperionDB;

impl HyperionDB {
    pub async fn query_expression(&self, expr: &Expr) -> Vec<Value> {
        let keys = self.evaluate_expr(expr).await;
        let mut result_values = Vec::new();

        for key in keys {
            if let Some(value) = self.get(&key).await {
                result_values.push(value);
            }
        }

        result_values
    }

    fn evaluate_expr<'a>(&'a self, expr: &'a Expr) -> Pin<Box<dyn Future<Output = HashSet<String>> + Send + 'a>> {
        Box::pin(async move {
            match expr {
                Expr::Condition(cond) => {
                    if let Some(index) = self.indices.get(&cond.field) {
                        index.query_keys(&cond.operator, &cond.value)
                    } else {
                        HashSet::new()
                    }
                }
                Expr::And(lhs, rhs) => {
                    let left_keys = self.evaluate_expr(lhs).await;
                    if left_keys.is_empty() {
                        return HashSet::new();
                    }
                    let right_keys = self.evaluate_expr(rhs).await;
                    left_keys.intersection(&right_keys).cloned().collect()
                }
                Expr::Or(lhs, rhs) => {
                    let left_keys = self.evaluate_expr(lhs).await;
                    let right_keys = self.evaluate_expr(rhs).await;
                    left_keys.union(&right_keys).cloned().collect()
                }
                Expr::Group(inner) => self.evaluate_expr(inner).await,
            }
        })
    }
}
