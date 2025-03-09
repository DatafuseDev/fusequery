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

use databend_common_base::base::OrderedFloat;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::Scalar;
use databend_common_sql::optimizer::InferFilterOptimizer;
use databend_common_sql::planner::binder::ColumnBinding;
use databend_common_sql::planner::binder::Visibility;
use databend_common_sql::planner::plans::BoundColumnRef;
use databend_common_sql::planner::plans::ComparisonOp;
use databend_common_sql::planner::plans::ConstantExpr;
use databend_common_sql::planner::plans::FunctionCall;
use databend_common_sql::planner::plans::ScalarExpr;
use databend_common_sql::IndexType;

// ===== Helper Functions =====

/// Creates a column reference with the given index, name, and data type
fn create_column_ref(index: IndexType, name: &str, data_type: DataType) -> ScalarExpr {
    let column = ColumnBinding {
        index,
        column_name: name.to_string(),
        data_type: Box::new(data_type),
        database_name: None,
        table_name: None,
        column_position: None,
        table_index: None,
        visibility: Visibility::Visible,
        virtual_expr: None,
    };
    ScalarExpr::BoundColumnRef(BoundColumnRef { column, span: None })
}

/// Creates an integer constant expression
fn create_int_constant(value: i64) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        value: Scalar::Number(NumberScalar::Int64(value)),
        span: None,
    })
}

/// Creates a float constant expression
fn create_float_constant(value: f64) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        value: Scalar::Number(NumberScalar::Float64(OrderedFloat(value))),
        span: None,
    })
}

/// Creates a comparison expression between left and right with the given operator
fn create_comparison(left: ScalarExpr, right: ScalarExpr, op: ComparisonOp) -> Result<ScalarExpr> {
    let func_name = op.to_func_name().to_string();
    Ok(ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name,
        arguments: vec![left, right],
        params: vec![],
    }))
}

/// Extracts the function name from a scalar expression
fn get_function_name(expr: &ScalarExpr) -> Option<&str> {
    if let ScalarExpr::FunctionCall(func) = expr {
        Some(&func.func_name)
    } else {
        None
    }
}

/// Extracts the column index from a scalar expression
fn get_column_index(expr: &ScalarExpr) -> Option<IndexType> {
    if let ScalarExpr::BoundColumnRef(col_ref) = expr {
        Some(col_ref.column.index)
    } else {
        None
    }
}

/// Extracts an integer value from a scalar expression
fn get_int_value(expr: &ScalarExpr) -> Option<i64> {
    if let ScalarExpr::ConstantExpr(constant) = expr {
        if let Scalar::Number(NumberScalar::Int64(value)) = constant.value {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
}

/// Extracts a boolean value from a scalar expression
fn get_bool_value(expr: &ScalarExpr) -> Option<bool> {
    if let ScalarExpr::ConstantExpr(constant) = expr {
        if let Scalar::Boolean(value) = constant.value {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
}

/// Finds a predicate with the given function name and column index
fn find_predicate(
    predicates: &[ScalarExpr],
    func_name: &str,
    col_index: IndexType,
    value: Option<i64>,
) -> bool {
    for pred in predicates {
        if let ScalarExpr::FunctionCall(func) = pred {
            if func.func_name != func_name {
                continue;
            }

            let left_index = get_column_index(&func.arguments[0]);
            if left_index != Some(col_index) {
                continue;
            }

            if let Some(expected_value) = value {
                let right_value = get_int_value(&func.arguments[1]);
                if right_value != Some(expected_value) {
                    continue;
                }
            }

            return true;
        }
    }
    false
}

/// Finds an equality predicate between two columns
fn find_equality_predicate(
    predicates: &[ScalarExpr],
    left_col_index: IndexType,
    right_col_index: IndexType,
) -> bool {
    for pred in predicates {
        if let ScalarExpr::FunctionCall(func) = pred {
            if func.func_name != "eq" {
                continue;
            }

            let left_index = get_column_index(&func.arguments[0]);
            let right_index = get_column_index(&func.arguments[1]);

            if (left_index == Some(left_col_index) && right_index == Some(right_col_index))
                || (left_index == Some(right_col_index) && right_index == Some(left_col_index))
            {
                return true;
            }
        }
    }
    false
}

/// Checks if the result is a single boolean constant with the given value
fn is_boolean_constant(result: &[ScalarExpr], value: bool) -> bool {
    if result.len() != 1 {
        return false;
    }

    get_bool_value(&result[0]) == Some(value)
}

/// Runs the optimizer with the given predicates and returns the result
fn run_optimizer(predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
    let optimizer = InferFilterOptimizer::new(None);
    optimizer.run(predicates)
}

// ===== Test Cases =====

#[test]
fn test_basic_comparison_simplification() -> Result<()> {
    // Setup common columns and constants
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let const_1 = create_int_constant(1);
    let const_5 = create_int_constant(5);
    let const_10 = create_int_constant(10);

    // Test: A > 1 AND A > 5 => A > 5
    {
        let pred_a_gt_1 = create_comparison(col_a.clone(), const_1.clone(), ComparisonOp::GT)?;
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;

        let result = run_optimizer(vec![pred_a_gt_1, pred_a_gt_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_column_index(&func.arguments[0]),
                Some(0),
                "First arg should be column A"
            );
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A < 10 AND A < 5 => A < 5
    {
        let pred_a_lt_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::LT)?;
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;

        let result = run_optimizer(vec![pred_a_lt_10, pred_a_lt_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A >= 5 AND A <= 5 => A = 5
    {
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_lte_5])?;

        assert!(
            find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate"
        );
    }

    // Test: A <= 5 AND A >= 5 => A = 5 (reverse order)
    {
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;

        let result = run_optimizer(vec![pred_a_lte_5, pred_a_gte_5])?;

        assert!(
            find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate (reverse order)"
        );
    }

    Ok(())
}

#[test]
fn test_contradiction_detection() -> Result<()> {
    // Setup common columns and constants
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let const_3 = create_int_constant(3);
    let const_5 = create_int_constant(5);

    // Test: A > 5 AND A < 3 => false (contradiction)
    {
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_lt_3 = create_comparison(col_a.clone(), const_3.clone(), ComparisonOp::LT)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_lt_3])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 3 AND A = 5 => false (contradiction)
    {
        let pred_a_eq_3 = create_comparison(col_a.clone(), const_3.clone(), ComparisonOp::Equal)?;
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_eq_3, pred_a_eq_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A != 5 => false (contradiction)
    {
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;
        let pred_a_ne_5 =
            create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_ne_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A > 5 AND A = 3 => false (contradiction)
    {
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_eq_3 = create_comparison(col_a.clone(), const_3.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_eq_3])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < 3 AND A >= 5 => false (contradiction)
    {
        let pred_a_lt_3 = create_comparison(col_a.clone(), const_3.clone(), ComparisonOp::LT)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;

        let result = run_optimizer(vec![pred_a_lt_3, pred_a_gte_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    Ok(())
}

#[test]
fn test_transitivity() -> Result<()> {
    // Setup common columns
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let col_b = create_column_ref(1, "B", DataType::Number(NumberDataType::Int64));
    let col_c = create_column_ref(2, "C", DataType::Number(NumberDataType::Int64));
    let col_d = create_column_ref(3, "D", DataType::Number(NumberDataType::Int64));

    // Test: A = B AND B = C => A = B AND B = C AND A = C (transitive equality)
    {
        let pred_a_eq_b = create_comparison(col_a.clone(), col_b.clone(), ComparisonOp::Equal)?;
        let pred_b_eq_c = create_comparison(col_b.clone(), col_c.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_eq_b, pred_b_eq_c])?;

        assert!(
            find_equality_predicate(&result, 0, 2),
            "Should infer A = C predicate through transitivity"
        );
    }

    // Test: A = B AND B = C AND C = D => should infer A = D (transitive equality chain)
    {
        let pred_a_eq_b = create_comparison(col_a.clone(), col_b.clone(), ComparisonOp::Equal)?;
        let pred_b_eq_c = create_comparison(col_b.clone(), col_c.clone(), ComparisonOp::Equal)?;
        let pred_c_eq_d = create_comparison(col_c.clone(), col_d.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_eq_b, pred_b_eq_c, pred_c_eq_d])?;

        assert!(
            find_equality_predicate(&result, 0, 3),
            "Should infer A = D predicate through transitive chain"
        );
    }

    Ok(())
}

#[test]
fn test_constant_propagation() -> Result<()> {
    // Setup common columns and constants
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let col_b = create_column_ref(1, "B", DataType::Number(NumberDataType::Int64));
    let col_c = create_column_ref(2, "C", DataType::Number(NumberDataType::Int64));
    let const_10 = create_int_constant(10);

    // Test: A = 10 AND A = B => A = 10 AND B = 10 AND A = B
    {
        let pred_a_eq_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::Equal)?;
        let pred_a_eq_b = create_comparison(col_a.clone(), col_b.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_eq_10, pred_a_eq_b])?;

        assert!(
            find_predicate(&result, "eq", 1, Some(10)),
            "Should infer B = 10 predicate through constant propagation"
        );
    }

    // Test: A = 10 AND A = B AND B = C => A = 10 AND B = 10 AND C = 10
    {
        let pred_a_eq_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::Equal)?;
        let pred_a_eq_b = create_comparison(col_a.clone(), col_b.clone(), ComparisonOp::Equal)?;
        let pred_b_eq_c = create_comparison(col_b.clone(), col_c.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_eq_10, pred_a_eq_b, pred_b_eq_c])?;

        assert!(
            find_predicate(&result, "eq", 2, Some(10)),
            "Should infer C = 10 predicate through transitive constant propagation"
        );
    }

    Ok(())
}

#[test]
fn test_mixed_comparison_types() -> Result<()> {
    // Setup columns with different data types
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));

    // Test: A > 5 AND A >= 5 => A > 5
    {
        let const_5 = create_int_constant(5);
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt"
        );
    }

    // Test: A < 5 AND A <= 5 => A < 5
    {
        let const_5 = create_int_constant(5);
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_lte_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt"
        );
    }

    // Test: A = 5 AND A >= 5 AND A <= 5 => A = 5
    {
        let const_5 = create_int_constant(5);
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gte_5, pred_a_lte_5])?;

        assert!(
            find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate"
        );
    }

    Ok(())
}

#[test]
fn test_boundary_cases() -> Result<()> {
    // Test with different data types
    let col_int8 = create_column_ref(0, "int8", DataType::Number(NumberDataType::Int8));
    let col_uint8 = create_column_ref(1, "uint8", DataType::Number(NumberDataType::UInt8));
    let col_float = create_column_ref(2, "float", DataType::Number(NumberDataType::Float64));

    // Test: int8 column with values at type boundaries
    {
        // i8::MIN = -128, i8::MAX = 127
        let const_min = create_int_constant(-128);
        let const_max = create_int_constant(127);

        // Test: int8 > -128 AND int8 < 127 => keep both predicates
        let pred_gt_min = create_comparison(col_int8.clone(), const_min.clone(), ComparisonOp::GT)?;
        let pred_lt_max = create_comparison(col_int8.clone(), const_max.clone(), ComparisonOp::LT)?;

        let result = run_optimizer(vec![pred_gt_min, pred_lt_max])?;

        assert_eq!(result.len(), 2, "Should keep both boundary predicates");
    }

    // Test: uint8 column with values at type boundaries
    {
        // u8::MIN = 0, u8::MAX = 255
        let const_min = create_int_constant(0);
        let const_max = create_int_constant(255);

        // Test: uint8 >= 0 AND uint8 <= 255 => no constraints (always true)
        let pred_gte_min =
            create_comparison(col_uint8.clone(), const_min.clone(), ComparisonOp::GTE)?;
        let pred_lte_max =
            create_comparison(col_uint8.clone(), const_max.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_gte_min, pred_lte_max])?;

        // The optimizer might simplify this in different ways, but it should not be a contradiction
        assert!(
            !is_boolean_constant(&result, false),
            "Should not be a contradiction"
        );
    }

    // Test: float column with equality and range checks
    {
        let const_5_0 = create_float_constant(5.0);

        // Test: float = 5.0 AND float >= 5.0 AND float <= 5.0 => float = 5.0
        let pred_eq_5 =
            create_comparison(col_float.clone(), const_5_0.clone(), ComparisonOp::Equal)?;
        let pred_gte_5 =
            create_comparison(col_float.clone(), const_5_0.clone(), ComparisonOp::GTE)?;
        let pred_lte_5 =
            create_comparison(col_float.clone(), const_5_0.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_eq_5, pred_gte_5, pred_lte_5])?;

        assert!(
            get_function_name(&result[0]) == Some("eq"),
            "Should infer float = 5.0 predicate"
        );
    }

    Ok(())
}

#[test]
fn test_edge_cases() -> Result<()> {
    // Setup common columns and constants
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));

    // Test: A > X AND A < X => false (contradiction)
    {
        let const_5 = create_int_constant(5);
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_lt_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A > X AND A = X => false (contradiction)
    {
        let const_5 = create_int_constant(5);
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_eq_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < X AND A = X => false (contradiction)
    {
        let const_5 = create_int_constant(5);
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_eq_5])?;

        assert!(
            is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A >= X AND A <= X => A = X (equality)
    {
        let const_5 = create_int_constant(5);
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_lte_5])?;

        assert!(
            find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate"
        );
    }

    // Test: A = X AND A >= X AND A <= X => A = X (redundant conditions)
    {
        let const_5 = create_int_constant(5);
        let pred_a_eq_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::Equal)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gte_5, pred_a_lte_5])?;

        assert!(
            find_predicate(&result, "eq", 0, Some(5)),
            "Should simplify to A = 5 predicate"
        );
    }

    Ok(())
}

#[test]
fn test_complex_cases() -> Result<()> {
    // Setup common columns and constants
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let col_b = create_column_ref(1, "B", DataType::Number(NumberDataType::Int64));

    // Test: A > 1 AND A < 10 AND A = B => B > 1 AND B < 10 AND A = B
    {
        let const_1 = create_int_constant(1);
        let const_10 = create_int_constant(10);

        let pred_a_gt_1 = create_comparison(col_a.clone(), const_1.clone(), ComparisonOp::GT)?;
        let pred_a_lt_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::LT)?;
        let pred_a_eq_b = create_comparison(col_a.clone(), col_b.clone(), ComparisonOp::Equal)?;

        let result = run_optimizer(vec![pred_a_gt_1, pred_a_lt_10, pred_a_eq_b])?;

        assert!(
            find_predicate(&result, "gt", 1, Some(1)),
            "Should infer B > 1 predicate through equality"
        );

        assert!(
            find_predicate(&result, "lt", 1, Some(10)),
            "Should infer B < 10 predicate through equality"
        );
    }

    Ok(())
}

#[test]
fn test_filter_simplification_with_not_equal() -> Result<()> {
    // Setup column and constant
    let col_a = create_column_ref(0, "A", DataType::Number(NumberDataType::Int64));
    let const_1 = create_int_constant(1);
    let const_5 = create_int_constant(5);
    let const_10 = create_int_constant(10);

    // Test: A != 1 AND A <= 1 => A < 1
    {
        let pred_a_ne_1 = create_comparison(col_a.clone(), const_1.clone(), ComparisonOp::NotEqual)?;
        let pred_a_lte_1 = create_comparison(col_a.clone(), const_1.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_ne_1, pred_a_lte_1])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt (less than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_column_index(&func.arguments[0]),
                Some(0),
                "First arg should be column A"
            );
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(1),
                "Second arg should be constant 1"
            );
        }
    }

    // Test: A <= 5 AND A != 5 => A < 5 (reverse order)
    {
        let pred_a_lte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LTE)?;
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;

        let result = run_optimizer(vec![pred_a_lte_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt (less than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_column_index(&func.arguments[0]),
                Some(0),
                "First arg should be column A"
            );
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A != 10 AND A <= 10 => A < 10 (with different constant)
    {
        let pred_a_ne_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::NotEqual)?;
        let pred_a_lte_10 = create_comparison(col_a.clone(), const_10.clone(), ComparisonOp::LTE)?;

        let result = run_optimizer(vec![pred_a_ne_10, pred_a_lte_10])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt (less than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(10),
                "Second arg should be constant 10"
            );
        }
    }

    // Test: A != 5 AND A >= 5 => A > 5
    {
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt (greater than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A >= 5 AND A != 5 => A > 5 (reverse order)
    {
        let pred_a_gte_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GTE)?;
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt (greater than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A != 5 AND A < 5 => A < 5 (redundant NotEqual)
    {
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lt_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt (less than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A < 5 AND A != 5 => A < 5 (redundant NotEqual, reverse order)
    {
        let pred_a_lt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::LT)?;
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("lt"),
            "Function should be lt (less than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A != 5 AND A > 5 => A > 5 (redundant NotEqual)
    {
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gt_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt (greater than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    // Test: A > 5 AND A != 5 => A > 5 (redundant NotEqual, reverse order)
    {
        let pred_a_gt_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::GT)?;
        let pred_a_ne_5 = create_comparison(col_a.clone(), const_5.clone(), ComparisonOp::NotEqual)?;

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should be simplified to one predicate");
        assert_eq!(
            get_function_name(&result[0]),
            Some("gt"),
            "Function should be gt (greater than)"
        );

        if let ScalarExpr::FunctionCall(func) = &result[0] {
            assert_eq!(
                get_int_value(&func.arguments[1]),
                Some(5),
                "Second arg should be constant 5"
            );
        }
    }

    Ok(())
}
