// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[cfg(test)]
mod comparison_test;

mod comparison;
mod comparison_eq;
mod comparison_gt;
mod comparison_gt_eq;
mod comparison_like;
mod comparison_lt;
mod comparison_lt_eq;
mod comparison_not_eq;
mod comparison_not_like;

pub use comparison::ComparisonFunction;
pub use comparison_eq::ComparisonEqFunction;
pub use comparison_gt::ComparisonGtFunction;
pub use comparison_gt_eq::ComparisonGtEqFunction;
pub use comparison_like::ComparisonLikeFunction;
pub use comparison_lt::ComparisonLtFunction;
pub use comparison_lt_eq::ComparisonLtEqFunction;
pub use comparison_not_eq::ComparisonNotEqFunction;
pub use comparison_not_like::ComparisonNotLikeFunction;
