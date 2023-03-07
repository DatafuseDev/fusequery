// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;

use common_catalog::plan::DataSourcePlan;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::Literal;
use common_expression::RemoteExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;

use crate::executor::explain::PlanStatsInfo;
use crate::optimizer::ColumnSet;
use crate::plans::JoinType;
use crate::ColumnBinding;
use crate::IndexType;

pub type ColumnID = String;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct TableScan {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub name_mapping: BTreeMap<String, IndexType>,
    pub source: Box<DataSourcePlan>,

    /// Only used for display
    pub table_index: IndexType,
    pub stat_info: Option<PlanStatsInfo>,
}

impl TableScan {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.name_mapping.len());
        let schema = self.source.schema();
        for (name, id) in self.name_mapping.iter() {
            let orig_field = schema.field_with_name(name)?;
            let data_type = DataType::from(orig_field.data_type());
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,

    // Assumption: expression's data type must be `DataType::Boolean`.
    pub predicates: Vec<RemoteExpr>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Filter {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Project {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub projections: Vec<usize>,

    /// Only used for display
    pub columns: ColumnSet,
    pub stat_info: Option<PlanStatsInfo>,
}

impl Project {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::new();
        for i in self.projections.iter() {
            fields.push(input_schema.field(*i).clone());
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EvalScalar {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub exprs: Vec<(RemoteExpr, IndexType)>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl EvalScalar {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for (expr, index) in self.exprs.iter() {
            let name = index.to_string();
            let data_type = expr.as_expr(&BUILTIN_FUNCTIONS).data_type().clone();
            fields.push(DataField::new(&name, data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Unnest {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    /// Columns need to be unnested. (offsets in the input DataBlock)
    pub offsets: Vec<usize>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Unnest {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = input_schema.fields().clone();
        for offset in &self.offsets {
            let f = &mut fields[*offset];
            let inner_type = f.data_type().as_array().unwrap();
            *f = DataField::new(f.name(), inner_type.wrap_nullable());
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregatePartial {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregatePartial {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        // TODO(dousir9) distinguish join scenario from other scenarios.
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        // if we push down agg_partial below join, the fields must contain:
        // (1) aggregates (added to the output_schema of the hash table, and then used by agg_final).
        // (2) join conditions (used by build_keys/prob_keys).
        // (3) group by expressions(equal to (2) now).
        for agg in self.agg_funcs.iter() {
            fields.push(DataField::new(
                &agg.output_column.to_string(),
                DataType::String,
            ));
            let arg_indices = &agg.arg_indices;
            for arg_index in arg_indices.into_iter() {
                fields.push(DataField::new(
                    &arg_index.to_string(),
                    DataType::String,
                ));
            }
        }
        // Join conditions
        // TODO(dousir9) may be we can replace group_by.iter() by reusing _group_by_key.
        for id in self.group_by.iter() {
            let data_type = self
                .input.output_schema()?
                .field_with_name(&id.to_string())?
                .data_type()
                .clone();
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        if !self.group_by.is_empty() {
            let method = DataBlock::choose_hash_method_with_types(
                &self
                    .group_by
                    .iter()
                    .map(|index| {
                        Ok(input_schema
                            .field_with_name(&index.to_string())?
                            .data_type()
                            .clone())
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?;
            fields.push(DataField::new("_group_by_key", method.data_type()));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AggregateFinal {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub before_group_by_schema: DataSchemaRef,

    pub limit: Option<usize>,
    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AggregateFinal {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        for agg in self.agg_funcs.iter() {
            let data_type = agg.sig.return_type.clone();
            fields.push(DataField::new(&agg.output_column.to_string(), data_type));
        }
        for id in self.group_by.iter() {
            let data_type = self
                .before_group_by_schema
                .field_with_name(&id.to_string())?
                .data_type()
                .clone();
            fields.push(DataField::new(&id.to_string(), data_type));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Sort {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub order_by: Vec<SortDesc>,
    // limit = Limit.limit + Limit.offset
    pub limit: Option<usize>,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Sort {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Limit {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub limit: Option<usize>,
    pub offset: usize,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl Limit {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashJoin {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub build: Box<PhysicalPlan>,
    pub probe: Box<PhysicalPlan>,
    pub build_keys: Vec<RemoteExpr>,
    pub probe_keys: Vec<RemoteExpr>,
    pub non_equi_conditions: Vec<RemoteExpr>,
    pub join_type: JoinType,
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl HashJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let mut fields = self.probe.output_schema()?.fields().clone();
        match self.join_type {
            JoinType::Left | JoinType::Single => {
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
            }
            JoinType::Right => {
                fields.clear();
                for field in self.probe.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().clone(),
                    ));
                }
            }
            JoinType::Full => {
                fields.clear();
                for field in self.probe.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().wrap_nullable(),
                    ));
                }
            }
            JoinType::LeftSemi | JoinType::LeftAnti => {
                // Do nothing
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                fields.clear();
                fields = self.build.output_schema()?.fields().clone();
            }
            JoinType::LeftMark | JoinType::RightMark => {
                fields.clear();
                let outer_table = if self.join_type == JoinType::RightMark {
                    &self.probe
                } else {
                    &self.build
                };
                fields = outer_table.output_schema()?.fields().clone();
                let name = if let Some(idx) = self.marker_index {
                    idx.to_string()
                } else {
                    "marker".to_string()
                };
                fields.push(DataField::new(
                    name.as_str(),
                    DataType::Nullable(Box::new(DataType::Boolean)),
                ));
            }

            _ => {
                for field in self.build.output_schema()?.fields() {
                    fields.push(DataField::new(
                        field.name().as_str(),
                        field.data_type().clone(),
                    ));
                }
            }
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Exchange {
    pub input: Box<PhysicalPlan>,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,
}

impl Exchange {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        self.input.output_schema()
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSource {
    /// Output schema of exchanged data
    pub schema: DataSchemaRef,

    /// Fragment ID of source fragment
    pub source_fragment_id: usize,
    pub query_id: String,
}

impl ExchangeSource {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FragmentKind {
    // Init-partition
    Init,
    // Partitioned by hash
    Normal,
    // Broadcast
    Expansive,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSink {
    pub input: Box<PhysicalPlan>,
    /// Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,

    /// Fragment ID of sink fragment
    pub destination_fragment_id: usize,
    /// Addresses of destination nodes
    pub destinations: Vec<String>,
    pub query_id: String,
}

impl ExchangeSink {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    /// A unique id of operator in a `PhysicalPlan` tree.
    /// Only used for display.
    pub plan_id: u32,

    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub pairs: Vec<(String, String)>,
    pub schema: DataSchemaRef,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedInsertSelect {
    pub input: Box<PhysicalPlan>,
    pub catalog: String,
    pub table_info: TableInfo,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
}

impl DistributedInsertSelect {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRefExt::create(vec![
            DataField::new("seg_loc", DataType::String),
            DataField::new("seg_info", DataType::String),
        ]))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum PhysicalPlan {
    TableScan(TableScan),
    Filter(Filter),
    Project(Project),
    EvalScalar(EvalScalar),
    Unnest(Unnest),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Sort(Sort),
    Limit(Limit),
    HashJoin(HashJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),

    /// Synthesized by fragmenter
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),
}

impl PhysicalPlan {
    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::TableScan(plan) => plan.output_schema(),
            PhysicalPlan::Filter(plan) => plan.output_schema(),
            PhysicalPlan::Project(plan) => plan.output_schema(),
            PhysicalPlan::EvalScalar(plan) => plan.output_schema(),
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.output_schema(),
            PhysicalPlan::Unnest(plan) => plan.output_schema(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            PhysicalPlan::TableScan(_) => "TableScan".to_string(),
            PhysicalPlan::Filter(_) => "Filter".to_string(),
            PhysicalPlan::Project(_) => "Project".to_string(),
            PhysicalPlan::EvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::AggregatePartial(_) => "AggregatePartial".to_string(),
            PhysicalPlan::AggregateFinal(_) => "AggregateFinal".to_string(),
            PhysicalPlan::Sort(_) => "Sort".to_string(),
            PhysicalPlan::Limit(_) => "Limit".to_string(),
            PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
            PhysicalPlan::Exchange(_) => "Exchange".to_string(),
            PhysicalPlan::UnionAll(_) => "UnionAll".to_string(),
            PhysicalPlan::DistributedInsertSelect(_) => "DistributedInsertSelect".to_string(),
            PhysicalPlan::ExchangeSource(_) => "Exchange Source".to_string(),
            PhysicalPlan::ExchangeSink(_) => "Exchange Sink".to_string(),
            PhysicalPlan::Unnest(_) => "Unnest".to_string(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_ref()).chain(std::iter::once(plan.build.as_ref())),
            ),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ExchangeSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::Unnest(plan) => Box::new(std::iter::once(plan.input.as_ref())),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionDesc {
    pub sig: AggregateFunctionSignature,
    pub output_column: IndexType,
    pub args: Vec<usize>,
    pub arg_indices: Vec<IndexType>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AggregateFunctionSignature {
    pub name: String,
    pub args: Vec<DataType>,
    pub params: Vec<Literal>,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SortDesc {
    pub asc: bool,
    pub nulls_first: bool,
    pub order_by: IndexType,
}
