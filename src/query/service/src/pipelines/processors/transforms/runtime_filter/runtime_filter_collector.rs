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
use std::collections::HashMap;
use std::sync::{Arc, mpsc, Mutex};
use std::sync::mpsc::{Receiver, Sender};

use common_catalog::plan::RuntimeFilterId;
use common_catalog::table_context::TableContext;
use common_exception::{ErrorCode, Result};
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::RemoteExpr;
use common_functions::scalars::BUILTIN_FUNCTIONS;

use storages_common_index::filters::FilterBuilder;
use storages_common_index::filters::Xor8Builder;
use storages_common_index::filters::Xor8Filter;

use crate::sessions::QueryContext;

pub struct RuntimeFilterCollector {
    pub filters_tx: Mutex<Sender<HashMap<RuntimeFilterId, Xor8Filter>>>,
    pub filters_rx: Mutex<Receiver<HashMap<RuntimeFilterId, Xor8Filter>>>,
    pub filter_builders: HashMap<RuntimeFilterId, Xor8Builder>,
}

impl RuntimeFilterCollector {
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            filters_tx: Mutex::new(tx),
            filters_rx: Mutex::new(rx),
            filter_builders: Default::default(),
        }
    }

    pub fn collect(
        &mut self,
        ctx: Arc<QueryContext>,
        exprs: &BTreeMap<RuntimeFilterId, RemoteExpr>,
        data: &DataBlock,
    ) -> Result<()> {
        let func_ctx = ctx.get_function_context()?;
        for (id, remote_expr) in exprs.iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            // expr represents equi condition in join build side
            // Such as: `select * from t1 inner join t2 on t1.a + 1 = t2.a + 2`
            // expr is `t2.a + 2`
            // First we need get expected values from data by expr
            let evaluator = Evaluator::new(data, func_ctx, &BUILTIN_FUNCTIONS);
            let column = evaluator
                .run(&expr)?
                .convert_to_full_column(expr.data_type(), data.num_rows());

            // Generate Xor8 filter by column
            if let Some(filter_builder) = self.filter_builders.get_mut(id) {
                for val in column.iter() {
                    filter_builder.add_key(&val);
                }
            } else {
                let mut filter_builder = Xor8Builder::create();
                for val in column.iter() {
                    filter_builder.add_key(&val);
                }
                self.filter_builders.insert(id.clone(), filter_builder);
            }
        }
        Ok(())
    }

    pub fn send(&mut self) -> Result<()> {
        let mut filters = HashMap::new();
        for (id, filter_builder) in self.filter_builders.iter_mut() {
            filters.insert(id.clone(), filter_builder.build()?);
        }
        let tx = self.filters_tx.lock().unwrap();
        if let Err(_ ) = tx.send(filters) {
            return Err(ErrorCode::Internal("send runtime filters fail"));
        }
        Ok(())
    }

    pub fn recv(&mut self) -> Result<HashMap<RuntimeFilterId, Xor8Filter>> {
        let rx = self.filters_rx.lock().unwrap();
        loop {
            let res = rx.try_recv();
            if res.is_ok() {
                return Ok(res.unwrap());
            }
        }
    }
}
