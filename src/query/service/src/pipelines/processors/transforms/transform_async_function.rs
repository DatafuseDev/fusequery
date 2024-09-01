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

use core::str;
use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::result::Result::Ok;
use std::sync::Arc;

use arrow_ipc::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_sql::plans::DictGetFunctionArgument;
use databend_common_sql::plans::DictionarySource;
use databend_common_storage::build_operator;
use databend_common_storages_fuse::TableContext;
use opendal::services::Redis;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::sql::executor::physical_plans::AsyncFunctionDesc;
use crate::sql::plans::AsyncFunctionArgument;
use crate::sql::IndexType;

pub struct TransformAsyncFunction {
    ctx: Arc<QueryContext>,
    // key is the index of async_func_desc
    operators: BTreeMap<usize, Arc<Operator>>,
    async_func_descs: Vec<AsyncFunctionDesc>,
}

impl TransformAsyncFunction {
    pub fn new(
        ctx: Arc<QueryContext>,
        async_func_descs: Vec<AsyncFunctionDesc>,
        operators: BTreeMap<usize, Arc<Operator>>,
    ) -> Self {
        Self {
            ctx,
            async_func_descs,
            operators,
        }
    }

    pub fn init_operators(&mut self) -> Result<()> {
        for (i, async_func_desc) in self.async_func_descs.iter().enumerate() {
            if let AsyncFunctionArgument::DictGetFunction(dict_arg) = &async_func_desc.func_arg {
                match &dict_arg.dict_source {
                    DictionarySource::Redis(conn_str) => {
                        let builder = Redis::default().endpoint(&conn_str);
                        let op = build_operator(builder)?;
                        self.operators.insert(i, Arc::new(op));
                    }
                    _ => {
                        todo!()
                    }
                }
            }
        }
        Ok(())
    }

    async fn get_or_create_operator(
        &mut self,
        key: usize,
        conn_str: String,
    ) -> Result<Arc<Operator>> {
        if let Some(operator) = self.operators.get(&key) {
            return Ok(operator.clone());
        } else {
            let builder = Redis::default().endpoint(&conn_str);
            let op = build_operator(builder)?;
            let op_arc = Arc::new(op);
            self.operators.insert(key, op_arc.clone());
            return Ok(op_arc.clone());
        }
    }

    // transform add sequence nextval column.
    async fn transform_sequence(
        &self,
        data_block: &mut DataBlock,
        sequence_name: &String,
        data_type: &DataType,
    ) -> Result<()> {
        let count = data_block.num_rows() as u64;
        let value = if count == 0 {
            UInt64Type::from_data(vec![])
        } else {
            let tenant = self.ctx.get_tenant();
            let catalog = self.ctx.get_default_catalog()?;
            let req = GetSequenceNextValueReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                count,
            };
            let resp = catalog.get_sequence_next_value(req).await?;
            let range = resp.start..resp.start + count;
            UInt64Type::from_data(range.collect::<Vec<u64>>())
        };
        let entry = BlockEntry {
            data_type: data_type.clone(),
            value: Value::Column(value),
        };
        data_block.add_column(entry);

        Ok(())
    }

    // transform add dict get column.
    async fn transform_dict_get(
        &mut self,
        data_block: &mut DataBlock,
        dict_arg: &DictGetFunctionArgument,
        arg_indices: &Vec<IndexType>,
        data_type: &DataType,
    ) -> Result<()> {
        let mut index = 0;
        let mut flag = false;
        for (i, async_func_desc) in self.async_func_descs.iter().enumerate() {
            match async_func_desc.func_arg {
                AsyncFunctionArgument::DictGetFunction(_) => {
                    index = i;
                    flag = true;
                }
                AsyncFunctionArgument::SequenceFunction(_) => continue,
            }
        }
        if !flag {
            return Err(ErrorCode::UnknownDictGetFunction(
                "Don't find `dict_get` function",
            ));
        }

        let conn_str: String;
        let op = match &dict_arg.dict_source {
            DictionarySource::Redis(conn_str) => {
                self.get_or_create_operator(index, conn_str.clone()).await?
            }
            _ => {
                todo!()
            }
        };
        // only support one key field.
        let arg_index = arg_indices[0];

        let entry = data_block.get_by_offset(arg_index);
        let value = match &entry.value {
            Value::Scalar(scalar) => {
                if let Scalar::String(key) = scalar {
                    let buffer = op.read(key).await;
                    let value = match buffer {
                        Ok(res) => String::from_utf8((&res.current()).to_vec()).unwrap(),
                        Err(_) => {
                            if let Some(default) = &dict_arg.default_res {
                                default.to_string()
                            } else {
                                "".to_string()
                            }
                        }
                    };
                    Value::Scalar(Scalar::String(value))
                } else {
                    Value::Scalar(Scalar::String("".to_string()))
                }
            }
            Value::Column(column) => {
                let mut builder = StringColumnBuilder::with_capacity(column.len(), 0);
                for scalar in column.iter() {
                    if let ScalarRef::String(key) = scalar {
                        let buffer = op.read(key).await;
                        let value = match buffer {
                            Ok(res) => String::from_utf8((&res.current()).to_vec()).unwrap(),
                            Err(_) => {
                                if let Some(default) = &dict_arg.default_res {
                                    default.to_string()
                                } else {
                                    "".to_string()
                                }
                            }
                        };
                        // let value = dict_arg.dict_get_operators.get_or_add_value(
                        //     conn_str.clone(),
                        //     key.to_string(),
                        //     dict_arg,
                        // ).await?;

                        // let value = if let Some(cached_val) = cache.get(key) {
                        //     cached_val.clone()
                        // } else {
                        //     drop(cache);
                        //     let res = op.read(&key).await;
                        //     let val = match res {
                        //         Ok(res) => String::from_utf8((&res.current()).to_vec()).unwrap(),
                        //         Err(_) => if let Some(default) = &dict_arg.default_res {
                        //             default.to_string()
                        //         } else {
                        //             "".to_string()
                        //         },
                        //     };
                        //     let mut cache = CACHE.write().unwrap();
                        //     cache.insert(key.to_string(), val.clone());
                        //     val
                        // };
                        builder.put_str(value.as_str());
                    }
                    builder.commit_row();
                }
                Value::Column(Column::String(builder.build()))
            }
        };
        let entry = BlockEntry {
            data_type: data_type.clone(),
            value,
        };
        data_block.add_column(entry);

        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncTransform for TransformAsyncFunction {
    const NAME: &'static str = "AsyncFunction";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data_block: DataBlock) -> Result<DataBlock> {
        self.init_operators()?;
        for async_func_desc in self.async_func_descs {
            match &async_func_desc.func_arg {
                AsyncFunctionArgument::SequenceFunction(sequence_name) => {
                    self.transform_sequence(
                        &mut data_block,
                        sequence_name,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
                AsyncFunctionArgument::DictGetFunction(dict_arg) => {
                    self.transform_dict_get(
                        &mut data_block,
                        dict_arg,
                        &async_func_desc.arg_indices,
                        &async_func_desc.data_type,
                    )
                    .await?;
                }
            }
        }
        Ok(data_block)
    }
}
