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

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field as ArrowField;
use arrow_schema::Fields;
use arrow_schema::Schema as ArrowSchema;
use databend_common_arrow::arrow::datatypes::DataType as Arrow2DataType;
use databend_common_arrow::arrow::datatypes::Field as Arrow2Field;
use databend_common_exception::Result;

use super::EXTENSION_KEY;
use crate::converts::arrow2::table_field_to_arrow2_field_ignore_inside_nullable;
use crate::Column;
use crate::DataBlock;
use crate::DataField;
use crate::DataSchema;
use crate::TableField;
use crate::TableSchema;

impl From<&DataSchema> for ArrowSchema {
    fn from(schema: &DataSchema) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| arrow_field_from_arrow2_field(Arrow2Field::from(f)))
            .collect::<Vec<_>>();
        ArrowSchema {
            fields: Fields::from(fields),
            metadata: schema.metadata.clone().into_iter().collect(),
        }
    }
}

impl From<&TableSchema> for ArrowSchema {
    fn from(schema: &TableSchema) -> Self {
        let fields = schema
            .fields
            .iter()
            .map(|f| arrow_field_from_arrow2_field(Arrow2Field::from(f)))
            .collect::<Vec<_>>();
        ArrowSchema {
            fields: Fields::from(fields),
            metadata: schema.metadata.clone().into_iter().collect(),
        }
    }
}

/// Parquet2 can't dealing with nested type like Tuple(int not null,int null) null, but for type like Tuple(int null,int null) null, it can work.
///
/// So when casting from TableSchema to Arrow2 schema, the inner type inherit the nullable property from outer type.
///
/// But when casting from TableSchema to Arrow-rs schema, there is no such problem, so the inside nullable is ignored.
pub fn table_schema_to_arrow_schema_ignore_inside_nullable(schema: &TableSchema) -> ArrowSchema {
    let fields = schema
        .fields
        .iter()
        .map(|f| {
            arrow_field_from_arrow2_field(table_field_to_arrow2_field_ignore_inside_nullable(f))
        })
        .collect::<Vec<_>>();
    ArrowSchema {
        fields: Fields::from(fields),
        metadata: schema.metadata.clone().into_iter().collect(),
    }
}

impl From<&TableField> for ArrowField {
    fn from(field: &TableField) -> Self {
        arrow_field_from_arrow2_field(Arrow2Field::from(field))
    }
}

impl From<&DataField> for ArrowField {
    fn from(field: &DataField) -> Self {
        arrow_field_from_arrow2_field(Arrow2Field::from(field))
    }
}

impl DataBlock {
    pub fn to_record_batch(self, data_schema: &DataSchema) -> Result<RecordBatch> {
        let mut arrays = Vec::with_capacity(self.columns().len());
        let mut arrow_fields = Vec::with_capacity(self.columns().len());
        for (entry, f) in self
            .convert_to_full()
            .columns()
            .iter()
            .zip(data_schema.fields())
        {
            let column = entry.value.to_owned().into_column().unwrap();
            let array = column.into_arrow_rs();
            let arrow_field = ArrowField::new(
                f.name(),
                array.data_type().clone(),
                f.data_type().is_nullable(),
            );
            arrays.push(array);
            arrow_fields.push(arrow_field);
        }
        let schema = Arc::new(ArrowSchema::new(arrow_fields));
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

impl Column {
    pub fn into_arrow_rs(self) -> Arc<dyn arrow_array::Array> {
        let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> = self.as_arrow();
        let arrow_array: Arc<dyn arrow_array::Array> = arrow2_array.into();
        arrow_array
    }
}

fn arrow_field_from_arrow2_field(field: Arrow2Field) -> ArrowField {
    let mut metadata = HashMap::new();

    let arrow2_data_type = if let Arrow2DataType::Extension(extension_type, ty, _) = field.data_type
    {
        metadata.insert(EXTENSION_KEY.to_string(), extension_type.clone());
        *ty
    } else {
        field.data_type
    };

    let data_type = match arrow2_data_type {
        Arrow2DataType::List(f) => ArrowDataType::List(Arc::new(arrow_field_from_arrow2_field(*f))),
        Arrow2DataType::LargeList(f) => {
            ArrowDataType::LargeList(Arc::new(arrow_field_from_arrow2_field(*f)))
        }
        Arrow2DataType::FixedSizeList(f, size) => {
            ArrowDataType::FixedSizeList(Arc::new(arrow_field_from_arrow2_field(*f)), size as _)
        }
        Arrow2DataType::Map(f, ordered) => {
            ArrowDataType::Map(Arc::new(arrow_field_from_arrow2_field(*f)), ordered)
        }
        Arrow2DataType::Struct(f) => {
            ArrowDataType::Struct(f.into_iter().map(arrow_field_from_arrow2_field).collect())
        }
        other => other.into(),
    };

    ArrowField::new(field.name, data_type, field.is_nullable).with_metadata(metadata)
}
