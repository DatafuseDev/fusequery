//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use common_base::BlockingWait;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_planners::Extras;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::datasources::index::RangeFilter;
use crate::datasources::table::fuse::io;
use crate::datasources::table::fuse::io::MetaInfoReader;
use crate::datasources::table::fuse::statistics::BlockStats;
use crate::datasources::table::fuse::BlockMeta;
use crate::datasources::table::fuse::SegmentInfo;
use crate::datasources::table::fuse::TableSnapshot;

pub struct MinMaxIndex {
    table_snapshot_loc: String,
    meta_reader: MetaInfoReader,
}

impl MinMaxIndex {
    pub fn new(table_snapshot: &TableSnapshot, meta_reader: &MetaInfoReader) -> Self {
        Self {
            table_snapshot_loc: io::snapshot_location(
                table_snapshot.snapshot_id.to_simple().to_string(),
            ),
            meta_reader: meta_reader.clone(),
        }
    }

    // Returns an iterator or stream would be better
    pub async fn apply(
        &self,
        schema: DataSchemaRef,
        push_down: Option<Extras>,
    ) -> common_exception::Result<Vec<BlockMeta>> {
        type Pred =
            Box<dyn Fn(&BlockStats) -> common_exception::Result<bool> + Send + Sync + Unpin>;
        let pred_true: fn() -> Pred = || Box::new(|_: &BlockStats| Ok(true));

        let block_pred: Pred = if let Some(exprs) = push_down {
            if let Some(expr) = exprs.filters {
                let verifiable_expression = RangeFilter::try_create(&expr, schema)?;
                Box::new(move |v: &BlockStats| verifiable_expression.eval(v))
            } else {
                pred_true()
            }
        } else {
            pred_true()
        };

        let da = self.meta_reader.data_accessor();
        let snapshot =
            common_dal::read_obj::<TableSnapshot>(da.clone(), self.table_snapshot_loc.clone())
                .await?;
        let segment_num = snapshot.segments.len();
        let segment_locs = snapshot.segments;
        if segment_locs.is_empty() {
            return Ok(vec![]);
        };
        let res = futures::stream::iter(segment_locs)
            .map(|seg_loc| async {
                let segment_info = common_dal::read_obj::<SegmentInfo>(da.clone(), seg_loc).await?;
                let r = if block_pred(&segment_info.summary.col_stats)? {
                    segment_info.blocks.into_iter().try_fold(
                        Vec::new(),
                        |mut acc, block_meta| {
                            if block_pred(&block_meta.col_stats)? {
                                acc.push(block_meta)
                            }
                            Ok::<_, ErrorCode>(acc)
                        },
                    )?
                } else {
                    vec![]
                };
                Ok::<_, ErrorCode>(r)
            })
            // configuration of the max size of buffered futures
            .buffered(std::cmp::min(10, segment_num))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(res.into_iter().flatten().collect())
    }
}

pub fn range_filter(
    table_snapshot: &TableSnapshot,
    schema: DataSchemaRef,
    push_down: Option<Extras>,
    meta_reader: MetaInfoReader,
) -> common_exception::Result<Vec<BlockMeta>> {
    let range_index = MinMaxIndex::new(table_snapshot, &meta_reader);
    async move { range_index.apply(schema, push_down).await }
        .wait_in(meta_reader.runtime(), None)?
}
