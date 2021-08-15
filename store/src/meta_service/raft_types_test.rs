// Copyright 2020 Datafuse Labs.
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

use std::ops::Bound;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::sled_serde::SledRangeSerde;
use crate::meta_service::NodeId;

#[test]
fn test_node_id_serde() -> anyhow::Result<()> {
    let id9: NodeId = 9;
    let id10: NodeId = 10;

    let got9 = id9.ser()?;
    let got10 = id10.ser()?;
    assert!(got9 < got10);

    let got9 = NodeId::de(got9)?;
    let got10 = NodeId::de(got10)?;
    assert_eq!(id9, got9);
    assert_eq!(id10, got10);

    Ok(())
}

#[test]
fn test_node_id_range_serde() -> anyhow::Result<()> {
    let a: NodeId = 8;
    let b: NodeId = 11;
    let got = (a..b).ser()?;
    let want = (
        Bound::Included(sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 8])),
        Bound::Excluded(sled::IVec::from(vec![0, 0, 0, 0, 0, 0, 0, 11])),
    );
    assert_eq!(want, got);
    Ok(())
}
