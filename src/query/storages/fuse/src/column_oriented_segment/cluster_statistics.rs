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

use databend_common_expression::Scalar;
use databend_storages_common_table_meta::meta::ClusterStatistics;

pub trait AbstractClusterStatistics {
    fn cluster_key_id(&self) -> u32;
    fn min(&self) -> &Vec<Scalar>;
    fn max(&self) -> &Vec<Scalar>;
}

impl AbstractClusterStatistics for ClusterStatistics {
    fn cluster_key_id(&self) -> u32 {
        self.cluster_key_id
    }

    fn min(&self) -> &Vec<Scalar> {
        &self.min
    }

    fn max(&self) -> &Vec<Scalar> {
        &self.max
    }
}
