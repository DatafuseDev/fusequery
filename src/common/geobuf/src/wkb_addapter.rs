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

use geozero::error::GeozeroError;
use geozero::GeozeroGeometry;

use crate::Geometry;
use crate::GeometryBuilder;

pub struct Wkb<B: AsRef<[u8]>>(pub B);

impl<B: AsRef<[u8]>> TryFrom<Wkb<B>> for Geometry {
    type Error = GeozeroError;

    fn try_from(value: Wkb<B>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geozero::wkb::Wkb(value.0).process_geom(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Wkb<Vec<u8>>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Wkb<Vec<u8>>, Self::Error> {
        use geozero::CoordDimensions;

        Ok(Wkb(geozero::ToWkb::to_ewkb(
            self,
            CoordDimensions::xy(),
            self.srid(),
        )?))
    }
}
