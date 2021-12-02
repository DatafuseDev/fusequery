// Copyright 2021 Datafuse Labs.
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

use std::io::Read;
use std::io::Seek;
use std::sync::Arc;

use common_exception::Result;
use futures::stream::Stream;
use futures::AsyncRead;
use futures::AsyncReadExt;
use futures::AsyncSeek;
use serde::de::DeserializeOwned;

pub type Bytes = Vec<u8>;

pub trait AsyncSeekableReader: futures::AsyncRead + futures::AsyncSeek {}

impl<T> AsyncSeekableReader for T where T: AsyncRead + AsyncSeek {}

pub type InputStream = Box<dyn AsyncSeekableReader + Send + Unpin>;

pub trait SeekableReader: Read + Seek {}

impl<T> SeekableReader for T where T: Read + Seek {}

#[async_trait::async_trait]
pub trait DataAccessor: Send + Sync {
    fn get_input_stream(&self, path: &str, stream_len: Option<u64>) -> Result<InputStream>;

    async fn put(&self, path: &str, content: Vec<u8>) -> Result<()>;

    async fn put_stream(
        &self,
        path: &str,
        input_stream: Box<
            dyn Stream<Item = std::result::Result<bytes::Bytes, std::io::Error>>
                + Send
                + Unpin
                + 'static,
        >,
        stream_len: usize,
    ) -> Result<()>;

    async fn read(&self, location: &str) -> Result<Vec<u8>> {
        let mut buffer = vec![];
        let cache = self.get_data_from_cache(location).await;
        if let Some(data) = cache {
            buffer = data;
        } else {
            let mut input_stream = self.get_input_stream(location, None)?;
            input_stream.read_to_end(&mut buffer).await?;
            self.put_data_to_cache(location, buffer.clone()).await?;
        }
        Ok(buffer)
    }

    async fn get_data_from_cache(&self, _location: &str) -> Option<Vec<u8>> {
        None
    }

    async fn put_data_to_cache(&self, _location: &str, _data: Vec<u8>) -> Result<()> {
        Ok(())
    }
}

pub async fn read_obj<T: DeserializeOwned>(da: Arc<dyn DataAccessor>, loc: String) -> Result<T> {
    let bytes = da.read(&loc).await?;
    let r = serde_json::from_slice::<T>(&bytes)?;
    Ok(r)
}
