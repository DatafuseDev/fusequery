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

//use std::net::SocketAddr;
//use std::sync::Arc;
//
use std::fs::File;
use std::io::Read;

use common_exception::Result;
use common_runtime::tokio;

use crate::api::HttpService;
use crate::clusters::Cluster;
use crate::configs::Config;
use crate::servers::Server;
use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_http_service_tls_server() -> Result<()> {
    let mut conf = Config::default();

    conf.api_tls_server_key = TEST_SERVER_KEY.to_owned();
    conf.api_tls_server_cert = TEST_SERVER_CERT.to_owned();

    let addr_str = "127.0.0.1:0";
    let cluster = Cluster::create_global(conf.clone())?;
    let mut srv = HttpService::create(conf.clone(), cluster.clone());
    let listening = srv.start(addr_str.parse()?).await?;
    let port = listening.port();

    // test cert is issued for "localhost"
    let url = format!("https://{}:{}/v1/hello", TEST_CN_NAME, port);

    // load cert
    let mut buf = Vec::new();
    File::open(TEST_CA_CERT)?.read_to_end(&mut buf)?;
    let cert = reqwest::Certificate::from_pem(&buf).unwrap();

    // kick off
    let client = reqwest::Client::builder()
        .add_root_certificate(cert)
        .build()
        .unwrap();
    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!("/v1/hello", resp.url().path());

    Ok(())
}
