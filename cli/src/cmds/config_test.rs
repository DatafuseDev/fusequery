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

use std::cell::RefCell;

use httpmock::Method::GET;
use httpmock::MockServer;
use tempfile::tempdir;

use crate::cmds::config::choose_mirror;
use crate::cmds::config::CustomMirror;
use crate::cmds::config::GithubMirror;
use crate::cmds::config::MirrorAsset;
use crate::cmds::Config;
use crate::cmds::Status;
use crate::error::Result;

#[test]
fn test_mirror() -> Result<()> {
    let mut conf = Config {
        group: "foo".to_string(),
        databend_dir: "/tmp/.databend".to_string(),
        clap: RefCell::new(Default::default()),
        mirror: GithubMirror {}.to_mirror(),
    };
    let t = tempdir()?;
    conf.databend_dir = t.path().to_str().unwrap().to_string();
    // Start a lightweight mock server.
    let server = MockServer::start();
    // Create a mock on the server.
    let _ = server.mock(|when, then| {
        when.method(GET).path("/v1/health");
        then.status(200)
            .header("content-type", "text/html")
            .body("health");
    });

    // situation 1: user defined mirror
    {
        let custom = CustomMirror {
            base_url: server.url("/v1/health"),
            databend_url: "".to_string(),
            databend_tag_url: "".to_string(),
            client_url: "".to_string(),
        };
        conf.mirror = custom.to_mirror();
        let mirror = choose_mirror(&conf).unwrap();
        assert_eq!(custom.to_mirror(), mirror);
        let status = Status::read(conf.clone()).unwrap();
        assert_eq!(status.mirrors.unwrap(), custom.to_mirror());
    }
    // situation 2: previous mirror
    {
        let status_mirror = CustomMirror {
            base_url: server.url("/v1/health"),
            databend_url: "".to_string(),
            databend_tag_url: "".to_string(),
            client_url: "".to_string(),
        };
        let mut status = Status::read(conf.clone()).unwrap();
        status.mirrors = Some(status_mirror.to_mirror());
        status.write().unwrap();
        let custom = GithubMirror {}.to_mirror();
        conf.mirror = custom;
        let mirror = choose_mirror(&conf).unwrap();
        assert_eq!(mirror, status_mirror.to_mirror());
        let status = Status::read(conf).unwrap();
        assert_eq!(status.mirrors.unwrap(), status_mirror.to_mirror());
    }
    Ok(())
}
