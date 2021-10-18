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
use std::thread::sleep;
use std::time::Duration;

use clap::App;
use clap::AppSettings;
use clap::Arg;
use clap::ArgMatches;
use serde::Deserialize;
use serde::Serialize;

use crate::cmds::queries::query::QueryCommand;
use crate::cmds::ClusterCommand;
use crate::cmds::PackageCommand;
use crate::cmds::Status;
use crate::cmds::VersionCommand;
use crate::cmds::Writer;
use crate::error::CliError;

const GITHUB_BASE_URL: &str = "https://github.com";
const GITHUB_DATABEND_URL: &str = "https://github.com/datafuselabs/databend/releases/download";
const GITHUB_DATABEND_TAG_URL: &str = "https://api.github.com/repos/datafuselabs/databend/tags";
const GITHUB_CLIENT_URL: &str = "https://github.com/ZhiHanZ/usql/releases/download";

#[derive(Clone, Debug)]
pub struct Config {
    //(TODO(zhihanz) remove those field as they already mentioned in Clap global flag)
    pub group: String,

    pub databend_dir: String,
    pub mirror: CustomMirror,
    pub clap: RefCell<ArgMatches>,
}

pub trait MirrorAsset {
    fn is_ok(&self) -> bool {
        if let Ok(res) = ureq::get(self.get_base_url().as_str()).call() {
            return res.status() % 100 != 4 && res.status() % 100 != 5;
        }
        false
    }
    fn get_base_url(&self) -> String;
    fn get_databend_url(&self) -> String;
    fn get_databend_tag_url(&self) -> String;
    fn get_client_url(&self) -> String;
    fn to_mirror(&self) -> CustomMirror {
        CustomMirror {
            base_url: self.get_base_url(),
            databend_url: self.get_databend_url(),
            databend_tag_url: self.get_databend_tag_url(),
            client_url: self.get_client_url(),
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct GithubMirror {}

impl MirrorAsset for GithubMirror {
    fn get_base_url(&self) -> String {
        GITHUB_BASE_URL.to_string()
    }
    fn get_databend_url(&self) -> String {
        GITHUB_DATABEND_URL.to_string()
    }
    fn get_databend_tag_url(&self) -> String {
        GITHUB_DATABEND_TAG_URL.to_string()
    }
    fn get_client_url(&self) -> String {
        GITHUB_CLIENT_URL.to_string()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct CustomMirror {
    pub(crate) base_url: String,
    pub(crate) databend_url: String,
    pub(crate) databend_tag_url: String,
    pub(crate) client_url: String,
}

impl CustomMirror {
    fn new(
        base_url: String,
        databend_url: String,
        databend_tag_url: String,
        client_url: String,
    ) -> Self {
        CustomMirror {
            base_url,
            databend_url,
            databend_tag_url,
            client_url,
        }
    }
}

impl MirrorAsset for CustomMirror {
    fn get_base_url(&self) -> String {
        self.base_url.clone()
    }

    fn get_databend_url(&self) -> String {
        self.databend_url.clone()
    }

    fn get_databend_tag_url(&self) -> String {
        self.databend_tag_url.clone()
    }

    fn get_client_url(&self) -> String {
        self.client_url.clone()
    }
}

// choose one mirror which could be connected
// if the mirror user provided works, it would choose it as default mirror, otherwise it would panic
// if user have not provided a mirror, it would validate on mirror stored in status and warn
// user if it could not be connected.
// in default situation(no mirror stored in status), we provided a pool of possible mirrors to use.
// it would select one working mirror as default mirror
pub fn choose_mirror(conf: &Config) -> Result<CustomMirror, CliError> {
    // try user defined mirror source at first
    let conf = conf.clone();
    let default = GithubMirror {};
    if default.to_mirror() != conf.mirror {
        let custom: CustomMirror = conf.mirror.clone();
        for _ in 0..5 {
            let custom: CustomMirror = conf.mirror.clone();
            if custom.is_ok() {
                let mut status = Status::read(conf).expect("cannot configure status");
                status.mirrors = Some(custom.to_mirror());
                status.write()?;
                return Ok(custom);
            } else {
                sleep(Duration::from_secs(1));
            }
        }
        return Err(CliError::Unknown(format!(
            "cannot connect to the provided mirror {:?}",
            custom
        )));
    }

    let status = Status::read(conf).expect("cannot configure status");
    let mut writer = Writer::create();
    if let Some(mirror) = status.mirrors {
        let custom: CustomMirror = mirror.clone();
        if !custom.is_ok() {
            writer.write_err(&*format!(
                "Mirror error: cannot connect to current mirror {:?}",
                mirror
            ))
        } else {
            return Ok(mirror);
        }
    }

    let default_mirrors: Vec<Box<dyn MirrorAsset>> = vec![Box::new(GithubMirror {})];
    for _ in 0..5 {
        for i in &default_mirrors {
            if i.is_ok() {
                return Ok(i.to_mirror());
            } else {
                sleep(Duration::from_secs(1));
            }
        }
    }

    Err(CliError::Unknown(
        "cannot find possible mirror to connect".to_string(),
    ))
}

impl Config {
    pub(crate) fn build_cli() -> App<'static> {
        App::new("bendctl")
            .setting(AppSettings::ColoredHelp)
            .arg(
                Arg::new("group")
                    .long("group")
                    .about("Sets the group name for configuration")
                    .default_value("test")
                    .env("DATABEND_GROUP")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("databend_dir")
                    .long("databend_dir")
                    .about("Sets the directory to store databend binaries(query and store)")
                    .default_value("~/.databend")
                    .env("databend_dir")
                    .global(true)
                    .takes_value(true)
                    .value_hint(clap::ValueHint::DirPath),
            )
            .arg(
                Arg::new("download_url")
                    .long("download_url")
                    .about("Sets the url to download databend binaries")
                    .default_value("https://github.com/datafuselabs/databend/releases/download")
                    .env("DOWNLOAD_URL")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("tag_url")
                    .long("tag_url")
                    .about("Sets the url to for databend tags")
                    .default_value("https://api.github.com/repos/datafuselabs/databend/tags")
                    .env("DOWNLOAD_URL")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("client_url")
                    .long("client_url")
                    .about("Sets the url to fetch databend query client")
                    .env("DOWNLOAD_CLIENT_URL")
                    .default_value("https://github.com/ZhiHanZ/usql/releases/download")
                    .global(true)
                    .takes_value(true),
            )
            .arg(
                Arg::new("validation_url")
                    .long("validation_url")
                    .about("Sets the url to validate on custom download network connection")
                    .env("DOWNLOAD_VALIDATION_URL")
                    .default_value("https://github.com")
                    .global(true)
                    .takes_value(true),
            )
            .subcommand(
                App::new("completion")
                    .setting(AppSettings::ColoredHelp)
                    .setting(AppSettings::DisableVersionFlag)
                    .about("Generate auto completion scripts for bash or zsh terminal")
                    .arg(
                        Arg::new("completion")
                            .takes_value(true)
                            .possible_values(&["bash", "zsh"]),
                    ),
            )
            .subcommand(PackageCommand::generate())
            .subcommand(VersionCommand::generate())
            .subcommand(ClusterCommand::generate())
            .subcommand(QueryCommand::generate())
    }
    pub fn create() -> Self {
        let clap = RefCell::new(Config::build_cli().get_matches());
        let config = Config {
            group: clap
                .clone()
                .into_inner()
                .value_of("group")
                .unwrap()
                .parse()
                .unwrap(),
            databend_dir: clap
                .clone()
                .into_inner()
                .value_of("databend_dir")
                .unwrap()
                .parse()
                .unwrap(),

            mirror: CustomMirror::new(
                clap.clone()
                    .into_inner()
                    .value_of("validation_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
                clap.clone()
                    .into_inner()
                    .value_of("download_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
                clap.clone()
                    .into_inner()
                    .value_of("tag_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
                clap.clone()
                    .into_inner()
                    .value_of("client_url")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            clap,
        };
        Config::build(config)
    }
    fn build(mut conf: Config) -> Self {
        let home_dir = dirs::home_dir().unwrap();
        let databend_dir = home_dir.join(".databend");

        if conf.databend_dir == "~/.databend" {
            conf.databend_dir = format!("{}/{}", databend_dir.to_str().unwrap(), conf.group);
        } else {
            conf.databend_dir = format!("{}/{}", conf.databend_dir, conf.group);
        }
        let res = choose_mirror(&conf);
        if let Ok(mirror) = res {
            conf.mirror = mirror.clone();
            let mut status = Status::read(conf.clone()).expect("cannot read status");
            status.mirrors = Some(mirror);
            status.write().expect("cannot write status");
        } else {
            panic!("{}", format!("{:?}", res.unwrap_err()))
        }
        conf
    }
}
