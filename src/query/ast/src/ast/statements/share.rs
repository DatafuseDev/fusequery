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

use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_meta_app::schema::OnExist;
use databend_common_meta_app::share::ShareGrantObjectName;
use databend_common_meta_app::share::ShareGrantObjectPrivilege;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use itertools::Itertools;

use super::UriLocation;
use crate::ast::Identifier;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateShareEndpointStmt {
    #[drive(skip)]
    pub create_option: OnExist,
    pub endpoint: Identifier,
    pub url: UriLocation,
    pub tenant: Identifier,
    #[drive(skip)]
    pub args: BTreeMap<String, String>,
    #[drive(skip)]
    pub comment: Option<String>,
}

impl Display for CreateShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let OnExist::Replace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "SHARE ENDPOINT ")?;
        if let OnExist::Keep = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.endpoint)?;
        write!(f, " URL={}", self.url)?;
        write!(f, " TENANT={} ARGS=(", self.tenant)?;
        for (k, v) in self.args.iter() {
            write!(f, "{}={},", k, v)?;
        }
        write!(f, ")")?;
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{comment}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct CreateShareStmt {
    #[drive(skip)]
    pub if_not_exists: bool,
    pub share: Identifier,
    #[drive(skip)]
    pub comment: Option<String>,
}

impl Display for CreateShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CREATE SHARE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{}", self.share)?;
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{comment}'")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropShareStmt {
    #[drive(skip)]
    pub if_exists: bool,
    pub share: Identifier,
}

impl Display for DropShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.share)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct GrantShareObjectStmt {
    pub share: Identifier,
    #[drive(skip)]
    pub object: ShareGrantObjectName,
    #[drive(skip)]
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for GrantShareObjectStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "GRANT {} ON {} TO SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct RevokeShareObjectStmt {
    pub share: Identifier,
    #[drive(skip)]
    pub object: ShareGrantObjectName,
    #[drive(skip)]
    pub privilege: ShareGrantObjectPrivilege,
}

impl Display for RevokeShareObjectStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "REVOKE {} ON {} FROM SHARE {}",
            self.privilege, self.object, self.share
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct AlterShareTenantsStmt {
    pub share: Identifier,
    #[drive(skip)]
    pub if_exists: bool,
    pub tenants: Vec<Identifier>,
    #[drive(skip)]
    pub is_add: bool,
}

impl Display for AlterShareTenantsStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ALTER SHARE ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.share)?;
        if self.is_add {
            write!(f, " ADD TENANTS = ")?;
        } else {
            write!(f, " REMOVE TENANTS = ")?;
        }
        write!(
            f,
            "{}",
            self.tenants.iter().map(|v| v.to_string()).join(",")
        )?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DescShareStmt {
    pub share: Identifier,
}

impl Display for DescShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESC SHARE {}", self.share)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowSharesStmt {}

impl Display for ShowSharesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW SHARES")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowShareEndpointStmt {}

impl Display for ShowShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW SHARE ENDPOINT")?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct DropShareEndpointStmt {
    #[drive(skip)]
    pub if_exists: bool,
    pub endpoint: Identifier,
}

impl Display for DropShareEndpointStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DROP SHARE ENDPOINT {}", self.endpoint)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowObjectGrantPrivilegesStmt {
    #[drive(skip)]
    pub object: ShareGrantObjectName,
}

impl Display for ShowObjectGrantPrivilegesStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW GRANTS ON {}", self.object)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub struct ShowGrantsOfShareStmt {
    #[drive(skip)]
    pub share_name: String,
}

impl Display for ShowGrantsOfShareStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SHOW GRANTS OF SHARE {}", self.share_name)?;

        Ok(())
    }
}
