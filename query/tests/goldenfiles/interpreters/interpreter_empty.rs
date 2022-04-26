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

use common_base::tokio;
use common_exception::Result;
use databend_query::interpreters::*;
use databend_query::sql::*;
use futures::TryStreamExt;
use goldenfile::Mint;
use pretty_assertions::assert_eq;

use crate::interpreters::interpreter_goldenfiles;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_empty_interpreter() -> Result<()> {
    common_tracing::init_default_ut_tracing();
    let ctx = crate::tests::create_query_context().await?;

    {
        let query = "/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */";
        let plan = PlanParser::parse(ctx.clone(), query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), plan)?;
        assert_eq!(executor.name(), "EmptyInterpreter");

        let stream = executor.execute(None).await?;
        let result = stream.try_collect::<Vec<_>>().await;
        assert!(result.is_ok())
    }

    {
        let mut mint = Mint::new("tests/goldenfiles/data");
        let mut file = mint.new_goldenfile("empty.txt").unwrap();

        interpreter_goldenfiles(
            &mut file,
            ctx.clone(),
            "SelectInterpreter",
            r#"/*!40101*/select number from numbers_mt(1)"#,
        )
        .await?;
    }

    Ok(())
}
