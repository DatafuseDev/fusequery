// Copyright 2022 Datafuse Labs.
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

use std::io::Write;

use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_array() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("array.txt").unwrap();

    test_create(file);
}

fn test_create(file: &mut impl Write) {
    run_ast(file, "[]", &[]);
    run_ast(file, "[NULL, 8, -10]", &[]);
    run_ast(file, "[['a', 'b'], []]", &[]);
    run_ast(file, r#"['a', 1, parse_json('{"foo":"bar"}')]"#, &[]);
    run_ast(
        file,
        r#"[parse_json('[]'), parse_json('{"foo":"bar"}')]"#,
        &[],
    );
}
