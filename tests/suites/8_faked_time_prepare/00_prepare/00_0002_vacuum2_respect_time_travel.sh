#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../../../shell_env.sh

stmt "create or replace table test_vacuum2_respect_time_travel(a int);"

stmt "insert into test_vacuum2_respect_time_travel values(1);"