#!/bin/bash
# Copyright 2020-2021 The Datafuse Authors.
# SPDX-License-Identifier: Apache-2.0.

echo "Starting Cluster fuse-query"
./scripts/deploy/fusequery-cluster-3-nodes.sh

SCRIPT_PATH="$( cd "$( dirname "$0" )" >/dev/null 2>&1 && pwd )"
cd "$SCRIPT_PATH/../../tests" || exit

echo "Starting fuse-test"
./fuse-test '^0[^4]_' --skip-dir '1_performance'