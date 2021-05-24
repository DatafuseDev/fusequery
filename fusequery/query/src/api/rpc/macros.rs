// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

// Match the result in the async.
// If error, send the error to the channel.
// Else, return the value.
macro_rules! match_async_result {
    ($VALUE:expr, $SENDER:ident) => {{
        match ($VALUE) {
            Err(e) => {
                let msg = format!("{:?}", e);
                log::error!("{}", msg);
                $SENDER.send(Err(tonic::Status::internal(msg))).await.ok();
                return;
            }
            Ok(v) => v,
        }
    }};
}
