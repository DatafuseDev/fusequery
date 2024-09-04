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

use mini_redis::Connection;
use mini_redis::Frame;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[allow(clippy::disallowed_methods)]
#[tokio::main]
async fn main() {
    // Bind the listener to the address
    let listener = TcpListener::bind("0.0.0.0:6379").await.unwrap();

    loop {
        // The second item contains the ip and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();

        // A new task is spawned for each inbound socket.  The socket is
        // moved to the new task and processed there.
        tokio::spawn(async move {
            process(socket).await;
        });
    }
}

async fn process(socket: TcpStream) {
    use std::collections::HashMap;

    use mini_redis::Command::Get;
    use mini_redis::Command::{self};

    // A hashmap is used to store data
    let mut db = HashMap::new();

    db.insert("a".to_string(), "abc".as_bytes().to_vec());
    db.insert("a".to_string(), "def".as_bytes().to_vec());

    // Connection, provided by `mini-redis`, handles parsing frames from
    // the socket
    let mut connection = Connection::new(socket);

    // Use `read_frame` to receive a command from the connection.
    while let Some(frame) = connection.read_frame().await.unwrap() {
        println!("GOT: {:?}", frame);
        let response = match Command::from_frame(frame).unwrap() {
            Get(cmd) => {
                if let Some(value) = db.get(cmd.key()) {
                    // `Frame::Bulk` expects data to be of type `Bytes`. This
                    // type will be covered later in the tutorial. For now,
                    // `&Vec<u8>` is converted to `Bytes` using `into()`.
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            _ => Frame::Simple("Ok".to_string()),
        };

        // Write the response to the client
        connection.write_frame(&response).await.unwrap();
    }
}
