pub mod client {
    tonic::include_proto!("client");
}

use client::kv_client::KvClient;
type ClientRequest = client::Request;
use client::response::Result;

use clap::Clap;

use rand::Rng;
use std::io::prelude::*;
use std::io::{self, BufReader};
use std::net::{IpAddr};

#[derive(Clap)]
struct Opts {
    #[clap(short, long, required = true)]
    server_ip: IpAddr,
}

#[tokio::main]
async fn main() {
    let opts = Opts::parse();

    let mut client = KvClient::connect(format!("http://{}:{}", opts.server_ip, 50051)).await.unwrap();

    let buf = BufReader::new(io::stdin());

    let mut rng = rand::thread_rng();
    let mut next_req = vec![];

    print!("> ");
    let _ = io::stdout().flush();
    for line in buf.lines() {
        let line = line.unwrap();
        if line == "" {
            let body = std::mem::replace(&mut next_req, vec![]).join("\n");
            //eprintln!("sending request:\n---\n{}\n---\n", body);
            println!("sending request...");

            let req = tonic::Request::new(ClientRequest {
                id: rng.gen(),
                body,
                timeout: 5000,
            });
            let resp = client.execute(req).await;

            match resp {
                Ok(r) => match r.into_inner().result.unwrap() {
                    Result::Read(r) => {
                        println!("result: {:?}", r.value);
                    }
                    Result::RedirectTo(m) => {
                        println!(
                            "Redirect to: {:?}",
                            m.maybe_id.map(|client::redirect_to::MaybeId::Id(i)| i)
                        );
                    }
                    Result::NodesUnreachable(client::Empty {}) => {
                        println!("Nodes unreachable");
                    }
                    Result::LostLeadership(m) => {
                        println!(
                            "Lost leadership: {:?}, could_handle: {}",
                            m.maybe_id.map(|client::lost_leadership::MaybeId::Id(i)| i),
                            m.could_handle
                        );
                    }
                    Result::Closing(client::Empty {}) => {
                        println!("Closing");
                    }
                    Result::Timeout(client::Empty {}) => {
                        println!("Timeout");
                    }
                    Result::TooManyRequests(client::Empty {}) => {
                        println!("Too many requests");
                    }
                    Result::CompileError(m) => {
                        println!("compile error: {}", m);
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
            println!("");
            print!("> ");
            let _ = io::stdout().flush();
        //println!("\nRESPONSE={:?}", resp);
        } else {
            next_req.push(line);
            print!(". ");
            let _ = io::stdout().flush();
        }
    }
}
