# LattiStore

Proof-of-concept implementation of a distributed key-value store with strictly serializable transactions.

## Building

Clone the repository:
```
git clone https://github.com/LattiStore/lattistore
cd lattistore
```
Pull the submodule (there is one with a protobuf file):
```
git submodule init
git submodule update
```

Use [Cargo](https://doc.rust-lang.org/cargo/getting-started/installation.html) to build the project:
```
cargo build
```

## Running

To start a node:
```
cargo run --bin server -- <args>
```

There are two possible arguments:
- `--my-ip <my-ip>`: the IP of this node
- `--node-ips <node-ips>`: the IPs of all nodes in the cluster (including this node) separated by spaces

See the Cluster section below.

To start a client:
```
cargo run --bin client -- --server-ip <server-ip>
```
where `<server-ip>` is the IP of one of the nodes.

The binaries `server` and `client` should also be available under the `target` directory after building, e.g.:
```
target/debug/client --server-ip 127.0.0.1
```

## Cluster

A cluster is a set of nodes. To start a cluster, start each node, passing it its own IP and the IPs of other nodes. For example:
- node 1: `cargo run --bin server --my-ip 127.0.0.1 --node-ips 127.0.0.1 127.0.0.2 127.0.0.3`
- node 2: `cargo run --bin server --my-ip 127.0.0.2 --node-ips 127.0.0.1 127.0.0.2 127.0.0.3`
- node 3: `cargo run --bin server --my-ip 127.0.0.3 --node-ips 127.0.0.1 127.0.0.2 127.0.0.3`

If every node can contact every other node, the cluster will eventually initialize (should be almost instantaneous).

The behavior is unspecified if two nodes are started with one containing the other's IP but not vice versa.

After the cluster is initialized it will remain available as long as a majority of nodes can contact each other.

Nodes use the port `50051` for everything (to contact each other and to handle client requests).

## Sending requests

The client application can be used to send transactions to the system. For example:
```
> put "a" "AAA";
. put "b" "BBB";
.
sending request...
result: {}

> a = get "a";
. b = get "b";
. put "c" a + b;
.
sending request...
result: {"a": "AAA", "b": "BBB"}

> get "c";
.
sending request...
result: {"c": "AAABBB"}

> if get "c" == "BBBAAA" { put "d" "D1"; } else { put "d" "D2"; }
.
sending request...
result: {"c": "AAABBB"}

> get "d";
.
sending request...
result: {"d": "D2"}
```

Each transaction is a simple imperative program --- a sequence of statements. It supports local variables, `if`s, and string concatenations, as seen in the above example. The full grammar can be found in [lang.lalrpop](src/front/lang.lalrpop).

Concurrently performed transactions are [strictly serializable](https://jepsen.io/consistency/models/strict-serializable): each appears to execute instantaneously somewhere between the request and the response, isolated from every other transaction. If no response arrives, the requested transaction may or may not be executed (but it will be executed at most once).

For every `get` expression appearing in the transaction such that the store contained a value under the accessed key, the value under this key that the store contained *right before the transaction was executed* (according to the order given by strict serializability) will appear in the result (the `result: {...}` line of the output). Some behavior might seem unintuitive:
```
> put "x" "xxx";
. get "x";
.
sending request...
result: {}

> put "x" "yyy";
. get "x";
.
sending request...
result: {"x": "xxx"}
```
The first result was empty since the key `"x"` did not have a value before the first transaction was executed. The second result gave `"xxx"` under key `"x"` since that was the value after the first transaction but before the second. Therefore `put` statements do not affect the `result` of the current transaction, only later ones. However, they do affect later `get`s in the same transaction:
```
> put "x" "xxx";
.
sending request...
result: {}

> put "x" "yyy";
. x = get "x";
. put "y" x;
.
sending request...
result: {"x": "xxx"}

> get "y";
.
sending request...
result: {"y": "yyy"}
```
As we can see in this example, the local variable `x` in the second transaction was assigned the value `"yyy"` from the previous `put`.

Summarizing, the transaction (the program) observes its own `put`s, but the result of the transaction does not.

The cluster implements a rotating "leader" which changes every 5 seconds in a round-robin fashion (I was too lazy to implement a proper failure detector). When a node is a leader it handles client requests; otherwise it redirects the requests to the node it believes to currently be the leader (returning the identifier of that node to the client). The example client does not handle redirections but simply reports the fact to stdout.

Mutliple nodes may believe to be a leader at the same time but the used algorithm will still guarantee strict serializability.

## How it works

The store uses LPaxos, a yet unpublished consensus/replication algorithm that will hopefully help me get a Master's degree. A link to the thesis should be published after it's defended.

## Other notes

Persistent storage is not implemented. Shutting down a server is equivalent to a permanent server failure. Therefore if at least half of the nodes are shut down the cluster becomes permanently unavailable.
