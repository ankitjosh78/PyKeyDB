# PyKeyDB

Thread-safe in-memory key-value database with write-ahead logging and async networking.

## Architecture

### Layered Design

The system follows a clean 3-layer architecture:

**Protocol Layer** (`server.py`)
- Async networking with `asyncio`
- Connection lifecycle management
- Delegates commands to session layer
- No business logic or transaction handling

**Session Layer** (`clientContext.py`)
- Per-client state management
- Transaction state machine (MULTI/EXEC/DISCARD)
- Command queueing during transactions
- Routes commands to execution engine

**Execution Engine** (`utils.apply_command` + `PyKeyDB`)
- Pure command → DB mutation
- Thread-safe operations via `RLock`
- WAL integration for durability
- No client state or networking concerns

### Core Components

**PyKeyDB** - Singleton-based in-memory store using `threading.RLock` for concurrent access. Operations are atomically logged to WAL before modifying in-memory state.

**WriteAheadLog** - Append-only log with JSON-serialized operations. Supports optional fsync for durability guarantees. Replays log on startup to reconstruct state.

**ClientContext** - Per-connection session state. Maintains transaction queue and FSM for MULTI/EXEC/DISCARD semantics.

### Thread Safety & Atomicity

**Concurrency model:**
- Double-checked locking for singleton initialization
- Reentrant locks (`RLock`) to allow nested acquisitions
- WAL writes are serialized per operation
- All mutations are guarded by locks

**Transaction atomicity:**
- `EXEC` runs synchronously (no `await` calls)
- Single event loop tick = no interleaving between queued commands
- Per-client transaction queue prevents cross-client interference
- Atomicity guaranteed by asyncio's cooperative scheduling

### Data Flow

**Normal operation:**
```
Client → Server → ClientContext → apply_command → PyKeyDB → WAL → Response
                                                      ↓
                                                 fsync (optional)
```

**Transaction mode:**
```
MULTI → [queue commands] → EXEC → batch apply_command (atomic) → Response
```

On restart, WAL is replayed to restore the last consistent state.

## Performance

Benchmarks run on 40,000 operations with 4 threads (measured with `python -m pykeydb.benchmark.benchmark`):

### String Operations

#### Without fsync (buffered writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SET       | 84,315 ops/sec | 47.04 µs | 21.17 µs |
| GET       | 2,658,345 ops/sec | 0.21 µs | 0.33 µs |

#### With fsync (durable writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SET       | 34,319 ops/sec | 115.99 µs | 1066.67 µs |
| GET       | 2,591,604 ops/sec | 0.22 µs | 0.38 µs |

### List Operations

#### Without fsync (buffered writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| LPUSH     | 6,530 ops/sec | 611.14 µs | 1945.75 µs |
| RPUSH     | 6,500 ops/sec | 612.85 µs | 1908.71 µs |
| LPOP      | 6,886 ops/sec | 579.02 µs | 1882.63 µs |
| LRANGE    | 2,273,077 ops/sec | 0.28 µs | 0.33 µs |

#### With fsync (durable writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| LPUSH     | 5,389 ops/sec | 741.30 µs | 2007.79 µs |
| RPUSH     | 5,526 ops/sec | 722.72 µs | 2003.37 µs |
| LPOP      | 5,856 ops/sec | 682.63 µs | 1979.71 µs |
| LRANGE    | 2,270,244 ops/sec | 0.28 µs | 0.33 µs |

### Hash Operations

#### Without fsync (buffered writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| HSET      | 2,915 ops/sec | 1369.87 µs | 2557.83 µs |
| HGET      | 1,561,346 ops/sec | 0.28 µs | 0.46 µs |
| HMGET     | 592,717 ops/sec | 1.89 µs | 0.87 µs |
| HGETALL   | 3,303,499 ops/sec | 0.14 µs | 0.17 µs |
| HDEL      | 3,282 ops/sec | 1216.95 µs | 2614.25 µs |

#### With fsync (durable writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| HSET      | 2,656 ops/sec | 1504.61 µs | 2661.12 µs |
| HGET      | 1,490,723 ops/sec | 0.31 µs | 0.58 µs |
| HMGET     | 602,076 ops/sec | 1.47 µs | 0.87 µs |
| HGETALL   | 3,416,285 ops/sec | 0.14 µs | 0.17 µs |
| HDEL      | 3,128 ops/sec | 1277.02 µs | 2725.08 µs |

### Set Operations

#### Without fsync (buffered writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SADD      | 4,732 ops/sec | 844.00 µs | 2101.08 µs |
| SISMEMBER | 1,809,180 ops/sec | 0.21 µs | 0.37 µs |
| SMEMBERS  | 3,367,110 ops/sec | 0.14 µs | 0.17 µs |
| SCARD     | 3,260,703 ops/sec | 0.15 µs | 0.17 µs |
| SREM      | 5,437 ops/sec | 734.09 µs | 1954.21 µs |

#### With fsync (durable writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SADD      | 4,283 ops/sec | 933.02 µs | 2082.54 µs |
| SISMEMBER | 1,854,786 ops/sec | 0.20 µs | 0.33 µs |
| SMEMBERS  | 3,392,910 ops/sec | 0.14 µs | 0.17 µs |
| SCARD     | 3,260,310 ops/sec | 0.15 µs | 0.17 µs |
| SREM      | 4,988 ops/sec | 799.83 µs | 1980.75 µs |

**Key Insights:**
- **fsync impact:** Reduces write throughput by ~60% for strings, ~15% for lists, ~10% for hashes/sets
- **Read performance:** GET, LRANGE, HGET, HGETALL, SISMEMBER, SMEMBERS, SCARD are memory-only operations, unaffected by fsync
- **Set operations:** SADD/SREM performance similar to list operations (~4-5K ops/sec)
- **SISMEMBER performance:** Extremely fast membership checks at ~1.8M ops/sec
- **SMEMBERS/SCARD performance:** Ultra-fast at 3M+ ops/sec for sets with 10K members
- **Thread safety:** All operations are thread-safe with proper locking

## Installation

```bash
pip install -e .
```

## Usage

```bash
python -m pykeydb.server.server
```

Default: `127.0.0.1:6379`

### Commands

**String operations:**
- `SET key value` - Store string value
- `GET key` - Retrieve string value
- `DEL key` - Delete key (any type)
- `TYPE key` - Get data type of key

**List operations:**
- `LPUSH key value [value ...]` - Prepend values to list
- `RPUSH key value [value ...]` - Append values to list
- `LPOP key` - Remove and return first element
- `RPOP key` - Remove and return last element
- `LRANGE key start stop` - Get list slice (use -1 for end)
- `LLEN key` - Get list length

**Hash operations:**
- `HSET key field value [field value ...]` - Set hash fields
- `HGET key field` - Get hash field value
- `HMGET key field [field ...]` - Get multiple hash field values
- `HGETALL key` - Get all hash fields and values
- `HDEL key field [field ...]` - Delete hash fields
- `HLEN key` - Get number of fields in hash
- `HEXISTS key field` - Check if hash field exists

**Set operations:**
- `SADD key member [member ...]` - Add members to set
- `SREM key member [member ...]` - Remove members from set
- `SISMEMBER key member` - Check if member exists in set
- `SMISMEMBER key member [member ...]` - Check multiple members
- `SMEMBERS key` - Get all members of set
- `SCARD key` - Get number of members in set
- `SPOP key` - Remove and return random member
- `SRANDMEMBER key [count]` - Get random member(s)

**Transactions:**
- `MULTI` - Begin transaction block
- `EXEC` - Execute all queued commands atomically
- `DISCARD` - Abort transaction and clear queue

### Examples

**String operations:**
```bash
nc 127.0.0.1 6379

SET mykey hello
> OK

GET mykey
> hello

TYPE mykey
> string

DEL mykey
> OK
```

**List operations:**
```bash
LPUSH mylist a b c
> (integer) 3

LRANGE mylist 0 -1
> 1) c
> 2) b
> 3) a

RPUSH mylist d e
> (integer) 5

LPOP mylist
> c

LLEN mylist
> (integer) 4

TYPE mylist
> list
```

**Hash operations:**
```bash
HSET user:1 name Alice age 30 city NYC
> (integer) 3

HGET user:1 name
> Alice

HMGET user:1 name age email
> 1) Alice
> 2) 30
> 3) (nil)

HGETALL user:1
> 1) name: Alice
> 2) age: 30
> 3) city: NYC

HEXISTS user:1 name
> (integer) 1

HLEN user:1
> (integer) 3

HDEL user:1 age
> (integer) 1

TYPE user:1
> hash
```

**Set operations:**
```bash
SADD myset apple banana cherry
> (integer) 3

SISMEMBER myset apple
> (bool) True

SISMEMBER myset grape
> (bool) False

SMISMEMBER myset apple grape banana
> 1) (bool) True
> 2) (bool) False
> 3) (bool) True

SMEMBERS myset
> 1) apple
> 2) banana
> 3) cherry

SCARD myset
> (integer) 3

SRANDMEMBER myset
> banana

SRANDMEMBER myset 2
> 1) apple
> 2) cherry

SPOP myset
> cherry

SREM myset banana
> (integer) 1

SMEMBERS myset
> 1) apple

TYPE myset
> set
```

**Type safety:**
```bash
SET mykey hello
> OK

LPUSH mykey value
> ERR WRONGTYPE: key is string, not list

HSET mykey field value
> ERR WRONGTYPE: key is string, not hash

SADD mykey member
> ERR WRONGTYPE: key is string, not set
```

**Transactions:**
```bash
MULTI
> OK

SET x 10
> QUEUED

SET y 20
> QUEUED

EXEC
> OK
> OK
```

## Roadmap

- [x] Write-ahead logging (WAL)
- [x] Thread-safe operations
- [x] Transaction support (MULTI/EXEC/DISCARD)
- [x] List data type (LPUSH, RPUSH, LPOP, RPOP, LRANGE, LLEN)
- [x] Hash data type (HSET, HGET, HMGET, HGETALL, HDEL, HLEN, HEXISTS)
- [x] Set data type (SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER)
- [x] Type system with WRONGTYPE errors
- [x] Benchmarking suite (strings, lists, hashes, sets)
- [ ] Numeric operations (INCR, DECR, INCRBY, INCRBYFLOAT)
- [ ] RESP protocol implementation
- [ ] TTL/expiration on keys
- [ ] Snapshot-based persistence
- [ ] WAL compaction/rotation
- [ ] Connection pooling
- [ ] Pub/sub messaging
- [ ] Replication support

## Project Structure

```
pykeydb/
  ├── db/
  │   ├── pyKeyDB.py              # Core KV store with per-path singletons
  │   ├── writeAheadLog.py        # WAL with per-path singletons
  │   ├── dataTypes.py            # TypedValue wrapper and DataType enum
  │   ├── keyValueDBInterface.py  # Abstract interface
  │   └── utils.py                # Command execution engine
  ├── benchmark/                  
  │   └── benchmark.py            # Performance tests (strings + lists)
  └── server/
      ├── server.py               # Protocol layer (async networking)
      └── clientContext.py        # Session layer (transactions)
```
