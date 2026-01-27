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

Benchmarks run on 40,000 operations (measured with `python -m pykeydb.benchmark.benchmark`):

### Without fsync (buffered writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SET       | 108,432 ops/sec | 36.55 µs | 19.67 µs |
| GET       | 3,233,641 ops/sec | 0.15 µs | 0.25 µs |

### With fsync (durable writes)

| Operation | Throughput | Avg Latency | P95 Latency |
|-----------|------------|-------------|-------------|
| SET       | 40,096 ops/sec | 98.93 µs | 1041.75 µs |
| GET       | 2,874,596 ops/sec | 0.17 µs | 0.29 µs |

**Trade-off:** Enabling fsync reduces SET throughput by ~60% but guarantees durability against crashes. GET operations are unaffected as they only read from memory.

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

**Basic operations:**
- `SET key value` - Write key-value pair
- `GET key` - Read value by key
- `DEL key` - Remove key
- `TYPE key` - Get value type

**Transactions:**
- `MULTI` - Begin transaction block
- `EXEC` - Execute all queued commands atomically
- `DISCARD` - Abort transaction and clear queue

### Examples

**Basic usage:**
```bash
nc 127.0.0.1 6379

SET mykey hello
> OK

GET mykey
> hello

DEL mykey
> OK
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

- [ ] RESP protocol implementation
- [ ] Data type support (lists, sets, hashes)
- [ ] TTL/expiration on keys
- [ ] Snapshot-based persistence
- [ ] WAL compaction/rotation
- [ ] Connection pooling
- [ ] Pub/sub messaging
- [ ] Replication support
- [ ] Benchmarking suite

## Project Structure

```
pykeydb/
  ├── db/
  │   ├── pyKeyDB.py              # Core KV store with singleton pattern
  │   ├── writeAheadLog.py        # WAL implementation
  │   ├── keyValueDBInterface.py  # Abstract interface
  │   └── utils.py                # Command execution engine
  ├── benchmark/                  
  │   └── benchmark.py            # Performance tests
  └── server/
      ├── server.py               # Protocol layer (networking)
      └── clientContext.py        # Session layer (transactions)
```
