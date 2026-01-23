# PyKeyDB

Thread-safe in-memory key-value database with write-ahead logging and async networking.

## Architecture

### Core Components

**PyKeyDB** - Singleton-based in-memory store using `threading.RLock` for concurrent access. Operations are atomically logged to WAL before modifying in-memory state.

**WriteAheadLog** - Append-only log with JSON-serialized operations. Supports optional fsync for durability guarantees. Replays log on startup to reconstruct state.

**Async Server** - `asyncio`-based TCP server handling concurrent client connections. Text-based protocol similar to Redis RESP.

### Thread Safety

- Double-checked locking for singleton initialization
- Reentrant locks (`RLock`) to allow nested acquisitions
- WAL writes are serialized per operation
- All mutations are guarded by locks

### Data Flow

```
Client Command → Server Parser → DB Operation → WAL Log → In-Memory Update → Response
                                        ↓
                                   fsync (optional)
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

- `SET key value` - Write key-value pair
- `GET key` - Read value by key
- `DEL key` - Remove key
- `TYPE key` - Get value type

### Example

```bash
nc 127.0.0.1 6379

SET mykey hello
> OK

GET mykey
> hello

DEL mykey
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
  │   └── keyValueDBInterface.py  # Abstract interface
  ├── benchmark/                  
  │   └── benchmark.py            # Benchmarking suite
  └── server/
      └── server.py               # Async TCP server
```
