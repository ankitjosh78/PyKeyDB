import time
import threading
import random
import string
import os
from statistics import mean

from pykeydb.db.pyKeyDB import get_pykey_db, dispose_pykey_db
from pykeydb.db.writeAheadLog import get_write_ahead_log, dispose_write_ahead_log

# Config
NUM_THREADS = 4
OPS_PER_THREAD = 10_000


def random_key():
    return "".join(random.choices(string.ascii_lowercase, k=16))


def setup_db(wal_path="benchmark.wal", use_fsync=True):
    """Setup a fresh DB instance for benchmarking"""
    # Clean up old instances (this also removes from factory caches)
    dispose_pykey_db(wal_path)
    dispose_write_ahead_log(wal_path)

    # Remove old WAL file
    if os.path.exists(wal_path):
        os.remove(wal_path)

    # Create new instances
    wal = get_write_ahead_log(wal_path, use_fsync=use_fsync)
    db = get_pykey_db(wal, wal_path)
    return db


def benchmark_set(db, thread_id, latencies):
    for i in range(OPS_PER_THREAD):
        key = f"key-{thread_id}-{i}"
        start = time.perf_counter()
        db.set(key, i)
        latencies.append(time.perf_counter() - start)


def benchmark_get(db, keys, latencies):
    for _ in range(OPS_PER_THREAD):
        key = random.choice(keys)
        start = time.perf_counter()
        db.get(key)
        latencies.append(time.perf_counter() - start)


def benchmark_lpush(db, thread_id, latencies):
    for i in range(OPS_PER_THREAD):
        key = f"list-{thread_id}"
        start = time.perf_counter()
        db.lpush(key, str(i))
        latencies.append(time.perf_counter() - start)


def benchmark_rpush(db, thread_id, latencies):
    for i in range(OPS_PER_THREAD):
        key = f"list-{thread_id}"
        start = time.perf_counter()
        db.rpush(key, str(i))
        latencies.append(time.perf_counter() - start)


def benchmark_lpop(db, list_keys, latencies):
    for _ in range(OPS_PER_THREAD):
        key = random.choice(list_keys)
        start = time.perf_counter()
        db.lpop(key)
        latencies.append(time.perf_counter() - start)


def benchmark_lrange(db, list_keys, latencies):
    for _ in range(OPS_PER_THREAD):
        key = random.choice(list_keys)
        start = time.perf_counter()
        db.lrange(key, 0, 99)  # Get first 100 elements
        latencies.append(time.perf_counter() - start)


def benchmark_hset(db, thread_id, latencies):
    for i in range(OPS_PER_THREAD):
        key = f"hash-{thread_id}"
        fields = {f"field{i}": f"value{i}"}
        start = time.perf_counter()
        db.hset(key, fields)
        latencies.append(time.perf_counter() - start)


def benchmark_hget(db, hash_keys, latencies):
    for i in range(OPS_PER_THREAD):
        key = random.choice(hash_keys)
        field = f"field{random.randint(0, OPS_PER_THREAD - 1)}"
        start = time.perf_counter()
        db.hget(key, field)
        latencies.append(time.perf_counter() - start)


def benchmark_hmget(db, hash_keys, latencies):
    for i in range(OPS_PER_THREAD):
        key = random.choice(hash_keys)
        fields = [
            f"field{j}" for j in range(random.randint(0, 9), random.randint(10, 20))
        ]
        start = time.perf_counter()
        db.hmget(key, *fields)
        latencies.append(time.perf_counter() - start)


def benchmark_hgetall(db, hash_keys, latencies):
    for _ in range(OPS_PER_THREAD):
        key = random.choice(hash_keys)
        start = time.perf_counter()
        db.hgetall(key)
        latencies.append(time.perf_counter() - start)


def benchmark_hdel(db, hash_keys, latencies):
    for i in range(OPS_PER_THREAD):
        key = random.choice(hash_keys)
        field = f"field{random.randint(0, OPS_PER_THREAD - 1)}"
        start = time.perf_counter()
        db.hdel(key, field)
        latencies.append(time.perf_counter() - start)


# Runner
def run_benchmark(name, target, *args):
    print(f"\n=== {name} ===")

    latencies = []
    threads = []

    start_time = time.perf_counter()

    for thread_id in range(NUM_THREADS):
        t = threading.Thread(target=target, args=(*args, thread_id, latencies))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    duration = time.perf_counter() - start_time

    if not latencies:
        print("No operations completed.")
        return

    total_ops = len(latencies)

    print(f"Total ops: {total_ops}")
    print(f"Total time: {duration:.2f}s")
    print(f"Throughput: {total_ops / duration:,.0f} ops/sec")
    print(f"Avg latency: {mean(latencies) * 1e6:.2f} µs")
    print(f"P95 latency: {sorted(latencies)[int(0.95 * total_ops)] * 1e6:.2f} µs")


if __name__ == "__main__":
    print("=" * 60)
    print("PyKeyDB Benchmark Suite")
    print("=" * 60)
    print(f"Threads: {NUM_THREADS}")
    print(f"Operations per thread: {OPS_PER_THREAD:,}")
    print(f"Total operations per benchmark: {NUM_THREADS * OPS_PER_THREAD:,}")
    print("=" * 60)

    # String operations
    db = setup_db()
    run_benchmark("SET benchmark", benchmark_set, db)

    keys = list(db._db.keys())

    def get_wrapper(db, thread_id, latencies):
        benchmark_get(db, keys, latencies)

    run_benchmark("GET benchmark", get_wrapper, db)

    # List operations
    print("\n" + "=" * 60)
    print("List Operations")
    print("=" * 60)

    db = setup_db()
    run_benchmark("LPUSH benchmark", benchmark_lpush, db)

    list_keys = [k for k in db._db.keys() if k.startswith("list-")]

    def lrange_wrapper(db, thread_id, latencies):
        benchmark_lrange(db, list_keys, latencies)

    run_benchmark("LRANGE benchmark", lrange_wrapper, db)

    db = setup_db()
    run_benchmark("RPUSH benchmark", benchmark_rpush, db)

    list_keys = [k for k in db._db.keys() if k.startswith("list-")]

    def lpop_wrapper(db, thread_id, latencies):
        benchmark_lpop(db, list_keys, latencies)

    run_benchmark("LPOP benchmark", lpop_wrapper, db)

    # Hash operations
    print("\n" + "=" * 60)
    print("Hash Operations")
    print("=" * 60)

    db = setup_db()
    run_benchmark("HSET benchmark", benchmark_hset, db)

    hash_keys = [k for k in db._db.keys() if k.startswith("hash-")]

    def hget_wrapper(db, thread_id, latencies):
        benchmark_hget(db, hash_keys, latencies)

    run_benchmark("HGET benchmark", hget_wrapper, db)

    def hmget_wrapper(db, thread_id, latencies):
        benchmark_hmget(db, hash_keys, latencies)

    run_benchmark("HMGET benchmark", hmget_wrapper, db)

    def hgetall_wrapper(db, thread_id, latencies):
        benchmark_hgetall(db, hash_keys, latencies)

    run_benchmark("HGETALL benchmark", hgetall_wrapper, db)

    def hdel_wrapper(db, thread_id, latencies):
        benchmark_hdel(db, hash_keys, latencies)

    run_benchmark("HDEL benchmark", hdel_wrapper, db)

    print("\n" + "=" * 60)
    print("Benchmark Complete!")
    print("=" * 60)
