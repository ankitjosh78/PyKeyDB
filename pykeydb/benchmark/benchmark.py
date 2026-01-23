import time
import threading
import random
import string
from statistics import mean

from pykeydb.db.pyKeyDB import PyKeyDB
from pykeydb.db.writeAheadLog import WriteAheadLog

# Config
NUM_THREADS = 4
OPS_PER_THREAD = 10_000


def random_key():
    return "".join(random.choices(string.ascii_lowercase, k=16))


def setup_db():
    WriteAheadLog.dispose()
    PyKeyDB.dispose()

    wal = WriteAheadLog("benchmark.wal", use_fsync=True)
    db = PyKeyDB(wal)
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
    db = setup_db()

    run_benchmark("SET benchmark", benchmark_set, db)

    keys = list(db._db.keys())

    # GET benchmark (reuse threads but different target)
    def get_wrapper(db, thread_id, latencies):
        benchmark_get(db, keys, latencies)

    run_benchmark("GET benchmark", get_wrapper, db)
