#!/usr/bin/env python3                                         # concurrency_lockstep_deterministic.py                        # Fully deterministic, orderless concurrency emulator:
# - Explicit epoch counter and synthetic time (no wall-clock dependency in receipts)
# - Two strikes per epoch (after producers, after consumers) mirrored in replay                                               # - Committee Merkle root excludes volatile timestamps and memory stamps
# - Non-blocking submits, then strike, then await              # - Mutex baseline included

import threading
import time
import math                                                    import random
import statistics                                              import hashlib                                                 from collections import defaultdict, deque, namedtuple

# ---------------- Utilities ----------------

def ns_to_ms(ns):                                                  return ns / 1_000_000.0

def percentile_ms(samples_ms, p):
    if not samples_ms:
        return float('nan')
    data_sorted = sorted(samples_ms)
    k = (len(data_sorted) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return data_sorted[int(k)]
    return data_sorted[f] * (c - k) + data_sorted[c] * (k - f)

def jains_index(services):
    if not services:
        return float('nan')
    total = sum(services)
    n = len(services)
    denom = sum(x * x for x in services)
    if denom == 0:
        return 1.0
    return (total * total) / (n * denom)

def hmac_sig(key: bytes, msg: bytes):
    return hashlib.sha256(key + msg).hexdigest()

def merkle_root(hashes):
    if not hashes:
        return hashlib.sha256(b"EMPTY").hexdigest()
    nodes = [hashlib.sha256(h.encode()).hexdigest() for h in hashes]
    while len(nodes) > 1:
        nxt = []
        for i in range(0, len(nodes), 2):
            left = nodes[i]
            right = nodes[i+1] if i+1 < len(nodes) else left
            nxt.append(hashlib.sha256((left + right).encode()).hexdigest())
        nodes = nxt
    return nodes[0]

# ---------------- Workload ----------------

class Workload:
    def __init__(self, min_ms=0.3, max_ms=0.7):
        self.min_ms = min_ms
        self.max_ms = max_ms
    def do_work(self):
        time.sleep(random.uniform(self.min_ms, self.max_ms) / 1000.0)

# ---------------- Intent & Receipt ----------------

Intent = namedtuple("Intent", [
    "intent_id","thread_id","topic_id","priority_weight","deadline_ms",
    "submitted_epoch","memory_stamp"
])

class Receipt:
    __slots__ = (
        "intent_id","thread_id","topic_id","accepted",
        "epoch_id","credits_after","priority_weight",
        "virtual_finish","signature"
    )
    def __init__(self, **kwargs):
        for k,v in kwargs.items():
            setattr(self, k, v)

# ---------------- Deterministic intent IDs ----------------

class IntentFactory:
    @staticmethod
    def make_intent_id(seed, tid, epoch, kind):
        raw = f"{seed}|{tid}|{epoch}|{kind}".encode()
        return int.from_bytes(hashlib.sha256(raw).digest()[:6], 'big')

# ---------------- Memory model (kept but excluded from signatures) ----------------

class MemoryModel:
    def __init__(self):
        self.lock = threading.Lock()
        self.last_acquire_epoch = -1
        self.last_release_epoch = -1
    def submit_acquire(self, epoch):
        with self.lock:
            self.last_acquire_epoch = epoch
            return epoch
    def strike_release(self, epoch):
        with self.lock:
            self.last_release_epoch = epoch
            return epoch

# ---------------- Arbitrator ----------------

class Arbitrator:
    def __init__(self, topic_id, epoch_ms, seed, tokens_per_epoch, sign_key):
        self.topic_id = topic_id
        self.epoch_ns = int(epoch_ms * 1_000_000)
        self.seed = seed
        self.epoch_id = 0
        self.intents = []
        self.credits = defaultdict(float)
        self.virtual_finish = defaultdict(float)
        self.lock = threading.Lock()
        self.receipts = deque()
        self.sign_key = sign_key
        self.tokens_per_epoch = tokens_per_epoch
        self.mem = MemoryModel()

    def set_epoch(self, epoch_id):
        self.epoch_id = epoch_id

    def submit_intent(self, intent: Intent):
        with self.lock:
            tid = intent.thread_id
            vf = self.virtual_finish[tid]
            epoch = self.epoch_id
            deadline = 0.0
            weight = 1.0
            salt = (hash((tid, self.seed, epoch)) & 0xffff)
            key = (epoch, vf, -deadline, -weight, salt)
            intent = intent._replace(memory_stamp=self.mem.submit_acquire(epoch))
            self.intents.append((key, intent))

    def strike(self, synthetic_now_epoch):
        with self.lock:
            # align arbitrator epoch with synthetic time
            self.epoch_id = synthetic_now_epoch
            if not self.intents:
                return []
            self.intents.sort(key=lambda x: x[0])
            win_count = min(self.tokens_per_epoch, len(self.intents))
            winners = [self.intents[i][1] for i in range(win_count)]
            losers  = [self.intents[j][1] for j in range(win_count, len(self.intents))]
            self.intents.clear()

            self.mem.strike_release(synthetic_now_epoch)
            receipts = []
            for w in winners:
                tid = w.thread_id
                wt = 1.0
                self.credits[tid] += wt
                self.virtual_finish[tid] += 1.0 / wt
                receipts.append(Receipt(
                    intent_id=w.intent_id,
                    thread_id=tid,
                    topic_id=self.topic_id,
                    accepted=True,
                    epoch_id=self.epoch_id,
                    credits_after=self.credits[tid],
                    priority_weight=wt,
                    virtual_finish=self.virtual_finish[tid],
                    signature=""
                ))
            for l in losers:
                tid = l.thread_id
                wt = 1.0
                self.credits[tid] += wt * 0.05
                receipts.append(Receipt(
                    intent_id=l.intent_id,
                    thread_id=tid,
                    topic_id=self.topic_id,
                    accepted=False,
                    epoch_id=self.epoch_id,
                    credits_after=self.credits[tid],
                    priority_weight=wt,
                    virtual_finish=self.virtual_finish[tid],
                    signature=""
                ))

            for r in receipts:
                self.receipts.append(r)
        return receipts

    def await_receipt(self, intent_id, timeout_ms=100):
        deadline = time.monotonic() + (timeout_ms / 1000.0)
        while time.monotonic() < deadline:
            with self.lock:
                for r in list(self.receipts):
                    if r.intent_id == intent_id:
                        self.receipts.remove(r)
                        return r
            time.sleep(0.0005)
        return None

# ---------------- NUMA shard ----------------

class NumaShard:
    def __init__(self, shard_id, topics, epoch_ms, seed, tokens_per_epoch, sign_key):
        self.epoch_ns = int(epoch_ms * 1_000_000)
        self.arbs = {
            t: Arbitrator(t, epoch_ms, seed + shard_id * 997 + i, tokens_per_epoch, sign_key)
            for i, t in enumerate(topics)
        }
    def set_epoch(self, epoch_id):
        for arb in self.arbs.values():
            arb.set_epoch(epoch_id)
    def submit(self, intent: Intent):
        self.arbs[intent.topic_id].submit_intent(intent)
    def strike_all(self, synthetic_now_epoch):
        receipts = []
        for arb in self.arbs.values():
            receipts.extend(arb.strike(synthetic_now_epoch))
        return receipts
    def await_receipt(self, topic_id, intent_id, timeout_ms=100):
        return self.arbs[topic_id].await_receipt(intent_id, timeout_ms)
    def epoch_id(self, topic_id):
        return self.arbs[topic_id].epoch_id

# ---------------- Committee ----------------

class Committee:
    def __init__(self, member_keys):
        self.member_keys = [k.encode() for k in member_keys]
    def key_for_epoch(self, epoch_id):
        return self.member_keys[epoch_id % len(self.member_keys)]
    def sign_receipt_set(self, receipts):
        if not receipts:
            return None, []
        epoch = receipts[0].epoch_id
        receipts_sorted = sorted(
            receipts,
            key=lambda r: (r.topic_id, r.intent_id, int(r.accepted))
        )
        key = self.key_for_epoch(epoch)
        leaf_hashes = []
        for r in receipts_sorted:
            # Exclude volatile timestamps and memory stamps: purely deterministic payload
            payload = (
                f"{r.intent_id}|{r.thread_id}|{r.topic_id}|{r.epoch_id}|"
                f"{int(r.accepted)}|{int(r.credits_after)}|{int(r.priority_weight)}|"
                f"{int(r.virtual_finish * 1_000_000)}"
            ).encode()
            sig = hmac_sig(key, payload)
            r.signature = sig
            leaf_hashes.append(sig)
        return merkle_root(leaf_hashes), receipts_sorted

# ---------------- MPMC queue ----------------

class MPMCQueue:
    def __init__(self, shard: NumaShard, topic_id: str):
        self.shard = shard
        self.topic_id = topic_id
        self.data = deque()
        self.lock = threading.Lock()

    # Non-blocking submit; awaiting is done outside after strikes
    def enqueue_submit(self, tid, intent_id, submitted_epoch, weight=1.0, deadline_ms=0.0):
        intent = Intent(intent_id, tid, self.topic_id, weight, deadline_ms, submitted_epoch, 0)
        self.shard.submit(intent)

    def dequeue_submit(self, tid, intent_id, submitted_epoch, weight=1.0, deadline_ms=0.0):
        intent = Intent(intent_id, tid, self.topic_id, weight, deadline_ms, submitted_epoch, 0)
        self.shard.submit(intent)

# ---------------- Simulator (lockstep emission) ----------------

class Simulator:
    def __init__(self, threads, shards, topics, epoch_ms, seed, capacity_per_second):
        self.threads = threads
        self.shards_count = shards
        self.topics = topics
        self.epoch_ms = epoch_ms
        self.epoch_ns = int(epoch_ms * 1_000_000)
        self.seed = seed
        self.capacity_per_second = capacity_per_second
        self.sign_key = b"local-arb-key"

        # One token per worker per epoch, split across shards
        total_tokens_per_epoch = self.threads
        tokens_per_shard = max(1, total_tokens_per_epoch // shards)

        self.shards = [NumaShard(i, topics, epoch_ms, seed, tokens_per_shard, self.sign_key)
                       for i in range(shards)]
        self.committee = Committee(member_keys=["A","B","C"])

        self.latencies_ms = defaultdict(list)
        self.accepts = defaultdict(int)
        self.deferrals = defaultdict(int)
        self.receipt_log = []
        self.intent_log = defaultdict(list)
        self.queue = MPMCQueue(self.shards[0], topics[0])  # single topic

        self.epoch_counter = 0  # explicit epoch

    def epoch_driver_tick(self, strikes=1):
        # Set shard epoch explicitly and strike with synthetic time
        for shard in self.shards:
            shard.set_epoch(self.epoch_counter)
        receipts = []
        for _ in range(strikes):
            for shard in self.shards:
                receipts.extend(shard.strike_all(self.epoch_counter))
        if receipts:
            root, receipts_sorted = self.committee.sign_receipt_set(receipts)
            if root:
                self.receipt_log.append((receipts_sorted[0].epoch_id, root, receipts_sorted))

    def run_orderless_lockstep(self, duration_s, workload: Workload):
        topic = self.topics[0]
        half = self.threads // 2
        epochs_total = max(1, int((duration_s * 1000) / self.epoch_ms))
        for _ in range(epochs_total):
            # Producers: submit only
            prod_ids = []
            epoch_now = self.epoch_counter
            for tid in range(half):
                iid = IntentFactory.make_intent_id(self.seed, tid, epoch_now, 0)
                self.queue.enqueue_submit(tid, iid, submitted_epoch=epoch_now, weight=1.0, deadline_ms=0.0)
                self.intent_log[epoch_now].append(("enq", tid, iid))
                prod_ids.append((tid, iid, time.monotonic_ns()))

            # strike immediately so producer receipts exist (synthetic epoch)
            self.epoch_driver_tick(strikes=1)

            # await producer receipts and do work
            for tid, iid, t0 in prod_ids:
                r = self.shards[0].await_receipt(topic, iid, timeout_ms=100)
                if r and r.accepted:
                    workload.do_work()
                    self.latencies_ms[tid].append(ns_to_ms(time.monotonic_ns() - t0))
                    self.accepts[tid] += 1
                else:
                    self.deferrals[tid] += 1

            # Consumers: submit only
            cons_ids = []
            for tid in range(half, self.threads):
                iid = IntentFactory.make_intent_id(self.seed, tid, epoch_now, 1)
                self.queue.dequeue_submit(tid, iid, submitted_epoch=epoch_now, weight=1.0, deadline_ms=0.0)
                self.intent_log[epoch_now].append(("deq", tid, iid))
                cons_ids.append((tid, iid, time.monotonic_ns()))

            # strike immediately so consumer receipts exist (synthetic epoch)
            self.epoch_driver_tick(strikes=1)

            # await consumer receipts and do work
            for tid, iid, t0 in cons_ids:
                r = self.shards[0].await_receipt(topic, iid, timeout_ms=100)
                if r and r.accepted:
                    workload.do_work()
                    self.latencies_ms[tid].append(ns_to_ms(time.monotonic_ns() - t0))
                    self.accepts[tid] += 1
                else:
                    self.deferrals[tid] += 1

            # advance to next epoch
            self.epoch_counter += 1
            time.sleep(self.epoch_ms / 1000.0)

    def summarize(self, duration_s):
        total_ops = sum(self.accepts.values())
        total_def = sum(self.deferrals.values())
        all_lat = [x for arr in self.latencies_ms.values() for x in arr]
        mean_ms = statistics.mean(all_lat) if all_lat else float('nan')
        med_ms = statistics.median(all_lat) if all_lat else float('nan')
        p95_ms = percentile_ms(all_lat, 0.95)
        p99_ms = percentile_ms(all_lat, 0.99)
        fair = jains_index([self.accepts[t] for t in sorted(self.accepts)]) if self.accepts else float('nan')

        print("\n=== Orderless lockstep ===")
        print(f"Throughput: {total_ops} over {duration_s:.2f}s ({(total_ops/duration_s) if duration_s else 0:.1f}/s)")
        print(f"Deferrals: {total_def}")
        print(f"Latency mean/median: {mean_ms:.3f} ms / {med_ms:.3f} ms")
        print(f"Latency p95/p99: {p95_ms:.3f} ms / {p99_ms:.3f} ms")
        print(f"Fairness (Jain's index): {fair:.4f}")
        if self.receipt_log:
            epoch, root, receipts_sorted = self.receipt_log[-1]
            print(f"Last epoch: {epoch}")
            print(f"Last receipt Merkle root: {root[:16]}...")
            for r in receipts_sorted[:5]:
                print(f"  intent={r.intent_id} accepted={r.accepted} sig={r.signature[:12]}...")

    def deterministic_replay(self):
        sim2 = Simulator(self.threads, self.shards_count, self.topics, self.epoch_ms, self.seed, self.capacity_per_second)
        sim2.epoch_counter = 0
        topic = self.topics[0]
        # Replay same two-strike cadence per epoch with the same IDs
        for epoch in sorted(self.intent_log.keys()):
            sim2.epoch_counter = epoch
            # Producers: submit
            prod_ids = []
            for kind, tid, iid in self.intent_log[epoch]:
                if kind == "enq":
                    sim2.queue.enqueue_submit(tid, iid, submitted_epoch=epoch, weight=1.0, deadline_ms=0.0)
                    prod_ids.append((tid, iid))
            # Strike producers
            sim2.epoch_driver_tick(strikes=1)
            # Await producers
            for tid, iid in prod_ids:
                sim2.shards[0].await_receipt(topic, iid, timeout_ms=100)

            # Consumers: submit
            cons_ids = []
            for kind, tid, iid in self.intent_log[epoch]:
                if kind == "deq":
                    sim2.queue.dequeue_submit(tid, iid, submitted_epoch=epoch, weight=1.0, deadline_ms=0.0)
                    cons_ids.append((tid, iid))
            # Strike consumers
            sim2.epoch_driver_tick(strikes=1)
            # Await consumers
            for tid, iid in cons_ids:
                sim2.shards[0].await_receipt(topic, iid, timeout_ms=100)

        roots1 = [x[1] for x in self.receipt_log]
        roots2 = [x[1] for x in sim2.receipt_log]
        ok = bool(roots1) and bool(roots2) and roots1[-1] == roots2[-1]
        print("\n=== Deterministic replay check ===")
        print("Last root match:", ok)

# ---------------- Mutex baseline ----------------

class MutexBaseline:
    def __init__(self, capacity_per_second):
        self.lock = threading.Lock()
        self.shared_counter = 0
        self.capacity_per_second = capacity_per_second
        self.per_second_quota = capacity_per_second
        self.last_sec = math.floor(time.monotonic())

    def try_enter(self):
        now_sec = math.floor(time.monotonic())
        if now_sec != self.last_sec:
            self.per_second_quota = self.capacity_per_second
            self.last_sec = now_sec
        if self.per_second_quota <= 0:
            return False
        acquired = self.lock.acquire(timeout=0.001)
        if not acquired:
            return False
        if self.per_second_quota <= 0:
            self.lock.release()
            return False
        self.per_second_quota -= 1
        return True

    def leave(self):
        self.shared_counter += 1
        self.lock.release()

def run_mutex_baseline(threads, duration_s, seed, capacity_per_second, workload):
    m = MutexBaseline(capacity_per_second)
    lat_ms = defaultdict(list)
    accepts = defaultdict(int)
    threads_arr = []
    stop_flag = threading.Event()

    def worker(tid):
        rng = random.Random(seed + tid)
        while not stop_flag.is_set():
            t0 = time.monotonic_ns()
            if m.try_enter():
                try:
                    workload.do_work()
                    lat_ms[tid].append(ns_to_ms(time.monotonic_ns() - t0))
                    accepts[tid] += 1
                finally:
                    m.leave()
            else:
                time.sleep(rng.uniform(0.0002, 0.001))

    for tid in range(threads):
        th = threading.Thread(target=worker, args=(tid,), daemon=True)
        threads_arr.append(th)
    for th in threads_arr:
        th.start()
    time.sleep(duration_s)
    stop_flag.set()
    for th in threads_arr:
        th.join(timeout=1.0)

    total_ops = sum(accepts.values())
    all_lat = [x for arr in lat_ms.values() for x in arr]
    mean_ms = statistics.mean(all_lat) if all_lat else float('nan')
    med_ms = statistics.median(all_lat) if all_lat else float('nan')
    p95_ms = percentile_ms(all_lat, 0.95)
    p99_ms = percentile_ms(all_lat, 0.99)
    fair = jains_index([accepts[t] for t in sorted(accepts)]) if accepts else float('nan')

    print("\n=== Mutex baseline ===")
    print(f"Throughput (accepted ops): {total_ops} over {duration_s:.2f}s ({total_ops/duration_s:.1f} ops/s)")
    print(f"Latency mean/median: {mean_ms:.3f} ms / {med_ms:.3f} ms")
    print(f"Latency p95/p99: {p95_ms:.3f} ms / {p99_ms:.3f} ms")
    print(f"Fairness (Jain's index): {fair:.4f}")
    print(f"Shared counter: {m.shared_counter}")

# ---------------- Main ----------------

def main():
    threads = 64
    shards = 2
    topics = ["queue"]
    epoch_ms = 2
    seed = 2025
    duration_s = 6
    capacity_per_second = 32000  # informational; tokens/epoch fixed by threads

    workload = Workload(min_ms=0.3, max_ms=0.7)

    print("Deterministic seed:", seed)
    print("Threads:", threads)
    print("Shards (NUMA sockets):", shards)
    print("Topics:", topics)
    print("Epoch (ms):", epoch_ms)
    print("Duration (s):", duration_s)
    print("Capacity (ops/s):", capacity_per_second)

    # Baseline
    run_mutex_baseline(threads, duration_s, seed, capacity_per_second, workload)

    # Orderless emulator
    sim = Simulator(threads, shards, topics, epoch_ms, seed, capacity_per_second)
    sim.run_orderless_lockstep(duration_s, workload)
    sim.summarize(duration_s)
    sim.deterministic_replay()

if __name__ == "__main__":
    main()
