<div align="center">

# 🚀 **sfq-bench: 175x Faster Lockless SFQ vs Mutex Hell** 
## **(C: 40x @128t | Python: 175x @64t + Merkle Replay)**

[![Throughput](https://img.shields.io/badge/Throughput-175x%20🚀-00ff00)](https://github.com/eldm-ethanmoore/sfq-bench/actions)
[![Stars](https://img.shields.io/github/stars/eldm-ethanmoore/sfq-bench?style=social)](https://github.com/eldm-ethanmoore/sfq-bench)

**Python (64t, Realistic Work)**: **Mutex=183 ops/s** → **SFQ=32k/s** | **Jain=1.0**  
**C (128t, Raw Ops)**: **0.18 Mops/s** → **7.3 Mops/s** | **10s Tails → 50ns**

![Throughput](plots/throughput.png) ![Latency](plots/latency.png)

</div>

## 🏆 **Benchmark Results**
| Lang  | Threads | **Mutex**     | **SFQ**   | **Speedup** | **Tail Latency** | **Fairness** |
|-------|---------|---------------|-----------|-------------|------------------|--------------|
| **C** | **128** | **0.181 Mops/s** | **7.30** | **40x**    | **10s → 50ns**  | N/A         |
| **Py**| **64**  | **183 ops/s**    | **32k**  | **175x**   | Predictable     | **1.000**   |

**Zero deferrals/retires. Deterministic Merkle roots match on replay!**

## 🎯 **Why SFQ Wins**
- **Lock Hell**: **N² contention** → **Spins + Tails Explode**
- **SFQ Magic** (`vfinish += 1/weight` = **Linux CFS**):
  1. **Submit** (ns, non-block)
  2. **Epoch Strike** (2ms heap sort)
  3. **Receipts** → **Work** → **Audit**

**Built on **Termux/Android**!** ⚡

## 🏃‍♂️ **Run It! (5s Setup)**
### **C: Blazing Raw**
```bash
gcc -O3 -pthread switch_case_bench.c -o bench
./bench --threads **256** --tasks 50k  # **~100x? POST RESULTS!**
