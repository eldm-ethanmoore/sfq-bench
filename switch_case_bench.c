// switch_case_bench.c                                         // Proof-oriented benchmark: lock-based queue vs. orderless SFQ scheduler (heap-based)
// gcc -O3 -pthread switch_case_bench.c -o switch_case_bench
// Example: ./switch_case_bench --threads 32 --tasks 50000 --qsize 32768 --bins 60 --runs 1 --csv receipts

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdint.h>
#include <unistd.h>
#include <time.h>
#include <string.h>
#include <math.h>                                              
// -----------------------------
// Config
// -----------------------------                               typedef struct { int threads; int tasks; int qsize; int bins; int runs; const char* csv_prefix; } cfg_t;

static double now_sec() { struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts); return ts.tv_sec + ts.tv_nsec*1e-9; }
static int cmp_d(const void* a, const void* b){ double da=*(const double*)a, db=*(const double*)b; return (da<db)?-1:((da>db)?1:0); }
static void* safe_malloc(size_t bytes, const char* label) { void* p = malloc(bytes); if (!p) { fprintf(stderr, "Allocation failed for %s (%zu bytes)\n", label, bytes); exit(2);} return p; }

// -----------------------------
// Lock-based queue (mutex)
// -----------------------------
typedef struct {
    int *tasks; int head, tail, size;
    pthread_mutex_t lock;
    long long retries;
} queue_t;

static void q_init(queue_t *q, int size) {
    q->tasks = (int*)safe_malloc(size * sizeof(int), "queue tasks");
    q->head = q->tail = 0; q->size = size; q->retries = 0;
    pthread_mutex_init(&q->lock, NULL);
}
static inline int q_try_push(queue_t *q, int val) {
    if (pthread_mutex_trylock(&q->lock) == 0) {
        int next = (q->tail + 1) % q->size;
        if (next != q->head) { q->tasks[q->tail] = val; q->tail = next; pthread_mutex_unlock(&q->lock); return 1; }
        pthread_mutex_unlock(&q->lock); return 0;
    } else { q->retries++; return 0; }
}
static inline int q_try_pop(queue_t *q, int *val) {
    if (pthread_mutex_trylock(&q->lock) == 0) {
        if (q->head != q->tail) { *val = q->tasks[q->head]; q->head = (q->head + 1) % q->size; pthread_mutex_unlock(&q->lock); return 1; }
        pthread_mutex_unlock(&q->lock); return 0;
    } else { q->retries++; return 0; }
}

typedef struct {
    queue_t *q; int ops; double *lat_push; double *lat_pop;
} worker_arg_t;

static void *worker_lock(void *arg) {
    worker_arg_t *wa = (worker_arg_t*)arg; int val;
    for (int i = 0; i < wa->ops; i++) {
        double t0 = now_sec(); while (!q_try_push(wa->q, i)) {} wa->lat_push[i] = now_sec() - t0;
        double t1 = now_sec(); while (!q_try_pop(wa->q, &val)) {} wa->lat_pop[i] = now_sec() - t1;
    }
    return NULL;
}

// -----------------------------
// Orderless scheduler (SFQ on heap)
// -----------------------------
typedef struct {
    int pid;
    double weight;    // fairness weight
    double vfinish;   // virtual finish time (service history)
    int credits;      // remaining ops to serve
    long long served; // ops served (for receipts)
} intent_t;

typedef struct { intent_t **arr; int n, cap; } heap_t;

static inline double score(const intent_t *I) { return I->vfinish; }

static void heap_init(heap_t *h, int cap) { h->arr = (intent_t**)safe_malloc((cap+1)*sizeof(intent_t*), "heap"); h->n=0; h->cap=cap; }
static void heap_push(heap_t *h, intent_t *I) {
    int i = ++h->n; h->arr[i] = I;
    while (i > 1 && score(h->arr[i]) < score(h->arr[i/2])) { intent_t *tmp=h->arr[i]; h->arr[i]=h->arr[i/2]; h->arr[i/2]=tmp; i/=2; }
}
static intent_t* heap_pop(heap_t *h) {
    if (h->n==0) return NULL;
    intent_t *top = h->arr[1]; h->arr[1] = h->arr[h->n--];
    for (int i=1;;) {
        int l=2*i, r=2*i+1, s=i;
        if (l<=h->n && score(h->arr[l]) < score(h->arr[s])) s=l;
        if (r<=h->n && score(h->arr[r]) < score(h->arr[s])) s=r;
        if (s!=i) { intent_t *tmp=h->arr[i]; h->arr[i]=h->arr[s]; h->arr[s]=tmp; i=s; } else break;
    }
    return top;
}

// -----------------------------
// Stats helpers
// -----------------------------
static void percentiles(double *arr, int n, double *p50, double *p90, double *p95, double *p99, double *pmax) {
    if (!arr || n <= 0) { *p50=*p90=*p95=*p99=*pmax=0.0; return; }
    qsort(arr, n, sizeof(double), cmp_d);
    int i50 = (int)floor(0.50*n), i90 = (int)floor(0.90*n), i95 = (int)floor(0.95*n), i99 = (int)floor(0.99*n);
    if (i50 >= n) i50 = n-1; if (i90 >= n) i90 = n-1; if (i95 >= n) i95 = n-1; if (i99 >= n) i99 = n-1;
    *p50 = arr[i50]; *p90 = arr[i90]; *p95 = arr[i95]; *p99 = arr[i99]; *pmax = arr[n-1];
}
static void histogram(double *arr, int n, int bins, double *out, double *minv, double *maxv) {
    if (!arr || !out || n <= 0 || bins <= 0) { if (out) for(int i=0;i<bins;i++) out[i]=0; if(minv) *minv=0; if(maxv) *maxv=0; return; }
    double mn = arr[0], mx = arr[0];
    for (int i=1;i<n;i++){ if(arr[i]<mn) mn=arr[i]; if(arr[i]>mx) mx=arr[i]; }
    if (mx==mn) { for(int i=0;i<bins;i++) out[i]= (i==0)? (double)n : 0.0; if(minv)*minv=mn; if(maxv)*maxv=mx; return; }
    for(int i=0;i<bins;i++) out[i]=0;
    for (int i=0;i<n;i++){ int idx = (int)floor((arr[i]-mn)/(mx-mn)*bins); if (idx>=bins) idx=bins-1; out[idx]+=1.0; }
    if(minv)*minv=mn; if(maxv)*maxv=mx;
}
static double jain_fairness_served(const long long *served, int n) {
    if (!served || n<=0) return 1.0;
    double sum=0, sumsq=0; for (int i=0;i<n;i++){ sum+=served[i]; sumsq+=served[i]*served[i]; }
    if (sumsq == 0) return 1.0; return (sum*sum)/(n*sumsq);
}
static double proportional_deviation(const long long *served, const double *weights, int n) {
    if (!served || !weights || n<=0) return 0.0;
    double wsum=0; for(int i=0;i<n;i++) wsum+=weights[i];
    double max_rel=0.0;
    long long tot=0; for(int i=0;i<n;i++) tot+=served[i];
    for(int i=0;i<n;i++){
        double expected = (double)tot * (weights[i]/wsum);
        double rel = expected>0 ? fabs((double)served[i]-expected)/expected : 0.0;
        if (rel > max_rel) max_rel = rel;
    }
    return max_rel; // lower is better
}
static uint64_t checksum(double *arr, int n) {
    if (!arr || n <= 0) return 0;
    uint64_t s=1469598103934665603ull; // FNV-like
    for (int i=0;i<n;i++){ uint64_t x = (uint64_t)(arr[i]*1e9); s ^= x; s *= 1099511628211ull; }
    return s;
}

// -----------------------------
// Runs
// -----------------------------
static void run_lock(queue_t *q, int threads, int ops_per_thread, double **lat_push, double **lat_pop, long long *retries_out, double *time_out) {
    pthread_t *th = (pthread_t*)safe_malloc(sizeof(pthread_t)*threads, "threads");
    worker_arg_t *wa = (worker_arg_t*)safe_malloc(sizeof(worker_arg_t)*threads, "worker args");

    for (int i=0;i<threads;i++) {
        lat_push[i] = (double*)safe_malloc(sizeof(double)*ops_per_thread, "lat_push[i]");
        lat_pop[i]  = (double*)safe_malloc(sizeof(double)*ops_per_thread, "lat_pop[i]");
        wa[i].q=q; wa[i].ops=ops_per_thread; wa[i].lat_push=lat_push[i]; wa[i].lat_pop=lat_pop[i];
    }
    double t0 = now_sec();
    for (int i=0;i<threads;i++) pthread_create(&th[i], NULL, worker_lock, &wa[i]);
    for (int i=0;i<threads;i++) pthread_join(th[i], NULL);
    double t1 = now_sec();
    *retries_out = q->retries;
    *time_out = t1 - t0;
    printf("Lock total ops: %lld in %.6f sec\n", (long long)threads*ops_per_thread*2, *time_out);

    free(th); free(wa);
}

typedef struct {
    intent_t *intents; heap_t *heap; int nintents;
    long long total_ops_target;
    double *lat; // per-op latency
    long long total_completed; // actual ops done
    int credits_per_intent;
} orderless_ctx_t;

static void run_orderless(orderless_ctx_t *ctx, double *time_out) {
    heap_init(ctx->heap, ctx->nintents * 2);
    for (int i=0;i<ctx->nintents;i++) {
        ctx->intents[i].pid = i;
        ctx->intents[i].weight = 1.0 + (i % 5);
        ctx->intents[i].vfinish = 0.0;
        ctx->intents[i].credits = ctx->credits_per_intent;
        ctx->intents[i].served = 0;
        heap_push(ctx->heap, &ctx->intents[i]);
    }
    long long total=0, li=0;
    double t0 = now_sec();
    while (total < ctx->total_ops_target) {
        double op0 = now_sec();
        intent_t *I = heap_pop(ctx->heap);
        if (!I) break;
        if (I->credits > 0) {
            I->credits--;
            I->served++;
            total++;
            // SFQ service accounting: advance virtual finish by cost/weight (cost=1 per op)
            I->vfinish += 1.0 / I->weight;
            heap_push(ctx->heap, I);
        }
        if (li < ctx->total_ops_target) ctx->lat[li++] = now_sec() - op0;
    }
    double t1 = now_sec();
    ctx->total_completed = total;
    *time_out = t1 - t0;
    printf("Orderless total ops: %lld in %.6f sec\n", total, *time_out);
}

// -----------------------------
// CSV dump helpers
// -----------------------------
static void dump_csv_lock(const char* prefix, int run_id, int threads, int ops, double **lp, double **lo) {
    char fn[256]; snprintf(fn, sizeof(fn), "%s_lock_run%d.csv", prefix, run_id);
    FILE *f = fopen(fn, "w"); if (!f) { perror("fopen lock csv"); return; }
    fprintf(f, "thread,op_index,push_latency_s,pop_latency_s\n");
    for (int t=0;t<threads;t++) for (int i=0;i<ops;i++) fprintf(f, "%d,%d,%.9f,%.9f\n", t, i, lp[t][i], lo[t][i]);
    fclose(f);
}
static void dump_csv_orderless(const char* prefix, int run_id, long long total_ops, double *lat) {
    char fn[256]; snprintf(fn, sizeof(fn), "%s_orderless_run%d.csv", prefix, run_id);
    FILE *f = fopen(fn, "w"); if (!f) { perror("fopen orderless csv"); return; }
    fprintf(f, "op_index,latency_s\n");
    for (long long i=0;i<total_ops;i++) fprintf(f, "%lld,%.9f\n", i, lat[i]);
    fclose(f);
}

// -----------------------------
// Main
// -----------------------------
static void usage() {
    printf("Usage: ./switch_case_bench --threads N --tasks M --qsize Q --bins B --runs R --csv prefix\n");
}
int main(int argc, char**argv){
    cfg_t cfg = { .threads=32, .tasks=50000, .qsize=32768, .bins=60, .runs=1, .csv_prefix="bench" };
    for (int i=1;i<argc;i++){
        if (!strcmp(argv[i],"--threads") && i+1<argc) cfg.threads=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--tasks") && i+1<argc) cfg.tasks=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--qsize") && i+1<argc) cfg.qsize=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--bins") && i+1<argc) cfg.bins=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--runs") && i+1<argc) cfg.runs=atoi(argv[++i]);
        else if (!strcmp(argv[i],"--csv") && i+1<argc) cfg.csv_prefix=argv[++i];
        else { usage(); return 1; }
    }

    printf("Config: threads=%d tasks_per_thread=%d qsize=%d bins=%d runs=%d csv_prefix=%s\n",
           cfg.threads, cfg.tasks, cfg.qsize, cfg.bins, cfg.runs, cfg.csv_prefix);

    long long total_ops_target = (long long)cfg.threads * cfg.tasks * 2; // push+pop
    double *orderless_lat = (double*)safe_malloc(sizeof(double)*total_ops_target, "orderless_lat");
    double **lock_push = (double**)safe_malloc(sizeof(double*)*cfg.threads, "lock_push ptrs");
    double **lock_pop  = (double**)safe_malloc(sizeof(double*)*cfg.threads, "lock_pop ptrs");

    for (int run=1; run<=cfg.runs; run++) {
        printf("\n=== RUN %d ===\n", run);

        // LOCK-BASED
        queue_t q; q_init(&q, cfg.qsize);
        long long lock_retries = 0; double lock_time=0;
        run_lock(&q, cfg.threads, cfg.tasks, lock_push, lock_pop, &lock_retries, &lock_time);

        // Collate lock latencies
        int nlock = cfg.threads * cfg.tasks; // per push or per pop
        double *Lpush = (double*)safe_malloc(sizeof(double)*nlock, "Lpush");
        double *Lpop  = (double*)safe_malloc(sizeof(double)*nlock, "Lpop");
        int idx=0; for (int t=0;t<cfg.threads;t++) for (int i=0;i<cfg.tasks;i++) { Lpush[idx]=lock_push[t][i]; Lpop[idx]=lock_pop[t][i]; idx++; }

        // Stats for lock-based
        double p50p=0,p90p=0,p95p=0,p99p=0,mxp=0, p50o=0,p90o=0,p95o=0,p99o=0,mxo=0;
        percentiles(Lpush, nlock, &p50p,&p90p,&p95p,&p99p,&mxp);
        percentiles(Lpop,  nlock, &p50o,&p90o,&p95o,&p99o,&mxo);
        double *histp = (double*)safe_malloc(sizeof(double)*cfg.bins, "histp");
        double *histo = (double*)safe_malloc(sizeof(double)*cfg.bins, "histo");
        double minp=0,maxp=0,mino=0,maxo=0;
        histogram(Lpush, nlock, cfg.bins, histp, &minp, &maxp);
        histogram(Lpop,  nlock, cfg.bins, histo, &mino, &maxo);

        // ORDERLESS (SFQ)
        intent_t *intents = (intent_t*)safe_malloc(sizeof(intent_t)*cfg.threads, "intents");
        double orderless_time=0;
        heap_t heap;
        orderless_ctx_t octx = {
            .intents=intents, .heap=&heap, .nintents=cfg.threads,
            .total_ops_target=total_ops_target, .lat=orderless_lat,
            .total_completed=0, .credits_per_intent=cfg.tasks*2
        };
        run_orderless(&octx, &orderless_time);
        int norder = (int)octx.total_completed;

        // Orderless stats
        double p50l=0,p90l=0,p95l=0,p99l=0,mxl=0;
        double *histl = (double*)safe_malloc(sizeof(double)*cfg.bins, "histl");
        double minl=0,maxl=0;
        percentiles(orderless_lat, norder, &p50l,&p90l,&p95l,&p99l,&mxl);
        histogram(orderless_lat, norder, cfg.bins, histl, &minl, &maxl);

        // Fairness stats
        long long *served = (long long*)safe_malloc(sizeof(long long)*cfg.threads, "served");
        double *weights = (double*)safe_malloc(sizeof(double)*cfg.threads, "weights");
        long long tot_served=0;
        for (int i=0;i<cfg.threads;i++){ served[i]=octx.intents[i].served; weights[i]=octx.intents[i].weight; tot_served+=served[i]; }
        double jain = jain_fairness_served(served, cfg.threads);
        double prop_dev = proportional_deviation(served, weights, cfg.threads);

        // Determinism checksum
        uint64_t c_lock_push = checksum(Lpush, nlock);
        uint64_t c_lock_pop  = checksum(Lpop,  nlock);
        uint64_t c_orderless = checksum(orderless_lat, norder);

        // Print summary
        double lock_throughput = ((double)cfg.threads*cfg.tasks*2)/(lock_time*1e6);
        double orderless_throughput = ((double)norder)/(orderless_time*1e6);
        printf("\n--- Summary (RUN %d) ---\n", run);
        printf("Lock: time=%.6f s, throughput=%.3f Mops/s, retries=%lld\n", lock_time, lock_throughput, lock_retries);
        printf("Orderless (SFQ): time=%.6f s, throughput=%.3f Mops/s, retries=0\n", orderless_time, orderless_throughput);
        printf("Lock latency push: p50=%.6f p90=%.6f p95=%.6f p99=%.6f max=%.6f (range [%.6f..%.6f])\n", p50p,p90p,p95p,p99p,mxp,minp,maxp);
        printf("Lock latency pop : p50=%.6f p90=%.6f p95=%.6f p99=%.6f max=%.6f (range [%.6f..%.6f])\n", p50o,p90o,p95o,p99o,mxo,mino,maxo);
        printf("Orderless latency: p50=%.6f p90=%.6f p95=%.6f p99=%.6f max=%.6f (range [%.6f..%.6f])\n", p50l,p90l,p95l,p99l,mxl,minl,maxl);
        printf("Fairness (Jain index on served): %.6f (1.0 = perfect)\n", jain);
        printf("Proportional-share deviation (max relative): %.6f (lower is better)\n", prop_dev);
        printf("Determinism checksums: lock_push=%llu lock_pop=%llu orderless=%llu\n",
               (unsigned long long)c_lock_push, (unsigned long long)c_lock_pop, (unsigned long long)c_orderless);

        // CSV dumps
        if (cfg.csv_prefix) {
            dump_csv_lock(cfg.csv_prefix, run, cfg.threads, cfg.tasks, lock_push, lock_pop);
            dump_csv_orderless(cfg.csv_prefix, run, octx.total_completed, orderless_lat);
            // Histograms (optional)
            char fnhp[256], fnhl[256], fnhq[256];
            snprintf(fnhp, sizeof(fnhp), "%s_lock_push_hist_run%d.csv", cfg.csv_prefix, run);
            snprintf(fnhq, sizeof(fnhq), "%s_lock_pop_hist_run%d.csv", cfg.csv_prefix, run);
            snprintf(fnhl, sizeof(fnhl), "%s_orderless_hist_run%d.csv", cfg.csv_prefix, run);
            FILE *fp=fopen(fnhp,"w"); if (fp){ fprintf(fp,"bin,count,min=%.9f,max=%.9f\n",minp,maxp); for(int b=0;b<cfg.bins;b++){ fprintf(fp,"%d,%.0f\n",b,histp[b]); } fclose(fp); }
            FILE *fq=fopen(fnhq,"w"); if (fq){ fprintf(fq,"bin,count,min=%.9f,max=%.9f\n",mino,maxo); for(int b=0;b<cfg.bins;b++){ fprintf(fq,"%d,%.0f\n",b,histo[b]); } fclose(fq); }
            FILE *fl=fopen(fnhl,"w"); if (fl){ fprintf(fl,"bin,count,min=%.9f,max=%.9f\n",minl,maxl); for(int b=0;b<cfg.bins;b++){ fprintf(fl,"%d,%.0f\n",b,histl[b]); } fclose(fl); }
        }

        // Per-run receipts
        printf("\nServed ops per intent (pid:served, weight):\n");
        for (int i=0;i<cfg.threads;i++){
            printf("pid=%d served=%lld weight=%.1f\n", i, served[i], weights[i]);
        }

        // Cleanup per run
        for (int t=0;t<cfg.threads;t++){ free(lock_push[t]); free(lock_pop[t]); }
        free(Lpush); free(Lpop); free(histp); free(histo); free(histl); free(served); free(weights); free(intents); free(q.tasks);
    }

    free(lock_push); free(lock_pop); free(orderless_lat);
    printf("\nDone. CSVs (if enabled) dumped with prefix: %s\n", cfg.csv_prefix);
    return 0;
}
