// Shopping Sessions pipeline: one native binary, CSV in, Parquet out.
//
// Data flow (every stage runs on all cores, no locks on the hot path):
//   1. links    : parallel parse -> hash-partitioned ID table -> union-find -> canonical name per ID
//   2. events   : parallel AVX2 CSV scan; each row is validated on the spot and appended to a
//                 thread-local buffer chosen by hash(event_id)            (radix partition #1)
//   3. dedup    : each event_id partition is deduplicated with a small cache-resident table
//                 (earliest ingest_ts wins, validity of the kept copy decides), the survivor's
//                 user_id is resolved to its person, and it is appended to a buffer chosen by
//                 hash(person name)                                        (radix partition #2)
//   4. clicks   : parallel scan + validation, resolved and routed straight into partition #2.
//                 Copies of a click are identical, so duplicates cannot change the latest click
//                 or the set of assisting campaigns: no dedup is needed.
//   5. people   : each person partition groups rows by exact name, sorts by (person, ts), and
//                 runs sessionization + last-click credit + 7-day assists in one sweep.
//   6. output   : Parquet written directly (one user file per worker, one campaign file).
#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace fs = std::filesystem;
using sv = std::string_view;
static constexpr int64_t DAY_MS = 86'400'000LL;
static constexpr int64_t CLICK_WINDOW_MS = 604'800'000LL;
static constexpr int64_t SESSION_GAP_MS = 1'800'000LL;
static constexpr int64_t LATE_MS = 86'400'000LL;
static constexpr int ND_BITS = 10;            // event_id partitions
static constexpr uint32_t ND = 1u << ND_BITS;
static constexpr int NP_BITS = 10;            // person partitions
static constexpr uint32_t NP = 1u << NP_BITS;
static constexpr int ID_BITS = 8;             // link-ID table partitions
static constexpr uint32_t NID = 1u << ID_BITS;

// --------------------------------- timing ----------------------------------
static auto T0 = std::chrono::steady_clock::now();
struct Phase {
    const char* name; std::chrono::steady_clock::time_point s = std::chrono::steady_clock::now();
    explicit Phase(const char* n) : name(n) {}
    ~Phase() {
        auto now = std::chrono::steady_clock::now();
        fprintf(stderr, "timing phase=%s elapsed_ms=%.1f at_ms=%.1f\n", name,
                std::chrono::duration<double, std::milli>(now - s).count(),
                std::chrono::duration<double, std::milli>(now - T0).count());
    }
};

// --------------------------------- hashing ---------------------------------
static inline uint64_t mix(uint64_t a, uint64_t b) {
    __uint128_t r = static_cast<__uint128_t>(a) * b;
    return static_cast<uint64_t>(r) ^ static_cast<uint64_t>(r >> 64);
}
static inline uint64_t hash_sv(sv s) noexcept {
    const unsigned char* p = reinterpret_cast<const unsigned char*>(s.data());
    size_t n = s.size();
    uint64_t a = 0, b = 0, seed = 0x243f6a8885a308d3ULL;
    if (n <= 16) {
        if (n >= 8) { std::memcpy(&a, p, 8); std::memcpy(&b, p + n - 8, 8); }
        else if (n >= 4) { uint32_t x, y; std::memcpy(&x, p, 4); std::memcpy(&y, p + n - 4, 4); a = x; b = y; }
        else if (n > 0) { a = (uint64_t(p[0]) << 16) | (uint64_t(p[n >> 1]) << 8) | p[n - 1]; }
    } else {
        size_t i = 0;
        while (n - i > 16) {
            uint64_t x, y; std::memcpy(&x, p + i, 8); std::memcpy(&y, p + i + 8, 8);
            seed = mix(x ^ 0xa0761d6478bd642fULL, y ^ seed); i += 16;
        }
        std::memcpy(&a, p + n - 16, 8); std::memcpy(&b, p + n - 8, 8);
    }
    uint64_t h = mix(a ^ 0xa0761d6478bd642fULL, b ^ seed ^ n);
    return mix(h ^ 0xe7037ed1a0b428dbULL, 0x8ebc6af09c88c6e3ULL ^ n);
}

// ------------------------------- file mapping ------------------------------
struct Mapping { const char* data = nullptr; size_t size = 0; };
static Mapping map_file(const fs::path& p) {
    int fd = ::open(p.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("open failed: " + p.string());
    struct stat st{};
    if (::fstat(fd, &st) != 0) throw std::runtime_error("fstat failed: " + p.string());
    Mapping m; m.size = static_cast<size_t>(st.st_size);
    if (m.size) {
        void* a = ::mmap(nullptr, m.size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (a == MAP_FAILED) throw std::runtime_error("mmap failed: " + p.string());
        ::madvise(a, m.size, MADV_WILLNEED);
        m.data = static_cast<const char*>(a);
    }
    ::close(fd); // the mapping stays valid; views into it live until process exit
    return m;
}
static std::vector<Mapping> map_csv_dir(const fs::path& dir) {
    std::vector<fs::path> paths;
    if (fs::exists(dir))
        for (const auto& e : fs::directory_iterator(dir))
            if (e.is_regular_file() && e.path().extension() == ".csv") paths.push_back(e.path());
    std::sort(paths.begin(), paths.end());
    std::vector<Mapping> maps;
    for (const auto& p : paths) maps.push_back(map_file(p));
    return maps;
}
// Line-aligned byte ranges covering every data line (header skipped).
static std::vector<sv> make_chunks(const std::vector<Mapping>& maps, size_t target) {
    std::vector<sv> chunks;
    for (const Mapping& m : maps) {
        sv data(m.data ? m.data : "", m.size);
        size_t pos = data.find('\n');
        if (pos == sv::npos) continue;
        ++pos;
        while (pos < data.size()) {
            size_t end = std::min(data.size(), pos + target);
            if (end < data.size()) {
                size_t nl = data.find('\n', end);
                end = (nl == sv::npos) ? data.size() : nl + 1;
            }
            chunks.emplace_back(data.substr(pos, end - pos));
            pos = end;
        }
    }
    // Largest first improves load balance.
    std::sort(chunks.begin(), chunks.end(), [](sv a, sv b) { return a.size() > b.size(); });
    return chunks;
}
// Chunk size for splitting input files across threads (overridable for boundary tests).
static size_t chunk_bytes(size_t dflt) {
    const char* e = std::getenv("PIPELINE_CHUNK_BYTES");
    return e ? std::max<size_t>(1, std::strtoull(e, nullptr, 10)) : dflt;
}
static size_t total_bytes(const std::vector<Mapping>& maps) { size_t t = 0; for (auto& m : maps) t += m.size; return t; }

template <class Fn>
static void run_threads(unsigned n, Fn&& fn) {
    std::vector<std::thread> pool;
    std::exception_ptr err; std::atomic<bool> failed{false};
    for (unsigned t = 0; t < n; ++t)
        pool.emplace_back([&, t] {
            try { fn(t); } catch (...) { if (!failed.exchange(true)) err = std::current_exception(); }
        });
    for (auto& th : pool) th.join();
    if (err) std::rethrow_exception(err);
}

// --------------------------------- CSV scan --------------------------------
// Quotes in bits [a, b) of the 128-bit word hi:lo (0 <= a <= b <= 128).
static inline int popcnt_range(uint64_t lo, uint64_t hi, unsigned a, unsigned b) {
    int c = 0;
    if (a < 64) { uint64_t m = lo >> a; unsigned n = std::min(b, 64u) - a; if (n < 64) m &= (uint64_t(1) << n) - 1; c += __builtin_popcountll(m); }
    if (b > 64) { unsigned a2 = a > 64 ? a - 64 : 0, n = b - 64 - a2; uint64_t m = hi >> a2; if (n < 64) m &= (uint64_t(1) << n) - 1; c += __builtin_popcountll(m); }
    return c;
}
// Masks of ',' or '\n' (sep) and of '"' (quo) for 64 bytes at p.
static inline void block_masks(const char* p, uint64_t& sep, uint64_t& quo) {
#ifdef __AVX2__
    const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
    __m256i x0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
    __m256i x1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
    uint32_t s0 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(x0, vc), _mm256_cmpeq_epi8(x0, vn))));
    uint32_t s1 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_or_si256(_mm256_cmpeq_epi8(x1, vc), _mm256_cmpeq_epi8(x1, vn))));
    uint32_t q0 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(x0, vq)));
    uint32_t q1 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(x1, vq)));
    sep = (uint64_t(s1) << 32) | s0; quo = (uint64_t(q1) << 32) | q0;
#else
    sep = quo = 0;
    for (int i = 0; i < 64; ++i) {
        if (p[i] == ',' || p[i] == '\n') sep |= uint64_t(1) << i;
        if (p[i] == '"') quo |= uint64_t(1) << i;
    }
#endif
}
// The dialect: only '\n' ends a line; a field is either quote-free, or wrapped in exactly one
// pair of quotes with no quote inside. Anything else is malformed, as is a line with the wrong
// number of fields (an empty line included). The loop stops only at ',' and '\n'; a field's
// quotes are counted from the quote masks of the current and previous 64-byte blocks.
template <size_t N, class Fn>
static void scan_lines(sv chunk, Fn&& fn) {
    const char* base = chunk.data();
    const char* end = base + chunk.size();
    std::array<sv, N> f;
    size_t fi = 0; const char* fs_ = base; const char* ls = base; bool bad = false;
    uintptr_t curB = 0; uint64_t curQ = 0, prevQ = 0;
    auto count_q = [&](const char* a, const char* b) -> int {
        uintptr_t ua = reinterpret_cast<uintptr_t>(a), ub = reinterpret_cast<uintptr_t>(b);
        if (ua + 64 >= curB && ub <= curB + 64)
            return popcnt_range(prevQ, curQ, static_cast<unsigned>(ua + 64 - curB), static_cast<unsigned>(ub + 64 - curB));
        int c = 0; for (const char* x = a; x < b; ++x) c += (*x == '"'); return c; // a long field
    };
    auto end_field = [&](const char* fe) {
        if (fi >= N) { bad = true; return; }
        int q = count_q(fs_, fe);
        if (q == 0) f[fi] = sv(fs_, static_cast<size_t>(fe - fs_));
        else if (q == 2 && fe - fs_ >= 2 && *fs_ == '"' && fe[-1] == '"') f[fi] = sv(fs_ + 1, static_cast<size_t>(fe - fs_ - 2));
        else { bad = true; return; }
        ++fi;
    };
    alignas(64) char tail[64];
    for (const char* b = base; b < end; b += 64) {
        uint64_t sep, quo;
        if (b + 64 <= end) block_masks(b, sep, quo);
        else {
            size_t n = static_cast<size_t>(end - b);
            std::memset(tail, 0, 64); std::memcpy(tail, b, n);
            block_masks(tail, sep, quo);
        }
        prevQ = curQ; curQ = quo; curB = reinterpret_cast<uintptr_t>(b);
        while (sep) {
            const char* c = b + __builtin_ctzll(sep); sep &= sep - 1;
            if (*c == ',') { if (!bad) end_field(c); fs_ = c + 1; }
            else {
                if (!bad) { end_field(c); if (!bad && fi == N) fn(f); }
                fi = 0; fs_ = ls = c + 1; bad = false;
            }
        }
    }
    if (ls < end) { // final line without '\n' (its last field may be empty)
        if (!bad) { end_field(end); if (!bad && fi == N) fn(f); }
    }
}

// Two-stage scan, same dialect and same result as scan_lines. Stage 1 runs over 64-byte blocks
// and writes every comma offset into an array; for each '\n' it records the offset and how many
// commas and quotes come before it. Stage 2 then walks whole lines: a line is well-formed only
// with exactly N-1 commas, and its quotes are valid iff every field starts with a quote exactly
// when it ends with one (length >= 2 when it does) and the line holds two quotes per such field.
// (Each wrapped field owns its two boundary quotes, so any other quote makes the count too high.)
template <size_t N, class Fn>
static void scan_lines_fast(sv chunk, Fn&& fn) {
    static_assert(N >= 2);
    if (chunk.size() >= (size_t(1) << 32) - 128) { scan_lines<N>(chunk, fn); return; }
    const char* base = chunk.data();
    const uint32_t n = static_cast<uint32_t>(chunk.size());
    constexpr uint32_t MAXL = 128;                                     // lines per stage-2 batch
    static thread_local std::vector<uint32_t> cmv;
    if (cmv.size() < 4096) cmv.resize(4096);
    uint32_t* cm = cmv.data();
    uint32_t cmN = 0, cmBase = 0;          // cm[i] holds comma number cmBase + i (counted in the chunk)
    uint32_t totC = 0, totQ = 0;           // commas and quotes before the current block
    uint32_t nlPos[MAXL + 64], nlC[MAXL + 64], nlQ[MAXL + 64]; uint32_t nl = 0;
    uint32_t ls = 0, lc = 0, lq = 0;       // current line start: offset, commas and quotes before it
    std::array<sv, N> f;
    auto line = [&](uint32_t le, uint32_t c1, uint32_t q1) {
        if (c1 - lc == N - 1) {
            const uint32_t* c = cm + (lc - cmBase);
            uint32_t s = ls;
            if (q1 == lq) {
                for (size_t i = 0; i + 1 < N; ++i) { f[i] = sv(base + s, c[i] - s); s = c[i] + 1; }
                f[N - 1] = sv(base + s, le - s);
                fn(f);
            } else {
                bool bad = false; uint32_t nq = 0;
                for (size_t i = 0; i < N; ++i) {
                    const uint32_t e = i + 1 < N ? c[i] : le;
                    const uint32_t len = e - s;
                    const bool qf = len >= 1 && base[s] == '"', qe = len >= 1 && base[e - 1] == '"';
                    bad |= (qf != qe) | (qf & (len < 2));
                    nq += qf;
                    f[i] = sv(base + s + qf, len - 2 * qf * (len >= 2));
                    s = e + 1;
                }
                if (!bad && 2 * nq == q1 - lq) fn(f);
            }
        }
        ls = le + 1; lc = c1; lq = q1;
    };
    auto drain = [&] {
        for (uint32_t k = 0; k < nl; ++k) line(nlPos[k], nlC[k], nlQ[k]);
        nl = 0;
        // keep only the commas of the unfinished line
        const uint32_t drop = lc - cmBase;
        if (drop) { std::memmove(cm, cm + drop, (cmN - drop) * sizeof(uint32_t)); cmN -= drop; cmBase = lc; }
    };
    alignas(64) char tail[64];
    for (uint32_t off = 0; off < n; off += 64) {
        uint64_t C, NL, Q;
        const char* p = base + off;
        if (n - off < 64) { std::memset(tail, 0, 64); std::memcpy(tail, p, n - off); p = tail; }
#ifdef __AVX2__
        {
            const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
            __m256i x0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p));
            __m256i x1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(p + 32));
            auto mm = [](__m256i a, __m256i b, __m256i v) {
                return (uint64_t(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, v)))) << 32) |
                       static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(a, v)));
            };
            C = mm(x0, x1, vc); NL = mm(x0, x1, vn); Q = mm(x0, x1, vq);
        }
#else
        C = NL = Q = 0;
        for (int i = 0; i < 64; ++i) {
            if (p[i] == ',') C |= uint64_t(1) << i;
            if (p[i] == '\n') NL |= uint64_t(1) << i;
            if (p[i] == '"') Q |= uint64_t(1) << i;
        }
#endif
        if (cmN + 64 > cmv.size()) { cmv.resize(cmv.size() * 2); cm = cmv.data(); }
        {
            uint64_t m = C; uint32_t* out = cm + cmN; const uint32_t cnt = static_cast<uint32_t>(__builtin_popcountll(C));
            for (int i = 0; i < 8; ++i) { out[i] = off + static_cast<uint32_t>(_tzcnt_u64(m)); m = _blsr_u64(m); }
            if (cnt > 8) for (int i = 8; i < 64 && m; ++i) { out[i] = off + static_cast<uint32_t>(_tzcnt_u64(m)); m = _blsr_u64(m); }
            cmN += cnt;
        }
        for (uint64_t m = NL; m; m = _blsr_u64(m)) {
            const uint32_t b = static_cast<uint32_t>(_tzcnt_u64(m));
            const uint64_t below = _bzhi_u64(~uint64_t(0), b);
            nlPos[nl] = off + b;
            nlC[nl] = totC + static_cast<uint32_t>(__builtin_popcountll(C & below));
            nlQ[nl] = totQ + static_cast<uint32_t>(__builtin_popcountll(Q & below));
            ++nl;
        }
        totC += static_cast<uint32_t>(__builtin_popcountll(C)); totQ += static_cast<uint32_t>(__builtin_popcountll(Q));
        if (nl >= MAXL) drain();
    }
    drain();
    if (ls < n) line(n, totC, totQ); // final line without '\n' (its last field may be empty)
}

// -------------------------------- timestamps -------------------------------
static inline bool ascii_digits(sv s) {
    if (s.empty()) return false;
    for (unsigned char c : s) if (c < '0' || c > '9') return false;
    return true;
}
static bool parse_u64_ascii(sv s, uint64_t& out) {
    if (!ascii_digits(s)) return false;
    uint64_t v = 0;
    for (unsigned char c : s) {
        uint32_t d = c - '0';
        if (v > (std::numeric_limits<uint64_t>::max() - d) / 10) return false;
        v = v * 10 + d;
    }
    out = v; return true;
}
static inline bool leap_year(int y) { return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0)); }
static inline int month_days(int y, int m) {
    static constexpr int ds[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    return ds[m] + (m == 2 && leap_year(y));
}
static int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
}
static inline bool dig(char c) { return c >= '0' && c <= '9'; }
static inline int d2(const char* s) { return (s[0] - '0') * 10 + (s[1] - '0'); }
// The calendar part "YYYY-MM-DD" -> days since 1970, or BAD_DATE when it is not a real date.
static constexpr int64_t BAD_DATE = std::numeric_limits<int64_t>::min();
static int64_t parse_date10(const char* p) {
    if (p[4] != '-' || p[7] != '-') return BAD_DATE;
    if (!(dig(p[0]) && dig(p[1]) && dig(p[2]) && dig(p[3]) && dig(p[5]) && dig(p[6]) && dig(p[8]) && dig(p[9]))) return BAD_DATE;
    int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8);
    if (mo < 1 || mo > 12 || d < 1 || d > month_days(y, mo)) return BAD_DATE;
    return days_from_civil(y, static_cast<unsigned>(mo), static_cast<unsigned>(d));
}
// A dataset holds a few weeks of dates, so each thread remembers the dates it has seen
// (valid or not) in a small direct-mapped table keyed by the ten date bytes.
struct DateMemo { uint64_t k8 = 0; uint16_t k2 = 0; int64_t days = BAD_DATE; }; // k8 = 0 never matches real text
static thread_local DateMemo dateMemo[128];
static inline int64_t date_days(const char* p) {
    uint64_t k8; uint16_t k2; std::memcpy(&k8, p, 8); std::memcpy(&k2, p + 8, 2);
    DateMemo& m = dateMemo[((k8 ^ (uint64_t(k2) << 40)) * 0x9E3779B97F4A7C15ULL) >> 57];
    if (m.k8 == k8 && m.k2 == k2) return m.days;
    m.k8 = k8; m.k2 = k2; m.days = parse_date10(p);
    return m.days;
}
static bool parse_f1_or_f2(sv s, int64_t& out) {
    if (s.size() != 20 && s.size() != 25) return false;
    const char* p = s.data();
    if (p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
    if (!(dig(p[11]) && dig(p[12]) && dig(p[14]) && dig(p[15]) && dig(p[17]) && dig(p[18]))) return false;
    int hh = d2(p + 11), mm = d2(p + 14), ss = d2(p + 17);
    if (hh > 23 || mm > 59 || ss > 59) return false;
    int64_t days = date_days(p);
    if (days == BAD_DATE) return false;
    int off = 0;
    if (s.size() == 20) { if (p[19] != 'Z') return false; }
    else {
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
        if (!(dig(p[20]) && dig(p[21]) && dig(p[23]) && dig(p[24]))) return false;
        int oh = d2(p + 20), om = d2(p + 23);
        if (oh > 23 || om > 59) return false;
        off = oh * 60 + om; if (p[19] == '-') off = -off;
    }
    int64_t sec = days * 86400 + hh * 3600 + mm * 60 + ss - int64_t(off) * 60;
    out = sec * 1000; // years are 4 digits, so no overflow is possible
    return true;
}
static inline bool parse_timestamp(sv s, int64_t& out) {
    if (s.size() >= 19 && s[4] == '-' && s[7] == '-') return parse_f1_or_f2(s, out);
    uint64_t n;
    if (!parse_u64_ascii(s, n)) return false;
    if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return false; // outside any window
    out = static_cast<int64_t>(n); return true;
}
static bool parse_f1(sv s, int64_t& out) { return s.size() == 20 && s.back() == 'Z' && parse_f1_or_f2(s, out); }

// ------------------------------- packed ID keys ------------------------------
// An ID of 1..10 bytes from [0-9A-Z_a-z] is packed 6 bits a byte, first byte highest, padded with
// zero codes. Codes follow ASCII order, so integer order is byte-wise order and integer equality
// is byte equality. Every ID in the generator's data has this shape (a letter and up to 8 base-36
// characters). Any other non-empty ID is interned and gets INTERNED | index; comparisons that
// involve one fall back to its bytes, so the result is exact for every input.
static constexpr uint64_t INTERNED = 1ULL << 63;
static constexpr uint64_t NO_KEY = ~0ULL;          // "no person": an invalid kept copy
static uint8_t PACK[256];
static char UNPACK[64];
static void init_pack() {
    int c = 1;
    auto add = [&](char lo, char hi) { for (char ch = lo; ch <= hi; ++ch) { PACK[static_cast<uint8_t>(ch)] = static_cast<uint8_t>(c); UNPACK[c] = ch; ++c; } };
    add('0', '9'); add('A', 'Z'); add('_', '_'); add('a', 'z'); // ASCII order: 0-9 < A-Z < _ < a-z
}
// Sharded so that even input made only of such IDs stays parallel.
struct Interner {
    static constexpr uint32_t SHARDS = 256, SHIFT = 54; // index = shard << SHIFT | local
    struct Shard { std::mutex m; std::unordered_map<std::string, uint32_t> ids; std::deque<std::string> names; };
    Shard shards[SHARDS];
    uint64_t get(sv s) {
        uint32_t sh = static_cast<uint32_t>(hash_sv(s) >> 56);
        Shard& S = shards[sh];
        std::lock_guard<std::mutex> g(S.m);
        auto it = S.ids.find(std::string(s));
        uint32_t i;
        if (it != S.ids.end()) i = it->second;
        else { i = static_cast<uint32_t>(S.names.size()); S.names.emplace_back(s); S.ids.emplace(S.names.back(), i); }
        return (uint64_t(sh) << SHIFT) | i;
    }
    std::string name(uint64_t idx) {
        Shard& S = shards[idx >> SHIFT];
        std::lock_guard<std::mutex> g(S.m);
        return S.names[static_cast<uint32_t>(idx & ((uint64_t(1) << SHIFT) - 1))];
    }
};
static Interner interner;
static inline bool pack_key(sv s, uint64_t& k) {
    size_t n = s.size();
    if (n == 0 || n > 10) return false;
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) {
        uint8_t c = PACK[static_cast<uint8_t>(s[i])];
        if (!c) return false;
        v = (v << 6) | c;
    }
    k = v << (6 * (10 - n));
    return true;
}
static inline uint64_t key_of(sv s) { // s must be non-empty
    uint64_t k;
    if (pack_key(s, k)) return k;
    return INTERNED | interner.get(s);
}
static std::string key_str(uint64_t k) {
    if (k & INTERNED) return interner.name(k & ~INTERNED);
    std::string s;
    for (int i = 9; i >= 0; --i) {
        uint32_t c = static_cast<uint32_t>((k >> (6 * i)) & 63);
        if (!c) break;
        s.push_back(UNPACK[c]);
    }
    return s;
}
static inline bool key_less(uint64_t a, uint64_t b) {
    if (!((a | b) & INTERNED)) return a < b;
    return key_str(a) < key_str(b); // std::string compares bytes as unsigned
}
static inline uint64_t khash(uint64_t k) { return mix(k ^ 0x9E3779B97F4A7C15ULL, 0xD6E8FEB86659FD93ULL); }

// u64 key -> u64 value, open addressing, capacity sized to ~60% load (not a power of two).
struct KeyMap {
    struct E { uint64_t k, v; };
    static constexpr uint64_t EMPTY = ~0ULL - 1; // never a real key (interned indexes stay small)
    std::vector<E> t; uint64_t cap = 0;
    void build(size_t n) { cap = n + n * 2 / 3 + 16; t.assign(cap, E{EMPTY, 0}); }
    static inline uint64_t slot(uint64_t h, uint64_t cap) { return ((h & 0xFFFFFFFFu) * cap) >> 32; }
    inline void prefetch(uint64_t h) const { if (cap) __builtin_prefetch(&t[slot(h, cap)]); }
    // returns the entry; fresh = inserted now
    E* upsert(uint64_t k, uint64_t h, bool& fresh) {
        for (uint64_t p = slot(h, cap);;) {
            E& e = t[p];
            if (e.k == EMPTY) { e.k = k; fresh = true; return &e; }
            if (e.k == k) { fresh = false; return &e; }
            if (++p == cap) p = 0;
        }
    }
    inline const E* find(uint64_t k, uint64_t h) const {
        if (!cap) return nullptr;
        for (uint64_t p = slot(h, cap);;) {
            const E& e = t[p];
            if (e.k == k) return &e;
            if (e.k == EMPTY) return nullptr;
            if (++p == cap) p = 0;
        }
    }
};

// Lookup tables keyed by ID text (products, campaigns). Packable IDs go through a KeyMap;
// any other spelling through an exact string map (never hit by the generator's data).
struct IdLookup {
    KeyMap packed; std::unordered_map<std::string, uint32_t> other;
    void build(size_t n) { packed.build(n); }
    bool insert(sv s, uint32_t v) { // keeps the first value for an ID
        uint64_t k;
        if (pack_key(s, k)) { bool fresh; KeyMap::E* e = packed.upsert(k, khash(k), fresh); if (fresh) e->v = v; return fresh; }
        return other.emplace(std::string(s), v).second;
    }
    inline void prefetch(sv s) const { uint64_t k; if (pack_key(s, k)) packed.prefetch(khash(k)); }
    inline int64_t find(sv s) const {
        uint64_t k;
        if (pack_key(s, k)) { const KeyMap::E* e = packed.find(k, khash(k)); return e ? static_cast<int64_t>(e->v) : -1; }
        if (other.empty()) return -1;
        auto it = other.find(std::string(s));
        return it == other.end() ? -1 : static_cast<int64_t>(it->second);
    }
};

// ----------------------------- parquet writer ------------------------------
struct Thrift {
    std::string b; int last = 0; std::vector<int> st;
    void varint(uint64_t v) { while (v >= 0x80) { b.push_back(char(v | 0x80)); v >>= 7; } b.push_back(char(v)); }
    void zz(int64_t v) { varint((static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63)); }
    void field(int id, int type) {
        int d = id - last;
        if (d > 0 && d <= 15) b.push_back(char((d << 4) | type)); else { b.push_back(char(type)); zz(id); }
        last = id;
    }
    void i32(int id, int64_t v) { field(id, 5); zz(v); }
    void i64(int id, int64_t v) { field(id, 6); zz(v); }
    void str(int id, sv s) { field(id, 8); varint(s.size()); b.append(s); }
    void sbeg(int id) { field(id, 12); st.push_back(last); last = 0; }
    void send() { b.push_back(0); last = st.back(); st.pop_back(); }
    void lbeg(int id, int et, size_t n) { field(id, 9); if (n < 15) b.push_back(char((n << 4) | et)); else { b.push_back(char(0xF0 | et)); varint(n); } }
    void ebeg() { st.push_back(last); last = 0; } // struct element inside a list
};
enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTE_ARRAY = 6 };
enum Logical { L_NONE, L_STRING, L_DATE };
struct Column {
    std::string name; PType type; Logical logical; bool optional = false;
    std::string data;            // PLAIN-encoded non-null values
    std::vector<uint8_t> defs;   // bit-packed definition levels (optional columns)
    void put_str(sv s) { uint32_t n = static_cast<uint32_t>(s.size()); data.append(reinterpret_cast<const char*>(&n), 4); data.append(s); }
    void put_i32(int32_t v) { data.append(reinterpret_cast<const char*>(&v), 4); }
    void put_i64(int64_t v) { data.append(reinterpret_cast<const char*>(&v), 8); }
    void put_def(size_t row, bool present) { if ((row & 7) == 0) defs.push_back(0); if (present) defs.back() |= uint8_t(1u << (row & 7)); }
};
static void write_parquet(const fs::path& path, std::vector<Column>& cols, size_t rows) {
    std::string out = "PAR1";
    struct Meta { size_t offset, size; };
    std::vector<Meta> metas;
    for (Column& c : cols) {
        std::string page;
        if (c.optional) {
            Thrift rle; rle.varint((c.defs.size() << 1) | 1); // one bit-packed run, width 1
            uint32_t len = static_cast<uint32_t>(rle.b.size() + c.defs.size());
            page.append(reinterpret_cast<const char*>(&len), 4); page.append(rle.b);
            page.append(reinterpret_cast<const char*>(c.defs.data()), c.defs.size());
        }
        page.append(c.data);
        Thrift h;
        h.i32(1, 0); h.i32(2, static_cast<int64_t>(page.size())); h.i32(3, static_cast<int64_t>(page.size()));
        h.sbeg(5); h.i32(1, static_cast<int64_t>(rows)); h.i32(2, 0); h.i32(3, 3); h.i32(4, 3); h.send();
        h.b.push_back(0);
        metas.push_back({out.size(), h.b.size() + page.size()});
        out += h.b; out += page;
        std::string().swap(c.data);
    }
    Thrift m;
    m.i32(1, 1);
    m.lbeg(2, 12, cols.size() + 1);
    m.ebeg(); m.str(4, "schema"); m.i32(5, static_cast<int64_t>(cols.size())); m.send();
    for (const Column& c : cols) {
        m.ebeg(); m.i32(1, c.type); m.i32(3, c.optional ? 1 : 0); m.str(4, c.name);
        if (c.logical == L_STRING) { m.i32(6, 0); m.sbeg(10); m.sbeg(1); m.send(); m.send(); }
        if (c.logical == L_DATE) { m.i32(6, 6); m.sbeg(10); m.sbeg(6); m.send(); m.send(); }
        m.send();
    }
    m.i64(3, static_cast<int64_t>(rows));
    if (rows) {
        m.lbeg(4, 12, 1);
        m.ebeg();
        m.lbeg(1, 12, cols.size());
        size_t total = 0;
        for (size_t i = 0; i < cols.size(); ++i) {
            const Column& c = cols[i]; total += metas[i].size;
            m.ebeg(); m.i64(2, static_cast<int64_t>(metas[i].offset));
            m.sbeg(3);
            m.i32(1, c.type);
            m.lbeg(2, 5, c.optional ? 2 : 1); m.zz(0); if (c.optional) m.zz(3);
            m.lbeg(3, 8, 1); m.varint(c.name.size()); m.b.append(c.name);
            m.i32(4, 0); m.i64(5, static_cast<int64_t>(rows));
            m.i64(6, static_cast<int64_t>(metas[i].size)); m.i64(7, static_cast<int64_t>(metas[i].size));
            m.i64(9, static_cast<int64_t>(metas[i].offset));
            m.send();
            m.send();
        }
        m.i64(2, static_cast<int64_t>(total)); m.i64(3, static_cast<int64_t>(rows));
        m.send();
    } else {
        m.lbeg(4, 12, 0);
    }
    m.str(6, "shopping-sessions native writer");
    m.b.push_back(0);
    out += m.b;
    uint32_t mlen = static_cast<uint32_t>(m.b.size());
    out.append(reinterpret_cast<const char*>(&mlen), 4);
    out += "PAR1";
    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) throw std::runtime_error("cannot create " + path.string());
    size_t off = 0;
    while (off < out.size()) {
        ssize_t w = ::write(fd, out.data() + off, out.size() - off);
        if (w <= 0) throw std::runtime_error("write failed: " + path.string());
        off += static_cast<size_t>(w);
    }
    ::close(fd);
}

// ------------------------------ memory arenas -------------------------------
// Row buffers come from big anonymous regions marked for transparent huge pages and
// populated a slab at a time: a few hundred faults instead of a quarter million.
struct Arena {
    char* base = nullptr; size_t cap = 0; std::atomic<size_t> used{0};
    static constexpr size_t SLAB = size_t(4) << 20;
    void init(size_t bytes) {
        cap = (bytes + SLAB - 1) / SLAB * SLAB;
        void* p = ::mmap(nullptr, cap, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
        if (p == MAP_FAILED) { base = nullptr; cap = 0; return; }
        base = static_cast<char*>(p);
        ::madvise(base, cap, MADV_HUGEPAGE);
    }
    char* slab() {
        size_t off = used.fetch_add(SLAB);
        char* p;
        if (off + SLAB <= cap) p = base + off;
        else { p = static_cast<char*>(std::aligned_alloc(4096, SLAB)); if (!p) throw std::bad_alloc(); ::madvise(p, SLAB, MADV_HUGEPAGE); }
#ifdef MADV_POPULATE_WRITE
        ::madvise(p, SLAB, MADV_POPULATE_WRITE);
#endif
        return p;
    }
    void release() { if (base) ::munmap(base, cap); base = nullptr; }
};
struct SlabAlloc { // per-thread bump allocator over arena slabs
    Arena* a = nullptr; char* cur = nullptr; size_t left = 0;
    void* get(size_t n) {
        n = (n + 63) & ~size_t(63);
        if (n > left) { cur = a->slab(); left = Arena::SLAB; }
        void* p = cur; cur += n; left -= n; return p;
    }
};
template <class T, uint32_t B>
struct BlockList {
    struct Blk { Blk* next; uint32_t n; alignas(64) T items[B]; };
    Blk* head = nullptr; Blk* tail = nullptr; size_t total = 0;
    inline void push(const T& v, SlabAlloc& al) {
        if (!tail || tail->n == B) {
            Blk* b = static_cast<Blk*>(al.get(sizeof(Blk))); b->next = nullptr; b->n = 0;
            if (tail) tail->next = b; else head = b;
            tail = b;
        }
        tail->items[tail->n++] = v; ++total;
    }
    // 64-byte records go straight to memory without first reading the destination line.
    inline void push_stream(const T& v, SlabAlloc& al) {
        static_assert(sizeof(T) == 64);
        if (!tail || tail->n == B) {
            Blk* b = static_cast<Blk*>(al.get(sizeof(Blk))); b->next = nullptr; b->n = 0;
            if (tail) tail->next = b; else head = b;
            tail = b;
        }
        T* dst = &tail->items[tail->n++]; ++total;
#ifdef __AVX2__
        const __m256i* s = reinterpret_cast<const __m256i*>(&v);
        _mm256_stream_si256(reinterpret_cast<__m256i*>(dst), _mm256_loadu_si256(s));
        _mm256_stream_si256(reinterpret_cast<__m256i*>(dst) + 1, _mm256_loadu_si256(s + 1));
#else
        *dst = v;
#endif
    }
    // Append n records; a whole number of cache lines at an aligned spot leaves through
    // non-temporal stores.
    inline void push_group(const T* src, uint32_t n, SlabAlloc& al) {
        if (!tail || tail->n + n > B) {
            Blk* b = static_cast<Blk*>(al.get(sizeof(Blk))); b->next = nullptr; b->n = 0;
            if (tail) tail->next = b; else head = b;
            tail = b;
        }
        T* dst = &tail->items[tail->n];
        const size_t bytes = size_t(n) * sizeof(T);
#ifdef __AVX2__
        if (bytes % 64 == 0 && (reinterpret_cast<uintptr_t>(dst) & 63) == 0) {
            const __m256i* s = reinterpret_cast<const __m256i*>(src);
            __m256i* q = reinterpret_cast<__m256i*>(dst);
            for (size_t k = 0; k < bytes / 32; ++k) _mm256_stream_si256(q + k, _mm256_loadu_si256(s + k));
        } else
#endif
            std::memcpy(dst, src, bytes);
        tail->n += n; total += n;
    }
    template <class F> inline void each(F&& f) const { for (Blk* b = head; b; b = b->next) for (uint32_t i = 0; i < b->n; ++i) f(b->items[i]); }
    size_t size() const { return total; }
};

template <class T> using Grid = std::vector<std::vector<std::vector<T>>>; // [worker][partition]

// Append-only list that hands out a stable address for each pushed row.
template <class T, uint32_t B>
struct RefList {
    struct Blk { Blk* next; uint32_t n; alignas(64) T items[B]; };
    Blk* head = nullptr; Blk* tail = nullptr; size_t total = 0;
    inline T* push(const T& v, SlabAlloc& al) {
        if (!tail || tail->n == B) {
            Blk* b = static_cast<Blk*>(al.get(sizeof(Blk))); b->next = nullptr; b->n = 0;
            if (tail) tail->next = b; else head = b;
            tail = b;
        }
        T* d = &tail->items[tail->n++]; *d = v; ++total; return d;
    }
    template <class F> inline void each(F&& f) const { for (Blk* b = head; b; b = b->next) for (uint32_t i = 0; i < b->n; ++i) f(b->items[i]); }
    size_t size() const { return total; }
};
template <class T, uint32_t B> using BGrid = std::vector<std::vector<BlockList<T, B>>>;

// ------------------------------- record types ------------------------------
// price_cat: purchase price (1..10,000,000 < 2^24) in the low 24 bits, category in the top 8.
// A non-purchase has price_cat == 0: only purchases ever need a category.
struct ERec { uint64_t eid, person; int64_t ingest; uint32_t ts, price_cat; };   // 32 bytes
struct PEv { uint64_t person; uint32_t ts, price_cat; };                          // 16 bytes
struct PCl { uint64_t person, cid; uint32_t ts, camp; };                          // 24 bytes
static_assert(sizeof(ERec) == 32 && sizeof(PEv) == 16 && sizeof(PCl) == 24);
static inline uint32_t part_of(uint64_t h, int bits) { return static_cast<uint32_t>(h >> (64 - bits)); }

// A person partition's events: rows, plus each row's ingest_ts in a parallel array that is read
// only if the row's event_id turns out to have other copies. push returns a reference to the row:
// block address | index << 48 (user-space addresses fit in 47 bits).
struct PList {
    static constexpr uint32_t B = 1024;
    struct Blk { Blk* next; uint32_t n; alignas(64) PEv items[B]; int64_t ingest[B]; };
    Blk* head = nullptr; Blk* tail = nullptr; size_t total = 0;
    inline uint64_t push(const PEv& v, int64_t ingest, SlabAlloc& al) {
        if (!tail || tail->n == B) {
            Blk* b = static_cast<Blk*>(al.get(sizeof(Blk))); b->next = nullptr; b->n = 0;
            if (reinterpret_cast<uintptr_t>(b) >> 47) throw std::runtime_error("unexpected address layout");
            if (tail) tail->next = b; else head = b;
            tail = b;
        }
        const uint32_t i = tail->n++;
        tail->items[i] = v; tail->ingest[i] = ingest; ++total;
        return reinterpret_cast<uintptr_t>(tail) | (uint64_t(i) << 48);
    }
    static inline Blk* blk(uint64_t ref) { return reinterpret_cast<Blk*>(ref & ((uint64_t(1) << 48) - 1)); }
    static inline PEv& row(uint64_t ref) { return blk(ref)->items[ref >> 48]; }
    static inline int64_t ingest_of(uint64_t ref) { return blk(ref)->ingest[ref >> 48]; }
    template <class F> inline void each(F&& f) const { for (Blk* b = head; b; b = b->next) for (uint32_t i = 0; i < b->n; ++i) f(b->items[i]); }
    size_t size() const { return total; }
};
// A copy that is not the first of its event_id to be claimed (or arrived after the table filled):
// kept in full for the fix-up. person == NO_KEY means the copy is invalid.
struct Side { uint64_t eid, person; int64_t ingest; uint32_t ts, price_cat; };
static_assert(sizeof(Side) == 32);

// Write-combining scatter: records for each partition are staged in a cache-line-aligned
// buffer of K records and leave it with non-temporal stores, so no destination line is ever
// read just to be overwritten.
template <class T, uint32_t K, uint32_t B>
struct Scatter {
    static_assert((sizeof(T) * K) % 64 == 0 && B % K == 0);
    std::vector<BlockList<T, B>>* lists = nullptr;
    T* stage = nullptr; std::vector<uint8_t> fill; SlabAlloc al;
    void init(std::vector<BlockList<T, B>>* l, Arena* a) {
        lists = l; al.a = a; fill.assign(l->size(), 0);
        stage = static_cast<T*>(std::aligned_alloc(64, l->size() * K * sizeof(T)));
    }
    inline void push(uint32_t p, const T& v) {
        T* s = stage + static_cast<size_t>(p) * K;
        s[fill[p]++] = v;
        if (fill[p] == K) { (*lists)[p].push_group(s, K, al); fill[p] = 0; }
    }
    void finish() {
        for (size_t p = 0; p < fill.size(); ++p)
            if (fill[p]) { (*lists)[p].push_group(stage + p * K, fill[p], al); fill[p] = 0; }
#ifdef __AVX2__
        _mm_sfence();
#endif
        std::free(stage); stage = nullptr;
    }
};

// Sort one person's events by ts. Most people have a few dozen events, where insertion sort
// beats std::sort; the heaviest (whales) get a two-pass LSD radix sort on the 32-bit ts.
template <class E>
static void sort_by_ts(E* a, size_t n, std::vector<E>& tmp) {
    if (n <= 32) {
        for (size_t i = 1; i < n; ++i) {
            E x = a[i]; size_t j = i;
            while (j > 0 && a[j - 1].ts > x.ts) { a[j] = a[j - 1]; --j; }
            a[j] = x;
        }
        return;
    }
    if (n < 65536) { std::sort(a, a + n, [](const E& x, const E& y) { return x.ts < y.ts; }); return; }
    tmp.resize(n);
    std::vector<uint32_t> cnt(65536);
    for (int pass = 0; pass < 2; ++pass) {
        const int sh = pass * 16;
        E* src = pass == 0 ? a : tmp.data(); E* dst = pass == 0 ? tmp.data() : a;
        std::fill(cnt.begin(), cnt.end(), 0);
        for (size_t i = 0; i < n; ++i) cnt[(src[i].ts >> sh) & 0xFFFF]++;
        uint32_t sum = 0; for (auto& c : cnt) { uint32_t v = c; c = sum; sum += v; }
        for (size_t i = 0; i < n; ++i) dst[cnt[(src[i].ts >> sh) & 0xFFFF]++] = src[i];
    }
}

// ---------------------------------- main -----------------------------------
int main() {
    try {
        init_pack();
        const char* d = std::getenv("DATA_DIR"); const char* o = std::getenv("OUTPUT_DIR");
        fs::path data = d ? d : "/data", output = o ? o : "/output";
        unsigned T = 8;
        if (const char* n = std::getenv("NCPU")) T = static_cast<unsigned>(std::max(1, std::atoi(n)));
        T = std::min(T, 64u);

        // ---- manifest ----
        int64_t ws, we;
        {
            std::ifstream in(data / "manifest.json");
            if (!in) throw std::runtime_error("cannot read manifest.json");
            std::string s((std::istreambuf_iterator<char>(in)), {});
            auto get = [&](const char* key) {
                size_t p = s.find(key); if (p == std::string::npos) throw std::runtime_error("manifest missing key");
                p = s.find(':', p + std::strlen(key)); p = s.find('"', p + 1); size_t q = s.find('"', p + 1);
                if (p == std::string::npos || q == std::string::npos) throw std::runtime_error("bad manifest");
                return s.substr(p + 1, q - p - 1);
            };
            std::string a = get("window_start"), b = get("window_end");
            if (!parse_f1(a, ws) || !parse_f1(b, we) || we <= ws) throw std::runtime_error("invalid manifest window");
        }
        const int64_t cws = ws - CLICK_WINDOW_MS;              // all times are stored relative to this
        if (cws < 0 || we - cws >= (int64_t(1) << 31)) throw std::runtime_error("unsupported window");
        const int64_t firstDay = ws / DAY_MS;
        const uint32_t dayCount = static_cast<uint32_t>((we - 1) / DAY_MS - firstDay + 1);
        fs::create_directories(output / "user_daily");
        fs::create_directories(output / "campaign_daily");

        // ---- lookups ----
        IdLookup products, campaigns;
        std::vector<std::string> catNames;
        std::vector<std::string> campNames, channels;
        std::vector<uint32_t> catRank;
        {
            Phase ph("lookups");
            catNames.push_back("unknown");
            std::unordered_map<std::string, uint32_t> catMap{{"unknown", 0}};
            Mapping pm = map_file(data / "products.csv");
            std::vector<std::pair<sv, sv>> rows;
            for (sv c : make_chunks({pm}, size_t(1) << 40))
                scan_lines_fast<2>(c, [&](const std::array<sv, 2>& f) { if (!f[0].empty()) rows.emplace_back(f[0], f[1]); });
            products.build(rows.size());
            for (auto& [pid, cat] : rows) {
                auto it = catMap.find(std::string(cat));
                uint32_t ci;
                if (it == catMap.end()) {
                    ci = static_cast<uint32_t>(catNames.size());
                    if (ci > 255) throw std::runtime_error("too many categories");
                    catNames.emplace_back(cat); catMap.emplace(std::string(cat), ci);
                } else ci = it->second;
                products.insert(pid, ci);
            }
            std::vector<uint32_t> order(catNames.size());
            for (uint32_t i = 0; i < order.size(); ++i) order[i] = i;
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return catNames[a] < catNames[b]; });
            catRank.resize(catNames.size());
            for (uint32_t r = 0; r < order.size(); ++r) catRank[order[r]] = r;

            Mapping cm = map_file(data / "campaigns.csv");
            std::vector<std::pair<sv, sv>> crow;
            for (sv c : make_chunks({cm}, size_t(1) << 40))
                scan_lines_fast<2>(c, [&](const std::array<sv, 2>& f) { if (!f[0].empty()) crow.emplace_back(f[0], f[1]); });
            campaigns.build(crow.size());
            for (auto& [cid, ch] : crow)
                if (campaigns.insert(cid, static_cast<uint32_t>(campNames.size()))) { campNames.emplace_back(cid); channels.emplace_back(ch); }
        }
        const uint32_t C = static_cast<uint32_t>(campNames.size());
        const uint32_t ORGANIC = C;

        // ---- links: ID table (key -> person key) via lock-free union-find ----
        std::vector<KeyMap> idParts(NID);
        auto resolve_h = [&](uint64_t k, uint64_t h) -> uint64_t {
            const KeyMap::E* e = idParts[part_of(h, ID_BITS)].find(k, h);
            return e ? e->v : k;
        };
        auto id_prefetch = [&](uint64_t h) { idParts[part_of(h, ID_BITS)].prefetch(h); };
        // The link step runs on a few threads of its own while events are read: events keep their
        // raw user key until dedup, which is when the ID table is first needed.
        auto run_links = [&](const unsigned TL) {
        {
            Phase ph("links");
            auto maps = map_csv_dir(data / "links");
            auto chunks = make_chunks(maps, chunk_bytes(1u << 20));
            std::vector<std::vector<std::pair<uint64_t, uint64_t>>> pairs(TL);
            Grid<uint64_t> ids(TL, std::vector<std::vector<uint64_t>>(NID));
            std::atomic<size_t> next{0};
            run_threads(TL, [&](unsigned t) {
                for (size_t i; (i = next.fetch_add(1)) < chunks.size();)
                    scan_lines_fast<2>(chunks[i], [&](const std::array<sv, 2>& f) {
                        if (f[0].empty() || f[1].empty()) return;
                        uint64_t a = key_of(f[0]), b = key_of(f[1]);
                        pairs[t].emplace_back(a, b);
                        ids[t][part_of(khash(a), ID_BITS)].push_back(a);
                        if (b != a) ids[t][part_of(khash(b), ID_BITS)].push_back(b);
                    });
            });
            // distinct IDs per partition -> local index; global index = base + local
            std::vector<uint32_t> base(NID + 1, 0), count(NID, 0);
            std::vector<std::vector<uint64_t>> localKeys(NID);
            std::atomic<uint32_t> np{0};
            run_threads(TL, [&](unsigned) {
                for (uint32_t p; (p = np.fetch_add(1)) < NID;) {
                    size_t n = 0; for (unsigned w = 0; w < TL; ++w) n += ids[w][p].size();
                    if (!n) continue;
                    KeyMap scratch; scratch.build(n);
                    for (unsigned w = 0; w < TL; ++w) {
                        for (uint64_t k : ids[w][p]) {
                            bool fresh; KeyMap::E* e = scratch.upsert(k, khash(k), fresh);
                            if (fresh) { e->v = localKeys[p].size(); localKeys[p].push_back(k); }
                        }
                        std::vector<uint64_t>().swap(ids[w][p]);
                    }
                    KeyMap& P = idParts[p];
                    P.build(localKeys[p].size());
                    for (size_t i = 0; i < localKeys[p].size(); ++i) {
                        bool fresh; KeyMap::E* e = P.upsert(localKeys[p][i], khash(localKeys[p][i]), fresh); e->v = i; // local index for now
                    }
                    count[p] = static_cast<uint32_t>(localKeys[p].size());
                }
            });
            for (uint32_t p = 0; p < NID; ++p) base[p + 1] = base[p] + count[p];
            const uint32_t G = base[NID];
            std::vector<uint64_t> gkey(G);
            for (uint32_t p = 0; p < NID; ++p) std::copy(localKeys[p].begin(), localKeys[p].end(), gkey.begin() + base[p]);
            auto gid = [&](uint64_t k) { uint64_t h = khash(k); uint32_t p = part_of(h, ID_BITS); return base[p] + static_cast<uint32_t>(idParts[p].find(k, h)->v); };
            std::vector<std::vector<std::pair<uint32_t, uint32_t>>> edges(TL);
            run_threads(TL, [&](unsigned t) {
                edges[t].reserve(pairs[t].size());
                for (const auto& [a, b] : pairs[t]) edges[t].emplace_back(gid(a), gid(b));
            });
            // Lock-free union-find: a root is only linked under a smaller index (CAS), so no
            // cycle can form; finds halve paths with CAS.
            std::unique_ptr<std::atomic<uint32_t>[]> parent(new std::atomic<uint32_t>[G]);
            std::unique_ptr<std::atomic<uint32_t>[]> best(new std::atomic<uint32_t>[G]);
            run_threads(TL, [&](unsigned t) {
                for (uint32_t i = t; i < G; i += TL) { parent[i].store(i, std::memory_order_relaxed); best[i].store(i, std::memory_order_relaxed); }
            });
            auto findr = [&](uint32_t x) {
                for (;;) {
                    uint32_t p = parent[x].load(std::memory_order_relaxed);
                    if (p == x) return x;
                    uint32_t g = parent[p].load(std::memory_order_relaxed);
                    if (g != p) parent[x].compare_exchange_weak(p, g, std::memory_order_relaxed);
                    x = g;
                }
            };
            run_threads(TL, [&](unsigned t) {
                for (const auto& [ga, gb] : edges[t]) {
                    uint32_t a = ga, b = gb;
                    for (;;) {
                        a = findr(a); b = findr(b);
                        if (a == b) break;
                        if (a < b) std::swap(a, b);
                        uint32_t expect = a;
                        if (parent[a].compare_exchange_strong(expect, b, std::memory_order_acq_rel)) break;
                    }
                }
            });
            // smallest ID (byte-wise) per component: CAS-min on the root's slot
            std::vector<uint32_t> root(G);
            run_threads(TL, [&](unsigned t) {
                for (uint32_t i = t; i < G; i += TL) {
                    uint32_t r = findr(i); root[i] = r;
                    if (r == i) continue;
                    uint32_t cur = best[r].load(std::memory_order_relaxed);
                    while (key_less(gkey[i], gkey[cur]) && !best[r].compare_exchange_weak(cur, i, std::memory_order_relaxed)) {}
                }
            });
            // ID table values become the person key
            std::atomic<uint32_t> np2{0};
            run_threads(TL, [&](unsigned) {
                for (uint32_t p; (p = np2.fetch_add(1)) < NID;)
                    for (KeyMap::E& e : idParts[p].t)
                        if (e.k != KeyMap::EMPTY) e.v = gkey[best[root[base[p] + e.v]].load(std::memory_order_relaxed)];
            });
            fprintf(stderr, "links: ids=%u\n", G);
        }
        };
        // Events go straight to their person's partition, so the ID table must be complete first.
        run_links(T);

        size_t eventBytes = 0, clickBytes = 0;
        std::vector<Mapping> eventMaps = map_csv_dir(data / "events"), clickMaps = map_csv_dir(data / "clicks");
        eventBytes = total_bytes(eventMaps); clickBytes = total_bytes(clickMaps);
        // ~70 bytes per generated event line, ~45 per click line; arenas fall back to malloc if short
        Arena pevArena, sideArena, pclArena;
        pevArena.init(eventBytes / 70 * (sizeof(PList::Blk) / PList::B) * 5 / 4 + size_t(T) * NP * sizeof(PList::Blk) + Arena::SLAB * 4 * T);
        sideArena.init(eventBytes / 70 * sizeof(Side) / 8 + Arena::SLAB * 4 * T);
        pclArena.init(clickBytes / 45 * sizeof(PCl) * 5 / 4 + Arena::SLAB * 4 * T);
        std::vector<std::vector<PList>> pev(T, std::vector<PList>(NP));
        BGrid<PCl, 1024> pcl(T, std::vector<BlockList<PCl, 1024>>(NP));
        constexpr int BATCH = 16; // rows whose hash-table lines are prefetched together

        // ---- events: one pass. Parse, validate, resolve the person and claim the event_id. ----
        // The global event_id table: key 0 = empty (no packed or interned key is 0), so the fresh
        // zero pages need no initialisation. ref is the claiming copy's row (valid) or the address
        // of its ingest_ts with bit 0 set (invalid). Copies that find the key taken go to `side`.
        struct ESlot { std::atomic<uint64_t> k; uint64_t ref; };
        static_assert(sizeof(ESlot) == 16);
        const uint64_t ecap = std::max<uint64_t>(1024, eventBytes / 45);
        const uint64_t elimit = ecap / 100 * 85;   // past this many claims, rows only go to `side`
        ESlot* etab;
        {
            void* m = ::mmap(nullptr, ecap * sizeof(ESlot), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
            if (m == MAP_FAILED) throw std::runtime_error("mmap failed: event_id table");
            ::madvise(m, ecap * sizeof(ESlot), MADV_HUGEPAGE);
            etab = static_cast<ESlot*>(m);
        }
        auto eslot = [&](uint64_t h) { return ((h & 0xFFFFFFFFu) * ecap) >> 32; };
        std::vector<RefList<Side, 1024>> side(T);
        std::vector<RefList<int64_t, 4096>> invIngest(T);
        std::vector<SlabAlloc> pevAl(T);
        for (unsigned t = 0; t < T; ++t) pevAl[t].a = &pevArena;
        {
            Phase ph("events_parse");
            auto chunks = make_chunks(eventMaps, chunk_bytes(4u << 20));
            std::atomic<size_t> next{0};
            std::atomic<uint64_t> claims{0};
            std::atomic<bool> full{false};
            run_threads(T, [&](unsigned t) {
                SlabAlloc& al = pevAl[t];
                SlabAlloc sal; sal.a = &sideArena;
                std::vector<PList>& mine = pev[t];
                struct Pd { uint64_t eid, eh, uk, uh; int64_t ingest; uint32_t ts, pc; bool valid; };
                Pd pend[BATCH]; int npend = 0; uint64_t localClaims = 0;
                auto drain = [&] {
                    for (int k = 0; k < npend; ++k) {
                        const Pd& x = pend[k];
                        const uint64_t person = x.valid ? resolve_h(x.uk, x.uh) : NO_KEY;
                        ESlot* slot = nullptr;
                        if (!full.load(std::memory_order_relaxed)) {
                            for (uint64_t q = eslot(x.eh);;) {
                                uint64_t cur = etab[q].k.load(std::memory_order_relaxed);
                                if (cur == 0) {
                                    if (etab[q].k.compare_exchange_strong(cur, x.eid, std::memory_order_relaxed)) { slot = &etab[q]; break; }
                                }
                                if (cur == x.eid) break; // another copy holds the key
                                if (++q == ecap) q = 0;
                            }
                        }
                        if (slot) {
                            if (x.valid) slot->ref = mine[part_of(khash(person), NP_BITS)].push(PEv{person, x.ts, x.pc}, x.ingest, al);
                            else slot->ref = reinterpret_cast<uintptr_t>(invIngest[t].push(x.ingest, sal)) | 1;
                            if (++localClaims == 1024) {
                                if (claims.fetch_add(localClaims, std::memory_order_relaxed) + localClaims > elimit) full.store(true, std::memory_order_relaxed);
                                localClaims = 0;
                            }
                        } else {
                            side[t].push(Side{x.eid, person, x.ingest, x.ts, x.pc}, sal);
                        }
                    }
                    npend = 0;
                };
                for (size_t i; (i = next.fetch_add(1)) < chunks.size();) {
                    scan_lines_fast<7>(chunks[i], [&](const std::array<sv, 7>& f) {
                        if (f[0].empty()) return;
                        int64_t ingest;
                        if (!parse_f1(f[3], ingest)) return; // ingest_ts is always valid F1
                        Pd& x = pend[npend];
                        x.eid = key_of(f[0]); x.eh = khash(x.eid); x.ingest = ingest; x.ts = 0; x.pc = 0;
                        __builtin_prefetch(&etab[eslot(x.eh)]);
                        int64_t ts; bool valid = false; uint32_t price = 0;
                        do {
                            if (f[1].empty() || f[5].empty()) break;
                            if (!parse_timestamp(f[2], ts) || ts < ws || ts >= we) break;
                            sv ty = f[4];
                            if (ty == "purchase") {
                                uint64_t p;
                                if (!parse_u64_ascii(f[6], p) || p < 1 || p > 10'000'000) break;
                                price = static_cast<uint32_t>(p);
                            } else if (ty != "view" && ty != "cart") break;
                            if (ingest - ts > LATE_MS) break;
                            valid = true;
                        } while (false);
                        x.valid = valid;
                        if (valid) {
                            x.uk = key_of(f[1]); x.uh = khash(x.uk); id_prefetch(x.uh);
                            x.ts = static_cast<uint32_t>(ts - cws);
                            if (price) { int64_t c = products.find(f[5]); x.pc = price | static_cast<uint32_t>(c < 0 ? 0 : c) << 24; } // 0 = unknown
                        }
                        if (++npend == BATCH) drain();
                    });
                }
                drain();
            });
        }
        // ---- fix-up: event_ids with more than one copy (and rows that arrived after the table filled) ----
        {
            Phase ph("events_fixup");
            constexpr int FB = 8; constexpr uint32_t NFB = 1u << FB;
            Grid<const Side*> fb(T, std::vector<std::vector<const Side*>>(NFB));
            run_threads(T, [&](unsigned t) { side[t].each([&](const Side& x) { fb[t][part_of(khash(x.eid), FB)].push_back(&x); }); });
            std::atomic<uint32_t> nb{0};
            run_threads(T, [&](unsigned t) {
                struct G { uint64_t eid; const Side* best; };
                std::vector<G> g;
                for (uint32_t b; (b = nb.fetch_add(1)) < NFB;) {
                    size_t n = 0; for (unsigned w = 0; w < T; ++w) n += fb[w][b].size();
                    if (!n) continue;
                    size_t cap = 16; while (cap < n * 2) cap <<= 1;
                    g.assign(cap, G{0, nullptr});
                    const uint64_t mask = cap - 1;
                    for (unsigned w = 0; w < T; ++w)
                        for (const Side* x : fb[w][b])
                            for (uint64_t q = khash(x->eid) & mask;; q = (q + 1) & mask) {
                                if (!g[q].best) { g[q] = G{x->eid, x}; break; }
                                if (g[q].eid == x->eid) { if (x->ingest < g[q].best->ingest) g[q].best = x; break; } // earliest ingest_ts
                            }
                    for (const G& e : g) {
                        if (!e.best) continue;
                        uint64_t ref = 0; bool claimed = false;
                        for (uint64_t q = eslot(khash(e.eid));;) {
                            const uint64_t k = etab[q].k.load(std::memory_order_relaxed);
                            if (k == e.eid) { ref = etab[q].ref; claimed = true; break; }
                            if (k == 0) break;
                            if (++q == ecap) q = 0;
                        }
                        if (claimed) {
                            const int64_t first = (ref & 1) ? *reinterpret_cast<const int64_t*>(ref & ~uint64_t(1)) : PList::ingest_of(ref);
                            if (first <= e.best->ingest) continue;   // the claimed copy is the kept one (equal ingest_ts: identical copies)
                            if (!(ref & 1)) PList::row(ref).person = NO_KEY; // it loses: never counted
                        }
                        const Side& w = *e.best;                    // the kept copy decides
                        if (w.person != NO_KEY) pev[t][part_of(khash(w.person), NP_BITS)].push(PEv{w.person, w.ts, w.price_cat}, w.ingest, pevAl[t]);
                    }
                }
            });
        }

        // ---- clicks: parse + validate + resolve + partition by person ----
        {
            Phase ph("clicks");
            auto chunks = make_chunks(clickMaps, chunk_bytes(4u << 20));
            std::atomic<size_t> next{0};
            run_threads(T, [&](unsigned t) {
                Scatter<PCl, 8, 1024> sc; sc.init(&pcl[t], &pclArena);
                struct Pend { PCl k; uint64_t uh; };
                Pend pend[BATCH]; int npend = 0;
                auto drain = [&] {
                    for (int k = 0; k < npend; ++k) {
                        Pend& x = pend[k];
                        x.k.person = resolve_h(x.k.person, x.uh);
                        sc.push(part_of(khash(x.k.person), NP_BITS), x.k);
                    }
                    npend = 0;
                };
                for (size_t i; (i = next.fetch_add(1)) < chunks.size();) {
                    scan_lines_fast<4>(chunks[i], [&](const std::array<sv, 4>& f) {
                        if (f[0].empty() || f[1].empty()) return;
                        int64_t ts;
                        if (!parse_timestamp(f[3], ts) || ts < cws || ts >= we) return;
                        int64_t c = campaigns.find(f[2]);
                        if (c < 0) return;
                        Pend& x = pend[npend++];
                        x.k.person = key_of(f[1]); x.uh = khash(x.k.person);
                        x.k.cid = key_of(f[0]); x.k.ts = static_cast<uint32_t>(ts - cws); x.k.camp = static_cast<uint32_t>(c);
                        id_prefetch(x.uh);
                        if (npend == BATCH) drain();
                    });
                    drain();
                }
                sc.finish();
            });
        }

        // ---- people: sessionize, attribute, aggregate ----
        struct CampAgg { uint64_t sessions = 0, purchasing = 0, revenue = 0, assisted = 0; };
        const size_t stride = C + 1;
        std::vector<std::vector<CampAgg>> campAgg(T);
        std::vector<size_t> userRows(T, 0);
        auto user_columns = [] {
            return std::vector<Column>{{"user_id", P_BYTE_ARRAY, L_STRING, false, {}, {}}, {"day", P_INT32, L_DATE, false, {}, {}},
                                       {"sessions", P_INT64, L_NONE, false, {}, {}}, {"events", P_INT64, L_NONE, false, {}, {}},
                                       {"revenue_cents", P_INT64, L_NONE, false, {}, {}}, {"top_category", P_BYTE_ARRAY, L_STRING, true, {}, {}}};
        };
        {
            Phase ph("people");
            std::vector<std::pair<size_t, uint32_t>> order(NP);
            for (uint32_t p = 0; p < NP; ++p) {
                size_t n = 0; for (unsigned w = 0; w < T; ++w) n += pev[w][p].size() + pcl[w][p].size() / 4;
                order[p] = {n, p};
            }
            std::sort(order.begin(), order.end(), std::greater<>());
            std::atomic<uint32_t> np{0};
            run_threads(T, [&](unsigned t) {
                auto& agg = campAgg[t]; agg.assign(dayCount * stride, CampAgg{});
                std::vector<Column> cols = user_columns();
                size_t& rows = userRows[t];
                struct PSlot { uint64_t key; uint32_t pid; };
                std::vector<PSlot> slots;
                std::vector<uint64_t> persons;
                std::vector<uint32_t> evPid, clPid, evOff, clOff, fill;
                struct EK { uint32_t ts, price_cat; };
                struct CK { uint32_t ts, camp; uint64_t cid; };
                std::vector<EK> ev, evTmp; std::vector<CK> cl;
                std::vector<uint32_t> count(C, 0), pos(C, 0), active;
                std::vector<std::pair<uint8_t, uint64_t>> catSum;
                for (uint32_t oi; (oi = np.fetch_add(1)) < NP;) {
                    const uint32_t p = order[oi].second;
                    size_t ne = 0, nc = 0;
                    for (unsigned w = 0; w < T; ++w) { ne += pev[w][p].size(); nc += pcl[w][p].size(); }
                    if (!ne) continue;
                    size_t cap = 16; while (cap < ne * 2) cap <<= 1;
                    slots.assign(cap, PSlot{0, 0xFFFFFFFFu}); persons.clear();
                    const uint64_t mask = cap - 1;
                    auto pid_of = [&](uint64_t key, bool insert) -> uint32_t {
                        for (uint64_t q = khash(key) & mask;; q = (q + 1) & mask) {
                            PSlot& s = slots[q];
                            if (s.pid == 0xFFFFFFFFu) {
                                if (!insert) return 0xFFFFFFFFu;
                                s.key = key; s.pid = static_cast<uint32_t>(persons.size()); persons.push_back(key);
                                return s.pid;
                            }
                            if (s.key == key) return s.pid;
                        }
                    };
                    // counting sort events by person
                    evPid.clear(); evPid.reserve(ne);
                    for (unsigned w = 0; w < T; ++w) // a row marked NO_KEY lost dedup in the fix-up
                        pev[w][p].each([&](const PEv& e) { evPid.push_back(e.person == NO_KEY ? 0xFFFFFFFFu : pid_of(e.person, true)); });
                    const size_t P = persons.size();
                    evOff.assign(P + 1, 0);
                    for (uint32_t x : evPid) if (x != 0xFFFFFFFFu) evOff[x + 1]++;
                    for (size_t k = 0; k < P; ++k) evOff[k + 1] += evOff[k];
                    ev.resize(ne);
                    fill.assign(evOff.begin(), evOff.end() - 1);
                    {
                        size_t idx = 0;
                        for (unsigned w = 0; w < T; ++w)
                            pev[w][p].each([&](const PEv& e) { const uint32_t x = evPid[idx++]; if (x != 0xFFFFFFFFu) ev[fill[x]++] = EK{e.ts, e.price_cat}; });
                    }
                    // counting sort clicks by person (clicks of people without events are dropped)
                    clPid.clear(); clPid.reserve(nc);
                    for (unsigned w = 0; w < T; ++w) pcl[w][p].each([&](const PCl& c) { clPid.push_back(pid_of(c.person, false)); });
                    clOff.assign(P + 1, 0);
                    for (uint32_t x : clPid) if (x != 0xFFFFFFFFu) clOff[x + 1]++;
                    for (size_t k = 0; k < P; ++k) clOff[k + 1] += clOff[k];
                    cl.resize(clOff[P]);
                    fill.assign(clOff.begin(), clOff.end() - 1);
                    {
                        size_t idx = 0;
                        for (unsigned w = 0; w < T; ++w)
                            pcl[w][p].each([&](const PCl& c) {
                                uint32_t x = clPid[idx++];
                                if (x != 0xFFFFFFFFu) cl[fill[x]++] = CK{c.ts, c.camp, c.cid};
                            });
                    }
                    for (size_t pid = 0; pid < P; ++pid) {
                        const size_t eb = evOff[pid], ee = evOff[pid + 1];
                        const size_t cb = clOff[pid], ce = clOff[pid + 1];
                        if (ee - eb > 1) sort_by_ts(ev.data() + eb, ee - eb, evTmp);
                        if (ce - cb > 1) std::sort(cl.begin() + cb, cl.begin() + ce, [](const CK& a, const CK& b) {
                            if (a.ts != b.ts) return a.ts < b.ts;
                            return key_less(a.cid, b.cid);
                        });
                        const std::string name = key_str(persons[pid]);

                        size_t lo = cb, hi = cb, runStart = cb;
                        uint64_t udSessions = 0, udEvents = 0, udRevenue = 0; int64_t udDay = -1;
                        catSum.clear();
                        auto flush_day = [&]() {
                            if (udDay < 0) return;
                            cols[0].put_str(name); cols[1].put_i32(static_cast<int32_t>(firstDay + udDay));
                            cols[2].put_i64(static_cast<int64_t>(udSessions)); cols[3].put_i64(static_cast<int64_t>(udEvents));
                            cols[4].put_i64(static_cast<int64_t>(udRevenue));
                            if (catSum.empty()) cols[5].put_def(rows, false);
                            else {
                                size_t bi = 0;
                                for (size_t k = 1; k < catSum.size(); ++k)
                                    if (catSum[k].second > catSum[bi].second ||
                                        (catSum[k].second == catSum[bi].second && catRank[catSum[k].first] < catRank[catSum[bi].first])) bi = k;
                                cols[5].put_def(rows, true); cols[5].put_str(catNames[catSum[bi].first]);
                            }
                            ++rows; udSessions = udEvents = udRevenue = 0; catSum.clear();
                        };
                        for (size_t i = eb; i < ee;) {
                            const uint32_t S = ev[i].ts;          // session start
                            const int64_t day = (cws + static_cast<int64_t>(S)) / DAY_MS - firstDay;
                            if (day != udDay) { flush_day(); udDay = day; }
                            // qualifying clicks: S - 7d <= ts <= S, kept as the window [lo, hi)
                            const int64_t cutoff = static_cast<int64_t>(S) - CLICK_WINDOW_MS;
                            while (hi < ce && cl[hi].ts <= S) {
                                uint32_t cp = cl[hi].camp;
                                if (count[cp]++ == 0) { pos[cp] = static_cast<uint32_t>(active.size()); active.push_back(cp); }
                                if (hi == cb || cl[hi].ts != cl[hi - 1].ts) runStart = hi; // first (smallest id) of the latest ts
                                ++hi;
                            }
                            while (lo < hi && static_cast<int64_t>(cl[lo].ts) < cutoff) {
                                uint32_t cp = cl[lo].camp;
                                if (--count[cp] == 0) { uint32_t mv = active.back(); active[pos[cp]] = mv; pos[mv] = pos[cp]; active.pop_back(); }
                                ++lo;
                            }
                            const uint32_t credit = lo < hi ? cl[runStart].camp : ORGANIC;
                            CampAgg* dayAgg = &agg[static_cast<size_t>(day) * stride];
                            if (credit != ORGANIC) for (uint32_t cp : active) dayAgg[cp].assisted++;
                            uint64_t sRevenue = 0, sEvents = 0; bool sPurchase = false;
                            uint32_t prev = S;
                            for (; i < ee; ++i) {
                                const uint32_t ts = ev[i].ts;
                                if (ts - prev > SESSION_GAP_MS) break; // equal timestamps never split
                                prev = ts; ++sEvents;
                                const uint32_t pc = ev[i].price_cat;
                                if (pc) {
                                    const uint32_t price = pc & 0xFFFFFFu; const uint8_t c = static_cast<uint8_t>(pc >> 24);
                                    sPurchase = true; sRevenue += price;
                                    bool found = false;
                                    for (auto& cs : catSum) if (cs.first == c) { cs.second += price; found = true; break; }
                                    if (!found) catSum.emplace_back(c, price);
                                }
                            }
                            ++udSessions; udEvents += sEvents; udRevenue += sRevenue;
                            CampAgg& ca = dayAgg[credit];
                            ca.sessions++; if (sPurchase) ca.purchasing++; ca.revenue += sRevenue;
                        }
                        flush_day();
                        for (uint32_t cp : active) count[cp] = 0;
                        active.clear();
                    }
                }
                if (rows) write_parquet(output / "user_daily" / ("part-" + std::to_string(t) + ".parquet"), cols, rows);
            });
        }
        {
            Phase ph("campaign_output");
            size_t anyUser = 0; for (size_t r : userRows) anyUser += r;
            if (!anyUser) { auto cols = user_columns(); write_parquet(output / "user_daily" / "part-0.parquet", cols, 0); }
            std::vector<Column> cols{{"campaign_id", P_BYTE_ARRAY, L_STRING, false, {}, {}}, {"channel", P_BYTE_ARRAY, L_STRING, false, {}, {}},
                                     {"day", P_INT32, L_DATE, false, {}, {}}, {"sessions", P_INT64, L_NONE, false, {}, {}},
                                     {"purchasing_sessions", P_INT64, L_NONE, false, {}, {}}, {"revenue_cents", P_INT64, L_NONE, false, {}, {}},
                                     {"assisted_sessions", P_INT64, L_NONE, false, {}, {}}};
            size_t rows = 0;
            for (uint32_t day = 0; day < dayCount; ++day)
                for (uint32_t c = 0; c <= C; ++c) {
                    CampAgg a;
                    for (unsigned t = 0; t < T; ++t) {
                        const CampAgg& x = campAgg[t][static_cast<size_t>(day) * stride + c];
                        a.sessions += x.sessions; a.purchasing += x.purchasing; a.revenue += x.revenue; a.assisted += x.assisted;
                    }
                    if (a.sessions == 0 && a.assisted == 0) continue;
                    cols[0].put_str(c == ORGANIC ? sv("organic") : sv(campNames[c]));
                    cols[1].put_str(c == ORGANIC ? sv("organic") : sv(channels[c]));
                    cols[2].put_i32(static_cast<int32_t>(firstDay + day));
                    cols[3].put_i64(static_cast<int64_t>(a.sessions)); cols[4].put_i64(static_cast<int64_t>(a.purchasing));
                    cols[5].put_i64(static_cast<int64_t>(a.revenue)); cols[6].put_i64(static_cast<int64_t>(a.assisted));
                    ++rows;
                }
            write_parquet(output / "campaign_daily" / "part-0.parquet", cols, rows);
        }
        fprintf(stderr, "timing phase=total elapsed_ms=%.1f\n",
                std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - T0).count());
        fflush(stderr);
        _exit(0); // skip freeing gigabytes of buffers; every output file is already closed
    } catch (const std::exception& e) {
        fprintf(stderr, "pipeline error: %s\n", e.what());
        return 1;
    }
}
