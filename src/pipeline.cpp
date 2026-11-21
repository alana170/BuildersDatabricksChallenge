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
#include <memory>
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
// The dialect: only '\n' ends a line; a field is either quote-free, or wrapped in
// exactly one pair of quotes with no quote inside. Anything else is malformed, as is
// a line with the wrong number of fields (an empty line included).
template <size_t N, class Fn>
static void scan_lines(sv chunk, Fn&& fn) {
    const char* base = chunk.data();
    const char* end = base + chunk.size();
    std::array<sv, N> f;
    size_t fi = 0; const char* fs_ = base; const char* ls = base; int q = 0; const char* q0 = nullptr; const char* q1 = nullptr; bool bad = false;
    auto end_field = [&](const char* fe) {
        if (fi >= N) { bad = true; return; }
        if (q == 0) f[fi] = sv(fs_, static_cast<size_t>(fe - fs_));
        else if (q == 2 && q0 == fs_ && q1 == fe - 1) f[fi] = sv(fs_ + 1, static_cast<size_t>(fe - fs_ - 2));
        else { bad = true; return; }
        ++fi;
    };
    auto on_char = [&](const char* c) {
        char ch = *c;
        if (ch == ',') { if (!bad) end_field(c); fs_ = c + 1; q = 0; }
        else if (ch == '"') { if (q == 0) q0 = c; else if (q == 1) q1 = c; ++q; }
        else { // '\n'
            if (!bad) { end_field(c); if (!bad && fi == N) fn(f); }
            fi = 0; fs_ = ls = c + 1; q = 0; bad = false;
        }
    };
    const char* b = base;
#ifdef __AVX2__
    const __m256i vc = _mm256_set1_epi8(','), vq = _mm256_set1_epi8('"'), vn = _mm256_set1_epi8('\n');
    for (; b + 64 <= end; b += 64) {
        __m256i x0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b));
        __m256i x1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + 32));
        uint32_t m0 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_or_si256(_mm256_or_si256(
            _mm256_cmpeq_epi8(x0, vc), _mm256_cmpeq_epi8(x0, vq)), _mm256_cmpeq_epi8(x0, vn))));
        uint32_t m1 = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_or_si256(_mm256_or_si256(
            _mm256_cmpeq_epi8(x1, vc), _mm256_cmpeq_epi8(x1, vq)), _mm256_cmpeq_epi8(x1, vn))));
        uint64_t m = (static_cast<uint64_t>(m1) << 32) | m0;
        while (m) { on_char(b + __builtin_ctzll(m)); m &= m - 1; }
    }
#endif
    for (; b < end; ++b) { char ch = *b; if (ch == ',' || ch == '"' || ch == '\n') on_char(b); }
    if (ls < end) { // final line without '\n' (its last field may be empty)
        if (!bad) { end_field(end); if (!bad && fi == N) fn(f); }
    }
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
static bool parse_f1_or_f2(sv s, int64_t& out) {
    if (s.size() != 20 && s.size() != 25) return false;
    const char* p = s.data();
    if (p[4] != '-' || p[7] != '-' || p[10] != 'T' || p[13] != ':' || p[16] != ':') return false;
    if (!(dig(p[0]) && dig(p[1]) && dig(p[2]) && dig(p[3]) && dig(p[5]) && dig(p[6]) && dig(p[8]) && dig(p[9]) &&
          dig(p[11]) && dig(p[12]) && dig(p[14]) && dig(p[15]) && dig(p[17]) && dig(p[18]))) return false;
    int y = d2(p) * 100 + d2(p + 2), mo = d2(p + 5), d = d2(p + 8), hh = d2(p + 11), mm = d2(p + 14), ss = d2(p + 17);
    if (mo < 1 || mo > 12 || d < 1 || d > month_days(y, mo) || hh > 23 || mm > 59 || ss > 59) return false;
    int off = 0;
    if (s.size() == 20) { if (p[19] != 'Z') return false; }
    else {
        if ((p[19] != '+' && p[19] != '-') || p[22] != ':') return false;
        if (!(dig(p[20]) && dig(p[21]) && dig(p[23]) && dig(p[24]))) return false;
        int oh = d2(p + 20), om = d2(p + 23);
        if (oh > 23 || om > 59) return false;
        off = oh * 60 + om; if (p[19] == '-') off = -off;
    }
    int64_t sec = days_from_civil(y, mo, d) * 86400 + hh * 3600 + mm * 60 + ss - int64_t(off) * 60;
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

// ------------------------------- names & maps -------------------------------
// A Name carries its bytes inline when they fit in 16 bytes (every ID in the generated
// data does), so equality tests and output never go back to the mapped CSV bytes.
// Comparisons are always exact: hash, then length, then bytes.
struct Name {
    uint64_t h; uint32_t len; uint32_t aux;
    union { char inl[16]; const char* ptr; } u;
    inline const char* data() const { return len <= 16 ? u.inl : u.ptr; }
    inline sv view() const { return sv(data(), len); }
    inline bool eq(const Name& o) const { return h == o.h && len == o.len && std::memcmp(data(), o.data(), len) == 0; }
    inline bool eq(sv s, uint64_t hh) const { return h == hh && len == s.size() && std::memcmp(data(), s.data(), len) == 0; }
    static inline Name make(sv s, uint64_t hh) {
        Name n; n.h = hh; n.len = static_cast<uint32_t>(s.size()); n.aux = 0;
        if (n.len <= 16) { std::memset(n.u.inl, 0, 16); std::memcpy(n.u.inl, s.data(), n.len); } else n.u.ptr = s.data();
        return n;
    }
};
static_assert(sizeof(Name) == 32);
static constexpr uint32_t EMPTY_LEN = 0xFFFFFFFFu;

struct StrMap { // read-only after build; value kept in key.aux
    std::vector<Name> tab; uint64_t mask = 0;
    void build(size_t expected) {
        size_t cap = 16; while (cap < expected * 2 + 16) cap <<= 1;
        Name e{}; e.len = EMPTY_LEN; tab.assign(cap, e); mask = cap - 1;
    }
    bool insert(sv k, uint32_t v) { // keeps the first value for a key
        uint64_t h = hash_sv(k);
        for (uint64_t p = h & mask;; p = (p + 1) & mask) {
            Name& e = tab[p];
            if (e.len == EMPTY_LEN) { e = Name::make(k, h); e.aux = v; return true; }
            if (e.eq(k, h)) return false;
        }
    }
    inline void prefetch(uint64_t h) const { __builtin_prefetch(&tab[h & mask]); }
    inline int64_t find(sv k, uint64_t h) const {
        for (uint64_t p = h & mask;; p = (p + 1) & mask) {
            const Name& e = tab[p];
            if (e.len == EMPTY_LEN) return -1;
            if (e.eq(k, h)) return e.aux;
        }
    }
    inline int64_t find(sv k) const { return find(k, hash_sv(k)); }
};

// Linked-ID table: one 64-byte line per ID holding the ID and its person's name.
struct alignas(64) IdEntry { Name key; Name canon; };
struct IdTable {
    // Capacity is not a power of two: slot = (low 32 hash bits * cap) >> 32, so the table
    // can be sized to ~60% load from the distinct-ID count.
    struct Part { std::vector<IdEntry> tab; uint64_t cap = 0; uint32_t base = 0, count = 0; };
    std::vector<Part> parts = std::vector<Part>(NID);
    static inline uint32_t part_of(uint64_t h) { return static_cast<uint32_t>(h >> (64 - ID_BITS)); }
    static inline uint64_t slot_of(uint64_t h, uint64_t cap) { return ((h & 0xFFFFFFFFu) * cap) >> 32; }
    inline void prefetch(uint64_t h) const {
        const Part& P = parts[part_of(h)];
        if (P.cap) __builtin_prefetch(&P.tab[slot_of(h, P.cap)]);
    }
    inline const IdEntry* find(sv s, uint64_t h) const {
        const Part& P = parts[part_of(h)];
        if (!P.cap) return nullptr;
        for (uint64_t p = slot_of(h, P.cap);;) {
            const IdEntry& e = P.tab[p];
            if (e.key.len == EMPTY_LEN) return nullptr;
            if (e.key.eq(s, h)) return &e;
            if (++p == P.cap) p = 0;
        }
    }
    // Insert-or-find during the build; returns the entry and whether it was new.
    static IdEntry* upsert(std::vector<IdEntry>& tab, uint64_t cap, const Name& k, bool& fresh) {
        for (uint64_t p = slot_of(k.h, cap);;) {
            IdEntry& e = tab[p];
            if (e.key.len == EMPTY_LEN) { e.key = k; fresh = true; return &e; }
            if (e.key.eq(k)) { fresh = false; return &e; }
            if (++p == cap) p = 0;
        }
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
    template <class F> inline void each(F&& f) const { for (Blk* b = head; b; b = b->next) for (uint32_t i = 0; i < b->n; ++i) f(b->items[i]); }
    size_t size() const { return total; }
};

// ------------------------------- record types ------------------------------
struct alignas(64) ERec {   // one parsed event line (partition #1); person resolved for valid rows
    Name name;              // name.aux = ts relative to the click-window start
    const char* eid; int64_t ingest;
    uint32_t eidLen, eidHash, price; uint8_t type, cat, valid, pad;
};
struct PEv {                // one surviving event (partition #2)
    Name name;              // name.aux = ts
    uint32_t price; uint8_t type, cat; uint16_t pad;
};
struct PCl {                // one valid click (partition #2)
    Name name;              // name.aux = ts
    const char* cid; uint32_t cidLen, camp;
};
template <class T> using Grid = std::vector<std::vector<std::vector<T>>>; // [worker][partition]
template <class T, uint32_t B> using BGrid = std::vector<std::vector<BlockList<T, B>>>;
static inline uint32_t ppart(uint64_t nh) { return static_cast<uint32_t>((nh >> 20) & (NP - 1)); }

// ---------------------------------- main -----------------------------------
int main() {
    try {
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
        if (cws < 0 || we - cws >= (int64_t(1) << 32)) throw std::runtime_error("unsupported window");
        const int64_t firstDay = ws / DAY_MS;
        const uint32_t dayCount = static_cast<uint32_t>((we - 1) / DAY_MS - firstDay + 1);
        fs::create_directories(output / "user_daily");
        fs::create_directories(output / "campaign_daily");

        // ---- lookups ----
        StrMap products, campaigns;
        std::vector<sv> catNames, campNames, channels;
        std::vector<uint32_t> catRank;
        const uint32_t unknownCat = 0;
        {
            Phase ph("lookups");
            catNames.push_back("unknown");
            StrMap catMap; catMap.build(64); catMap.insert("unknown", 0);
            Mapping pm = map_file(data / "products.csv");
            std::vector<std::pair<sv, sv>> rows;
            for (sv c : make_chunks({pm}, size_t(1) << 40))
                scan_lines<2>(c, [&](const std::array<sv, 2>& f) { if (!f[0].empty()) rows.emplace_back(f[0], f[1]); });
            products.build(rows.size());
            for (auto& [pid, cat] : rows) {
                int64_t ci = catMap.find(cat);
                if (ci < 0) {
                    ci = static_cast<int64_t>(catNames.size());
                    if (ci > 255) throw std::runtime_error("too many categories");
                    catNames.push_back(cat); catMap.insert(cat, static_cast<uint32_t>(ci));
                }
                products.insert(pid, static_cast<uint32_t>(ci));
            }
            std::vector<uint32_t> order(catNames.size());
            for (uint32_t i = 0; i < order.size(); ++i) order[i] = i;
            std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return catNames[a] < catNames[b]; });
            catRank.resize(catNames.size());
            for (uint32_t r = 0; r < order.size(); ++r) catRank[order[r]] = r;

            Mapping cm = map_file(data / "campaigns.csv");
            std::vector<std::pair<sv, sv>> crow;
            for (sv c : make_chunks({cm}, size_t(1) << 40))
                scan_lines<2>(c, [&](const std::array<sv, 2>& f) { if (!f[0].empty()) crow.emplace_back(f[0], f[1]); });
            campaigns.build(crow.size());
            for (auto& [cid, ch] : crow)
                if (campaigns.insert(cid, static_cast<uint32_t>(campNames.size()))) { campNames.push_back(cid); channels.push_back(ch); }
        }
        const uint32_t C = static_cast<uint32_t>(campNames.size());
        const uint32_t ORGANIC = C;

        // ---- links: ID table + union-find ----
        IdTable idt;
        {
            Phase ph("links");
            auto maps = map_csv_dir(data / "links");
            auto chunks = make_chunks(maps, chunk_bytes(1u << 20));
            struct Side { sv s; uint64_t h; };
            std::vector<std::vector<std::pair<Side, Side>>> pairs(T);
            Grid<Side> ids(T, std::vector<std::vector<Side>>(NID));
            std::atomic<size_t> next{0};
            auto lt0 = std::chrono::steady_clock::now();
            auto lap = [&](const char* what) {
                auto now = std::chrono::steady_clock::now();
                fprintf(stderr, "  links.%s %.1f ms\n", what, std::chrono::duration<double, std::milli>(now - lt0).count()); lt0 = now;
            };
            run_threads(T, [&](unsigned t) {
                for (size_t i; (i = next.fetch_add(1)) < chunks.size();)
                    scan_lines<2>(chunks[i], [&](const std::array<sv, 2>& f) {
                        if (f[0].empty() || f[1].empty()) return;
                        Side a{f[0], hash_sv(f[0])}, b{f[1], hash_sv(f[1])};
                        pairs[t].emplace_back(a, b);
                        ids[t][IdTable::part_of(a.h)].push_back(a);
                        if (b.h != a.h || b.s != a.s) ids[t][IdTable::part_of(b.h)].push_back(b);
                    });
            });
            lap("parse");
            // Build each ID partition independently; local index -> global id.
            std::vector<std::vector<IdEntry*>> byLocal(NID);
            std::atomic<uint32_t> np{0};
            run_threads(T, [&](unsigned) {
                for (uint32_t p; (p = np.fetch_add(1)) < NID;) {
                    size_t n = 0; for (unsigned w = 0; w < T; ++w) n += ids[w][p].size();
                    if (!n) continue;
                    auto& P = idt.parts[p];
                    IdEntry empty{}; empty.key.len = EMPTY_LEN;
                    // pass 1: dedup the sides (n >= distinct IDs) in a roomy scratch table
                    std::vector<IdEntry> scratch(n + n / 2 + 16, empty);
                    for (unsigned w = 0; w < T; ++w) {
                        for (const Side& sd : ids[w][p]) {
                            bool fresh; IdEntry* e = IdTable::upsert(scratch, scratch.size(), Name::make(sd.s, sd.h), fresh);
                            if (fresh) e->key.aux = P.count++;
                        }
                        std::vector<Side>().swap(ids[w][p]);
                    }
                    // pass 2: the final table at ~60% load
                    P.cap = P.count + P.count * 2 / 3 + 8;
                    P.tab.assign(P.cap, empty);
                    byLocal[p].resize(P.count);
                    for (const IdEntry& e : scratch)
                        if (e.key.len != EMPTY_LEN) { bool fresh; byLocal[p][e.key.aux] = IdTable::upsert(P.tab, P.cap, e.key, fresh); }
                }
            });
            lap("tables");
            uint32_t G = 0;
            for (auto& P : idt.parts) { P.base = G; G += P.count; }
            std::vector<IdEntry*> gent(G);
            for (uint32_t p = 0; p < NID; ++p) for (size_t i = 0; i < byLocal[p].size(); ++i) gent[idt.parts[p].base + i] = byLocal[p][i];
            auto gid = [&](const Side& s) { return idt.parts[IdTable::part_of(s.h)].base + idt.find(s.s, s.h)->key.aux; };
            // resolve both sides of every link in parallel; the union loop then touches integers only
            std::vector<std::vector<std::pair<uint32_t, uint32_t>>> edges(T);
            run_threads(T, [&](unsigned t) {
                edges[t].reserve(pairs[t].size());
                for (const auto& [sa, sb] : pairs[t]) edges[t].emplace_back(gid(sa), gid(sb));
            });
            // Lock-free union-find: a root only ever gets linked under a smaller index, by CAS,
            // so no cycle can form; finds compress paths by halving with CAS.
            std::unique_ptr<std::atomic<uint32_t>[]> parent(new std::atomic<uint32_t>[G]);
            std::unique_ptr<std::atomic<uint32_t>[]> best(new std::atomic<uint32_t>[G]);
            run_threads(T, [&](unsigned t) {
                for (uint32_t i = t; i < G; i += T) { parent[i].store(i, std::memory_order_relaxed); best[i].store(i, std::memory_order_relaxed); }
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
            run_threads(T, [&](unsigned t) {
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
            lap("union");
            // smallest name per component: CAS-min on the root's slot (byte-wise comparison)
            std::vector<uint32_t> root(G);
            run_threads(T, [&](unsigned t) {
                for (uint32_t i = t; i < G; i += T) {
                    uint32_t r = findr(i); root[i] = r;
                    if (r == i) continue;
                    sv mine = gent[i]->key.view();
                    uint32_t cur = best[r].load(std::memory_order_relaxed);
                    while (mine < gent[cur]->key.view() && !best[r].compare_exchange_weak(cur, i, std::memory_order_relaxed)) {}
                }
            });
            run_threads(T, [&](unsigned t) {
                for (uint32_t i = t; i < G; i += T) { gent[i]->canon = gent[best[root[i]].load(std::memory_order_relaxed)]->key; gent[i]->canon.aux = 0; }
            });
            lap("canon");
            fprintf(stderr, "links: ids=%u\n", G);
        }
        // user_id -> person name (the smallest ID of the component, or the ID itself)
        auto resolve = [&](sv user, uint64_t h) -> Name {
            const IdEntry* e = idt.find(user, h);
            return e ? e->canon : Name::make(user, h);
        };

        size_t eventBytes = 0, clickBytes = 0;
        std::vector<Mapping> eventMaps = map_csv_dir(data / "events"), clickMaps = map_csv_dir(data / "clicks");
        eventBytes = total_bytes(eventMaps); clickBytes = total_bytes(clickMaps);
        // ~85 bytes per generated event line, ~55 per click line; arenas fall back to malloc if short
        Arena erecArena, pevArena, pclArena;
        erecArena.init(eventBytes / 85 * sizeof(ERec) * 5 / 4 + Arena::SLAB * 4 * T);
        pevArena.init(eventBytes / 85 * sizeof(PEv) * 5 / 4 + Arena::SLAB * 4 * T);
        pclArena.init(clickBytes / 55 * sizeof(PCl) * 5 / 4 + Arena::SLAB * 4 * T);
        BGrid<PEv, 1024> pev(T, std::vector<BlockList<PEv, 1024>>(NP));
        BGrid<PCl, 512> pcl(T, std::vector<BlockList<PCl, 512>>(NP));
        constexpr int BATCH = 16; // rows whose hash-table lines are prefetched together

        // ---- events: parse + validate + partition by event_id ----
        {
            BGrid<ERec, 512> erec(T, std::vector<BlockList<ERec, 512>>(ND));
            {
                Phase ph("events_parse");
                auto chunks = make_chunks(eventMaps, chunk_bytes(4u << 20));
                std::atomic<size_t> next{0};
                run_threads(T, [&](unsigned t) {
                    auto& out = erec[t];
                    SlabAlloc al{&erecArena};
                    struct Pend { ERec r; sv user, prod; uint64_t ph; uint32_t part; };
                    Pend pend[BATCH]; int npend = 0;
                    auto drain = [&] {
                        for (int k = 0; k < npend; ++k) {
                            Pend& x = pend[k];
                            uint32_t ts = x.r.name.aux;
                            x.r.name = resolve(x.user, x.r.name.h); x.r.name.aux = ts;
                            int64_t c = products.find(x.prod, x.ph);
                            x.r.cat = static_cast<uint8_t>(c < 0 ? unknownCat : static_cast<uint32_t>(c));
                            out[x.part].push_stream(x.r, al);
                        }
                        npend = 0;
                    };
                    for (size_t i; (i = next.fetch_add(1)) < chunks.size();) {
                        scan_lines<7>(chunks[i], [&](const std::array<sv, 7>& f) {
                            if (f[0].empty()) return;
                            int64_t ingest;
                            if (!parse_f1(f[3], ingest)) return; // ingest_ts is always valid F1
                            uint64_t h = hash_sv(f[0]);
                            ERec r;
                            r.eid = f[0].data(); r.eidLen = static_cast<uint32_t>(f[0].size());
                            r.eidHash = static_cast<uint32_t>(h); r.ingest = ingest;
                            r.valid = 0; r.price = 0; r.type = 0; r.cat = 0; r.pad = 0;
                            const uint32_t part = static_cast<uint32_t>(h >> (64 - ND_BITS));
                            int64_t ts;
                            do {
                                if (f[1].empty() || f[5].empty()) break;
                                if (!parse_timestamp(f[2], ts) || ts < ws || ts >= we) break;
                                sv ty = f[4];
                                if (ty == "view") r.type = 0; else if (ty == "cart") r.type = 1; else if (ty == "purchase") r.type = 2; else break;
                                if (r.type == 2) {
                                    uint64_t p;
                                    if (!parse_u64_ascii(f[6], p) || p < 1 || p > 10'000'000) break;
                                    r.price = static_cast<uint32_t>(p);
                                }
                                if (ingest - ts > LATE_MS) break;
                                r.valid = 1;
                            } while (false);
                            if (!r.valid) { r.name.h = 0; r.name.len = 0; r.name.aux = 0; out[part].push_stream(r, al); return; }
                            Pend& x = pend[npend++];
                            x.r = r; x.user = f[1]; x.prod = f[5]; x.part = part;
                            x.r.name.h = hash_sv(f[1]); x.r.name.aux = static_cast<uint32_t>(ts - cws);
                            x.ph = hash_sv(f[5]);
                            idt.prefetch(x.r.name.h); products.prefetch(x.ph);
                            if (npend == BATCH) drain();
                        });
                        drain();
                    }
#ifdef __AVX2__
                    _mm_sfence();
#endif
                });
            }
            {
                Phase ph("events_dedup");
                std::atomic<uint32_t> np{0};
                run_threads(T, [&](unsigned t) {
                    std::vector<uint32_t> slots; std::vector<const ERec*> best;
                    auto& out = pev[t];
                    SlabAlloc al{&pevArena};
                    for (uint32_t p; (p = np.fetch_add(1)) < ND;) {
                        size_t n = 0; for (unsigned w = 0; w < T; ++w) n += erec[w][p].size();
                        size_t cap = 16; while (cap < n * 2) cap <<= 1;
                        slots.assign(cap, 0); best.clear();
                        const uint64_t mask = cap - 1;
                        for (unsigned w = 0; w < T; ++w)
                            erec[w][p].each([&](const ERec& r) {
                                for (uint64_t q = r.eidHash & mask;; q = (q + 1) & mask) {
                                    uint32_t sl = slots[q];
                                    if (!sl) { best.push_back(&r); slots[q] = static_cast<uint32_t>(best.size()); break; }
                                    const ERec* b = best[sl - 1];
                                    if (b->eidHash == r.eidHash && b->eidLen == r.eidLen && std::memcmp(b->eid, r.eid, r.eidLen) == 0) {
                                        if (r.ingest < b->ingest) best[sl - 1] = &r; // earliest ingest_ts wins
                                        break;
                                    }
                                }
                            });
                        for (const ERec* r : best) {
                            if (!r->valid) continue; // the kept copy decides: an invalid one drops the event
                            PEv e; e.name = r->name; e.price = r->price; e.type = r->type; e.cat = r->cat; e.pad = 0;
                            out[ppart(e.name.h)].push(e, al);
                        }
                    }
                });
            }
            erecArena.release();
        }

        // ---- clicks: parse + validate + resolve + partition by person ----
        {
            Phase ph("clicks");
            auto chunks = make_chunks(clickMaps, chunk_bytes(4u << 20));
            std::atomic<size_t> next{0};
            run_threads(T, [&](unsigned t) {
                auto& out = pcl[t];
                SlabAlloc al{&pclArena};
                struct Pend { PCl k; sv user; };
                Pend pend[BATCH]; int npend = 0;
                auto drain = [&] {
                    for (int k = 0; k < npend; ++k) {
                        Pend& x = pend[k];
                        uint32_t ts = x.k.name.aux;
                        x.k.name = resolve(x.user, x.k.name.h); x.k.name.aux = ts;
                        out[ppart(x.k.name.h)].push(x.k, al);
                    }
                    npend = 0;
                };
                for (size_t i; (i = next.fetch_add(1)) < chunks.size();) {
                    scan_lines<4>(chunks[i], [&](const std::array<sv, 4>& f) {
                        if (f[0].empty() || f[1].empty()) return;
                        int64_t ts;
                        if (!parse_timestamp(f[3], ts) || ts < cws || ts >= we) return;
                        int64_t c = campaigns.find(f[2]);
                        if (c < 0) return;
                        Pend& x = pend[npend++];
                        x.user = f[1];
                        x.k.name.h = hash_sv(f[1]); x.k.name.aux = static_cast<uint32_t>(ts - cws);
                        x.k.cid = f[0].data(); x.k.cidLen = static_cast<uint32_t>(f[0].size()); x.k.camp = static_cast<uint32_t>(c);
                        idt.prefetch(x.k.name.h);
                        if (npend == BATCH) drain();
                    });
                    drain();
                }
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
                std::vector<uint32_t> slots;       // name -> local person id
                std::vector<Name> persons;
                std::vector<uint32_t> evPid, clPid, evOff, clOff;
                struct EK { uint32_t ts, price; uint8_t type, cat; uint16_t pad; };
                struct CK { uint32_t ts, camp; const char* cid; uint32_t cidLen; };
                std::vector<EK> ev; std::vector<CK> cl;
                std::vector<uint32_t> count(C, 0), pos(C, 0), active;
                std::vector<std::pair<uint8_t, uint64_t>> catSum;
                for (uint32_t oi; (oi = np.fetch_add(1)) < NP;) {
                    const uint32_t p = order[oi].second;
                    size_t ne = 0, nc = 0;
                    for (unsigned w = 0; w < T; ++w) { ne += pev[w][p].size(); nc += pcl[w][p].size(); }
                    if (!ne) continue;
                    size_t cap = 16; while (cap < ne * 2) cap <<= 1;
                    slots.assign(cap, 0); persons.clear();
                    const uint64_t mask = cap - 1;
                    auto pid_of = [&](const Name& nm, bool insert) -> int64_t {
                        for (uint64_t q = (nm.h ^ (nm.h >> 29)) & mask;; q = (q + 1) & mask) {
                            uint32_t sl = slots[q];
                            if (!sl) {
                                if (!insert) return -1;
                                persons.push_back(nm); slots[q] = static_cast<uint32_t>(persons.size());
                                return static_cast<int64_t>(persons.size() - 1);
                            }
                            if (persons[sl - 1].eq(nm)) return sl - 1;
                        }
                    };
                    // counting sort events by person
                    evPid.clear(); evPid.reserve(ne);
                    for (unsigned w = 0; w < T; ++w) pev[w][p].each([&](const PEv& e) { evPid.push_back(static_cast<uint32_t>(pid_of(e.name, true))); });
                    const size_t P = persons.size();
                    evOff.assign(P + 1, 0);
                    for (uint32_t x : evPid) evOff[x + 1]++;
                    for (size_t k = 0; k < P; ++k) evOff[k + 1] += evOff[k];
                    ev.resize(ne);
                    {
                        std::vector<uint32_t> fill(evOff.begin(), evOff.end() - 1);
                        size_t idx = 0;
                        for (unsigned w = 0; w < T; ++w)
                            pev[w][p].each([&](const PEv& e) { ev[fill[evPid[idx++]]++] = EK{e.name.aux, e.price, e.type, e.cat, 0}; });
                    }
                    // counting sort clicks by person (clicks of people without events are dropped)
                    clPid.clear(); clPid.reserve(nc);
                    for (unsigned w = 0; w < T; ++w) pcl[w][p].each([&](const PCl& c) { clPid.push_back(static_cast<uint32_t>(pid_of(c.name, false))); });
                    clOff.assign(P + 1, 0);
                    for (uint32_t x : clPid) if (x != 0xFFFFFFFFu) clOff[x + 1]++;
                    for (size_t k = 0; k < P; ++k) clOff[k + 1] += clOff[k];
                    cl.resize(clOff[P]);
                    {
                        std::vector<uint32_t> fill(clOff.begin(), clOff.end() - 1);
                        size_t idx = 0;
                        for (unsigned w = 0; w < T; ++w)
                            pcl[w][p].each([&](const PCl& c) {
                                uint32_t x = clPid[idx++];
                                if (x != 0xFFFFFFFFu) cl[fill[x]++] = CK{c.name.aux, c.camp, c.cid, c.cidLen};
                            });
                    }
                    for (size_t pid = 0; pid < P; ++pid) {
                        const size_t eb = evOff[pid], ee = evOff[pid + 1];
                        const size_t cb = clOff[pid], ce = clOff[pid + 1];
                        if (ee - eb > 1) std::sort(ev.begin() + eb, ev.begin() + ee, [](const EK& a, const EK& b) { return a.ts < b.ts; });
                        if (ce - cb > 1) std::sort(cl.begin() + cb, cl.begin() + ce, [](const CK& a, const CK& b) {
                            if (a.ts != b.ts) return a.ts < b.ts;
                            int r = std::memcmp(a.cid, b.cid, std::min(a.cidLen, b.cidLen));
                            return r != 0 ? r < 0 : a.cidLen < b.cidLen;
                        });
                        const sv name = persons[pid].view();

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
                                if (ev[i].type == 2) {
                                    sPurchase = true; sRevenue += ev[i].price;
                                    uint8_t c = ev[i].cat; bool found = false;
                                    for (auto& cs : catSum) if (cs.first == c) { cs.second += ev[i].price; found = true; break; }
                                    if (!found) catSum.emplace_back(c, ev[i].price);
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
                    cols[0].put_str(c == ORGANIC ? sv("organic") : campNames[c]);
                    cols[1].put_str(c == ORGANIC ? sv("organic") : channels[c]);
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
