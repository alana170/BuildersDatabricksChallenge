#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <initializer_list>
#include <cstring>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;
using sv = std::string_view;
static constexpr int64_t DAY_MS = 86'400'000LL;
static constexpr int64_t CLICK_WINDOW_MS = 604'800'000LL;
static constexpr int64_t SESSION_GAP_MS = 1'800'000LL;
static constexpr int64_t LATE_MS = 86'400'000LL;
static constexpr uint32_t NO_USER = std::numeric_limits<uint32_t>::max();
static constexpr int MAX_CATEGORIES = 32; // generator has 20 categories plus "unknown"
static constexpr int BUCKETS = 64;

static void logmsg(const std::string& s) { std::cerr << s << '\n'; }

// ------------------------- byte hashing / string pool ----------------------
static inline uint64_t hash_bytes(sv s) noexcept {
    // libstdc++'s string_view hash uses its optimized byte hash (MurmurHash2
    // family), avoiding a per-byte FNV dependency chain on tens of millions
    // of event and click IDs. It is stable within this process, which is all
    // the in-memory hash tables require.
    return static_cast<uint64_t>(std::hash<sv>{}(s));
}
struct ViewHash {
    using is_transparent = void;
    size_t operator()(sv s) const noexcept { return static_cast<size_t>(hash_bytes(s)); }
    size_t operator()(const std::string& s) const noexcept { return (*this)(sv(s)); }
};
struct ViewEq {
    using is_transparent = void;
    bool operator()(sv a, sv b) const noexcept { return a == b; }
    bool operator()(const std::string& a, sv b) const noexcept { return sv(a) == b; }
    bool operator()(sv a, const std::string& b) const noexcept { return a == sv(b); }
    bool operator()(const std::string& a, const std::string& b) const noexcept { return a == b; }
};

struct UserPool {
    std::deque<std::string> names; // references remain stable across insertions
    std::unordered_map<sv, uint32_t, ViewHash, ViewEq> ids;
    std::vector<uint32_t> parent, sz, min_id;

    uint32_t intern(sv s) {
        auto it = ids.find(s);
        if (it != ids.end()) return it->second;
        if (names.size() >= static_cast<size_t>(NO_USER)) throw std::runtime_error("too many user IDs");
        uint32_t id = static_cast<uint32_t>(names.size());
        names.emplace_back(s);
        sv stored(names.back());
        ids.emplace(stored, id);
        parent.push_back(id); sz.push_back(1); min_id.push_back(id);
        return id;
    }
    uint32_t find(uint32_t x) {
        while (parent[x] != x) {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }
    void unite(uint32_t a, uint32_t b) {
        a = find(a); b = find(b);
        if (a == b) return;
        if (sz[a] < sz[b]) std::swap(a, b);
        parent[b] = a;
        sz[a] += sz[b];
        const auto& na = names[min_id[a]];
        const auto& nb = names[min_id[b]];
        if (nb < na) min_id[a] = min_id[b];
    }
    std::vector<uint32_t> canonical_ids() {
        std::vector<uint32_t> out(names.size());
        for (uint32_t i = 0; i < names.size(); ++i) {
            uint32_t r = find(i);
            out[i] = min_id[r];
        }
        return out;
    }
};

// ------------------------------- CSV parser --------------------------------
// The challenge dialect has no commas, quotes, or newlines inside a value.
// A quote is valid only when it wraps the entire field. CR is deliberately kept.
template<size_t N>
static bool parse_csv_line(sv line, std::array<sv, N>& out) {
    if (line.empty()) return false;
    size_t start = 0;
    for (size_t i = 0; i < N; ++i) {
        size_t comma = line.find(',', start);
        size_t end = (comma == sv::npos) ? line.size() : comma;
        if (i + 1 < N) {
            if (comma == sv::npos) return false;
        } else if (comma != sv::npos) {
            return false;
        }
        sv field = line.substr(start, end - start);
        if (!field.empty() && field.front() == '"') {
            if (field.size() < 2 || field.back() != '"') return false;
            if (field.substr(1, field.size() - 2).find('"') != sv::npos) return false;
            out[i] = field.substr(1, field.size() - 2);
        } else {
            if (field.find('"') != sv::npos) return false;
            out[i] = field;
        }
        start = end + 1;
    }
    return start == line.size() + 1;
}

template<class Fn>
static void each_data_line(sv data, Fn&& fn) {
    // First line is always the header. Only LF terminates a line.
    size_t pos = data.find('\n');
    if (pos == sv::npos) return;
    ++pos;
    while (pos < data.size()) {
        size_t end = data.find('\n', pos);
        if (end == sv::npos) end = data.size();
        sv line = data.substr(pos, end - pos);
        fn(line);
        if (end == data.size()) break;
        pos = end + 1;
    }
}

template<class Fn>
static void each_chunk_line(sv data, Fn&& fn) {
    size_t pos=0;
    while (pos<data.size()) {
        size_t end=data.find('\n',pos);
        if (end==sv::npos) end=data.size();
        fn(data.substr(pos,end-pos));
        if (end==data.size()) break;
        pos=end+1;
    }
}

template<class Fn>
static void parallel_chunks(const std::vector<sv>& chunks, unsigned threads, Fn&& fn) {
    if (chunks.empty()) return;
    threads=std::max(1u,std::min<unsigned>(threads,static_cast<unsigned>(chunks.size())));
    std::atomic<size_t> next{0};
    std::exception_ptr failure;
    std::mutex failureMutex;
    std::vector<std::thread> pool;
    pool.reserve(threads);
    for (unsigned t=0;t<threads;++t) {
        pool.emplace_back([&]() {
            try {
                for (;;) {
                    size_t i=next.fetch_add(1,std::memory_order_relaxed);
                    if (i>=chunks.size()) break;
                    fn(chunks[i]);
                }
            } catch (...) {
                std::lock_guard<std::mutex> lock(failureMutex);
                if (!failure) failure=std::current_exception();
            }
        });
    }
    for (auto& thread:pool) thread.join();
    if (failure) std::rethrow_exception(failure);
}

struct Mapping {
    int fd = -1;
    const char* data = nullptr;
    size_t size = 0;
    fs::path path;
    Mapping() = default;
    explicit Mapping(const fs::path& p) : path(p) {
        fd = ::open(p.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("open failed: " + p.string());
        struct stat st{};
        if (::fstat(fd, &st) != 0) throw std::runtime_error("fstat failed: " + p.string());
        size = static_cast<size_t>(st.st_size);
        if (size) {
            void* m = ::mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
            if (m == MAP_FAILED) throw std::runtime_error("mmap failed: " + p.string());
            data = static_cast<const char*>(m);
        }
    }
    Mapping(const Mapping&) = delete;
    Mapping& operator=(const Mapping&) = delete;
    Mapping(Mapping&& o) noexcept : fd(o.fd), data(o.data), size(o.size), path(std::move(o.path)) {
        o.fd = -1; o.data = nullptr; o.size = 0;
    }
    Mapping& operator=(Mapping&& o) noexcept {
        if (this != &o) { reset(); fd=o.fd; data=o.data; size=o.size; path=std::move(o.path); o.fd=-1; o.data=nullptr; o.size=0; }
        return *this;
    }
    void reset() {
        if (data && size) ::munmap(const_cast<char*>(data), size);
        if (fd >= 0) ::close(fd);
        fd = -1; data = nullptr; size = 0;
    }
    ~Mapping() { reset(); }
    sv view() const { return sv(data ? data : "", size); }
};

static std::vector<Mapping> map_csv_dir(const fs::path& dir) {
    std::vector<fs::path> paths;
    if (fs::exists(dir)) {
        for (const auto& e : fs::directory_iterator(dir)) {
            if (e.is_regular_file() && e.path().extension() == ".csv") paths.push_back(e.path());
        }
    }
    std::sort(paths.begin(), paths.end());
    std::vector<Mapping> maps;
    maps.reserve(paths.size());
    for (const auto& p : paths) maps.emplace_back(p);
    return maps;
}
// Split mapped CSV files into line-aligned chunks. This lets independent
// workers parse the very large, highly uneven part files without copying
// input bytes or relying on one worker per file.
static std::vector<sv> make_line_chunks(const std::vector<Mapping>& maps, size_t targetBytes = 2u<<20) {
    std::vector<sv> chunks;
    for (const Mapping& m : maps) {
        sv data=m.view();
        size_t header=data.find('\n');
        if (header==sv::npos) continue;
        size_t pos=header+1;
        while (pos<data.size()) {
            size_t target=std::min(data.size(),pos+targetBytes);
            size_t end=target;
            if (target<data.size()) {
                size_t nl=data.find('\n',target);
                end=(nl==sv::npos)?data.size():nl+1;
            }
            chunks.emplace_back(data.substr(pos,end-pos));
            pos=end;
        }
    }
    return chunks;
}


static uint64_t count_data_lines(const std::vector<Mapping>& maps) {
    uint64_t count = 0;
    for (const auto& m : maps) {
        sv s = m.view();
        if (s.empty()) continue;
        uint64_t nl = 0;
        for (char c : s) nl += (c == '\n');
        uint64_t lines = nl + (s.back() != '\n');
        if (lines > 0) count += lines - 1; // skip header in each file
    }
    return count;
}

// ------------------------------- timestamps --------------------------------
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
    out = v;
    return true;
}
static bool leap_year(int y) { return (y % 4 == 0) && ((y % 100 != 0) || (y % 400 == 0)); }
static int month_days(int y, int m) {
    static constexpr int ds[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (m < 1 || m > 12) return 0;
    return ds[m] + (m == 2 && leap_year(y));
}
// Days since 1970-01-01, proleptic Gregorian calendar.
static int64_t days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = static_cast<unsigned>(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(doe) - 719468;
}
static void civil_from_days(int64_t z, int& y, unsigned& m, unsigned& d) {
    z += 719468;
    const int64_t era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = static_cast<unsigned>(z - era * 146097);
    const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    y = static_cast<int>(yoe) + static_cast<int>(era * 400);
    const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    const unsigned mp = (5*doy + 2)/153;
    d = doy - (153*mp+2)/5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
}
static int digit2(sv s, size_t p) { return (s[p]-'0')*10 + (s[p+1]-'0'); }
static int digit4(sv s, size_t p) { return (s[p]-'0')*1000 + (s[p+1]-'0')*100 + (s[p+2]-'0')*10 + (s[p+3]-'0'); }
static bool parse_f1_or_f2(sv s, int64_t& out) {
    if (s.size() != 20 && s.size() != 25) return false;
    if (s[4]!='-' || s[7]!='-' || s[10]!='T' || s[13]!=':' || s[16]!=':') return false;
    for (size_t p : {size_t(0),size_t(1),size_t(2),size_t(3),size_t(5),size_t(6),size_t(8),size_t(9),size_t(11),size_t(12),size_t(14),size_t(15),size_t(17),size_t(18)})
        if (s[p] < '0' || s[p] > '9') return false;
    int y = digit4(s,0), mo = digit2(s,5), d = digit2(s,8);
    int hh = digit2(s,11), mm = digit2(s,14), ss = digit2(s,17);
    if (mo < 1 || mo > 12 || d < 1 || d > month_days(y,mo) || hh > 23 || mm > 59 || ss > 59) return false;
    int offsetMinutes = 0;
    if (s.size() == 20) {
        if (s[19] != 'Z') return false;
    } else {
        if (s[19] != '+' && s[19] != '-') return false;
        if (s[22] != ':') return false;
        if (s[20]<'0' || s[20]>'9' || s[21]<'0' || s[21]>'9' || s[23]<'0' || s[23]>'9' || s[24]<'0' || s[24]>'9') return false;
        int oh = digit2(s,20), om = digit2(s,23);
        if (oh > 23 || om > 59) return false;
        offsetMinutes = oh * 60 + om;
        if (s[19] == '-') offsetMinutes = -offsetMinutes;
    }
    __int128 sec = static_cast<__int128>(days_from_civil(y,mo,d))*86400 + hh*3600 + mm*60 + ss;
    sec -= static_cast<int64_t>(offsetMinutes) * 60;
    __int128 ms = sec * 1000;
    if (ms < std::numeric_limits<int64_t>::min() || ms > std::numeric_limits<int64_t>::max()) return false;
    out = static_cast<int64_t>(ms);
    return true;
}
static bool parse_timestamp(sv s, int64_t& out) {
    if (s.size() >= 19 && s.size() >= 10 && s[4] == '-' && s[7] == '-')
        return parse_f1_or_f2(s,out);
    uint64_t n;
    if (!parse_u64_ascii(s,n)) return false;
    if (n > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) return false; // outside any valid window
    out = static_cast<int64_t>(n);
    return true;
}
static bool parse_f1(sv s, int64_t& out) {
    if (s.size() != 20 || s.back() != 'Z') return false;
    return parse_f1_or_f2(s,out);
}
static std::string date_string_from_day(int64_t day) {
    int y; unsigned m,d; civil_from_days(day,y,m,d);
    char buf[16];
    std::snprintf(buf,sizeof(buf),"%04d-%02u-%02u",y,m,d);
    return std::string(buf);
}

// ------------------------------- flat dedup --------------------------------
// A compact open-addressed table. Slots encode a 32-bit hash fingerprint and
// (row index + 1); the row retains a string_view into its memory-mapped input.
static size_t next_pow2(size_t n) {
    if (n <= 2) return 2;
    --n;
    for (size_t s=1; s < sizeof(size_t)*8; s <<= 1) n |= n >> s;
    return n + 1;
}
struct EventRaw {
    sv id;
    int64_t ts = std::numeric_limits<int64_t>::min();
    int64_t ingest = 0;
    sv userId;
    uint32_t price = 0;
    uint8_t type = 255;
    uint8_t category = 0;
    uint8_t product_ok = 0;
    uint8_t price_ok = 1;
};
struct ClickRaw {
    sv id;
    int64_t ts = std::numeric_limits<int64_t>::min();
    sv userId;
    uint32_t campaign = std::numeric_limits<uint32_t>::max();
    uint8_t valid = 0;
};
template<class Row>
struct FlatDedup {
    std::vector<uint64_t> slots;
    std::vector<Row> rows;
    size_t mask = 0;
    explicit FlatDedup(uint64_t maxRows) {
        long double need = static_cast<long double>(std::max<uint64_t>(maxRows,2)) / 0.68L;
        if (need > static_cast<long double>(std::numeric_limits<size_t>::max()/2)) throw std::runtime_error("dedup table too large");
        size_t cap = next_pow2(static_cast<size_t>(need));
        slots.assign(cap,0);
        mask = cap-1;
        rows.reserve(static_cast<size_t>(maxRows));
    }
    template<class Fill, class IsEarlier>
    void insert(sv key, uint64_t h, Fill&& fill, IsEarlier&& isEarlier) {
        uint32_t fp = static_cast<uint32_t>(h >> 32);
        size_t p = static_cast<size_t>(h) & mask;
        for (;;) {
            uint64_t slot = slots[p];
            if (slot == 0) {
                uint32_t idx = static_cast<uint32_t>(rows.size());
                if (idx == std::numeric_limits<uint32_t>::max()) throw std::runtime_error("too many unique IDs in dedup");
                Row r{}; r.id = key; fill(r);
                rows.push_back(std::move(r));
                slots[p] = (static_cast<uint64_t>(fp) << 32) | (static_cast<uint64_t>(idx) + 1);
                return;
            }
            uint32_t sfp = static_cast<uint32_t>(slot >> 32);
            uint32_t idx = static_cast<uint32_t>(slot) - 1;
            if (sfp == fp && rows[idx].id == key) {
                if (isEarlier(rows[idx])) {
                    sv oldid = rows[idx].id;
                    fill(rows[idx]);
                    rows[idx].id = oldid;
                }
                return;
            }
            p = (p + 1) & mask;
        }
    }
};

template<class Row>
struct ShardedDedup {
    struct Shard {
        std::mutex mutex;
        FlatDedup<Row> table;
        explicit Shard(uint64_t expected):table(expected) {}
    };
    std::array<std::unique_ptr<Shard>,BUCKETS> shards;
    explicit ShardedDedup(uint64_t maxRows) {
        uint64_t mean=(maxRows+BUCKETS-1)/BUCKETS;
        uint64_t expected=std::max<uint64_t>(8,mean+mean/6+64);
        for (auto& shard:shards) shard=std::make_unique<Shard>(expected);
    }
    template<class Fill, class IsEarlier>
    void insert(sv key, uint64_t h, Fill&& fill, IsEarlier&& isEarlier) {
        // Shard on the high hash bits; FlatDedup uses the low bits for its
        // initial slot, so the two choices remain independent.
        size_t shardIndex=static_cast<size_t>((h>>32)&(BUCKETS-1));
        Shard& shard=*shards[shardIndex];
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.table.insert(key,h,std::forward<Fill>(fill),std::forward<IsEarlier>(isEarlier));
    }
    std::vector<Row> collect() {
        size_t count=0;
        for (const auto& shard:shards) count+=shard->table.rows.size();
        std::vector<Row> result;
        result.reserve(count);
        for (auto& shard:shards) {
            auto& rows=shard->table.rows;
            for (Row& row:rows) result.push_back(std::move(row));
        }
        return result;
    }
};

// ------------------------------- lookup data -------------------------------
struct Lookups {
    std::unordered_map<std::string,uint8_t,ViewHash,ViewEq> productCategory;
    std::vector<std::string> categories;
    std::unordered_map<std::string,uint32_t,ViewHash,ViewEq> campaignIndex;
    std::vector<std::string> campaignNames, channels;
    uint8_t unknownCategory = 0;
    uint8_t add_category(const std::string& s) {
        auto it = std::find(categories.begin(),categories.end(),s);
        if (it != categories.end()) return static_cast<uint8_t>(it-categories.begin());
        if (categories.size() >= MAX_CATEGORIES) throw std::runtime_error("category count exceeds configured limit");
        categories.push_back(s);
        return static_cast<uint8_t>(categories.size()-1);
    }
};
static void read_lookups(const fs::path& data, Lookups& l) {
    l.unknownCategory = l.add_category("unknown");
    {
        Mapping m(data/"products.csv");
        each_data_line(m.view(), [&](sv line) {
            std::array<sv,2> f{};
            if (!parse_csv_line(line,f)) return;
            if (f[0].empty()) return;
            std::string product(f[0]);
            uint8_t cat = l.add_category(std::string(f[1]));
            l.productCategory.emplace(std::move(product),cat);
        });
    }
    {
        Mapping m(data/"campaigns.csv");
        each_data_line(m.view(), [&](sv line) {
            std::array<sv,2> f{};
            if (!parse_csv_line(line,f) || f[0].empty()) return;
            uint32_t id = static_cast<uint32_t>(l.campaignNames.size());
            std::string name(f[0]);
            l.campaignIndex.emplace(name,id);
            l.campaignNames.push_back(std::move(name));
            l.channels.emplace_back(f[1]);
        });
    }
}

static std::pair<int64_t,int64_t> read_window(const fs::path& path) {
    std::ifstream in(path);
    if (!in) throw std::runtime_error("cannot read manifest.json");
    std::string s((std::istreambuf_iterator<char>(in)),{});
    auto extract = [&](const char* key)->std::string {
        size_t p=s.find(key); if (p==std::string::npos) throw std::runtime_error("manifest missing key");
        p=s.find(':',p+std::strlen(key)); if(p==std::string::npos) throw std::runtime_error("bad manifest");
        p=s.find('"',p+1); if(p==std::string::npos) throw std::runtime_error("bad manifest");
        size_t q=s.find('"',p+1); if(q==std::string::npos) throw std::runtime_error("bad manifest");
        return s.substr(p+1,q-p-1);
    };
    int64_t a,b; auto as=extract("window_start"), bs=extract("window_end");
    if (!parse_f1(as,a) || !parse_f1(bs,b) || b<=a || (b-a)%DAY_MS!=0)
        throw std::runtime_error("invalid manifest window: ["+as+"] ["+bs+"]");
    return {a,b};
}

// ------------------------------- core records -------------------------------
struct Event {
    uint32_t person;
    int64_t ts;
    uint32_t price;
    uint8_t type;
    uint8_t category;
    uint16_t pad = 0;
};
struct Click {
    uint32_t person;
    int64_t ts;
    sv click_id;
    uint32_t campaign;
};
struct UserDayAgg {
    uint64_t sessions=0, events=0, revenue=0;
    std::array<uint64_t,MAX_CATEGORIES> byCategory{};
};
struct CampaignAgg {
    uint64_t sessions=0, purchasing=0, revenue=0, assisted=0;
};
struct SessionState {
    int64_t start=0;
    uint32_t day=0;
    uint32_t credit=0;
    uint64_t revenue=0;
    bool hasPurchase=false;
};

static unsigned get_threads() {
    const char* p = std::getenv("NCPU");
    unsigned n = p ? static_cast<unsigned>(std::max(1,std::atoi(p))) : std::thread::hardware_concurrency();
    if (!n) n=1;
    return std::min<unsigned>(n,BUCKETS);
}

// Convert date/time to a daily bucket and update rolling click state while
// processing a single person's sorted event timeline.
static void process_person(
    const std::vector<Event>& ev, size_t eb, size_t ee,
    const std::vector<Click>& cl, size_t cb, size_t ce,
    const UserPool& users, const Lookups& lookups,
    int64_t windowStart, uint32_t dayCount,
    std::vector<CampaignAgg>& campaignAgg,
    std::ofstream& userOut,
    std::vector<uint32_t>& seenGeneration,
    std::vector<uint32_t>& campaignCount,
    std::vector<int32_t>& campaignPosition,
    std::vector<uint32_t>& activeCampaigns,
    uint32_t generation,
    const std::vector<std::string>& dayStrings
) {
    if (dayCount > 32) throw std::runtime_error("window longer than 32 days");

    // Sessions and their start days are monotone in the sorted event stream.
    // We therefore need only one person/day aggregate, not a zeroed 32-day
    // matrix for every person. This also handles sessions crossing midnight:
    // all events are charged to the current session's start day.
    UserDayAgg userDay{};
    uint32_t userDayIndex = 0;
    bool haveUserDay = false;
    const std::string& personName = users.names[ev[eb].person];
    auto flush_user_day = [&]() {
        if (!haveUserDay || userDay.sessions == 0) return;
        int best=-1; uint64_t bestRevenue=0;
        for (size_t k=0;k<lookups.categories.size();++k) {
            uint64_t v=userDay.byCategory[k];
            if (v==0) continue;
            if (best<0 || v>bestRevenue ||
                (v==bestRevenue && lookups.categories[k] < lookups.categories[best])) {
                best=static_cast<int>(k); bestRevenue=v;
            }
        }
        userOut << personName << ',' << dayStrings[userDayIndex] << ','
                << userDay.sessions << ',' << userDay.events << ','
                << userDay.revenue << ',';
        if (best>=0) userOut << lookups.categories[best];
        userOut << '\n';
        userDay = UserDayAgg{};
        haveUserDay = false;
    };

    // The active clicks are a contiguous sliding interval in the person's
    // sorted click array: [activeBegin, nextClick). No per-person queue alloc.
    size_t activeBegin=cb, nextClick=cb;
    activeCampaigns.clear();
    int64_t latestTs=std::numeric_limits<int64_t>::min();
    uint32_t latestCampaign=static_cast<uint32_t>(lookups.campaignNames.size());

    SessionState session{};
    bool haveSession=false;
    int64_t previousTs=0;

    auto begin_session = [&](int64_t start) {
        session = SessionState{};
        session.start = start;
        session.day = static_cast<uint32_t>((start-windowStart)/DAY_MS);
        if (session.day >= dayCount) throw std::runtime_error("session day outside manifest window");

        // A new start day proves that the preceding day's sessions are all
        // complete, so emit it and reuse the same compact accumulator.
        if (haveUserDay && userDayIndex != session.day) flush_user_day();
        if (!haveUserDay) {
            userDayIndex = session.day;
            haveUserDay = true;
        }

        int64_t cutoff = start - CLICK_WINDOW_MS;
        // Expire clicks older than the inclusive seven-day boundary.
        while (activeBegin < nextClick && cl[activeBegin].ts < cutoff) {
            uint32_t campaign=cl[activeBegin].campaign;
            if (--campaignCount[campaign] == 0) {
                int32_t pos=campaignPosition[campaign];
                uint32_t moved=activeCampaigns.back();
                activeCampaigns[pos]=moved;
                campaignPosition[moved]=pos;
                activeCampaigns.pop_back();
                campaignPosition[campaign]=-1;
            }
            ++activeBegin;
        }
        if (activeBegin == nextClick) {
            latestTs=std::numeric_limits<int64_t>::min();
            latestCampaign=static_cast<uint32_t>(lookups.campaignNames.size());
        }
        // Add all clicks at or before the session start. A click at exactly
        // the start is included; a click one millisecond after it is not.
        while (nextClick < ce && cl[nextClick].ts <= start) {
            const Click& c=cl[nextClick];
            if (c.ts < cutoff) {
                // Since clicks are sorted by timestamp, all earlier clicks
                // outside the window precede every active click.
                activeBegin=nextClick+1;
                if (!activeCampaigns.empty()) throw std::runtime_error("rolling-window invariant failed");
            } else {
                uint32_t camp=c.campaign;
                if (seenGeneration[camp] != generation) {
                    seenGeneration[camp]=generation;
                    campaignCount[camp]=0;
                    campaignPosition[camp]=-1;
                }
                if (campaignCount[camp]++ == 0) {
                    campaignPosition[camp]=static_cast<int32_t>(activeCampaigns.size());
                    activeCampaigns.push_back(camp);
                }
                // Sorted by (timestamp, click_id), so the first click at the
                // newest timestamp is the byte-wise-smallest tie winner.
                if (c.ts > latestTs) { latestTs=c.ts; latestCampaign=camp; }
            }
            ++nextClick;
        }
        uint32_t organic=static_cast<uint32_t>(lookups.campaignNames.size());
        session.credit = activeBegin < nextClick ? latestCampaign : organic;
        if (session.credit != organic) {
            size_t base=static_cast<size_t>(session.day)*(lookups.campaignNames.size()+1);
            for (uint32_t camp : activeCampaigns) campaignAgg[base+camp].assisted++;
        }
        haveSession=true;
    };

    auto finish_session = [&]() {
        if (!haveSession) return;
        ++userDay.sessions;
        size_t base=static_cast<size_t>(session.day)*(lookups.campaignNames.size()+1);
        CampaignAgg& c=campaignAgg[base+session.credit];
        ++c.sessions;
        if (session.hasPurchase) ++c.purchasing;
        c.revenue += session.revenue;
        haveSession=false;
    };

    for (size_t i=eb;i<ee;++i) {
        const Event& e=ev[i];
        if (!haveSession || e.ts-previousTs > SESSION_GAP_MS) {
            finish_session();
            begin_session(e.ts);
        }
        ++userDay.events;
        if (e.type==2) {
            session.hasPurchase=true;
            session.revenue += e.price;
            userDay.revenue += e.price;
            userDay.byCategory[e.category] += e.price;
        }
        previousTs=e.ts;
    }
    finish_session();
    flush_user_day();
}

static void process_bucket(
    int b, const std::vector<Event>& events, const std::vector<Click>& clicks,
    const UserPool& users, const Lookups& lookups, int64_t windowStart,
    uint32_t dayCount, const std::vector<std::string>& dayStrings,
    std::vector<CampaignAgg>& campaignAgg, const fs::path& scratch
) {
    std::ofstream out(scratch/("user_"+std::to_string(b)+".csv"),std::ios::binary);
    if (!out) throw std::runtime_error("cannot create user aggregation temp file");
    out << "user_id,day,sessions,events,revenue_cents,top_category\n";
    std::vector<uint32_t> seen(lookups.campaignNames.size(),0), count(lookups.campaignNames.size(),0);
    std::vector<int32_t> position(lookups.campaignNames.size(),-1);
    std::vector<uint32_t> activeCampaigns;
    activeCampaigns.reserve(lookups.campaignNames.size());
    uint32_t generation=0;
    size_t ep=0, cp=0;
    const uint32_t campaignCount=static_cast<uint32_t>(lookups.campaignNames.size());
    campaignAgg.assign(static_cast<size_t>(dayCount)*(campaignCount+1),CampaignAgg{});

    while (ep<events.size()) {
        uint32_t person=events[ep].person;
        size_t ee=ep+1;
        while (ee<events.size() && events[ee].person==person) ++ee;
        while (cp<clicks.size() && clicks[cp].person<person) ++cp;
        size_t ce=cp;
        while (ce<clicks.size() && clicks[ce].person==person) ++ce;
        ++generation;
        if (generation==0) { std::fill(seen.begin(),seen.end(),0); generation=1; }
        process_person(events,ep,ee,clicks,cp,ce,users,lookups,windowStart,dayCount,
                       campaignAgg,out,seen,count,position,activeCampaigns,generation,dayStrings);
        ep=ee;
        cp=ce;
    }
    out.flush();
    if (!out) throw std::runtime_error("failed writing user aggregation temp file");
}

// ---------------------------------- main ------------------------------------
int main() {
    try {
        const char* d = std::getenv("DATA_DIR");
        const char* o = std::getenv("OUTPUT_DIR");
        const char* s = std::getenv("SCRATCH_DIR");
        fs::path data=d?d:"/data", output=o?o:"/output", scratch=s? s:"/scratch";
        fs::create_directories(scratch);
        fs::create_directories(output/"user_daily");
        fs::create_directories(output/"campaign_daily");
        unsigned threads=get_threads();
        auto [windowStart,windowEnd]=read_window(data/"manifest.json");
        uint32_t dayCount=static_cast<uint32_t>((windowEnd-windowStart)/DAY_MS);
        std::vector<std::string> dayStrings(dayCount);
        int64_t startDay=windowStart/DAY_MS;
        for(uint32_t i=0;i<dayCount;++i) dayStrings[i]=date_string_from_day(startDay+i);
        logmsg("window days="+std::to_string(dayCount)+" workers="+std::to_string(threads));

        Lookups lookups;
        read_lookups(data,lookups);
        logmsg("lookups: products="+std::to_string(lookups.productCategory.size())+
               " campaigns="+std::to_string(lookups.campaignNames.size())+
               " categories="+std::to_string(lookups.categories.size()));

        UserPool users;
        // Links are consumed first, so all components are resolved before any
        // event or click is transformed into a person ID.
        {
            auto maps=map_csv_dir(data/"links");
            uint64_t lines=count_data_lines(maps);
            users.ids.reserve(static_cast<size_t>(std::min<uint64_t>(lines*2+1024,20'000'000)));
            for (const auto& m : maps) each_data_line(m.view(), [&](sv line) {
                std::array<sv,2> f{};
                if (!parse_csv_line(line,f) || f[0].empty() || f[1].empty()) return;
                uint32_t a=users.intern(f[0]), b=users.intern(f[1]);
                users.unite(a,b);
            });
            logmsg("links parsed; IDs="+std::to_string(users.names.size()));
        }

        // Event deduplication uses the earliest ingest_ts before validation.
        std::vector<Event> events;
        {
            auto maps=map_csv_dir(data/"events");
            uint64_t rawCount=count_data_lines(maps);
            logmsg("event input lines="+std::to_string(rawCount));
            ShardedDedup<EventRaw> dedup(rawCount);
            auto chunks=make_line_chunks(maps);
            parallel_chunks(chunks,threads,[&](sv chunk) {
                each_chunk_line(chunk,[&](sv line) {
                    std::array<sv,7> f{};
                    if (!parse_csv_line(line,f) || f[0].empty()) return;
                    int64_t ingest;
                    if (!parse_f1(f[3],ingest)) return; // ingest_ts is guaranteed F1
                    uint64_t h=hash_bytes(f[0]);
                    dedup.insert(f[0],h,[&](EventRaw& r) {
                        r.ts=std::numeric_limits<int64_t>::min();
                        parse_timestamp(f[2],r.ts); // parse failure leaves sentinel
                        r.ingest=ingest;
                        r.userId=f[1];
                        if (f[4]=="view") r.type=0;
                        else if (f[4]=="cart") r.type=1;
                        else if (f[4]=="purchase") r.type=2;
                        else r.type=255;
                        r.product_ok=!f[5].empty();
                        r.category=lookups.unknownCategory;
                        if (r.product_ok) {
                            auto it=lookups.productCategory.find(std::string(f[5]));
                            if (it!=lookups.productCategory.end()) r.category=it->second;
                        }
                        r.price=0; r.price_ok=1;
                        if (r.type==2) {
                            uint64_t p;
                            if (!parse_u64_ascii(f[6],p) || p<1 || p>10'000'000) r.price_ok=0;
                            else r.price=static_cast<uint32_t>(p);
                        }
                    },[&](const EventRaw& old) { return ingest < old.ingest; });
                });
            });
            std::vector<EventRaw> rows=dedup.collect();
            logmsg("unique event IDs="+std::to_string(rows.size()));
            events.reserve(rows.size());
            for (const EventRaw& r : rows) {
                if (r.userId.empty() || r.ts==std::numeric_limits<int64_t>::min()) continue;
                if (r.ts<windowStart || r.ts>=windowEnd) continue;
                if (r.type>2 || !r.product_ok || (r.type==2 && !r.price_ok)) continue;
                if (static_cast<__int128>(r.ingest)-r.ts > LATE_MS) continue;
                uint32_t user=users.intern(r.userId);
                events.push_back(Event{user,r.ts,r.price,r.type,r.category,0});
            }
            logmsg("valid non-late events="+std::to_string(events.size()));
        }

                // Click IDs are unique after keeping any one well-formed nonempty copy.
        std::vector<Click> clicks;
        std::vector<Mapping> clickMappings; // click_id views remain valid through sorting/attribution
        {
            clickMappings=map_csv_dir(data/"clicks");
            auto& maps=clickMappings;
            uint64_t rawCount=count_data_lines(maps);
            logmsg("click input lines="+std::to_string(rawCount));
            ShardedDedup<ClickRaw> dedup(rawCount);
            const int64_t clickWindowStart=windowStart-CLICK_WINDOW_MS;
            auto chunks=make_line_chunks(maps);
            parallel_chunks(chunks,threads,[&](sv chunk) {
                each_chunk_line(chunk,[&](sv line) {
                    std::array<sv,4> f{};
                    if (!parse_csv_line(line,f) || f[0].empty()) return;
                    uint64_t h=hash_bytes(f[0]);
                    dedup.insert(f[0],h,[&](ClickRaw& r) {
                        r.valid=0; r.userId=f[1];
                        int64_t ts;
                        if (!f[1].empty() && parse_timestamp(f[3],ts) && ts>=clickWindowStart && ts<windowEnd) {
                            auto it=lookups.campaignIndex.find(std::string(f[2]));
                            if (it!=lookups.campaignIndex.end()) {
                                r.ts=ts; r.campaign=it->second; r.valid=1;
                            }
                        }
                    },[](const ClickRaw&) { return false; });
                });
            });
            std::vector<ClickRaw> rows=dedup.collect();
            logmsg("unique click IDs="+std::to_string(rows.size()));
            clicks.reserve(rows.size());
            for (const ClickRaw& r : rows) {
                if (r.valid && !r.userId.empty())
                    clicks.push_back(Click{users.intern(r.userId),r.ts,r.id,r.campaign});
            }
            logmsg("valid clicks="+std::to_string(clicks.size()));
        }

                // Resolve all IDs, including unused link IDs, to the byte-wise smallest
        // member of each transitive component.
        std::vector<uint32_t> canonical=users.canonical_ids();
        for (Event& e : events) e.person=canonical[e.person];
        // The click vector carries raw user indexes until this canonical remap.
        for (Click& c : clicks) c.person=canonical[c.person];
        std::vector<uint32_t>().swap(canonical);
        // No more ID lookups are needed. Keep only the names used in output.
        users.ids.clear(); users.ids.rehash(0);
        std::vector<uint32_t>().swap(users.parent);
        std::vector<uint32_t>().swap(users.sz);
        std::vector<uint32_t>().swap(users.min_id);

        // Bucket by canonical person. All events and clicks for a person land
        // in the same bucket, so per-bucket sorting/sessionization is independent.
        std::vector<std::vector<Event>> eventBuckets(BUCKETS);
        std::vector<std::vector<Click>> clickBuckets(BUCKETS);
        std::array<size_t,BUCKETS> eventCounts{}, clickCounts{};
        for (const Event& e : events) ++eventCounts[e.person % BUCKETS];
        for (const Click& c : clicks) ++clickCounts[c.person % BUCKETS];
        for (int b=0;b<BUCKETS;++b) { eventBuckets[b].reserve(eventCounts[b]); clickBuckets[b].reserve(clickCounts[b]); }
        for (Event& e : events) eventBuckets[e.person % BUCKETS].push_back(std::move(e));
        for (Click& c : clicks) clickBuckets[c.person % BUCKETS].push_back(std::move(c));
        std::vector<Event>().swap(events);
        std::vector<Click>().swap(clicks);
        logmsg("partitioned events and clicks into " + std::to_string(BUCKETS) + " independent buckets");

        std::vector<std::vector<CampaignAgg>> bucketAgg(BUCKETS);
        std::atomic<int> next{0};
        std::mutex errorMutex;
        std::string workerError;
        auto worker=[&]() {
            for (;;) {
                int b=next.fetch_add(1);
                if (b>=BUCKETS) break;
                try {
                    auto& ev=eventBuckets[b]; auto& cl=clickBuckets[b];
                    std::sort(ev.begin(),ev.end(),[](const Event& a,const Event& b){
                        if(a.person!=b.person) return a.person<b.person;
                        return a.ts<b.ts;
                    });
                    std::sort(cl.begin(),cl.end(),[](const Click& a,const Click& b){
                        if(a.person!=b.person) return a.person<b.person;
                        if(a.ts!=b.ts) return a.ts<b.ts;
                        return a.click_id<b.click_id;
                    });
                    process_bucket(b,ev,cl,users,lookups,windowStart,dayCount,dayStrings,bucketAgg[b],scratch);
                } catch(const std::exception& e) {
                    std::lock_guard<std::mutex> lk(errorMutex);
                    if(workerError.empty()) workerError=e.what();
                }
            }
        };
        std::vector<std::thread> pool;
        for(unsigned i=0;i<threads;++i) pool.emplace_back(worker);
        for(auto& t:pool) t.join();
        if(!workerError.empty()) throw std::runtime_error(workerError);

        // Merge the small campaign/day accumulators from the independent buckets.
        size_t campaignStride=lookups.campaignNames.size()+1;
        std::vector<CampaignAgg> totalCampaign(static_cast<size_t>(dayCount)*campaignStride);
        for(int b=0;b<BUCKETS;++b) {
            const auto& part=bucketAgg[b];
            for(size_t i=0;i<part.size();++i) {
                totalCampaign[i].sessions += part[i].sessions;
                totalCampaign[i].purchasing += part[i].purchasing;
                totalCampaign[i].revenue += part[i].revenue;
                totalCampaign[i].assisted += part[i].assisted;
            }
        }
        std::ofstream campaignOut(scratch/"campaign_daily.csv",std::ios::binary);
        if(!campaignOut) throw std::runtime_error("cannot create campaign aggregation file");
        campaignOut << "campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n";
        for(uint32_t day=0;day<dayCount;++day) {
            for(uint32_t c=0;c<=lookups.campaignNames.size();++c) {
                const CampaignAgg& a=totalCampaign[static_cast<size_t>(day)*campaignStride+c];
                if(a.sessions==0 && a.assisted==0) continue;
                if(c==lookups.campaignNames.size()) campaignOut << "organic,organic,";
                else campaignOut << lookups.campaignNames[c] << ',' << lookups.channels[c] << ',';
                campaignOut << dayStrings[day] << ',' << a.sessions << ',' << a.purchasing << ',' << a.revenue << ',' << a.assisted << '\n';
            }
        }
        campaignOut.flush();
        if(!campaignOut) throw std::runtime_error("failed writing campaign aggregation file");
        logmsg("aggregation complete");
        return 0;
    } catch(const std::exception& e) {
        std::cerr << "pipeline error: " << e.what() << '\n';
        return 1;
    }
}
