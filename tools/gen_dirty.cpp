// Synthetic data generator that follows the shape and dirt described in rules.md.
// Usage: gen SEED EVENTS CLICKS OUTDIR
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <string>
#include <vector>
#include <numeric>
#include <cmath>
namespace fs = std::filesystem;
using u64 = uint64_t; using i64 = int64_t;
static std::mt19937_64 R;
static u64 rnd(u64 n) { return R() % n; }
static double U() { return (R() >> 11) * (1.0 / 9007199254740992.0); }
static bool P(double p) { return U() < p; }

static i64 days_from_civil(int y, unsigned m, unsigned d) {
    y -= m <= 2; const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (i64)era * 146097 + doe - 719468;
}
static void civil(i64 z, int& y, unsigned& m, unsigned& d) {
    z += 719468; const i64 era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = (unsigned)(z - era * 146097);
    const unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    y = (int)yoe + (int)(era * 400); const unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    const unsigned mp = (5*doy + 2)/153; d = doy - (153*mp+2)/5 + 1; m = mp + (mp < 10 ? 3 : -9); y += (m <= 2);
}
static std::string f1(i64 ms) {
    i64 s = ms / 1000; i64 day = s / 86400; i64 r = s % 86400; if (r < 0) { r += 86400; day--; }
    int y; unsigned m, d; civil(day, y, m, d); char b[40];
    snprintf(b, sizeof b, "%04d-%02u-%02uT%02d:%02d:%02dZ", y, m, d, (int)(r/3600), (int)(r/60%60), (int)(r%60));
    return b;
}
static std::string f2(i64 ms, int offMin) {
    i64 s = ms / 1000 + offMin * 60; i64 day = s / 86400; i64 r = s % 86400; if (r < 0) { r += 86400; day--; }
    int y; unsigned m, d; civil(day, y, m, d); char b[48]; int a = std::abs(offMin);
    snprintf(b, sizeof b, "%04d-%02u-%02uT%02d:%02d:%02d%c%02d:%02d", y, m, d, (int)(r/3600), (int)(r/60%60), (int)(r%60),
             offMin < 0 ? '-' : '+', a / 60, a % 60);
    return b;
}
static std::string fmt_ts(i64 ms) {
    if (ms % 1000 != 0) { std::string s = std::to_string(ms); if (P(0.05)) s = "000" + s; return s; }
    double u = U();
    if (u < 0.58) return f1(ms);
    if (u < 0.68) { int off = ((int)rnd(23) - 10) * 60 + (P(0.2) ? 30 : 0); if (off > 720) off = 720; return f2(ms, off); }
    std::string s = std::to_string(ms); if (P(0.05)) s = "00" + s; return s;
}
static std::string bad_ts(i64 ms, i64 ws, i64 we) {
    switch (rnd(16)) {
    case 0: return "";
    case 1: { auto s = f1(ms); s.insert(19, ".123"); return s; }
    case 2: { auto s = f1(ms); s[19] = 'z'; return s; }
    case 3: { auto s = f1(ms); s[10] = ' '; return s; }
    case 4: { auto s = f1(ms); s.pop_back(); return s; }
    case 5: return std::to_string(ms) + ".0";
    case 6: return "1.79e12";
    case 7: return "-5";
    case 8: { auto s = f1(ms); s[5] = '1'; s[6] = '3'; return s; }
    case 9: { auto s = f1(ms); s[11] = '2'; s[12] = '4'; return s; }
    case 10: { auto s = f1(ms); s[17] = '6'; s[18] = '0'; return s; }
    case 11: { auto s = f2(ms, 0); s.replace(19, 6, "+24:00"); return s; }
    case 12: { auto s = f2(ms, 0); s.replace(19, 6, "+0200"); return s; }
    case 13: return f1(we + rnd(3) * 1000);
    case 14: return f1(ws - 1000 - rnd(3600) * 1000);
    default: return f1(ms) + "\r";
    }
}
static std::string quote(const std::string& s) { return P(0.2) ? "\"" + s + "\"" : s; }
// Same shape as the official generator: a letter, then base 36 of a 40-bit scrambled number.
static std::string idstr(char p, u64 x) {
    static const char* D = "0123456789abcdefghijklmnopqrstuvwxyz";
    x &= (u64(1) << 40) - 1; char b[16]; int at = 16;
    do { b[--at] = D[x % 36]; x /= 36; } while (x);
    return std::string(1, p) + std::string(b + at, 16 - at);
}
static std::string upper(std::string s) { for (auto& c : s) c = (char)toupper((unsigned char)c); return s; }

struct Files {
    std::vector<FILE*> f; std::vector<double> cum;
    Files(const fs::path& dir, int n, const char* header) {
        fs::create_directories(dir);
        std::vector<double> w(n); for (int i = 0; i < n; ++i) w[i] = 1.0 / (i + 1);
        std::shuffle(w.begin(), w.end(), R);
        int empties = 1 + (int)rnd(3);
        double t = 0; cum.resize(n);
        for (int i = 0; i < n; ++i) {
            char nm[64]; snprintf(nm, sizeof nm, "part-%05d.csv", i);
            FILE* fp = fopen((dir / nm).c_str(), "w"); setvbuf(fp, nullptr, _IOFBF, 1 << 20); fputs(header, fp); fputc('\n', fp); f.push_back(fp);
            if (i < empties) w[i] = 0;
            t += w[i]; cum[i] = t;
        }
        for (auto& c : cum) c /= t;
    }
    FILE* pick() { double u = U(); return f[std::lower_bound(cum.begin(), cum.end(), u) - cum.begin()]; }
    ~Files() { for (auto fp : f) fclose(fp); }
};
static std::string malformed(const std::string& line) {
    switch (rnd(4)) { case 0: return ""; case 1: return line + ",x"; case 2: return "\"" + line; default: { auto p = line.find(','); return p == std::string::npos ? line : line.substr(p + 1); } }
}

int main(int argc, char** argv) {
    if (argc < 5) { fprintf(stderr, "usage: gen SEED EVENTS CLICKS OUT\n"); return 1; }
    u64 seed = std::stoull(argv[1]); u64 E = std::stoull(argv[2]); u64 C = std::stoull(argv[3]); fs::path out = argv[4];
    R.seed(seed);
    fs::create_directories(out);
    // window: random midnight 2025..2028
    i64 d0 = days_from_civil(2025, 1, 1), d1 = days_from_civil(2028, 12, 1);
    i64 ws = (d0 + (i64)rnd(d1 - d0)) * 86400000LL, we = ws + 14 * 86400000LL;
    { FILE* m = fopen((out / "manifest.json").c_str(), "w"); fprintf(m, "{\"window_start\": \"%s\", \"window_end\": \"%s\"}\n", f1(ws).c_str(), f1(we).c_str()); fclose(m); }
    const int NPROD = 50000, NCAMP = 2000, NCAT = 20;
    { FILE* p = fopen((out / "products.csv").c_str(), "w"); fputs("product_id,category\n", p);
      for (int i = 0; i < NPROD; ++i) fprintf(p, "p%06d,cat_%02d\n", i, (int)rnd(NCAT)); fclose(p); }
    const char* chans[] = {"search", "social", "email", "display", "affiliate"};
    { FILE* p = fopen((out / "campaigns.csv").c_str(), "w"); fputs("campaign_id,channel\n", p);
      for (int i = 0; i < NCAMP; ++i) fprintf(p, "c_%d,%s\n", i, chans[rnd(5)]); fclose(p); }

    double dupRate = 0.02 + U() * 0.02, clickDup = 0.01 + U() * 0.02, badTs = 0.003 + U() * 0.004, late = 0.005 + U() * 0.01;
    u64 NP = std::max<u64>(10, E / 30);
    // activity weights: Zipf-like; 5 whales take ~1.5% each
    std::vector<double> w(NP); for (u64 i = 0; i < NP; ++i) w[i] = 1.0 / std::pow((double)(rnd(NP) + 1), 0.9);
    double tot = std::accumulate(w.begin(), w.end(), 0.0);
    for (int i = 0; i < 5 && i < (int)NP; ++i) w[i] = tot * 0.015 / 0.925;
    tot = std::accumulate(w.begin(), w.end(), 0.0);
    std::vector<u64> nev(NP); u64 assigned = 0;
    for (u64 i = 0; i < NP; ++i) { nev[i] = (u64)std::floor(w[i] / tot * E); assigned += nev[i]; }
    while (assigned < E) { nev[rnd(NP)]++; assigned++; }

    // IDs per person
    u64 idctr = 0; auto newid = [&]() { return idstr('u', (idctr++ * 0x9E3779B97F4A7C15ULL) >> 24); };
    std::vector<std::vector<std::string>> ids(NP);
    Files links(out / "links", 4 + (int)rnd(21), "user_id_a,user_id_b");
    auto put_link = [&](std::string a, std::string b) {
        if (P(0.002)) a = ""; if (P(0.002)) b = "";
        if (P(1.0 / 2000)) a = upper(a); if (P(1.0 / 2000)) b = upper(b);
        std::string line = quote(a) + "," + quote(b);
        if (P(0.0005)) line = malformed(line);
        FILE* f = links.pick(); fputs(line.c_str(), f); fputc('\n', f);
        if (P(0.08)) { std::string l2 = P(0.5) ? quote(b) + "," + quote(a) : quote(a) + "," + quote(b); FILE* g = links.pick(); fputs(l2.c_str(), g); fputc('\n', g); }
    };
    std::vector<std::string> devices; int ndev = 3 + (int)rnd(4); for (int i = 0; i < ndev; ++i) devices.push_back(idstr('d', R() >> 24));
    std::vector<int> usesDevice(NP, -1);
    for (u64 p = 0; p < NP; ++p) {
        int k; double u = U();
        bool chain = (p >= 5 && p < 8) || P(1.0 / 20000);
        if (p < 5) k = 3 + (int)rnd(4);
        else if (chain) k = 50 + (int)rnd(351);
        else k = u < 0.45 ? 1 : u < 0.75 ? 2 : u < 0.88 ? 3 : 4 + (int)rnd(5);
        for (int j = 0; j < k; ++j) ids[p].push_back(newid());
        if (chain) { std::vector<std::string> o = ids[p]; std::shuffle(o.begin(), o.end(), R); for (int j = 1; j < k; ++j) put_link(o[j - 1], o[j]); }
        else for (int j = 1; j < k; ++j) put_link(ids[p][rnd(j)], ids[p][j]);
        if (P(0.05)) put_link(ids[p][rnd(k)], newid());
        if (P(0.01)) put_link(ids[p][0], ids[p][0]);
        if (P(0.004)) { usesDevice[p] = (int)rnd(ndev); put_link(ids[p][rnd(k)], devices[usesDevice[p]]); }
    }
    for (int i = 1; i < ndev; ++i) put_link(devices[0], devices[i]); // devices link to each other

    auto pick_id = [&](u64 p) -> std::string {
        auto& v = ids[p];
        std::string s;
        if (usesDevice[p] >= 0 && P(0.1)) s = devices[usesDevice[p]];
        else s = (v.size() > 50) ? v[rnd(v.size())] : (P(0.5) ? v[0] : v[rnd(v.size())]);
        if (P(1.0 / 200000)) s = upper(s);
        if (P(0.002)) s = "";
        return s;
    };
    // product zipf
    std::vector<double> pc(NPROD); { double t = 0; for (int i = 0; i < NPROD; ++i) { t += 1.0 / (i + 1); pc[i] = t; } for (auto& x : pc) x /= t; }
    auto pick_product = [&]() -> std::string {
        if (P(0.005)) { u64 r = rnd(10); if (r == 0) return ""; if (r < 5) return "P" + std::to_string(100000 + rnd(NPROD)).substr(1); return "p9" + std::to_string(rnd(99999)); }
        int i = (int)(std::lower_bound(pc.begin(), pc.end(), U()) - pc.begin()); char b[16]; snprintf(b, sizeof b, "p%06d", i); return b;
    };
    std::vector<double> cc(NCAMP); std::vector<int> whaleC; { int nw = 3 + (int)rnd(3); for (int i = 0; i < nw; ++i) whaleC.push_back((int)rnd(NCAMP));
        double t = 0; for (int i = 0; i < NCAMP; ++i) { t += 1.0 / std::pow(i + 1, 0.6); cc[i] = t; } for (auto& x : cc) x /= t; }
    auto pick_campaign = [&]() -> std::string {
        if (P(0.005)) return P(0.5) ? "C_" + std::to_string(rnd(NCAMP)) : "c_x" + std::to_string(rnd(100));
        int i = P(0.3) ? whaleC[rnd(whaleC.size())] : (int)(std::lower_bound(cc.begin(), cc.end(), U()) - cc.begin());
        return "c_" + std::to_string(i);
    };

    std::vector<std::string> evrows; evrows.reserve((size_t)(E * 1.06));
    std::vector<std::string> clrows; clrows.reserve((size_t)(C * 1.1));
    u64 eid = 0, cid = 0;
    auto mk_event_id = [&]() { return idstr('e', (eid++ * 0xD1B54A32D192ED03ULL) >> 20); };
    auto mk_click_id = [&]() { return idstr('k', (cid++ * 0xD1B54A32D192ED03ULL) >> 20); };
    auto add_click = [&](u64 p, i64 ts) {
        std::string id = mk_click_id(), user = pick_id(p), camp = pick_campaign();
        std::string t = P(badTs) ? bad_ts(ts, ws, we) : fmt_ts(ts);
        int copies = P(clickDup) ? 2 + (int)rnd(2) : 1;
        for (int c = 0; c < copies; ++c) {
            std::string line = quote(id) + "," + quote(user) + "," + quote(camp) + "," + quote(t);
            if (P(0.0005)) line = malformed(line);
            clrows.push_back(std::move(line));
        }
    };
    double clicksPerSessionTarget = (double)C / std::max<u64>(1, E / 6);
    for (u64 p = 0; p < NP; ++p) {
        u64 n = nev[p]; if (!n) continue;
        // sessions: mean ~6 events each, spread through the window
        u64 nsess = std::max<u64>(1, n / 6);
        std::vector<i64> starts(nsess);
        for (auto& s : starts) s = ws + (i64)rnd((u64)(we - ws - 3600000));
        std::sort(starts.begin(), starts.end());
        u64 left = n;
        for (u64 si = 0; si < nsess && left; ++si) {
            u64 k = (si + 1 == nsess) ? left : std::min<u64>(left, 1 + rnd(11));
            left -= k;
            i64 t = starts[si];
            bool msPrec = P(0.3);
            if (!msPrec) t -= t % 1000;
            // clicks before session
            if (P(0.6)) {
                int nc = 1 + (int)rnd(std::max(1, (int)(clicksPerSessionTarget * 2)));
                for (int c = 0; c < nc; ++c) add_click(p, t - (i64)rnd(604800) * 1000);
            }
            if (P(0.01)) { i64 b[] = {604800000, 604800001, 0, -1}; add_click(p, t - b[rnd(4)]); add_click(p, t - b[rnd(4)]); }
            for (u64 j = 0; j < k; ++j) {
                if (j) {
                    i64 gap; double g = U();
                    if (g < 0.001) gap = 1800000 + (i64)rnd(2); else if (g < 0.05) gap = 0; else gap = (i64)(1 + rnd(600)) * 1000;
                    if (msPrec) gap += (i64)rnd(1000);
                    t += gap;
                }
                if (t >= we) t = we - 1;
                std::string id = mk_event_id(), user = pick_id(p);
                const char* type; double ty = U(); type = ty < 0.03 ? "purchase" : ty < 0.13 ? "cart" : "view";
                std::string typ = type; if (P(0.001)) typ = P(0.5) ? "Purchase" : "cart ";
                std::string prod = pick_product();
                std::string price = (typ == "purchase") ? std::to_string(100 + rnd(49901)) : (P(0.5) ? "" : std::to_string(rnd(1000)));
                if (typ == "purchase" && P(0.05)) { const char* bp[] = {"12.5", "-300", "0", "", "10000001", "+5", "1e3"}; price = bp[rnd(7)]; }
                if (typ == "purchase" && P(0.02)) price = "0" + price;
                bool isBad = P(badTs);
                std::string tsS = isBad ? bad_ts(t, ws, we) : fmt_ts(t);
                i64 ing = t + (i64)rnd(120000);
                if (P(late)) { u64 r = rnd(10); ing = t + (r == 0 ? 86400000 : r == 1 ? 86401000 : 86400000 + (i64)rnd(4 * 86400) * 1000); }
                ing -= ing % 1000; if (ing < t - 5000) ing = t;
                int extra = P(dupRate) ? 1 + (int)rnd(3) : 0;
                bool firstBad = extra && P(0.02);
                bool diffUser = extra && P(1.0 / 64);
                std::string last = P(0.2) ? "\r" : "";
                last = ""; // generator never writes \r
                for (int c = 0; c <= extra; ++c) {
                    i64 ci = ing + (c ? (i64)(1 + rnd(600)) * 1000 * c : 0);
                    std::string ts2 = (firstBad && c == 0) ? bad_ts(t, ws, we) : (firstBad ? fmt_ts(t) : tsS);
                    std::string u2 = (diffUser && c) ? pick_id(rnd(NP)) : user;
                    std::string line = quote(id) + "," + quote(u2) + "," + quote(ts2) + "," + quote(f1(ci)) + "," + quote(typ) + "," + quote(prod) + "," + quote(price);
                    if (P(0.0005)) line = malformed(line);
                    evrows.push_back(std::move(line));
                }
            }
        }
    }
    // shuffle and write
    std::shuffle(evrows.begin(), evrows.end(), R);
    std::shuffle(clrows.begin(), clrows.end(), R);
    {
        Files ev(out / "events", 24 + (int)rnd(137), "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents");
        for (auto& l : evrows) { FILE* f = ev.pick(); fputs(l.c_str(), f); fputc('\n', f); }
    }
    {
        Files cl(out / "clicks", 8 + (int)rnd(41), "click_id,user_id,campaign_id,ts");
        for (auto& l : clrows) { FILE* f = cl.pick(); fputs(l.c_str(), f); fputc('\n', f); }
    }
    fprintf(stderr, "event rows=%zu click rows=%zu people=%llu ids=%llu window=%s\n", evrows.size(), clrows.size(), (unsigned long long)NP, (unsigned long long)idctr, f1(ws).c_str());
}
