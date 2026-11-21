#!/usr/bin/env python3
"""Slow, independent implementation written directly from rules.md (no shared code with
src/pipeline.cpp). Writes user_daily.csv / campaign_daily.csv in the fixture format.
Usage: reference.py DATA_DIR OUT_DIR"""
import csv, glob, json, os, re, sys
from datetime import datetime, timezone

DATA, OUT = sys.argv[1], sys.argv[2]
DAY, GAP, WEEK, LATE = 86_400_000, 1_800_000, 604_800_000, 86_400_000


def lines(path):
    with open(path, "rb") as f:
        raw = f.read().decode("utf-8")
    parts = raw.split("\n")
    if parts and parts[-1] == "":
        parts.pop()  # text after the last \n is a line only if non-empty
    return parts[1:]  # header


def parse(line, n):
    if line == "":
        return None
    fields = line.split(",")
    if len(fields) != n:
        return None
    out = []
    for f in fields:
        if '"' in f:
            if len(f) >= 2 and f[0] == '"' and f[-1] == '"' and '"' not in f[1:-1]:
                f = f[1:-1]
            else:
                return None
        out.append(f)
    return out


def rows(d, n):
    for p in sorted(glob.glob(os.path.join(DATA, d, "*.csv"))):
        for ln in lines(p):
            r = parse(ln, n)
            if r is not None:
                yield r


DIG = re.compile(r"[0-9]+\Z", re.ASCII)
F1 = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(Z|[+-][0-9]{2}:[0-9]{2})\Z", re.ASCII)


def ts_ms(s):
    if DIG.match(s):
        return int(s)
    m = F1.match(s)
    if not m:
        return None
    y, mo, d, hh, mi, ss = map(int, m.groups()[:6])
    if hh > 23 or mi > 59 or ss > 59:
        return None
    try:
        t = datetime(y, mo, d, hh, mi, ss, tzinfo=timezone.utc)
    except ValueError:
        return None
    off = 0
    z = m.group(7)
    if z != "Z":
        oh, om = int(z[1:3]), int(z[4:6])
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * (1 if z[0] == "+" else -1)
    return int(t.timestamp()) * 1000 - off * 60_000


man = json.load(open(os.path.join(DATA, "manifest.json")))
WS = ts_ms(man["window_start"]); WE = ts_ms(man["window_end"])

# links -> person name
parent = {}
def find(x):
    while parent[x] != x:
        parent[x] = parent[parent[x]]; x = parent[x]
    return x
for a, b in rows("links", 2):
    if a == "" or b == "":
        continue
    for x in (a, b):
        parent.setdefault(x, x)
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb
best = {}
for x in parent:
    r = find(x)
    if r not in best or x.encode() < best[r].encode():
        best[r] = x
def person(u):
    return best[find(u)] if u in parent else u

cat_of = {}
with open(os.path.join(DATA, "products.csv"), encoding="utf-8") as f:
    for i, ln in enumerate(f.read().split("\n")[1:]):
        r = parse(ln, 2)
        if r and r[0] and r[0] not in cat_of:
            cat_of[r[0]] = r[1]
channel = {}
with open(os.path.join(DATA, "campaigns.csv"), encoding="utf-8") as f:
    for ln in f.read().split("\n")[1:]:
        r = parse(ln, 2)
        if r and r[0] and r[0] not in channel:
            channel[r[0]] = r[1]

# events: dedup by smallest ingest, then validate
kept = {}
for eid, user, ts, ing, typ, prod, price in rows("events", 7):
    if eid == "":
        continue
    ig = ts_ms(ing)
    if eid not in kept or ig < kept[eid][0]:
        kept[eid] = (ig, user, ts, typ, prod, price)
events = {}  # person -> list of (ts, price or None, category)
for eid, (ig, user, ts, typ, prod, price) in kept.items():
    t = ts_ms(ts)
    if user == "" or t is None or not (WS <= t < WE) or typ not in ("view", "cart", "purchase") or prod == "":
        continue
    pr = None
    if typ == "purchase":
        if not DIG.match(price) or not (1 <= int(price) <= 10_000_000):
            continue
        pr = int(price)
    if ig - t > LATE:
        continue
    events.setdefault(person(user), []).append((t, pr, cat_of.get(prod, "unknown")))

clicks = {}
seen = set()
for cid, user, camp, ts in rows("clicks", 4):
    if cid == "" or cid in seen:
        continue
    seen.add(cid)
    t = ts_ms(ts)
    if user == "" or t is None or not (WS - WEEK <= t < WE) or camp not in channel:
        continue
    clicks.setdefault(person(user), []).append((t, cid, camp))

ud, cd = {}, {}
def day_of(t):
    return datetime.fromtimestamp(t // 1000, timezone.utc).date().isoformat()
for pname, evs in events.items():
    evs.sort(key=lambda e: e[0])
    sessions, cur, prev = [], None, None
    for e in evs:
        if cur is None or e[0] - prev > GAP:
            cur = []; sessions.append(cur)
        cur.append(e); prev = e[0]
    cl = clicks.get(pname, [])
    for s in sessions:
        S = s[0][0]; day = day_of(S)
        q = [c for c in cl if S - WEEK <= c[0] <= S]
        if q:
            mt = max(c[0] for c in q)
            credit = min((c for c in q if c[0] == mt), key=lambda c: c[1].encode())[2]
        else:
            credit = "organic"
        rev = sum(e[1] for e in s if e[1] is not None)
        purch = any(e[1] is not None for e in s)
        u = ud.setdefault((pname, day), [0, 0, 0, {}])
        u[0] += 1; u[1] += len(s); u[2] += rev
        for e in s:
            if e[1] is not None:
                u[3][e[2]] = u[3].get(e[2], 0) + e[1]
        c = cd.setdefault((credit, day), [0, 0, 0, 0])
        c[0] += 1; c[1] += purch; c[2] += rev
        for camp in {c[2] for c in q}:
            cd.setdefault((camp, day), [0, 0, 0, 0])[3] += 1

os.makedirs(OUT, exist_ok=True)
with open(os.path.join(OUT, "user_daily.csv"), "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["user_id", "day", "sessions", "events", "revenue_cents", "top_category"])
    for (p, d), (s, e, r, cats) in ud.items():
        top = min(cats.items(), key=lambda kv: (-kv[1], kv[0].encode()))[0] if cats else ""
        w.writerow([p, d, s, e, r, top])
with open(os.path.join(OUT, "campaign_daily.csv"), "w", newline="") as f:
    w = csv.writer(f, lineterminator="\n")
    w.writerow(["campaign_id", "channel", "day", "sessions", "purchasing_sessions", "revenue_cents", "assisted_sessions"])
    for (c, d), v in cd.items():
        w.writerow([c, "organic" if c == "organic" else channel[c], d] + v)
