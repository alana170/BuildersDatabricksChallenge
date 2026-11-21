#!/usr/bin/env python3
"""Tiny adversarial datasets that pile up the rules.md corner cases. fuzz.py SEED OUTDIR"""
import json, os, random, sys
from datetime import datetime, timezone, timedelta

seed, out = int(sys.argv[1]), sys.argv[2]
R = random.Random(seed)
DAY, MIN, H = 86_400_000, 60_000, 3_600_000
# window start: a random midnight, sometimes around Feb 29 2028
base = datetime(2028, 2, 26, tzinfo=timezone.utc) if R.random() < 0.3 else datetime(2025 + R.randrange(3), 1 + R.randrange(12), 1 + R.randrange(28), tzinfo=timezone.utc)
ws = int(base.timestamp() * 1000)
ndays = R.choice([1, 2, 3, 14])
we = ws + ndays * DAY

def f1(ms):
    return datetime.fromtimestamp(ms // 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
def f2(ms, off):
    t = datetime.fromtimestamp(ms // 1000, timezone.utc) + timedelta(minutes=off)
    sign = "-" if off < 0 else "+"
    return t.strftime("%Y-%m-%dT%H:%M:%S") + f"{sign}{abs(off)//60:02d}:{abs(off)%60:02d}"
def ts_text(ms):
    if ms % 1000:
        return ("000" if R.random() < 0.2 else "") + str(ms)
    r = R.random()
    if r < 0.4: return f1(ms)
    if r < 0.6: return f2(ms, R.choice([-600, -240, -30, 0, 330, 720]))
    if r < 0.65: return f2(ms, 0).replace("+00:00", "-00:00")
    return ("00" if R.random() < 0.2 else "") + str(ms)
def bad_ts(ms):
    s = f1(ms)
    return R.choice(["", s.replace("Z", "z"), s.replace("T", " "), s[:-1], s[:-1] + ".000Z", str(ms) + ".0", "1.79e12", "-5",
                     s[:5] + "13" + s[7:], s[:8] + "30" + s[10:] if s[5:7] == "02" else s[:8] + "32" + s[10:],
                     s[:11] + "24" + s[13:], s[:17] + "60Z", f2(ms, 0).replace("+00:00", "+24:00"), f2(ms, 0).replace("+00:00", "+05:60"),
                     f2(ms, 0).replace("+00:00", "+0000"), f1(we), f1(ws - 1000), "２０２６", s + "\r", "2029-02-29T00:00:00Z", "86400000"])
def q(s):
    return f'"{s}"' if R.random() < 0.25 else s
def maybe_bad_line(line, nf):
    r = R.random()
    if r < 0.02: return ""
    if r < 0.04: return line + ",x"
    if r < 0.06: return line.rsplit(",", 1)[0]
    if r < 0.07: return '"' + line
    if r < 0.08: return line.replace(",", ',"', 1)
    if r < 0.085: return 'a"b' + line[line.index(","):]
    return line

# identities
base_ids = ["u1", "U1", "u10", "u2", " u3", "u3 ", "u3", "a", "b", "c", "z9", "Zed", "dev1", "x" * 20, "x" * 17 + "a", "m" * 16, "Ω"]
ids = R.sample(base_ids, k=R.randrange(5, len(base_ids)))
users = ids + [f"p{i}" for i in range(R.randrange(3, 12))]
links = []
for _ in range(R.randrange(0, 25)):
    a, b = R.choice(users), R.choice(users)
    links.append((a, b))
links += [("only_link_a", R.choice(users)), ("AAA_smallest", "only_link_a")] if R.random() < 0.5 else []

products = [f"p{i:02d}" for i in range(8)]
cats = R.sample(["toys", "books", "Books", "garden", "unknown_not", "a", "b"], 4)
campaigns = [f"c_{i}" for i in range(6)]

os.makedirs(f"{out}/events"); os.makedirs(f"{out}/clicks"); os.makedirs(f"{out}/links")
with open(f"{out}/manifest.json", "w") as f: json.dump({"window_start": f1(ws), "window_end": f1(we)}, f)
with open(f"{out}/products.csv", "w") as f:
    f.write("product_id,category\n")
    for p in products: f.write(f"{p},{R.choice(cats)}\n")
with open(f"{out}/campaigns.csv", "w") as f:
    f.write("campaign_id,channel\n")
    for c in campaigns: f.write(f"{c},{R.choice(['search', 'social', 'email'])}\n")

# session anchors chosen to collide
anchors = [ws, ws + 1, we - 1, ws + DAY - 1, ws + DAY - 10 * MIN] + [ws + R.randrange(we - ws) for _ in range(6)]
anchors = [a - a % 1000 if R.random() < 0.7 else a for a in anchors]
offs = [0, 0, 1, 1000, 30 * MIN, 30 * MIN + 1, 29 * MIN, 31 * MIN, 2 * H, 61 * MIN]
ev_lines, ev_id = [], 0
for _ in range(R.randrange(20, 160)):
    ev_id += 1
    eid = f"e{ev_id}" if R.random() < 0.9 else R.choice(["", "e1", "E1", " e2"])
    t = R.choice(anchors) + R.choice(offs) * R.choice([1, 1, 2, 3])
    user = R.choice(users + [""] if R.random() < 0.05 else users)
    typ = R.choice(["view"] * 5 + ["cart"] * 2 + ["purchase"] * 4 + ["Purchase", "cart ", "VIEW", ""])
    prod = R.choice(products + ["zz", "P01", ""] if R.random() < 0.1 else products)
    price = R.choice(["100", "250", "0250", "100", "10000000", "10000001", "0", "", "1.5", "-1", " 5", "1000\r", "５", "99"]) if typ == "purchase" else R.choice(["", "x", "12"])
    copies = 1 + (R.random() < 0.3) * R.randrange(1, 4)
    ing0 = t + R.choice([0, 1000, -5000, 60_000, DAY, DAY + 1, DAY + 1000, 3 * DAY])
    ing0 -= ing0 % 1000
    if not eid.startswith("e") or eid in ("e1",):  # reused ids: keep ingest unique across reuses
        ing0 += ev_id * 7000
    prev = None
    for c in range(copies):
        same = c > 0 and R.random() < 0.3      # equal ingest_ts => identical values (quoting may differ)
        if same:
            u, tt, ing = prev
        else:
            ing = ing0 + c * R.choice([1000, 600_000])
            tt = bad_ts(t) if R.random() < 0.08 else ts_text(t)
            u = user if R.random() < 0.85 or c == 0 else R.choice(users)
        prev = (u, tt, ing)
        line = ",".join([q(eid), q(u), q(tt), q(f1(ing)), q(typ), q(prod), q(price)])
        ev_lines.append(maybe_bad_line(line, 7) if not same else line)
R.shuffle(ev_lines)

cl_lines = []
for k in range(R.randrange(0, 80)):
    cid = R.choice([f"k{k}", f"k{k}", f"k{k}", "", f"K{k}"])
    t = R.choice(anchors) - R.choice([0, 1, -1, 7 * DAY, 7 * DAY + 1, 7 * DAY - 1, H, 3 * DAY, 30 * MIN])
    camp = R.choice(campaigns + ["c_99", "C_1"] if R.random() < 0.1 else campaigns)
    user = R.choice(users)
    tt = bad_ts(t) if R.random() < 0.05 else ts_text(t)
    line = ",".join([q(cid), q(user), q(camp), q(tt)])
    cl_lines.append(maybe_bad_line(line, 4))
    if R.random() < 0.2:  # identical copy, quoted differently
        cl_lines.append(",".join([q(cid), q(user), q(camp), q(tt)]))
R.shuffle(cl_lines)
lk_lines = [maybe_bad_line(",".join([q(a), q(b)]), 2) for a, b in links]
lk_lines += [",".join([q("u1"), q("")])] if R.random() < 0.3 else []

def write_parts(d, header, lines, nparts):
    parts = [[] for _ in range(nparts)]
    for ln in lines: parts[R.randrange(nparts)].append(ln)
    for i, p in enumerate(parts):
        mode = R.random()
        with open(f"{d}/part-{i}.csv", "w", newline="") as f:
            if mode < 0.05 and not p: continue  # zero-byte file
            body = header + "\n" + "".join(x + "\n" for x in p)
            if p and R.random() < 0.3: body = body[:-1]  # no trailing newline
            f.write(body)
write_parts(f"{out}/events", "event_id,user_id,ts,ingest_ts,event_type,product_id,price_cents", ev_lines, R.randrange(1, 4))
write_parts(f"{out}/clicks", "click_id,user_id,campaign_id,ts", cl_lines, R.randrange(1, 3))
write_parts(f"{out}/links", "user_id_a,user_id_b", lk_lines, R.randrange(1, 3))
