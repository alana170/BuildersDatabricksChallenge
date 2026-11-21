#!/usr/bin/env python3
import csv, os, shutil, subprocess, sys, tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
BIN=ROOT/'pipeline'
FIX=ROOT/'fixtures'

def read_rows(path):
    with open(path,newline='',encoding='utf-8') as f:
        rows=list(csv.DictReader(f))
    out=[]
    for r in rows:
        # normalize schema types for comparison
        def n(k): return int(r[k])
        if 'user_id' in r:
            out.append((r['user_id'],r['day'],n('sessions'),n('events'),n('revenue_cents'),r['top_category'] or None))
        else:
            out.append((r['campaign_id'],r['channel'],r['day'],n('sessions'),n('purchasing_sessions'),n('revenue_cents'),n('assisted_sessions')))
    return sorted(out,key=lambda x: tuple('' if v is None else str(v) for v in x))

bad=[]
for case in sorted(p for p in FIX.iterdir() if p.is_dir()):
    data=case/'input'
    if not data.exists(): data=case/'data'
    with tempfile.TemporaryDirectory() as td:
        scratch=Path(td)/'scratch'; out=Path(td)/'out'
        scratch.mkdir(); out.mkdir()
        env=dict(os.environ,DATA_DIR=str(data),OUTPUT_DIR=str(out),SCRATCH_DIR=str(scratch),NCPU='3')
        p=subprocess.run([str(BIN)],env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        if p.returncode:
            bad.append(case.name); print('FAIL',case.name,'EXIT',p.returncode,p.stderr[-3000:]); continue
        try:
            a=read_rows(scratch/'campaign_daily.csv')
            b=read_rows(case/'expected'/'campaign_daily.csv')
            c=read_rows(scratch/'user_0.csv')
            for f in scratch.glob('user_*.csv'):
                if f.name!='user_0.csv': c+=read_rows(f)
            d=read_rows(case/'expected'/'user_daily.csv')
            c=sorted(c,key=lambda x: tuple('' if v is None else str(v) for v in x))
            ok=(a==b and c==d)
        except Exception as ex:
            bad.append(case.name); print('FAIL',case.name,'CHECK ERROR',repr(ex)); continue
        if ok: print('ok  ',case.name)
        else:
            bad.append(case.name)
            print('FAIL',case.name)
            for name,x,y in [('user',c,d),('campaign',a,b)]:
                if x!=y:
                    print(' ',name,'actual',x,'expected',y)
                    break
print(f'\n{36-len(bad)}/36 pass')
sys.exit(bool(bad))
