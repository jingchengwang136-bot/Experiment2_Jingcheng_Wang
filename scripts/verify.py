"""Independent Decimal/SQLite reference checks against actual Hadoop output.
Usage: python scripts/verify.py TRAIN TEST OUTPUT REPORT_JSON
No Python-generated outputs are substituted for Hadoop results.
"""
import csv, sys, json, math, hashlib, sqlite3
from collections import defaultdict, Counter
from decimal import Decimal, getcontext
from pathlib import Path
getcontext().prec = 40
train, test, root, report = map(Path, sys.argv[1:])
names = ['CUST_SAVING_AMT','CUST_FINA_AMT','CUST_PRIFINA_AMT','CUST_FOND_AMT','CUST_NADEBT_AMT','CUST_YBT_AMT','CUST_METAL_AMT','CUST_ASSET_AMT','CUST_DEBT_AMT','G_OS_PRCP_SUM']
types = ['存款','理财','私人理财','基金','国债','银保通','贵金属']
levels = ['低资产','中等资产','较高资产','高资产']
missing = Counter()
def num(v, k=''):
    if v.strip().lower() in ('','nan','null','na'):
        missing[k] += 1
        return Decimal(0)
    return Decimal(v)
rows = []
org = defaultdict(lambda: [0,Decimal(0),Decimal(0),0])
labels = Counter()
with train.open(encoding='utf-8-sig',newline='') as f:
    for r in csv.DictReader(f):
        m = [num(r[k],k) for k in names]
        y = int(r['bad_good']); labels[y]+=1
        o = r['OPEN_ORG_NUM'] or 'UNKNOWN'
        s = org[o]; s[0]+=1; s[1]+=m[7]; s[2]+=m[8]; s[3]+=y
        rows.append((r['CUST_ID'],o,m,y,r['LOAN_FLAG'] or 'UNKNOWN',r['CRED_FLAG'] or 'UNKNOWN'))
n = len(rows)
# Independent global SQL ordering, kept outside the submitted MapReduce algorithm.
db=sqlite3.connect(':memory:')
db.execute('create table assets (value real)')
db.executemany('insert into assets values (?)',[(float(r[2][7]),) for r in rows])
q=[db.execute('select value from assets order by value limit 1 offset ?',((n*k+3)//4-1,)).fetchone()[0] for k in (1,2,3)]
def level(a): return next((i for i,t in enumerate(q) if float(a)<=t),3)
above=defaultdict(lambda:[0,0]); sums=[[Decimal(0) for _ in range(7)] for _ in range(4)]; sizes=Counter()
for _,o,m,_,_,_ in rows:
    above[o][int(m[7]*org[o][0]<org[o][1])]+=1
    l=level(m[7]);sizes[l]+=1
    for j in range(7): sums[l][j]+=m[j]
def lines(task):
    for p in sorted((root/task).glob('part-*')):
        yield from p.read_text(encoding='utf-8').splitlines()
actual1={k:v.split(',') for k,v in (s.split('\t') for s in lines('task1'))}
assert set(actual1)==set(org)
for k,v in org.items(): assert [Decimal(x) for x in actual1[k]]==list(map(Decimal,v)), ('task1',k)
actual2={k:list(map(int,v.split(','))) for k,v in (s.split('\t') for s in lines('task2'))}
assert actual2==dict(above),'task2'
actualq=[float(x.split(':')[1]) for x in list(lines('task3'))[0].split('\t')]
assert q==actualq,('task3',q,actualq)
actual4=list(lines('task4'));assert len(actual4)==28
for l in range(4):
    total=sum(sums[l]);ratios=[x/total if total else Decimal(0) for x in sums[l]]
    order=sorted(range(7),key=lambda j:(-ratios[j],j))
    for position,j in enumerate(order):
        ln,kind,value=actual4[l*7+position].split('\t')
        assert (ln,kind)==(levels[l],types[j])
        assert abs(Decimal(value)-ratios[j])<=Decimal('0.0000000051')
def features(o,m,loan,cred):
    return [('+' if x>=0 else '-')+str(math.floor(math.log10(1+abs(float(x))))) for x in m]+[o,loan,cred,str(level(m[7]))]
counts=Counter(); vocabulary=[set() for _ in range(14)]
for _,o,m,y,loan,cred in rows:
    counts[f'C|{y}']+=1
    for j,x in enumerate(features(o,m,loan,cred)):
        counts[f'F|{y}|{j}|{x}']+=1;vocabulary[j].add(x)
actualmodel={k:int(v) for k,v in (s.split('\t') for s in lines('model'))}
assert dict(counts)==actualmodel,'model counts'
pred={}; predcount=Counter()
for s in lines('task5'):
    k,v=s.split('\t');assert k not in pred,'duplicate prediction';assert v in ('0','1');pred[k]=int(v);predcount[v]+=1
testids=set(); mismatches=0
with test.open(encoding='utf-8-sig',newline='') as f:
    for r in csv.DictReader(f):
        rid=r['CUST_ID'];assert rid not in testids,'duplicate input ID';testids.add(rid)
        feat=features(r['OPEN_ORG_NUM'] or 'UNKNOWN',[num(r[k]) for k in names],r['LOAN_FLAG'] or 'UNKNOWN',r['CRED_FLAG'] or 'UNKNOWN')
        scores=[math.log((labels[y]+1)/(n+2))+sum(math.log((counts[f'F|{y}|{j}|{v}']+1)/(labels[y]+len(vocabulary[j])+1)) for j,v in enumerate(feat)) for y in range(2)]
        expected=int(scores[1]>scores[0]);mismatches+=pred.get(rid)!=expected
assert set(pred)==testids and mismatches==0,('predictions',mismatches)
result={'status':'PASS','train_rows':n,'train_unique_ids':len(set(r[0] for r in rows)),'test_rows':len(testids),'organizations':len(org),'labels':dict(labels),'quantiles_nearest_rank':q,'layer_sizes':{levels[k]:sizes[k] for k in range(4)},'prediction_counts':dict(predcount),'total_assets':str(sum(x[1] for x in org.values())),'total_debts':str(sum(x[2] for x in org.values())),'missing_numeric_fields_train':{k:v for k,v in missing.items() if k},'checks':['task1 exact Decimal aggregates','task2 exact cross-multiplied means','task3 independent SQLite global ordering','task4 all 28 ratios and ordering','task5 full training count equality','task5 all test IDs and independent prediction equality'],'sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (train,test)}}
report.parent.mkdir(parents=True,exist_ok=True);report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(result,ensure_ascii=False,indent=2))
