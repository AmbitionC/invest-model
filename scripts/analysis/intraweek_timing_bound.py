# -*- coding: utf-8 -*-
"""周度卖出 + **周内**择时天花板（owner 2026-08-04：「如果按周度呢？」）

与 intramonth_timing_bound.py 同口径，只把窗口从「月」换成「周」：
  · 基准对照：月频卖 5%（现行）vs 周频卖 1.15%（等效减仓速度）
  · 周内择时上界＝该周最高收盘（完美后视，含前视，仅作天花板）／下界＝该周最低收盘
窗口越短、窗内振幅越小 ⟹ 择时天花板必然更薄。这是几何事实，不是实证发现。
只读 results/ 下 CSV，不落库、不联网。
"""
import sys; sys.path.insert(0,"scripts/analysis")
import numpy as np, pandas as pd
from pathlib import Path
from long_window_backtest import CASH, LEGS, RF, RUNG, FRAC, first_tradable, prep
root=Path("results")
SRC={"沪深300":("index_dump_000300_SH.csv","close",None),
     "创业板":("spread_full_history.csv","chinext",None),
     "科创50":("index_dump_000688_SH.csv","close",None),
     "红利":("index_dump_000922_CSI.csv","close","index_dump_H00922_CSI.csv")}
fear=pd.read_csv(root/"fear_daily_dump.csv",dtype={"trade_date":str})
fmap=dict(zip(fear.trade_date,pd.to_numeric(fear.score)))
def prep2(f,col,trf):
    d,ret=prep(root,f,col,trf)
    wk=pd.to_datetime(d.trade_date).dt.isocalendar()
    d["wid"]=wk.year.astype(str)+"-"+wk.week.astype(str)
    d["ym"]=d.trade_date.str[:6]
    for key,tag in (("wid","w"),("ym","m")):
        g=d.groupby(key)["c"]
        d[f"{tag}_hi"]=g.transform("max"); d[f"{tag}_lo"]=g.transform("min")
    return d,ret
def run(df,ret,fmap,nm,d0,d1,mode,freq="M",frac=0.05,exec_mode="close"):
    d,c=df.trade_date.values,df.c.values
    rr=ret.pct_change().fillna(0).values if ret is not None else None
    i0=int(np.searchsorted(d,d0)); i1=int(np.searchsorted(d,d1,side="right"))
    mul=1.30*1.10 if nm=="创业板" else 1.30
    cash,units,nav=100.0,0.0,1.0
    last,pend=-999,[]; armed,in_ep=np.ones(4,bool),False
    curve,pos,ns,gain=[],[],0,[]
    for i in range(i0,i1):
        ci=float(c[i])
        if i>i0:
            cash*=(1+CASH)**((pd.Timestamp(d[i])-pd.Timestamp(d[i-1])).days/365.25)
            nav=nav*(1+rr[i]) if rr is not None else ci
        elif rr is None: nav=ci
        r=df.iloc[i]
        for k_,fr,_t,adj in [x for x in pend if x[2]==i]:
            if k_=="B":
                a=cash*fr
                if a>0.05: units+=a/nav; cash-=a
            else:
                s=units*fr
                if s>0: cash+=s*nav*adj; units-=s; ns+=1; gain.append(adj-1)
        pend=[x for x in pend if x[2]>i]
        sig,f=[],fmap.get(d[i],np.nan)
        if f==f and f>=75 and i-last>20 and r.r1250==r.r1250 and ci<r.r1250: sig.append(("B",0.50,1.0))
        if f==f and f>=75: last=i
        if mode=="ladder":
            dd=ci/r.peak-1
            if dd<=-RUNG[0]:
                if not in_ep: in_ep,armed[:]=True,True
                j=max([k2 for k2,th in enumerate(RUNG) if dd<=-th] or [0])
                if armed[j] and r.we: armed[j]=False; sig.append(("B",FRAC[j],1.0))
            elif in_ep and dd>=-RUNG[0]*0.5: in_ep,armed[:]=False,True
        elif r.we and r.exp==r.exp and ci<r.exp*(0.90 if nm=="创业板" else 1.0):
            sig.append(("B",0.20,1.0))
        hit = r.we if freq=="W" else r.me
        tag = "w" if freq=="W" else "m"
        if hit and r.exp==r.exp and ci>r.exp*mul and units>0:
            adj = (float(r[f"{tag}_hi"])/ci if exec_mode=="hi" else
                   float(r[f"{tag}_lo"])/ci if exec_mode=="lo" else 1.0)
            sig.append(("S",frac,adj))
        for k_,fr,adj in sig: pend.append((k_,fr,min(i+1,i1-1),adj))
        tv=cash+units*nav; curve.append(tv); pos.append(units*nav/tv)
    v=np.array(curve); pk=np.maximum.accumulate(v)
    yrs=(pd.Timestamp(d[i1-1])-pd.Timestamp(d[i0])).days/365.25
    ann=(v[-1]/100.0)**(1/yrs)-1
    vol=float(pd.Series(v).pct_change().dropna().std()*np.sqrt(250))
    return dict(ann=ann,sharpe=(ann-RF)/vol,mdd=float(((v-pk)/pk).min()),ns=ns,
                avg_gain=float(np.mean(gain)) if gain else 0.0)
data={nm:prep2(f,c,t) for nm,(f,c,t) in SRC.items()}
MODE={nm:m for nm,_,_,_,_,m in LEGS}
st={nm:first_tradable(data[nm][0],MODE[nm],None) for nm in data}
en={nm:str(data[nm][0].trade_date.iloc[-1]) for nm in data}

print("="*104)
print("周度卖出：基准对照（月频5% 与等效周频1.15%）")
print("="*104)
print(f"{'方案':>22s}"+"".join(f"{nm:>21s}" for nm in data))
print(f"{'':>22s}"+"".join(f"{'年化':>9s}{'夏普':>7s}{'卖笔':>5s}" for _ in data))
base={}
for lab,fq,fr in (("月频卖5%（现行）","M",0.05),("周频卖1.15%（等效）","W",0.0115)):
    cells=""
    for nm in data:
        r=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],fq,fr,"close")
        base[(lab,nm)]=r; cells+=f"{r['ann']:>9.2%}{r['sharpe']:>7.2f}{r['ns']:>5d}"
    print(f"{lab:>22s}"+cells)

print("\n"+"="*104)
print("周内择时天花板：卖出触发与规模不变，只改「周内哪一天成交」")
print("="*104)
print(f"{'成交时点':>22s}"+"".join(f"{nm:>21s}" for nm in data))
for lab,em in (("周末收盘（基准）","close"),("★上界：周内最高收盘","hi"),("下界：周内最低收盘","lo")):
    cells=""
    for nm in data:
        r=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"W",0.0115,em)
        b=base[("周频卖1.15%（等效）",nm)]
        cells+=f"{r['ann']:>11.2%}{(r['ann']-b['ann'])*100:>+10.2f}"
    print(f"{lab:>22s}"+cells)
print(f"\n{'每笔幅度对照':>22s}")
for nm in data:
    hw=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"W",0.0115,"hi")
    lw=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"W",0.0115,"lo")
    hm=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"M",0.05,"hi")
    print(f"  {nm:8s} 周内完美 {hw['avg_gain']:+.2%}/笔（周内振幅 {hw['avg_gain']-lw['avg_gain']:.2%}）"
          f"　vs　月内完美 {hm['avg_gain']:+.2%}/笔")
