# -*- coding: utf-8 -*-
"""月内择时的**可行域标定**（owner 2026-08-04：「为什么不在一段周期内结合技术指标算顶再卖」）

按 SOP「判据设计前置要求」：动手测任何指标之前，先算天花板。
卖出的**触发条件与规模全不变**（月频、闸上方、卖持仓 5%），**只改月内哪一天成交**：
  上界＝该月最高收盘（**完美后视，含前视，仅作天花板用**）
  下界＝该月最低收盘
  中性＝月内(高+低)/2
任何技术指标的月内择时，其效果**必然落在上下界之间**，且只能拿到上界的一小部分。

只读 results/ 下 CSV，不落库、不联网。
用法：PYTHONPATH=scripts/analysis python scripts/analysis/intramonth_timing_bound.py
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
    ym=d.trade_date.str[:6]
    # 该月的最高/最低收盘在第几个 index（用于替换成交价）
    d["ym"]=ym
    g=d.groupby("ym")["c"]
    d["m_hi"]=g.transform("max"); d["m_lo"]=g.transform("min")
    if ret is not None:
        d["nav_col"]=ret.values
    return d,ret

def run(df,ret,fmap,nm,d0,d1,mode,exec_mode="close"):
    """exec_mode: close=月末次日收盘（现行）｜hi=该月最高收盘｜lo=该月最低收盘｜mid=月内均价"""
    d,c=df.trade_date.values,df.c.values
    rr=ret.pct_change().fillna(0).values if ret is not None else None
    i0=int(np.searchsorted(d,d0)); i1=int(np.searchsorted(d,d1,side="right"))
    mul=1.30*1.10 if nm=="创业板" else 1.30
    cash,units,nav=100.0,0.0,1.0
    last,pend=-999,[]; armed,in_ep=np.ones(4,bool),False
    curve,pos,ns=[],[],0; gain=[]
    for i in range(i0,i1):
        ci=float(c[i])
        if i>i0:
            cash*=(1+CASH)**((pd.Timestamp(d[i])-pd.Timestamp(d[i-1])).days/365.25)
            nav=nav*(1+rr[i]) if rr is not None else ci
        elif rr is None: nav=ci
        r=df.iloc[i]
        for k_,fr,_t,pxadj in [x for x in pend if x[2]==i]:
            if k_=="B":
                a=cash*fr
                if a>0.05: units+=a/nav; cash-=a
            else:
                s=units*fr
                if s>0:
                    cash+=s*nav*pxadj; units-=s; ns+=1; gain.append(pxadj-1)
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
        if r.me and r.exp==r.exp and ci>r.exp*mul and units>0:
            # 成交价相对"月末收盘"的比值——这是唯一被改动的东西
            if exec_mode=="hi":   adj=float(r.m_hi)/ci
            elif exec_mode=="lo": adj=float(r.m_lo)/ci
            elif exec_mode=="mid":adj=float((r.m_hi+r.m_lo)/2)/ci
            else: adj=1.0
            sig.append(("S",0.05,adj))
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

print("="*100)
print("月内择时可行域标定：卖出触发与规模全不变，只改月内成交日")
print("="*100)
print(f"{'方案':>26s}"+"".join(f"{nm:>20s}" for nm in data))
print(f"{'':>26s}"+"".join(f"{'年化':>10s}{'Δ vs 现行':>10s}" for _ in data))
base={nm:run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"close") for nm in data}
for lab,mode in (("现行：月末收盘","close"),
                 ("★上界：月内最高收盘","hi"),
                 ("中性：月内(高+低)/2","mid"),
                 ("下界：月内最低收盘","lo")):
    cells=""
    for nm in data:
        r=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],mode)
        cells+=f"{r['ann']:>10.2%}{(r['ann']-base[nm]['ann'])*100:>+10.2f}"
    print(f"{lab:>26s}"+cells)

print("\n每笔卖出的成交价相对月末收盘的平均优势（＝月内择时能拿到的每笔幅度）：")
for nm in data:
    hi=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"hi")
    lo=run(*data[nm],fmap,nm,st[nm],en[nm],MODE[nm],"lo")
    print(f"  {nm:8s} 完美卖在月内最高 {hi['avg_gain']:+.2%}/笔｜最差卖在月内最低 {lo['avg_gain']:+.2%}/笔"
          f"｜月内平均振幅 {(hi['avg_gain']-lo['avg_gain']):.2%}")
