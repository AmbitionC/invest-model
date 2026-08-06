# -*- coding: utf-8 -*-
"""卖出执行时点敏感性扫描（owner 2026-08-04：「为什么卖出的执行时间一定要放在月末？」）

两问分开测：
  ① **月内哪一天**——把「月末」换成「月内第 k 个交易日」(k=1..20)，看结果散多大。
  ② **什么频率**——周频/月频/季频，用等效减仓速度做对照（否则是在比"卖多少"不是"多久卖一次"）。

**这是稳健性扫描，不是提案**：只报离散度、不做取舍。扫描结果若指向某个方向，
按 SOP 属"假设生成"而非证据——要改规则须另立提案 + 预登记判据 + 样本外印证。
只读 results/ 下 CSV，不落库、不联网。

用法：PYTHONPATH=scripts/analysis python scripts/analysis/sell_timing_scan.py
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
    d["k"]=d.groupby(ym).cumcount()+1          # 月内第几个交易日
    d["n_in_m"]=d.groupby(ym)["k"].transform("max")
    return d,ret

def run_k(df,ret,fmap,nm,d0,d1,mode,sell_k):
    """sell_k=None → 月末（现行）；否则为月内第 k 个交易日（k>该月天数则退到月末）。"""
    d,c=df.trade_date.values,df.c.values
    rr=ret.pct_change().fillna(0).values if ret is not None else None
    i0=int(np.searchsorted(d,d0)); i1=int(np.searchsorted(d,d1,side="right"))
    mul=1.30*1.10 if nm=="创业板" else 1.30
    cash,units,nav=100.0,0.0,1.0
    last,pend=-999,[]; armed,in_ep=np.ones(4,bool),False
    curve,pos,ns=[],[],0
    for i in range(i0,i1):
        ci=float(c[i])
        if i>i0:
            cash*=(1+CASH)**((pd.Timestamp(d[i])-pd.Timestamp(d[i-1])).days/365.25)
            nav=nav*(1+rr[i]) if rr is not None else ci
        elif rr is None: nav=ci
        r=df.iloc[i]
        for k_,fr,_t in [x for x in pend if x[2]==i]:
            if k_=="B":
                a=cash*fr
                if a>0.05: units+=a/nav; cash-=a
            else:
                s=units*fr
                if s>0: cash+=s*nav; units-=s; ns+=1
        pend=[x for x in pend if x[2]>i]
        sig,f=[],fmap.get(d[i],np.nan)
        if f==f and f>=75 and i-last>20 and r.r1250==r.r1250 and ci<r.r1250: sig.append(("B",0.50))
        if f==f and f>=75: last=i
        if mode=="ladder":
            dd=ci/r.peak-1
            if dd<=-RUNG[0]:
                if not in_ep: in_ep,armed[:]=True,True
                j=max([k2 for k2,th in enumerate(RUNG) if dd<=-th] or [0])
                if armed[j] and r.we: armed[j]=False; sig.append(("B",FRAC[j]))
            elif in_ep and dd>=-RUNG[0]*0.5: in_ep,armed[:]=False,True
        elif r.we and r.exp==r.exp and ci<r.exp*(0.90 if nm=="创业板" else 1.0):
            sig.append(("B",0.20))
        hit = r.me if sell_k is None else (r.k==min(sell_k,r.n_in_m))
        if hit and r.exp==r.exp and ci>r.exp*mul and units>0: sig.append(("S",0.05))
        for k_,fr in sig: pend.append((k_,fr,min(i+1,i1-1)))
        tv=cash+units*nav; curve.append(tv); pos.append(units*nav/tv)
    v=np.array(curve); pk=np.maximum.accumulate(v)
    yrs=(pd.Timestamp(d[i1-1])-pd.Timestamp(d[i0])).days/365.25
    ann=(v[-1]/100.0)**(1/yrs)-1
    vol=float(pd.Series(v).pct_change().dropna().std()*np.sqrt(250))
    return dict(ann=ann,sharpe=(ann-RF)/vol,mdd=float(((v-pk)/pk).min()),ns=ns)

data={nm:prep2(f,c,t) for nm,(f,c,t) in SRC.items()}
MODE={nm:m for nm,_,_,_,_,m in LEGS}
st={nm:first_tradable(data[nm][0],MODE[nm],None) for nm in data}
en={nm:str(data[nm][0].trade_date.iloc[-1]) for nm in data}

print("卖出执行日敏感性：月内第 k 个交易日（k=1 是月初第一天；「月末」＝现行）\n")
print(f"{'执行日':>8s}" + "".join(f"{nm:>26s}" for nm in data))
print(f"{'':>8s}" + "".join(f"{'年化':>9s}{'夏普':>8s}{'卖笔数':>9s}" for _ in data))
rows={}
for k in [None,1,3,5,8,10,12,15,18,20]:
    lab="月末" if k is None else f"第{k}个"
    cells=""; rec={}
    for nm in data:
        d,ret=data[nm]
        r=run_k(d,ret,fmap,nm,st[nm],en[nm],MODE[nm],k)
        cells+=f"{r['ann']:>9.2%}{r['sharpe']:>8.2f}{r['ns']:>9d}"
        rec[nm]=r
    rows[lab]=rec
    print(f"{lab:>8s}"+cells)

print("\n各腿在 k=1..20 全部执行日上的离散度（不含月末）：")
allk={}
for nm in data:
    d,ret=data[nm]
    vals=[run_k(d,ret,fmap,nm,st[nm],en[nm],MODE[nm],k) for k in range(1,21)]
    a=[v["ann"]*100 for v in vals]; s=[v["sharpe"] for v in vals]
    me=run_k(d,ret,fmap,nm,st[nm],en[nm],MODE[nm],None)
    allk[nm]=(a,s,me)
    print(f"  {nm:8s} 年化 min {min(a):.2f}% / 中位 {np.median(a):.2f}% / max {max(a):.2f}%"
          f"  极差 {max(a)-min(a):.2f}pp｜月末现行 {me['ann']*100:.2f}%"
          f"（排在 {sum(1 for x in a if x < me['ann']*100)}/20 分位）"
          f"｜夏普极差 {max(s)-min(s):.3f}")

print("\n" + "="*96)
print("卖出检查频率：周频 vs 月频 vs 季频（同等减仓速度做对照）")
print("="*96)
def run_freq(df,ret,fmap,nm,d0,d1,mode,freq,frac):
    d,c=df.trade_date.values,df.c.values
    rr=ret.pct_change().fillna(0).values if ret is not None else None
    i0=int(np.searchsorted(d,d0)); i1=int(np.searchsorted(d,d1,side="right"))
    mul=1.30*1.10 if nm=="创业板" else 1.30
    cash,units,nav=100.0,0.0,1.0
    last,pend=-999,[]; armed,in_ep=np.ones(4,bool),False
    curve,pos,ns=[],[],0
    for i in range(i0,i1):
        ci=float(c[i])
        if i>i0:
            cash*=(1+CASH)**((pd.Timestamp(d[i])-pd.Timestamp(d[i-1])).days/365.25)
            nav=nav*(1+rr[i]) if rr is not None else ci
        elif rr is None: nav=ci
        r=df.iloc[i]
        for k_,fr,_t in [x for x in pend if x[2]==i]:
            if k_=="B":
                a=cash*fr
                if a>0.05: units+=a/nav; cash-=a
            else:
                s=units*fr
                if s>0: cash+=s*nav; units-=s; ns+=1
        pend=[x for x in pend if x[2]>i]
        sig,f=[],fmap.get(d[i],np.nan)
        if f==f and f>=75 and i-last>20 and r.r1250==r.r1250 and ci<r.r1250: sig.append(("B",0.50))
        if f==f and f>=75: last=i
        if mode=="ladder":
            dd=ci/r.peak-1
            if dd<=-RUNG[0]:
                if not in_ep: in_ep,armed[:]=True,True
                j=max([k2 for k2,th in enumerate(RUNG) if dd<=-th] or [0])
                if armed[j] and r.we: armed[j]=False; sig.append(("B",FRAC[j]))
            elif in_ep and dd>=-RUNG[0]*0.5: in_ep,armed[:]=False,True
        elif r.we and r.exp==r.exp and ci<r.exp*(0.90 if nm=="创业板" else 1.0):
            sig.append(("B",0.20))
        if freq=="W": hit=r.we
        elif freq=="M": hit=r.me
        else:
            hit=r.me and int(str(d[i])[4:6])%3==0
        if hit and r.exp==r.exp and ci>r.exp*mul and units>0: sig.append(("S",frac))
        for k_,fr in sig: pend.append((k_,fr,min(i+1,i1-1)))
        tv=cash+units*nav; curve.append(tv); pos.append(units*nav/tv)
    v=np.array(curve); pk=np.maximum.accumulate(v)
    yrs=(pd.Timestamp(d[i1-1])-pd.Timestamp(d[i0])).days/365.25
    ann=(v[-1]/100.0)**(1/yrs)-1
    vol=float(pd.Series(v).pct_change().dropna().std()*np.sqrt(250))
    return dict(ann=ann,sharpe=(ann-RF)/vol,mdd=float(((v-pk)/pk).min()),ns=ns,
                pos=float(np.mean(pos)))
CASES=[("周频 卖1.15%","W",0.0115),("周频 卖5%","W",0.05),
       ("月频 卖5%（现行）","M",0.05),("季频 卖15%","Q",0.15)]
print(f"{'方案':>18s}"+"".join(f"{nm:>30s}" for nm in data))
print(f"{'':>18s}"+"".join(f"{'年化':>9s}{'夏普':>8s}{'回撤':>8s}{'卖笔':>5s}" for _ in data))
for lab,fq,fr in CASES:
    cells=""
    for nm in data:
        d,ret=data[nm]
        r=run_freq(d,ret,fmap,nm,st[nm],en[nm],MODE[nm],fq,fr)
        cells+=f"{r['ann']:>9.2%}{r['sharpe']:>8.2f}{r['mdd']:>8.1%}{r['ns']:>5d}"
    print(f"{lab:>18s}"+cells)
print("\n  注：周频卖 1.15% ≈ 月频卖 5% 的等效减仓速度（1−0.9885^4.33≈5%）")
