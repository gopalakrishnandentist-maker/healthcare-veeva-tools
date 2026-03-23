#!/usr/bin/env python3
"""
HCO Duplicate Check Tool (Veeva-style HCO extracts with exploded addresses)

Usage:
  python hco_dupe_tool.py --input "raw_hco.xlsx" --sheet "Sheet0" --outdir "./out" --shared-threshold 5

Outputs (in outdir):
  - HCO_1row_per_VID.xlsx
  - HCO_Dupe_Check_Outputs.xlsx
  - AUTO_MERGE.csv, REVIEW.csv, NOT_DUP.csv, AUTO_MERGE_CLUSTERS.csv, Shared_Phones.csv

Approach:
  1) Collapse to 1 row per hco.vid__v (aggregate phones/pins/cities/states/addresses as sets)
  2) Candidate generation (blocking) to avoid O(n^2):
       - name+city, pin+nameprefix, phone, pin+addrprefix
  3) Rule-based classification:
       AUTO (Green):
         G1 NAME + PIN + STRONG ADDRESS
         G2 PHONE + PIN + STRONG NAME (phone must not be shared >= threshold)
       NOT DUP (Red):
         R2 GEO_CONFLICT_NO_LOCK (strong name but state/pin conflict and no phone match)
       REVIEW (Amber):
         scored candidates that don't qualify for AUTO/NOT_DUP

Notes:
  - HCO matching is inherently riskier than HCP. These AUTO rules are intentionally conservative.
  - 'Shared phone' guardrail is critical (facility switchboards reused across records).
"""

from __future__ import annotations
import argparse, os, re, unicodedata
from collections import defaultdict, Counter
import pandas as pd

try:
    from rapidfuzz import fuzz
    def sim(a,b):
        return fuzz.token_sort_ratio(a,b)
except Exception:
    import difflib
    def sim(a,b):
        return int(100*difflib.SequenceMatcher(None,a,b).ratio())

def _norm_text(x):
    if pd.isna(x):
        return ""
    s=str(x).strip().lower()
    s=unicodedata.normalize("NFKD", s)
    s="".join(ch for ch in s if not unicodedata.combining(ch))
    s=re.sub(r"[^a-z0-9\s]", " ", s)
    s=re.sub(r"\s+", " ", s).strip()
    return s

def norm_phone(x):
    if pd.isna(x):
        return None
    digits=re.sub(r"\D", "", str(x))
    if len(digits)<8:
        return None
    if len(digits)>10:
        digits=digits[-10:]
    return digits

def norm_pin(x):
    if pd.isna(x):
        return None
    d=re.sub(r"\D","", str(x))
    if not d or d=="0":
        return None
    if len(d)>6:
        d=d[-6:]
    return d

def agg_set(series, norm_func):
    out=set()
    for v in series:
        nv=norm_func(v)
        if nv:
            out.add(nv)
    return out

class DSU:
    def __init__(self):
        self.parent={}
        self.rank={}
    def find(self,x):
        if x not in self.parent:
            self.parent[x]=x; self.rank[x]=0
        if self.parent[x]!=x:
            self.parent[x]=self.find(self.parent[x])
        return self.parent[x]
    def union(self,a,b):
        ra, rb = self.find(a), self.find(b)
        if ra==rb: return
        if self.rank[ra]<self.rank[rb]:
            ra,rb=rb,ra
        self.parent[rb]=ra
        if self.rank[ra]==self.rank[rb]:
            self.rank[ra]+=1

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--sheet", default=None)
    ap.add_argument("--outdir", default=".")
    ap.add_argument("--shared-threshold", type=int, default=5)
    args=ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)

    # Load
    if args.input.lower().endswith(".csv"):
        df=pd.read_csv(args.input)
    else:
        xl=pd.ExcelFile(args.input)
        sheet=args.sheet or xl.sheet_names[0]
        df=pd.read_excel(args.input, sheet_name=sheet)

    # Columns (as provided)
    vid_col='hco.vid__v (VID)'
    name_col='hco.corporate_name__v (CORPORATE NAME)'
    city_col='address.locality__v (CITY)'
    state_col='address.administrative_area__v (STATE/PROVINCE)'
    pin_col='address.postal_code__v (ZIP/POSTAL CODE)'
    addr1_col='address.address_line_1__v (ADDRESS LINE 1)'
    fmt_addr_col='address.formatted_address__v (FULL ADDRESS)'
    phone_col='address.phone_1__v (PHONE 1)'
    hco_type_col='hco.hco_type__v (HCO TYPE)'
    major_col='hco.major_class_of_trade__v (MAJOR CLASS OF TRADE)'

    # Normalized helpers
    df["_hco_name_norm"]=df[name_col].apply(_norm_text)
    df["_city_norm"]=df[city_col].apply(_norm_text)
    df["_state_norm"]=df[state_col].apply(_norm_text)
    df["_pin_norm"]=df[pin_col].apply(norm_pin)
    df["_addr1_norm"]=df[addr1_col].apply(_norm_text)
    df["_fmtaddr_norm"]=df[fmt_addr_col].apply(_norm_text)
    df["_phone_norm"]=df[phone_col].apply(norm_phone)
    df["_hco_type_norm"]=df[hco_type_col].apply(_norm_text) if hco_type_col in df.columns else ""
    df["_major_norm"]=df[major_col].apply(_norm_text) if major_col in df.columns else ""

    # Collapse to 1 row per HCO
    rows=[]
    for vid, g in df.groupby(vid_col, sort=False):
        name_mode = g["_hco_name_norm"].value_counts().idxmax() if g["_hco_name_norm"].notna().any() else ""
        phones = agg_set(g["_phone_norm"], lambda x: x)
        pins = agg_set(g["_pin_norm"], lambda x: x)
        cities = set(g["_city_norm"].dropna().astype(str).tolist()); cities.discard("")
        states = set(g["_state_norm"].dropna().astype(str).tolist()); states.discard("")
        addr1s = set(g["_addr1_norm"].dropna().astype(str).tolist()); addr1s.discard("")
        fmtaddrs = set(g["_fmtaddr_norm"].dropna().astype(str).tolist()); fmtaddrs.discard("")
        addresses = addr1s if addr1s else fmtaddrs

        hco_type = g["_hco_type_norm"].value_counts().idxmax() if g["_hco_type_norm"].notna().any() else ""
        major = g["_major_norm"].value_counts().idxmax() if g["_major_norm"].notna().any() else ""

        rows.append({
            "vid": str(vid),
            "name_norm": name_mode,
            "phones": sorted([p for p in phones if p]),
            "pins": sorted([p for p in pins if p]),
            "cities": sorted([c for c in cities if c]),
            "states": sorted([s for s in states if s]),
            "addresses": sorted([a for a in addresses if a]),
            "hco_type": hco_type,
            "major_class": major,
        })

    canon=pd.DataFrame(rows)
    canon_idx=canon.set_index("vid", drop=False)

    # Shared phone guardrail
    phone_freq=Counter()
    for _, r in canon.iterrows():
        for p in r["phones"]:
            phone_freq[p]+=1
    shared_phones={p for p,c in phone_freq.items() if c>=args.shared_threshold}

    # Blocking
    blocks=defaultdict(list)
    for vid, r in canon_idx.iterrows():
        name=r["name_norm"] or ""
        city=(r["cities"][0] if r["cities"] else "")
        name_prefix=name[:8] if name else ""
        city_key=city[:12] if city else ""
        if name and city_key:
            blocks[("name_city", name, city_key)].append(vid)
        for pin in r["pins"]:
            if pin and name_prefix:
                blocks[("pin_name", pin, name_prefix)].append(vid)
        for p in r["phones"]:
            if p:
                blocks[("phone", p)].append(vid)
        for pin in r["pins"]:
            if pin and r["addresses"]:
                ap=(r["addresses"][0][:10])
                blocks[("pin_addrp", pin, ap)].append(vid)

    cand_pairs=set()
    for _, vids in blocks.items():
        if len(vids)<2:
            continue
        if len(vids)>500:
            vids=vids[:500]
        for i in range(len(vids)):
            for j in range(i+1, len(vids)):
                a,b=vids[i], vids[j]
                if a>b: a,b=b,a
                cand_pairs.add((a,b))

    # Compare helpers
    def pin_overlap(a,b):
        return len(set(a["pins"]) & set(b["pins"]))>0
    def state_overlap(a,b):
        return len(set(a["states"]) & set(b["states"]))>0
    def city_overlap(a,b):
        return len(set(a["cities"]) & set(b["cities"]))>0
    def phone_overlap(a,b):
        return set(a["phones"]) & set(b["phones"])
    def addr_best(a):
        if a["addresses"]:
            return max(a["addresses"], key=len)
        return ""
    def addr_sim(a,b):
        aa=addr_best(a); bb=addr_best(b)
        if not aa or not bb:
            return 0
        return sim(aa,bb)

    AUTO=[]; REVIEW=[]; NOTD=[]
    for a,b in sorted(cand_pairs):
        ar=canon_idx.loc[a]; br=canon_idx.loc[b]
        ns=sim(ar["name_norm"], br["name_norm"]) if ar["name_norm"] and br["name_norm"] else 0
        ps=phone_overlap(ar, br)
        pins_ok=pin_overlap(ar, br)
        addrS=addr_sim(ar, br)
        city_ok=city_overlap(ar, br)
        state_ok=state_overlap(ar, br)

        # AUTO rules
        if ns>=92 and pins_ok and addrS>=90:
            AUTO.append((a,b,"G1_NAME+PIN+ADDRESS", ns, addrS, int(pins_ok), int(city_ok), int(state_ok),
                         ";".join(sorted(ps))[:200]))
            continue

        if ps and pins_ok and ns>=92 and not any(p in shared_phones for p in ps):
            AUTO.append((a,b,"G2_PHONE+PIN+STRONG_NAME", ns, addrS, int(pins_ok), int(city_ok), int(state_ok),
                         ";".join(sorted(ps))[:200]))
            continue

        # NOT DUP
        if ns>=92 and (not pins_ok) and (not state_ok) and (not ps):
            NOTD.append((a,b,"R2_GEO_CONFLICT_NO_LOCK", ns, addrS, int(pins_ok), int(city_ok), int(state_ok), ""))
            continue

        # REVIEW
        score=0; reasons=[]
        if ns>=92: score+=35; reasons.append("NAME_STRONG")
        elif ns>=85: score+=25; reasons.append("NAME_MED")
        elif ns>=75: score+=15; reasons.append("NAME_WEAK")

        if pins_ok: score+=25; reasons.append("PIN")
        if city_ok: score+=10; reasons.append("CITY")
        if state_ok: score+=10; reasons.append("STATE")
        if addrS>=90: score+=25; reasons.append("ADDR_STRONG")
        elif addrS>=80: score+=15; reasons.append("ADDR_MED")
        if ps:
            if any(p in shared_phones for p in ps):
                score+=5; reasons.append("PHONE_SHARED")
            else:
                score+=15; reasons.append("PHONE")

        if ar["hco_type"] and br["hco_type"] and ar["hco_type"]==br["hco_type"]:
            score+=5; reasons.append("TYPE_MATCH")
        if ar["major_class"] and br["major_class"] and ar["major_class"]==br["major_class"]:
            score+=5; reasons.append("MAJOR_MATCH")

        if score>=50:
            REVIEW.append((a,b,"REVIEW", score, ns, addrS, int(pins_ok), int(city_ok), int(state_ok),
                           ";".join(sorted(ps))[:200], ",".join(reasons)))
        else:
            NOTD.append((a,b,"LOW_SCORE", ns, addrS, int(pins_ok), int(city_ok), int(state_ok), ";".join(sorted(ps))[:200]))

    auto_df=pd.DataFrame(AUTO, columns=["vid_a","vid_b","rule","name_similarity","address_similarity","pin_overlap","city_overlap","state_overlap","matched_phones"])
    review_df=pd.DataFrame(REVIEW, columns=["vid_a","vid_b","status","score","name_similarity","address_similarity","pin_overlap","city_overlap","state_overlap","matched_phones","reasons"])
    notdup_df=pd.DataFrame(NOTD, columns=["vid_a","vid_b","reason","name_similarity","address_similarity","pin_overlap","city_overlap","state_overlap","matched_phones"])

    # clusters
    dsu=DSU()
    for a,b, *_ in AUTO:
        dsu.union(a,b)
    clusters=defaultdict(set)
    for a,b, *_ in AUTO:
        root=dsu.find(a)
        clusters[root].add(a); clusters[root].add(b)
    cluster_rows=[]
    cid=1
    for _, members in clusters.items():
        if len(members)<2:
            continue
        for vid in sorted(members):
            cluster_rows.append((cid, vid))
        cid+=1
    clusters_df=pd.DataFrame(cluster_rows, columns=["cluster_id","vid"])

    shared_df=pd.DataFrame(
        [{"phone":p, "vid_count":c} for p,c in phone_freq.items() if c>=args.shared_threshold]
    ).sort_values("vid_count", ascending=False) if phone_freq else pd.DataFrame(columns=["phone","vid_count"])

    summary_df=pd.DataFrame([{
        "raw_rows": df.shape[0],
        "raw_columns": df.shape[1],
        "unique_hco_vids": canon.shape[0],
        "candidate_pairs_evaluated": len(cand_pairs),
        "auto_merge_pairs": len(auto_df),
        "review_pairs": len(review_df),
        "not_dup_pairs": len(notdup_df),
        "auto_clusters": clusters_df["cluster_id"].nunique() if not clusters_df.empty else 0,
        "shared_threshold": args.shared_threshold,
        "shared_phones_count": len(shared_phones),
    }])

    # write outputs
    canon_out=os.path.join(args.outdir,"HCO_1row_per_VID.xlsx")
    out_xlsx=os.path.join(args.outdir,"HCO_Dupe_Check_Outputs.xlsx")
    auto_csv=os.path.join(args.outdir,"AUTO_MERGE.csv")
    review_csv=os.path.join(args.outdir,"REVIEW.csv")
    notdup_csv=os.path.join(args.outdir,"NOT_DUP.csv")
    clusters_csv=os.path.join(args.outdir,"AUTO_MERGE_CLUSTERS.csv")
    shared_csv=os.path.join(args.outdir,"Shared_Phones.csv")

    with pd.ExcelWriter(canon_out, engine="openpyxl") as w:
        canon.to_excel(w, index=False, sheet_name="HCO_Canonical")

    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as w:
        summary_df.to_excel(w, index=False, sheet_name="Summary")
        canon.to_excel(w, index=False, sheet_name="HCO_Canonical")
        auto_df.to_excel(w, index=False, sheet_name="AUTO_MERGE")
        review_df.to_excel(w, index=False, sheet_name="REVIEW")
        notdup_df.to_excel(w, index=False, sheet_name="NOT_DUP")
        clusters_df.to_excel(w, index=False, sheet_name="AUTO_CLUSTERS")
        shared_df.to_excel(w, index=False, sheet_name="Shared_Phones")

    auto_df.to_csv(auto_csv, index=False)
    review_df.to_csv(review_csv, index=False)
    notdup_df.to_csv(notdup_csv, index=False)
    clusters_df.to_csv(clusters_csv, index=False)
    shared_df.to_csv(shared_csv, index=False)

    print("Done.")
    print("Wrote:", canon_out)
    print("Wrote:", out_xlsx)

if __name__ == "__main__":
    main()
