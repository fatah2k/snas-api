from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional
import httpx

SUPABASE_URL = "https://fglmvdewfvxlqiwhpwue.supabase.co"
SUPABASE_KEY = "sb_publishable_MOT4mA0OUFjPLDsRmWU0aA_OK3zUDhw"

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

app = FastAPI(title="Somali National Address System API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def db(table, params=""):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}"
    r = httpx.get(url, headers={**HEADERS, "Prefer": "count=exact"})
    return r.json(), int(r.headers.get("content-range", "0/0").split("/")[-1])

@app.get("/")
def root():
    return {"system": "Somali National Address System", "version": "2.0.0", "status": "live", "docs": "/docs"}

@app.get("/v1/health")
def health():
    _, s = db("snas_states")
    _, d = db("snas_districts")
    _, st = db("snas_streets")
    return {"status": "healthy", "counts": {"states": s, "districts": d, "streets": st}}

@app.get("/v1/stats")
def stats():
    _, s = db("snas_states")
    _, d = db("snas_districts")
    _, total = db("snas_streets")
    _, named = db("snas_streets", "is_nameless=eq.false")
    _, nameless = db("snas_streets", "is_nameless=eq.true")
    return {"stats": {"states": s, "districts": d, "total_streets": total, "named_streets": named, "nameless_streets": nameless, "pct_named": round(named/total*100,1) if total else 0}}

@app.get("/v1/states")
def states():
    data, _ = db("snas_states", "order=name_en")
    return {"count": len(data), "states": data}

@app.get("/v1/districts")
def districts():
    data, _ = db("snas_districts", "select=postcode_prefix,name_en,name_so,is_urban,survey_status&order=name_en")
    return {"count": len(data), "districts": data}

@app.get("/v1/postcode/{prefix}")
def postcode(prefix: str):
    data, _ = db("snas_districts", f"postcode_prefix=eq.{prefix.upper()}&select=postcode_prefix,name_en,name_so,is_urban,survey_status")
    if not data:
        raise HTTPException(404, f"Postcode '{prefix}' not found")
    return data[0]

@app.get("/v1/geocode")
def geocode(q: str = Query(...), limit: int = Query(10)):
    data, _ = db("snas_streets", f"name_en=ilike.*{q}*&select=street_id,name_en,name_so,highway_class,is_nameless,district_id&limit={limit}")
    return {"query": q, "count": len(data), "results": data}

@app.get("/v1/districts/{prefix}/streets")
def district_streets(prefix: str, nameless: Optional[bool] = None, limit: int = 100, offset: int = 0):
    d, _ = db("snas_districts", f"postcode_prefix=eq.{prefix.upper()}&select=district_id,name_en")
    if not d:
        raise HTTPException(404, "District not found")
    did = d[0]["district_id"]
    extra = f"&is_nameless=eq.{str(nameless).lower()}" if nameless is not None else ""
    data, _ = db("snas_streets", f"district_id=eq.{did}&select=street_id,name_en,name_so,highway_class,is_nameless&limit={limit}&offset={offset}{extra}")
    return {"district": prefix.upper(), "count": len(data), "streets": data}

@app.get("/v1/figures")
def figures(tier: Optional[int] = None, gender: Optional[str] = None):
    params = "is_eligible=eq.true&select=figure_id,full_name_so,full_name_en,tier,category,gender,biography_en&order=tier"
    if tier:
        params += f"&tier=eq.{tier}"
    if gender:
        params += f"&gender=eq.{gender.upper()}"
    data, _ = db("snas_figures", params)
    return {"count": len(data), "figures": data}

@app.get("/v1/survey-status")
def survey():
    data, _ = db("vw_district_survey_status")
    return {"total": len(data), "districts": data}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("SNAS_FastAPI_Server:app", host="0.0.0.0", port=8000, reload=True)
