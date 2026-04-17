# =============================================================================
# SNAS FastAPI Server — Somali National Address System
# Version: 2.0 — Uses Supabase Python client (no DB password needed)
# Project: fglmvdewfvxlqiwhpwue (West EU, Ireland)
# Organisation: Amir Geosystems
# =============================================================================

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client
from typing import Optional

SUPABASE_URL = "https://fglmvdewfvxlqiwhpwue.supabase.co"
SUPABASE_KEY = "sb_publishable_MOT4mA0OUFjPLDsRmWU0aA_OK3zUDhw"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI(
    title="Somali National Address System API",
    description="UPU S42-compliant bilingual address system. 333,055 roads. 92 districts. 8 Federal States.",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", tags=["System"])
def root():
    return {"system": "Somali National Address System", "version": "2.0.0", "organisation": "Amir Geosystems", "status": "live", "docs": "/docs"}

@app.get("/v1/health", tags=["System"])
def health_check():
    try:
        states    = supabase.table("snas_states").select("*", count="exact").execute()
        districts = supabase.table("snas_districts").select("*", count="exact").execute()
        streets   = supabase.table("snas_streets").select("*", count="exact").execute()
        return {"status": "healthy", "counts": {"states": states.count, "districts": districts.count, "streets": streets.count}}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

@app.get("/v1/stats", tags=["System"])
def national_stats():
    try:
        states    = supabase.table("snas_states").select("*", count="exact").execute()
        districts = supabase.table("snas_districts").select("*", count="exact").execute()
        streets   = supabase.table("snas_streets").select("*", count="exact").execute()
        named     = supabase.table("snas_streets").select("*", count="exact").eq("is_nameless", False).execute()
        nameless  = supabase.table("snas_streets").select("*", count="exact").eq("is_nameless", True).execute()
        figures   = supabase.table("snas_figures").select("*", count="exact").eq("is_eligible", True).execute()
        total = streets.count or 0
        return {"system": "Somali National Address System", "stats": {"states": states.count, "districts": districts.count, "total_streets": total, "named_streets": named.count, "nameless_streets": nameless.count, "pct_named": round(named.count / total * 100, 1) if total > 0 else 0, "approved_figures": figures.count}}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/states", tags=["Reference Data"])
def list_states():
    try:
        res = supabase.table("snas_states").select("*").order("name_en").execute()
        return {"count": len(res.data), "states": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/districts", tags=["Reference Data"])
def list_districts(state: Optional[str] = Query(None)):
    try:
        res = supabase.table("snas_districts").select("postcode_prefix, name_en, name_so, is_urban, survey_status, region_id").order("name_en").execute()
        return {"count": len(res.data), "districts": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/postcode/{prefix}", tags=["Address Lookup"])
def postcode_lookup(prefix: str):
    try:
        res = supabase.table("snas_districts").select("postcode_prefix, name_en, name_so, is_urban, survey_status").eq("postcode_prefix", prefix.upper()).execute()
        if not res.data:
            raise HTTPException(status_code=404, detail=f"Postcode '{prefix.upper()}' not found")
        return res.data[0]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/geocode", tags=["Address Lookup"])
def forward_geocode(q: str = Query(...), limit: int = Query(10, ge=1, le=50)):
    try:
        res = supabase.table("snas_streets").select("street_id, name_en, name_so, highway_class, is_nameless, naming_tier, district_id").ilike("name_en", f"%{q}%").limit(limit).execute()
        res_so = supabase.table("snas_streets").select("street_id, name_en, name_so, highway_class, is_nameless, naming_tier, district_id").ilike("name_so", f"%{q}%").limit(limit).execute()
        seen = set()
        results = []
        for row in (res.data or []) + (res_so.data or []):
            if row["street_id"] not in seen:
                seen.add(row["street_id"])
                results.append(row)
        return {"query": q, "count": len(results), "results": results[:limit]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/districts/{prefix}/streets", tags=["Address Lookup"])
def district_streets(prefix: str, nameless: Optional[bool] = Query(None), limit: int = Query(100, ge=1, le=1000), offset: int = Query(0, ge=0)):
    try:
        d = supabase.table("snas_districts").select("district_id, name_en").eq("postcode_prefix", prefix.upper()).execute()
        if not d.data:
            raise HTTPException(status_code=404, detail=f"District '{prefix.upper()}' not found")
        district_id = d.data[0]["district_id"]
        q = supabase.table("snas_streets").select("street_id, name_en, name_so, highway_class, is_nameless, naming_tier").eq("district_id", district_id).range(offset, offset + limit - 1)
        if nameless is not None:
            q = q.eq("is_nameless", nameless)
        res = q.execute()
        return {"district": prefix.upper(), "district_name": d.data[0]["name_en"], "count": len(res.data), "streets": res.data}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/figures", tags=["Reference Data"])
def list_figures(tier: Optional[int] = Query(None), gender: Optional[str] = Query(None)):
    try:
        q = supabase.table("snas_figures").select("figure_id, full_name_so, full_name_en, tier, category, gender, birth_year, death_year, biography_en, values_tags").eq("is_eligible", True)
        if tier:
            q = q.eq("tier", tier)
        if gender:
            q = q.eq("gender", gender.upper())
        res = q.order("tier").order("full_name_en").execute()
        return {"count": len(res.data), "figures": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/gender-audit", tags=["Compliance"])
def gender_audit():
    try:
        res = supabase.table("vw_district_gender_stats").select("*").execute()
        data = res.data or []
        return {"total_districts": len(data), "compliant": sum(1 for r in data if r.get("mandate_status") == "COMPLIANT"), "non_compliant": sum(1 for r in data if r.get("mandate_status") == "NON_COMPLIANT"), "districts": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/survey-status", tags=["Operations"])
def survey_status():
    try:
        res = supabase.table("vw_district_survey_status").select("*").execute()
        return {"total_districts": len(res.data), "districts": res.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("SNAS_FastAPI_Server:app", host="0.0.0.0", port=8000, reload=True)
