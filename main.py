"""
서울 재개발/재건축 현황 추적 v2
- 뉴스 기반 단계 자동 업데이트
- 뉴스 기반 시세(초투/총투) 반자동 추출
- 재개발/재건축 구분
- 불확실 데이터 ⚠️ 표시 및 수동 확인 기능
"""
import os
import asyncio
import re
import sqlite3
import io
import json
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager
from collections import Counter
from typing import Optional

import httpx
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import openpyxl

NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
PORT = int(os.getenv("PORT", 8000))

DB_PATH = Path("/tmp/redev.db")

STAGE_KEYWORDS = {
    "관리처분인가": ["관리처분계획인가", "관리처분 인가", "관리처분인가"],
    "사업시행인가": ["사업시행계획인가", "사업시행 인가", "사업시행인가"],
    "이주": ["이주 개시", "이주 시작", "이주율"],
    "철거": ["철거 개시", "철거 진행", "본격 철거"],
    "착공": ["착공", "기공식", "공사 시작"],
    "분양": ["일반분양", "분양 개시", "분양 공고"],
    "준공": ["준공", "입주 시작", "입주 완료"],
}
STAGE_ORDER = {
    "사업시행인가": 3, "관리처분인가": 4,
    "관리처분<철거>": 5, "관리처분<분양>": 5,
    "이주": 5, "철거": 5, "착공/분양": 6, "준공/입주": 7,
}

# ─── 시드 데이터: type 필드 추가 (재개발/재건축) ──────
# 방배 5·6·13·14는 재건축, 나머지는 재개발
SEED = [
    ("동작","재개발","관리처분인가",4,"노량진1",2992,13.2,19.5,"8","2026.4.21 관리처분인가"),
    ("동작","재개발","관리처분<철거>",5,"노량진2",415,11.8,17.3,"4",""),
    ("동작","재개발","사업시행인가",3,"노량진3",1150,12.2,20.5,"8.5",""),
    ("동작","재개발","관리처분인가",4,"노량진4",845,12.3,18.8,"4.5",""),
    ("동작","재개발","관리처분인가",4,"노량진5",743,11.3,18.7,"5",""),
    ("동작","재개발","착공/분양",6,"노량진6",1499,13.2,18.2,"분양중","라클라체자이드파인"),
    ("동작","재개발","관리처분인가",4,"노량진7",576,10,18.3,"5.5",""),
    ("동작","재개발","관리처분<철거>",5,"노량진8",983,12.4,18.8,"4.5",""),
    ("동작","재개발","관리처분<철거>",5,"흑석9",1536,17.1,22.2,"4",""),
    ("동작","재개발","관리처분<철거>",5,"흑석11",1522,17.8,20.8,"4",""),
    ("동작","재개발","사업시행인가",3,"상도5",508,4.5,13.4,"7.5",""),
    ("관악","재개발","관리처분<분양>",5,"봉천412",997,7.3,13.2,"0.25",""),
    ("관악","재개발","사업시행인가",3,"봉천413",855,6,13.2,"7.5",""),
    ("관악","재개발","관리처분<철거>",5,"신림2",1487,5.1,9.7,"4",""),
    ("관악","재개발","관리처분<분양>",5,"신림3",571,1.4,10.6,"1",""),
    ("영등포","재개발","관리처분<분양>",5,"당산12",707,11.1,15.8,"1.5",""),
    ("영등포","재개발","관리처분<철거>",5,"영등포1-13",659,11.6,13.8,"3.5",""),
    ("양천","재개발","사업시행인가",3,"신정1-3",211,2.5,9.6,"5.5",""),
    ("양천","재개발","사업시행인가",3,"신정4",1713,4.4,12.4,"8",""),
    ("강서","재개발","관리처분인가",4,"방화5",1526,5.3,13.5,"6",""),
    ("강서","재개발","사업시행인가",3,"방화3",1476,4.9,12.3,"8.5",""),
    ("서초","재건축","관리처분<분양>",6,"방배5",3065,24,30.8,"2","디에이치방배 2026.8입주"),
    ("서초","재건축","관리처분<철거>",5,"방배13",2369,18.4,27.5,"4.5",""),
    ("서초","재건축","관리처분<철거>",5,"방배6",1111,19.4,25.6,"2",""),
    ("서초","재건축","관리처분<철거>",5,"방배14",487,19.7,22.8,"3.5",""),
    ("송파","재개발","관리처분인가",4,"마천4",1381,8,16.3,"5.5",""),
    ("송파","재개발","사업시행인가",3,"마천3",2364,6.3,15.9,"9.5",""),
    ("용산","재개발","관리처분인가",4,"한남2",1537,20.2,30.5,"7.5",""),
    ("용산","재개발","관리처분<철거>",5,"한남3",5816,20.2,33.9,"5.5",""),
    ("용산","재개발","사업시행인가",3,"한남4",2686,22,32,"10",""),
    ("용산","재개발","사업시행인가",3,"한남5",2660,22,34.5,"10",""),
    ("중구","재개발","관리처분인가",4,"신당8",1215,8.5,16.5,"5.5",""),
    ("성동","재개발","관리처분인가",4,"금호16",595,6.9,15.8,"5.5",""),
    ("성동","재개발","사업시행인가",3,"금호1",525,12,17.1,"7.5",""),
    ("성동","재개발","관리처분<분양>",5,"행당7",958,16.2,20.7,"1",""),
    ("성동","재개발","관리처분<분양>",5,"응봉1",1670,14.1,17.1,"3",""),
    ("종로","재개발","사업시행인가",3,"사직2",468,9,17.3,"7",""),
    ("서대문","재개발","관리처분인가",4,"북아현2",2316,9.8,17.5,"7","2026.4.23 관리처분인가"),
    ("서대문","재개발","사업시행인가",3,"북아현3",4776,7,17,"9.5",""),
    ("서대문","재개발","관리처분<분양>",5,"영천구역",199,9.5,12.2,"1.5",""),
    ("서대문","재개발","관리처분인가",4,"홍제3",634,3.6,12.8,"5",""),
    ("서대문","재개발","관리처분<철거>",5,"홍은13",827,5.7,8.5,"1.5",""),
    ("서대문","재개발","관리처분<철거>",5,"연희1",1002,7.9,12.2,"3.5",""),
    ("서대문","재개발","관리처분인가",4,"가재울8",283,8.5,12,"1",""),
    ("마포","재개발","관리처분<분양>",5,"공덕1",1121,17.1,21,"2.5",""),
    ("마포","재개발","관리처분<분양>",5,"마포로3-3",239,9.2,15.9,"2.5",""),
    ("은평","재개발","관리처분인가",4,"수색8",578,6,11.7,"4.5",""),
    ("은평","재개발","관리처분인가",4,"증산5",1694,5.2,12.5,"5.5",""),
    ("은평","재개발","관리처분<분양>",5,"신사1",424,7.1,10,"1.5",""),
    ("은평","재개발","관리처분<철거>",5,"대조1",2451,6.5,12.6,"2.5",""),
    ("은평","재개발","관리처분<철거>",5,"갈현1",4116,6.2,10.3,"4.5",""),
    ("은평","재개발","사업시행인가",3,"불광5",2393,4.1,12.2,"6.5",""),
    ("동대문","재개발","관리처분<분양>",5,"청량리7",761,7.6,9.9,"1",""),
    ("동대문","재개발","관리처분인가",4,"제기6",423,7.1,11.1,"5.5",""),
    ("동대문","재개발","사업시행인가",3,"청량리8",610,6.5,15.2,"8",""),
    ("동대문","재개발","관리처분인가",4,"제기4",909,8.8,12.8,"4.5",""),
    ("동대문","재개발","관리처분<분양>",5,"이문1",3071,7.1,13.8,"0.25",""),
    ("동대문","재개발","관리처분<분양>",5,"이문3-1",4172,9.1,13.2,"1.5",""),
    ("동대문","재개발","관리처분<분양>",5,"휘경3",1806,11.6,14,"1",""),
    ("동대문","재개발","관리처분인가",4,"이문4",3541,5.1,13.8,"6",""),
    ("동대문","재개발","관리처분<분양>",5,"답십리17",326,3.3,13.3,"1",""),
    ("성북","재개발","관리처분<분양>",5,"보문5",199,3.4,13.4,"2",""),
    ("성북","재개발","관리처분<철거>",5,"삼선5",1223,6.3,9.5,"2.5",""),
    ("성북","재개발","관리처분<철거>",5,"동선2",334,4.1,9.4,"3.5",""),
    ("성북","재개발","관리처분<분양>",5,"장위4",2840,9.5,13.2,"1",""),
    ("성북","재개발","관리처분<철거>",5,"장위10",2004,6.9,11,"4",""),
    ("성북","재개발","관리처분<분양>",5,"장위6",1637,7.4,13.1,"3",""),
    ("성북","재개발","관리처분인가",4,"정릉골재개발",1411,2.3,11.6,"5",""),
    ("성북","재개발","관리처분인가",4,"신월곡1",2244,10.5,15.7,"5",""),
    ("강북","재개발","관리처분인가",4,"미아3측",1037,5.2,11.1,"5.5",""),
    ("강북","재개발","사업시행인가",3,"미아9-2",1798,6.1,11.3,"8",""),
    ("중랑","재개발","관리처분<분양>",5,"중화1",1057,2.6,11.3,"1.5",""),
    ("노원","재개발","사업시행인가",3,"상계1",1388,3.4,10.3,"6",""),
    ("노원","재개발","사업시행인가",3,"상계2",2126,3.8,10.5,"6.5",""),
    ("노원","재개발","관리처분인가",4,"백사마을",2473,3.7,10.6,"5",""),
]

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    DB_PATH.parent.mkdir(exist_ok=True, parents=True)
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS districts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            project_type TEXT DEFAULT '재개발',
            stage TEXT, stage_order INTEGER,
            district_name TEXT UNIQUE, households INTEGER,
            initial_investment REAL, total_investment REAL,
            time_required TEXT, notes TEXT,
            last_updated TEXT, source_url TEXT,
            pending_initial REAL, pending_total REAL,
            pending_reason TEXT, pending_sources TEXT,
            pending_since TEXT,
            initial_confirmed INTEGER DEFAULT 1,
            total_confirmed INTEGER DEFAULT 1
        );
        CREATE TABLE IF NOT EXISTS change_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id INTEGER, field TEXT,
            old_value TEXT, new_value TEXT,
            source TEXT, changed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS news_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            district_id INTEGER, title TEXT, url TEXT UNIQUE,
            description TEXT, published_at TEXT, collected_at TEXT,
            extracted_prices TEXT
        );
    """)
    # 기존 DB 마이그레이션 (컬럼 추가)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(districts)").fetchall()]
    migrations = {
        "project_type": "TEXT DEFAULT '재개발'",
        "pending_initial": "REAL",
        "pending_total": "REAL",
        "pending_reason": "TEXT",
        "pending_sources": "TEXT",
        "pending_since": "TEXT",
        "initial_confirmed": "INTEGER DEFAULT 1",
        "total_confirmed": "INTEGER DEFAULT 1",
    }
    for col, typ in migrations.items():
        if col not in cols:
            try:
                conn.execute(f"ALTER TABLE districts ADD COLUMN {col} {typ}")
            except sqlite3.OperationalError:
                pass
    
    news_cols = [r[1] for r in conn.execute("PRAGMA table_info(news_items)").fetchall()]
    if "extracted_prices" not in news_cols:
        try:
            conn.execute("ALTER TABLE news_items ADD COLUMN extracted_prices TEXT")
        except sqlite3.OperationalError:
            pass
    
    count = conn.execute("SELECT COUNT(*) c FROM districts").fetchone()["c"]
    if count == 0:
        for row in SEED:
            try:
                conn.execute("""INSERT INTO districts 
                    (location,project_type,stage,stage_order,district_name,households,
                     initial_investment,total_investment,time_required,notes,last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (*row, datetime.now().isoformat()))
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        print(f"✅ {len(SEED)}개 시드 데이터 투입")
    conn.commit()

# ─── 뉴스 수집 ────────────────────────────────
async def fetch_naver_news(query: str, days: int = 7):
    if not NAVER_CLIENT_ID:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://openapi.naver.com/v1/search/news.json",
                params={"query": query, "display": 30, "sort": "date"},
                headers={"X-Naver-Client-Id": NAVER_CLIENT_ID,
                         "X-Naver-Client-Secret": NAVER_CLIENT_SECRET}
            )
            data = r.json()
    except Exception as e:
        print(f"  네이버 뉴스 에러: {e}")
        return []
    
    cutoff = datetime.now() - timedelta(days=days)
    items = []
    for it in data.get("items", []):
        try:
            pub = datetime.strptime(it["pubDate"], "%a, %d %b %Y %H:%M:%S %z")
            if pub.replace(tzinfo=None) < cutoff:
                continue
            items.append({
                "title": re.sub(r"<.*?>", "", it["title"]),
                "description": re.sub(r"<.*?>", "", it["description"]),
                "url": it["originallink"] or it["link"],
                "published_at": pub.isoformat(),
            })
        except:
            continue
    return items

def detect_stage_change(news_items):
    for item in news_items:
        text = item["title"] + " " + item["description"]
        for stage, kws in STAGE_KEYWORDS.items():
            for kw in kws:
                if re.search(rf"{kw}.{{0,10}}(획득|받았|완료|인가|결정|통과|고시)", text):
                    return stage, item["url"]
    return None, None

# ─── 시세 추출 핵심 로직 ───────────────────────
def extract_prices_from_news(news_items, district_name):
    """
    뉴스에서 (키워드, 금액) 튜플 추출
    반환: {'initial': [(억단위, 출처, 문맥), ...], 'total': [...]}
    """
    initial_prices = []  # 초투 후보 (프리미엄, 피, 입주권 프리미엄 등)
    total_prices = []    # 총투 후보 (매매가, 조합원 분양가, 입주권 전체 등)
    
    # 초투 관련 키워드 (프리미엄, 피)
    initial_patterns = [
        r"(?:프리미엄|피|P|분담금)\s*(?:이|은|가|는)?\s*(?:약|최대|평균)?\s*(\d+(?:\.\d+)?)\s*억",
        r"(?:초기\s*투자금|초투|초기비용)\s*(?:이|은|가|는)?\s*(?:약|최대)?\s*(\d+(?:\.\d+)?)\s*억",
        r"(\d+(?:\.\d+)?)\s*억\s*(?:원)?\s*(?:프리미엄|피\s)",
    ]
    
    # 총투 관련 키워드 (매매가, 입주권 전체)
    total_patterns = [
        r"(?:매매가|호가|입주권)\s*(?:이|은|가|는)?\s*(?:약|최대|평균)?\s*(\d+(?:\.\d+)?)\s*억",
        r"(?:총\s*투자금|총투|총\s*비용)\s*(?:이|은|가|는)?\s*(?:약|최대)?\s*(\d+(?:\.\d+)?)\s*억",
        r"(?:조합원\s*분양가|조합원가)\s*(?:이|은|가|는)?\s*(?:약|최대)?\s*(\d+(?:\.\d+)?)\s*억",
        r"(\d+(?:\.\d+)?)\s*억\s*(?:원)?\s*(?:에\s*거래|에\s*팔려|에\s*매매|에\s*매물)",
    ]
    
    for item in news_items:
        text = (item["title"] + " " + item["description"]).replace(",", "")
        # 구역명이 본문에 정확히 언급된 경우만 채택 (노이즈 방지)
        if district_name not in text:
            continue
        
        for pat in initial_patterns:
            for m in re.finditer(pat, text):
                val = float(m.group(1))
                if 0.5 <= val <= 50:  # 합리 범위
                    # 문맥 20자
                    start = max(0, m.start()-15)
                    end = min(len(text), m.end()+15)
                    initial_prices.append({
                        "value": val, "url": item["url"], "title": item["title"],
                        "context": text[start:end]
                    })
        
        for pat in total_patterns:
            for m in re.finditer(pat, text):
                val = float(m.group(1))
                if 1 <= val <= 100:
                    start = max(0, m.start()-15)
                    end = min(len(text), m.end()+15)
                    total_prices.append({
                        "value": val, "url": item["url"], "title": item["title"],
                        "context": text[start:end]
                    })
    
    return {"initial": initial_prices, "total": total_prices}

def consolidate_prices(price_candidates, tolerance=0.3):
    """
    여러 뉴스에서 추출된 가격 후보를 검증.
    2건 이상에서 유사 금액(±tolerance억) 확인 시 '확실', 
    1건만이면 '미확인' (pending)
    """
    if not price_candidates:
        return None, None, []
    
    # 비슷한 값끼리 그룹핑
    values = [p["value"] for p in price_candidates]
    groups = {}
    for p in price_candidates:
        matched = False
        for key in groups:
            if abs(p["value"] - key) <= tolerance:
                groups[key].append(p)
                matched = True
                break
        if not matched:
            groups[p["value"]] = [p]
    
    # 가장 많이 언급된 값
    best_key = max(groups, key=lambda k: len(groups[k]))
    best_group = groups[best_key]
    
    if len(best_group) >= 2:
        # 확실: 평균값 반환
        avg = sum(p["value"] for p in best_group) / len(best_group)
        return "confirmed", round(avg, 1), best_group
    else:
        # 미확인: 최신 값 반환
        return "pending", best_group[0]["value"], best_group

# ─── 구역 업데이트 ─────────────────────────────
async def update_single_district(district):
    name = district["district_name"]
    ptype = district["project_type"] or "재개발"
    query = f"{name} {ptype}"
    
    news_items = await fetch_naver_news(query, days=7)
    
    conn = get_db()
    updates = {}
    
    # 1) 단계 변경 감지
    new_stage, source_url = detect_stage_change(news_items)
    if new_stage and new_stage in STAGE_ORDER:
        new_order = STAGE_ORDER[new_stage]
        if new_order > district["stage_order"]:
            updates["stage"] = new_stage
            updates["stage_order"] = new_order
            updates["source_url"] = source_url
    
    # 2) 시세 추출
    price_data = extract_prices_from_news(news_items, name)
    
    for field_type in ["initial", "total"]:
        status, value, sources = consolidate_prices(price_data[field_type])
        if not status:
            continue
        
        target_field = f"{field_type}_investment"
        confirmed_field = f"{field_type}_confirmed"
        pending_field = f"pending_{field_type}"
        
        current_value = district[target_field]
        
        if status == "confirmed":
            # 확실 데이터: 기존 값과 다르면 업데이트
            if current_value is None or abs((current_value or 0) - value) > 0.5:
                updates[target_field] = value
                updates[confirmed_field] = 1
                updates[pending_field] = None
                # 로그
                for field, _ in [(target_field, value)]:
                    pass  # change_log는 아래에서 통합
        elif status == "pending":
            # 미확인 데이터: 기존 값 유지 + pending 필드에 후보 저장
            if current_value is None or abs((current_value or 0) - value) > 0.5:
                updates[pending_field] = value
                updates["pending_reason"] = f"{field_type}: 뉴스 1건에서만 확인"
                updates["pending_sources"] = json.dumps([{
                    "url": s["url"], "title": s["title"], "context": s["context"]
                } for s in sources], ensure_ascii=False)
                updates["pending_since"] = datetime.now().isoformat()
                updates[confirmed_field] = 0
    
    # 3) 뉴스 저장 (추출된 가격 포함)
    for item in news_items[:10]:
        try:
            extracted = []
            t = (item["title"] + " " + item["description"]).replace(",", "")
            if name in t:
                for m in re.finditer(r"(\d+(?:\.\d+)?)\s*억", t):
                    extracted.append(m.group(1))
            conn.execute("""INSERT OR IGNORE INTO news_items
                (district_id, title, url, description, published_at, collected_at, extracted_prices)
                VALUES (?,?,?,?,?,?,?)""",
                (district["id"], item["title"], item["url"],
                 item["description"], item["published_at"], datetime.now().isoformat(),
                 json.dumps(extracted, ensure_ascii=False) if extracted else None))
        except:
            pass
    
    # 4) DB 업데이트 + 변경 이력
    if updates:
        for field, new_val in updates.items():
            old_val = district[field] if field in district.keys() else None
            if old_val != new_val:
                conn.execute("""INSERT INTO change_logs
                    (district_id, field, old_value, new_value, source, changed_at)
                    VALUES (?,?,?,?,?,?)""",
                    (district["id"], field, str(old_val or ""), str(new_val or ""),
                     source_url or "auto", datetime.now().isoformat()))
        
        updates["last_updated"] = datetime.now().isoformat()
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE districts SET {set_clause} WHERE id=?",
                    (*updates.values(), district["id"]))
        print(f"✅ {name}: {list(updates.keys())}")
    
    conn.commit()

async def scheduled_update():
    print(f"\n🔄 자동 업데이트: {datetime.now()}")
    conn = get_db()
    districts = conn.execute("SELECT * FROM districts WHERE stage_order >= 3").fetchall()
    sem = asyncio.Semaphore(3)
    async def wrapped(d):
        async with sem:
            await update_single_district(dict(d))
            await asyncio.sleep(1)
    await asyncio.gather(*[wrapped(d) for d in districts])
    print(f"✅ 완료")

# ─── FastAPI ───────────────────────────────────
scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

@asynccontextmanager
async def lifespan(app):
    init_db()
    scheduler.add_job(scheduled_update, "cron", hour=6, minute=0, id="daily")
    scheduler.start()
    print("✅ 서버 시작 - 매일 06:00 KST 자동 업데이트")
    yield
    scheduler.shutdown()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class FieldUpdate(BaseModel):
    field: str
    value: Optional[str] = None

@app.get("/api/districts")
async def get_districts():
    conn = get_db()
    rows = conn.execute("""SELECT * FROM districts WHERE stage_order >= 3
                          ORDER BY location, district_name""").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/districts/{did}")
async def get_detail(did: int):
    conn = get_db()
    d = conn.execute("SELECT * FROM districts WHERE id=?", (did,)).fetchone()
    if not d:
        raise HTTPException(404)
    logs = conn.execute("""SELECT * FROM change_logs WHERE district_id=?
                          ORDER BY changed_at DESC LIMIT 50""", (did,)).fetchall()
    news = conn.execute("""SELECT * FROM news_items WHERE district_id=?
                          ORDER BY published_at DESC LIMIT 15""", (did,)).fetchall()
    result = {"district": dict(d), "history": [dict(l) for l in logs],
              "news": [dict(n) for n in news]}
    # pending_sources 파싱
    if result["district"].get("pending_sources"):
        try:
            result["district"]["pending_sources"] = json.loads(result["district"]["pending_sources"])
        except:
            pass
    return result

@app.post("/api/districts/{did}/confirm-pending")
async def confirm_pending(did: int, field_type: str):
    """pending 값을 실제 값으로 승인"""
    conn = get_db()
    d = conn.execute("SELECT * FROM districts WHERE id=?", (did,)).fetchone()
    if not d:
        raise HTTPException(404)
    
    pending_field = f"pending_{field_type}"
    target_field = f"{field_type}_investment"
    confirmed_field = f"{field_type}_confirmed"
    
    pending_val = d[pending_field]
    if pending_val is None:
        raise HTTPException(400, "pending 값이 없습니다")
    
    old_val = d[target_field]
    conn.execute(f"""UPDATE districts SET 
        {target_field}=?, {confirmed_field}=1, {pending_field}=NULL,
        pending_reason=NULL, pending_sources=NULL,
        last_updated=? WHERE id=?""",
        (pending_val, datetime.now().isoformat(), did))
    conn.execute("""INSERT INTO change_logs
        (district_id, field, old_value, new_value, source, changed_at)
        VALUES (?,?,?,?,?,?)""",
        (did, target_field, str(old_val or ""), str(pending_val),
         "manual-confirm", datetime.now().isoformat()))
    conn.commit()
    return {"status": "confirmed", "value": pending_val}

@app.post("/api/districts/{did}/reject-pending")
async def reject_pending(did: int, field_type: str):
    """pending 값 거부 (기각)"""
    conn = get_db()
    pending_field = f"pending_{field_type}"
    conn.execute(f"""UPDATE districts SET {pending_field}=NULL 
        WHERE id=?""", (did,))
    conn.commit()
    return {"status": "rejected"}

@app.post("/api/districts/{did}/update-field")
async def update_field(did: int, update: FieldUpdate):
    """수동 필드 수정 (단계, 재개발/재건축, 초투, 총투, 메모)"""
    conn = get_db()
    d = conn.execute("SELECT * FROM districts WHERE id=?", (did,)).fetchone()
    if not d:
        raise HTTPException(404)
    
    allowed = {"stage", "project_type", "initial_investment", "total_investment",
               "households", "time_required", "notes"}
    if update.field not in allowed:
        raise HTTPException(400, f"수정 불가 필드: {update.field}")
    
    old_val = d[update.field]
    new_val = update.value
    
    # 숫자 필드 변환
    if update.field in ("initial_investment", "total_investment"):
        try:
            new_val = float(new_val) if new_val else None
        except:
            raise HTTPException(400, "숫자가 아닙니다")
    elif update.field == "households":
        try:
            new_val = int(new_val) if new_val else None
        except:
            raise HTTPException(400, "정수가 아닙니다")
    
    # 단계 변경 시 stage_order도 업데이트
    extra_updates = {}
    if update.field == "stage" and new_val in STAGE_ORDER:
        extra_updates["stage_order"] = STAGE_ORDER[new_val]
    
    # 시세 수동 수정 시 confirmed=1로 설정하고 pending 제거
    if update.field == "initial_investment":
        extra_updates["initial_confirmed"] = 1
        extra_updates["pending_initial"] = None
    elif update.field == "total_investment":
        extra_updates["total_confirmed"] = 1
        extra_updates["pending_total"] = None
    
    all_updates = {update.field: new_val, **extra_updates,
                   "last_updated": datetime.now().isoformat()}
    set_clause = ", ".join(f"{k}=?" for k in all_updates)
    conn.execute(f"UPDATE districts SET {set_clause} WHERE id=?",
                (*all_updates.values(), did))
    
    conn.execute("""INSERT INTO change_logs
        (district_id, field, old_value, new_value, source, changed_at)
        VALUES (?,?,?,?,?,?)""",
        (did, update.field, str(old_val or ""), str(new_val or ""),
         "manual", datetime.now().isoformat()))
    conn.commit()
    return {"status": "updated", "value": new_val}

@app.get("/api/pending-list")
async def pending_list():
    """확인 대기 중인 항목 목록"""
    conn = get_db()
    rows = conn.execute("""SELECT * FROM districts 
        WHERE pending_initial IS NOT NULL OR pending_total IS NOT NULL
        ORDER BY pending_since DESC""").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/refresh-all")
async def refresh_all(bg: BackgroundTasks):
    bg.add_task(scheduled_update)
    return {"status": "queued"}

@app.get("/api/export/excel")
async def export_excel():
    conn = get_db()
    rows = conn.execute("SELECT * FROM districts WHERE stage_order >= 3").fetchall()
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "재개발재건축현황"
    ws.append(["위치","유형","단계","구역명","세대수","초투","초투확인","총투","총투확인",
               "소요","메모","최종업데이트"])
    for r in rows:
        d = dict(r)
        ws.append([d["location"], d["project_type"], d["stage"], d["district_name"],
                  d["households"], d["initial_investment"],
                  "O" if d.get("initial_confirmed") else "⚠️미확인",
                  d["total_investment"],
                  "O" if d.get("total_confirmed") else "⚠️미확인",
                  d["time_required"], d["notes"], d["last_updated"]])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fn = f"재개발현황_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fn}"})

@app.get("/", response_class=HTMLResponse)
async def index():
    return FRONTEND_HTML


FRONTEND_HTML = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>서울 재개발/재건축 현황</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdn.tailwindcss.com"></script>
<style>body{font-family:-apple-system,'Apple SD Gothic Neo','Malgun Gothic',sans-serif}
.stage-사업시행인가{background:#dbeafe;color:#1e40af}
.stage-관리처분인가{background:#fef3c7;color:#92400e}
.stage-관리처분철거{background:#fee2e2;color:#991b1b}
.stage-관리처분분양{background:#dcfce7;color:#166534}
.stage-착공분양{background:#e0e7ff;color:#3730a3}
.type-재개발{background:#f1f5f9;color:#475569}
.type-재건축{background:#fce7f3;color:#9f1239}
tr.row:hover{background:#f8fafc;cursor:pointer}
th.sortable{cursor:pointer;user-select:none;position:relative;padding-right:18px!important}
th.sortable:hover{background:#e2e8f0}
th.sortable::after{content:'⇅';position:absolute;right:4px;opacity:0.3;font-size:10px}
th.sort-asc::after{content:'▲';opacity:1;color:#2563eb}
th.sort-desc::after{content:'▼';opacity:1;color:#2563eb}
.warn-cell{background:#fef3c7;position:relative}
.warn-cell::after{content:'⚠️';margin-left:4px}
</style></head>
<body class="bg-slate-50"><div class="max-w-screen-2xl mx-auto p-4">
<header class="flex justify-between items-center mb-4 flex-wrap gap-2">
<div><h1 class="text-2xl font-bold">서울 재개발/재건축 현황</h1>
<p class="text-sm text-slate-500"><span id="upd"></span> · 매일 06:00 자동 · 
확인대기 <span id="pending-cnt" class="text-amber-600 font-bold">0</span>건</p></div>
<div class="flex gap-2 flex-wrap">
<button onclick="showPendingList()" class="px-3 py-2 bg-amber-500 text-white rounded text-sm">⚠️ 확인대기</button>
<button onclick="refresh()" class="px-3 py-2 bg-blue-600 text-white rounded text-sm">🔄 지금 업데이트</button>
<a href="/api/export/excel" class="px-3 py-2 bg-green-600 text-white rounded text-sm">📥 엑셀</a></div>
</header>
<div id="stats" class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4"></div>
<div class="bg-white p-3 mb-3 rounded border flex gap-2 flex-wrap">
<select id="f-loc" onchange="render()" class="border rounded px-2 py-1 text-sm"><option value="">전체 위치</option></select>
<select id="f-type" onchange="render()" class="border rounded px-2 py-1 text-sm">
<option value="">전체 유형</option><option value="재개발">재개발</option><option value="재건축">재건축</option></select>
<select id="f-stage" onchange="render()" class="border rounded px-2 py-1 text-sm">
<option value="">전체 단계</option>
<option value="사업시행인가">사업시행인가</option>
<option value="관리처분인가">관리처분인가</option>
<option value="관리처분<철거>">관리처분&lt;철거&gt;</option>
<option value="관리처분<분양>">관리처분&lt;분양&gt;</option>
<option value="착공/분양">착공/분양</option>
<option value="준공/입주">준공/입주</option>
</select>
<input id="f-search" onkeyup="render()" placeholder="구역명 검색" class="border rounded px-3 py-1 text-sm ml-auto w-64">
</div>
<div class="bg-white rounded border overflow-x-auto"><table class="w-full text-sm">
<thead class="bg-slate-100"><tr>
<th class="p-2 text-left sortable" onclick="sortBy('location')">위치</th>
<th class="p-2 text-left sortable" onclick="sortBy('project_type')">유형</th>
<th class="p-2 text-left sortable" onclick="sortBy('stage_order')">단계</th>
<th class="p-2 text-left sortable" onclick="sortBy('district_name')">구역명</th>
<th class="p-2 text-right sortable" onclick="sortBy('households')">세대수</th>
<th class="p-2 text-right sortable" onclick="sortBy('initial_investment')">초투</th>
<th class="p-2 text-right sortable" onclick="sortBy('total_investment')">총투</th>
<th class="p-2 text-right sortable" onclick="sortBy('time_required')">소요</th>
<th class="p-2 text-center sortable" onclick="sortBy('last_updated')">업데이트</th>
</tr></thead><tbody id="tbody"></tbody></table></div></div>

<div id="modal" class="hidden fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick="this.classList.add('hidden')">
<div class="bg-white rounded-lg max-w-4xl w-full max-h-[90vh] overflow-y-auto p-5" onclick="event.stopPropagation()">
<div class="flex justify-between mb-4"><h2 id="m-title" class="text-xl font-bold"></h2>
<button onclick="document.getElementById('modal').classList.add('hidden')" class="text-2xl">&times;</button></div>
<div id="m-content"></div></div></div>

<script>
let data=[];
let sortState={key:'location',dir:'asc'};

async function load(){
  data=await fetch('/api/districts').then(r=>r.json());
  const pending=await fetch('/api/pending-list').then(r=>r.json());
  document.getElementById('pending-cnt').textContent=pending.length;
  
  const locs=[...new Set(data.map(d=>d.location))].sort();
  document.getElementById('f-loc').innerHTML='<option value="">전체 위치</option>'+locs.map(l=>`<option value="${l}">${l}</option>`).join('');
  const ss={};data.forEach(d=>ss[d.stage]=(ss[d.stage]||0)+1);
  document.getElementById('stats').innerHTML=[
    ['총 구역',data.length,'bg-slate-700'],
    ['사업시행인가',ss['사업시행인가']||0,'bg-blue-600'],
    ['관리처분인가',ss['관리처분인가']||0,'bg-amber-500'],
    ['철거/분양',(ss['관리처분<철거>']||0)+(ss['관리처분<분양>']||0),'bg-red-500'],
    ['착공/준공',(ss['착공/분양']||0),'bg-indigo-600']
  ].map(([l,v,c])=>`<div class="${c} text-white rounded-lg p-3"><div class="text-xs opacity-80">${l}</div><div class="text-2xl font-bold">${v}</div></div>`).join('');
  document.getElementById('upd').textContent='조회: '+new Date().toLocaleString('ko-KR');
  render();
}

function sortBy(key){
  if(sortState.key===key){sortState.dir=sortState.dir==='asc'?'desc':'asc';}
  else{sortState.key=key;sortState.dir='asc';}
  render();
}

function compareVal(a,b,key){
  let va=a[key], vb=b[key];
  if(va==null && vb==null) return 0;
  if(va==null) return 1;
  if(vb==null) return -1;
  const numKeys=['households','initial_investment','total_investment','stage_order'];
  if(numKeys.includes(key)) return (parseFloat(va)||0)-(parseFloat(vb)||0);
  if(key==='time_required'){
    const na=parseFloat(va), nb=parseFloat(vb);
    if(!isNaN(na) && !isNaN(nb)) return na-nb;
    if(!isNaN(na)) return -1;
    if(!isNaN(nb)) return 1;
    return String(va).localeCompare(String(vb),'ko');
  }
  if(key==='last_updated') return new Date(va)-new Date(vb);
  return String(va).localeCompare(String(vb),'ko');
}

function render(){
  const loc=document.getElementById('f-loc').value;
  const type=document.getElementById('f-type').value;
  const stage=document.getElementById('f-stage').value;
  const q=document.getElementById('f-search').value.toLowerCase();
  let f=data.filter(d=>
    (!loc||d.location===loc)&&
    (!type||d.project_type===type)&&
    (!stage||d.stage===stage)&&
    (!q||d.district_name.toLowerCase().includes(q))
  );
  f.sort((a,b)=>{const cmp=compareVal(a,b,sortState.key);return sortState.dir==='asc'?cmp:-cmp;});
  
  document.querySelectorAll('th.sortable').forEach(th=>th.classList.remove('sort-asc','sort-desc'));
  const colMap={'location':0,'project_type':1,'stage_order':2,'district_name':3,'households':4,
                'initial_investment':5,'total_investment':6,'time_required':7,'last_updated':8};
  const idx=colMap[sortState.key];
  if(idx!==undefined){
    const ths=document.querySelectorAll('th.sortable');
    if(ths[idx]) ths[idx].classList.add(sortState.dir==='asc'?'sort-asc':'sort-desc');
  }
  
  document.getElementById('tbody').innerHTML=f.map(d=>{
    const c='stage-'+(d.stage||'').replace(/[<>\\/]/g,'');
    const tc='type-'+(d.project_type||'재개발');
    const upd=d.last_updated?new Date(d.last_updated).toLocaleDateString('ko-KR'):'-';
    const initWarn=d.initial_confirmed===0||d.pending_initial!==null;
    const totalWarn=d.total_confirmed===0||d.pending_total!==null;
    return `<tr class="row border-t" onclick="detail(${d.id})">
<td class="p-2">${d.location}</td>
<td class="p-2"><span class="${tc} px-2 py-0.5 rounded text-xs">${d.project_type||'재개발'}</span></td>
<td class="p-2"><span class="${c} px-2 py-0.5 rounded text-xs">${d.stage}</span></td>
<td class="p-2 font-medium">${d.district_name}</td>
<td class="p-2 text-right">${d.households?.toLocaleString()||'-'}</td>
<td class="p-2 text-right ${initWarn?'warn-cell':''}">${d.initial_investment??'-'}${d.pending_initial!==null&&d.pending_initial!==undefined?`<br><span class="text-xs text-amber-600">→${d.pending_initial}?</span>`:''}</td>
<td class="p-2 text-right ${totalWarn?'warn-cell':''}">${d.total_investment??'-'}${d.pending_total!==null&&d.pending_total!==undefined?`<br><span class="text-xs text-amber-600">→${d.pending_total}?</span>`:''}</td>
<td class="p-2 text-right text-slate-500">${d.time_required||'-'}</td>
<td class="p-2 text-center text-xs text-slate-500">${upd}</td></tr>`;
  }).join('');
}

async function detail(id){
  const r=await fetch('/api/districts/'+id).then(r=>r.json());
  const d=r.district;
  document.getElementById('m-title').textContent=`${d.location} ${d.district_name} (${d.project_type||'재개발'})`;
  const nm=encodeURIComponent(d.district_name+' '+(d.project_type||'재개발'));
  
  // pending 섹션
  let pendingHtml='';
  if(d.pending_initial!==null&&d.pending_initial!==undefined){
    pendingHtml+=buildPendingSection('initial', d.initial_investment, d.pending_initial, d.pending_sources, id);
  }
  if(d.pending_total!==null&&d.pending_total!==undefined){
    pendingHtml+=buildPendingSection('total', d.total_investment, d.pending_total, d.pending_sources, id);
  }
  
  document.getElementById('m-content').innerHTML=`
${pendingHtml}
<div class="grid grid-cols-2 gap-3 mb-4 text-sm">
<div><span class="text-slate-500">유형:</span> 
<select onchange="updateField(${id},'project_type',this.value)" class="border rounded px-2 py-1 text-sm">
<option value="재개발" ${d.project_type==='재개발'?'selected':''}>재개발</option>
<option value="재건축" ${d.project_type==='재건축'?'selected':''}>재건축</option>
</select></div>
<div><span class="text-slate-500">단계:</span> 
<select onchange="updateField(${id},'stage',this.value)" class="border rounded px-2 py-1 text-sm">
<option ${d.stage==='사업시행인가'?'selected':''}>사업시행인가</option>
<option ${d.stage==='관리처분인가'?'selected':''}>관리처분인가</option>
<option value="관리처분<철거>" ${d.stage==='관리처분<철거>'?'selected':''}>관리처분&lt;철거&gt;</option>
<option value="관리처분<분양>" ${d.stage==='관리처분<분양>'?'selected':''}>관리처분&lt;분양&gt;</option>
<option value="착공/분양" ${d.stage==='착공/분양'?'selected':''}>착공/분양</option>
<option value="준공/입주" ${d.stage==='준공/입주'?'selected':''}>준공/입주</option>
</select></div>
<div><span class="text-slate-500">세대수:</span> 
<input type="number" value="${d.households||''}" onblur="updateField(${id},'households',this.value)" class="border rounded px-2 py-1 text-sm w-24"></div>
<div><span class="text-slate-500">소요:</span> 
<input type="text" value="${d.time_required||''}" onblur="updateField(${id},'time_required',this.value)" class="border rounded px-2 py-1 text-sm w-24"></div>
<div><span class="text-slate-500">초투(억):</span> 
<input type="number" step="0.1" value="${d.initial_investment||''}" onblur="updateField(${id},'initial_investment',this.value)" class="border rounded px-2 py-1 text-sm w-24">
${d.initial_confirmed===0?'<span class="text-amber-600 text-xs">⚠️미확인</span>':''}</div>
<div><span class="text-slate-500">총투(억):</span> 
<input type="number" step="0.1" value="${d.total_investment||''}" onblur="updateField(${id},'total_investment',this.value)" class="border rounded px-2 py-1 text-sm w-24">
${d.total_confirmed===0?'<span class="text-amber-600 text-xs">⚠️미확인</span>':''}</div>
<div class="col-span-2"><span class="text-slate-500">메모:</span> 
<input type="text" value="${d.notes||''}" onblur="updateField(${id},'notes',this.value)" class="border rounded px-2 py-1 text-sm w-full"></div>
</div>

<div class="mb-4"><p class="font-medium mb-2">🔗 외부 링크</p>
<div class="grid grid-cols-3 md:grid-cols-5 gap-2 text-xs">
<a href="https://search.naver.com/search.naver?where=news&query=${nm}&sort=1" target="_blank" class="bg-blue-50 text-blue-700 p-2 rounded text-center">📰 뉴스</a>
<a href="https://search.naver.com/search.naver?where=post&query=${nm}&sort=1" target="_blank" class="bg-green-50 text-green-700 p-2 rounded text-center">📝 블로그</a>
<a href="https://new.land.naver.com/search?sk=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-orange-50 text-orange-700 p-2 rounded text-center">🏠 부동산</a>
<a href="https://hogangnono.com/search?q=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-purple-50 text-purple-700 p-2 rounded text-center">📊 호갱노노</a>
<a href="https://cleanup.seoul.go.kr/search/searchResult.do?searchWord=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-slate-100 text-slate-700 p-2 rounded text-center">🏛️ 정보몽땅</a>
</div></div>

<p class="font-medium mb-2">📰 최근 뉴스 (자동 수집, 가격 언급 하이라이트)</p>
${r.news.length===0?'<p class="text-sm text-slate-400 p-2">수집된 뉴스 없음</p>':
'<ul class="space-y-2 mb-4">'+r.news.map(n=>{
  const prices=n.extracted_prices?JSON.parse(n.extracted_prices):[];
  const priceBadge=prices.length>0?`<span class="inline-block ml-2 bg-yellow-100 text-yellow-800 text-xs px-2 py-0.5 rounded">💰 ${prices.join(', ')}억</span>`:'';
  return `<li class="text-sm border-l-2 ${prices.length>0?'border-yellow-400':'border-blue-400'} pl-2"><a href="${n.url}" target="_blank" class="font-medium hover:text-blue-600">${n.title}</a>${priceBadge}<div class="text-xs text-slate-500">${(n.description||'').slice(0,120)}...</div></li>`;
}).join('')+'</ul>'}

<p class="font-medium mb-2">📜 변경 이력</p>
${r.history.length===0?'<p class="text-sm text-slate-400 p-2">변경 이력 없음</p>':
'<table class="w-full text-xs border"><thead class="bg-slate-50"><tr><th class="p-2 text-left">시각</th><th class="p-2 text-left">필드</th><th class="p-2 text-left">이전</th><th class="p-2 text-left">이후</th><th class="p-2 text-left">출처</th></tr></thead><tbody>'+
r.history.map(l=>`<tr class="border-t"><td class="p-2">${new Date(l.changed_at).toLocaleDateString('ko-KR')}</td><td class="p-2">${l.field}</td><td class="p-2 text-red-600">${l.old_value||'-'}</td><td class="p-2 text-green-600">${l.new_value||'-'}</td><td class="p-2 text-xs text-slate-500">${l.source||'-'}</td></tr>`).join('')+'</tbody></table>'}`;
  document.getElementById('modal').classList.remove('hidden');
}

function buildPendingSection(ftype, curVal, pendVal, sources, id){
  const typeName=ftype==='initial'?'초투':'총투';
  let srcList='';
  if(sources && Array.isArray(sources)){
    srcList=sources.map(s=>`<li class="text-xs"><a href="${s.url}" target="_blank" class="text-blue-600 hover:underline">${s.title}</a><div class="text-slate-500">${s.context}</div></li>`).join('');
  }
  return `<div class="mb-4 p-3 bg-amber-50 border border-amber-300 rounded">
<p class="font-medium text-amber-900 mb-1">⚠️ ${typeName} 확인 필요</p>
<p class="text-sm mb-2">현재값: <b>${curVal??'없음'}억</b> → 뉴스에서 발견된 값: <b class="text-amber-700">${pendVal}억</b> (출처 1건)</p>
${srcList?`<ul class="list-disc pl-5 mb-2">${srcList}</ul>`:''}
<div class="flex gap-2">
<button onclick="confirmPending(${id},'${ftype}')" class="px-3 py-1 bg-green-600 text-white rounded text-xs">✅ 승인 (값 변경)</button>
<button onclick="rejectPending(${id},'${ftype}')" class="px-3 py-1 bg-slate-400 text-white rounded text-xs">❌ 기각 (기존 유지)</button>
</div></div>`;
}

async function confirmPending(id, ftype){
  if(!confirm('이 값으로 업데이트할까요?'))return;
  const r=await fetch(`/api/districts/${id}/confirm-pending?field_type=${ftype}`,{method:'POST'});
  const j=await r.json();
  if(j.status==='confirmed'){alert('승인되었습니다');load();detail(id);}
}

async function rejectPending(id, ftype){
  const r=await fetch(`/api/districts/${id}/reject-pending?field_type=${ftype}`,{method:'POST'});
  if((await r.json()).status==='rejected'){alert('기각되었습니다');load();detail(id);}
}

async function updateField(id, field, value){
  try{
    await fetch(`/api/districts/${id}/update-field`,{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body:JSON.stringify({field,value:String(value)})
    });
    load();
  }catch(e){alert('저장 실패');}
}

async function showPendingList(){
  const pending=await fetch('/api/pending-list').then(r=>r.json());
  if(pending.length===0){alert('확인 대기 중인 항목이 없습니다');return;}
  document.getElementById('m-title').textContent=`⚠️ 확인 대기 목록 (${pending.length}건)`;
  document.getElementById('m-content').innerHTML=`
<table class="w-full text-sm border"><thead class="bg-slate-100"><tr>
<th class="p-2 text-left">구역</th><th class="p-2 text-left">유형</th>
<th class="p-2 text-right">현재값</th><th class="p-2 text-right">뉴스값</th>
<th class="p-2 text-center">작업</th></tr></thead><tbody>
${pending.map(d=>{
  let rows='';
  if(d.pending_initial!==null){
    rows+=`<tr class="border-t"><td class="p-2"><a href="#" onclick="detail(${d.id});return false;" class="text-blue-600">${d.district_name}</a></td>
<td class="p-2">초투</td><td class="p-2 text-right">${d.initial_investment??'-'}</td>
<td class="p-2 text-right text-amber-700 font-bold">${d.pending_initial}</td>
<td class="p-2 text-center"><button onclick="detail(${d.id})" class="bg-blue-500 text-white px-2 py-1 rounded text-xs">확인</button></td></tr>`;
  }
  if(d.pending_total!==null){
    rows+=`<tr class="border-t"><td class="p-2"><a href="#" onclick="detail(${d.id});return false;" class="text-blue-600">${d.district_name}</a></td>
<td class="p-2">총투</td><td class="p-2 text-right">${d.total_investment??'-'}</td>
<td class="p-2 text-right text-amber-700 font-bold">${d.pending_total}</td>
<td class="p-2 text-center"><button onclick="detail(${d.id})" class="bg-blue-500 text-white px-2 py-1 rounded text-xs">확인</button></td></tr>`;
  }
  return rows;
}).join('')}
</tbody></table>`;
  document.getElementById('modal').classList.remove('hidden');
}

async function refresh(){
  if(!confirm('지금 전체 업데이트를 실행할까요? (약 2-3분 소요)'))return;
  await fetch('/api/refresh-all',{method:'POST'});
  alert('업데이트 시작됨. 3분 후 페이지를 새로고침하세요.');
}

load();setInterval(load,60000);
</script></body></html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
