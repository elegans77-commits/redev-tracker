"""
서울 재개발 현황 추적 - 클라우드 배포 버전
Railway에서 자동 실행되는 FastAPI 서버
"""
import os
import asyncio
import re
import sqlite3
import io
from datetime import datetime, timedelta
from pathlib import Path
from contextlib import asynccontextmanager
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import openpyxl

# ─── 환경변수 ───────────────────────────────────
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
PORT = int(os.getenv("PORT", 8000))

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36"

# ─── DB 경로 ────────────────────────────────────
DB_PATH = Path("/tmp/redev.db")  # Railway 임시 저장소

STAGE_KEYWORDS = {
    "관리처분인가": ["관리처분계획인가", "관리처분 인가", "관리처분인가"],
    "사업시행인가": ["사업시행계획인가", "사업시행 인가", "사업시행인가"],
    "이주": ["이주 개시", "이주 시작"],
    "철거": ["철거 개시", "철거 진행"],
    "착공": ["착공", "기공식"],
    "분양": ["일반분양", "분양 개시"],
    "준공": ["준공", "입주 시작"],
}
STAGE_ORDER = {
    "사업시행인가": 3, "관리처분인가": 4,
    "관리처분<철거>": 5, "관리처분<분양>": 5,
    "이주": 5, "철거": 5, "착공/분양": 6, "준공/입주": 7,
}

# ─── 시드 데이터 (75개 구역) ────────────────────
SEED = [
    ("동작","관리처분인가",4,"노량진1",2992,13.2,19.5,"8","2026.4.21 관리처분인가"),
    ("동작","관리처분<철거>",5,"노량진2",415,11.8,17.3,"4",""),
    ("동작","사업시행인가",3,"노량진3",1150,12.2,20.5,"8.5",""),
    ("동작","관리처분인가",4,"노량진4",845,12.3,18.8,"4.5",""),
    ("동작","관리처분인가",4,"노량진5",743,11.3,18.7,"5",""),
    ("동작","착공/분양",6,"노량진6",1499,13.2,18.2,"분양중","라클라체자이드파인"),
    ("동작","관리처분인가",4,"노량진7",576,10,18.3,"5.5",""),
    ("동작","관리처분<철거>",5,"노량진8",983,12.4,18.8,"4.5",""),
    ("동작","관리처분<철거>",5,"흑석9",1536,17.1,22.2,"4",""),
    ("동작","관리처분<철거>",5,"흑석11",1522,17.8,20.8,"4",""),
    ("동작","사업시행인가",3,"상도5",508,4.5,13.4,"7.5",""),
    ("관악","관리처분<분양>",5,"봉천412",997,7.3,13.2,"0.25",""),
    ("관악","사업시행인가",3,"봉천413",855,6,13.2,"7.5",""),
    ("관악","관리처분<철거>",5,"신림2",1487,5.1,9.7,"4",""),
    ("관악","관리처분<분양>",5,"신림3",571,1.4,10.6,"1",""),
    ("영등포","관리처분<분양>",5,"당산12",707,11.1,15.8,"1.5",""),
    ("영등포","관리처분<철거>",5,"영등포1-13",659,11.6,13.8,"3.5",""),
    ("양천","사업시행인가",3,"신정1-3",211,2.5,9.6,"5.5",""),
    ("양천","사업시행인가",3,"신정4",1713,4.4,12.4,"8",""),
    ("강서","관리처분인가",4,"방화5",1526,5.3,13.5,"6",""),
    ("강서","사업시행인가",3,"방화3",1476,4.9,12.3,"8.5",""),
    ("서초","관리처분<분양>",6,"방배5",3065,24,30.8,"2","디에이치방배 2026.8입주"),
    ("서초","관리처분<철거>",5,"방배13",2369,18.4,27.5,"4.5",""),
    ("서초","관리처분<철거>",5,"방배6",1111,19.4,25.6,"2",""),
    ("서초","관리처분<철거>",5,"방배14",487,19.7,22.8,"3.5",""),
    ("송파","관리처분인가",4,"마천4",1381,8,16.3,"5.5",""),
    ("송파","사업시행인가",3,"마천3",2364,6.3,15.9,"9.5",""),
    ("용산","관리처분인가",4,"한남2",1537,20.2,30.5,"7.5",""),
    ("용산","관리처분<철거>",5,"한남3",5816,20.2,33.9,"5.5",""),
    ("용산","사업시행인가",3,"한남4",2686,22,32,"10",""),
    ("용산","사업시행인가",3,"한남5",2660,22,34.5,"10",""),
    ("중구","관리처분인가",4,"신당8",1215,8.5,16.5,"5.5",""),
    ("성동","관리처분인가",4,"금호16",595,6.9,15.8,"5.5",""),
    ("성동","사업시행인가",3,"금호1",525,12,17.1,"7.5",""),
    ("성동","관리처분<분양>",5,"행당7",958,16.2,20.7,"1",""),
    ("성동","관리처분<분양>",5,"응봉1",1670,14.1,17.1,"3",""),
    ("종로","사업시행인가",3,"사직2",468,9,17.3,"7",""),
    ("서대문","관리처분인가",4,"북아현2",2316,9.8,17.5,"7","2026.4.23 관리처분인가"),
    ("서대문","사업시행인가",3,"북아현3",4776,7,17,"9.5",""),
    ("서대문","관리처분<분양>",5,"영천구역",199,9.5,12.2,"1.5",""),
    ("서대문","관리처분인가",4,"홍제3",634,3.6,12.8,"5",""),
    ("서대문","관리처분<철거>",5,"홍은13",827,5.7,8.5,"1.5",""),
    ("서대문","관리처분<철거>",5,"연희1",1002,7.9,12.2,"3.5",""),
    ("서대문","관리처분인가",4,"가재울8",283,8.5,12,"1",""),
    ("마포","관리처분<분양>",5,"공덕1",1121,17.1,21,"2.5",""),
    ("마포","관리처분<분양>",5,"마포로3-3",239,9.2,15.9,"2.5",""),
    ("은평","관리처분인가",4,"수색8",578,6,11.7,"4.5",""),
    ("은평","관리처분인가",4,"증산5",1694,5.2,12.5,"5.5",""),
    ("은평","관리처분<분양>",5,"신사1",424,7.1,10,"1.5",""),
    ("은평","관리처분<철거>",5,"대조1",2451,6.5,12.6,"2.5",""),
    ("은평","관리처분<철거>",5,"갈현1",4116,6.2,10.3,"4.5",""),
    ("은평","사업시행인가",3,"불광5",2393,4.1,12.2,"6.5",""),
    ("동대문","관리처분<분양>",5,"청량리7",761,7.6,9.9,"1",""),
    ("동대문","관리처분인가",4,"제기6",423,7.1,11.1,"5.5",""),
    ("동대문","사업시행인가",3,"청량리8",610,6.5,15.2,"8",""),
    ("동대문","관리처분인가",4,"제기4",909,8.8,12.8,"4.5",""),
    ("동대문","관리처분<분양>",5,"이문1",3071,7.1,13.8,"0.25",""),
    ("동대문","관리처분<분양>",5,"이문3-1",4172,9.1,13.2,"1.5",""),
    ("동대문","관리처분<분양>",5,"휘경3",1806,11.6,14,"1",""),
    ("동대문","관리처분인가",4,"이문4",3541,5.1,13.8,"6",""),
    ("동대문","관리처분<분양>",5,"답십리17",326,3.3,13.3,"1",""),
    ("성북","관리처분<분양>",5,"보문5",199,3.4,13.4,"2",""),
    ("성북","관리처분<철거>",5,"삼선5",1223,6.3,9.5,"2.5",""),
    ("성북","관리처분<철거>",5,"동선2",334,4.1,9.4,"3.5",""),
    ("성북","관리처분<분양>",5,"장위4",2840,9.5,13.2,"1",""),
    ("성북","관리처분<철거>",5,"장위10",2004,6.9,11,"4",""),
    ("성북","관리처분<분양>",5,"장위6",1637,7.4,13.1,"3",""),
    ("성북","관리처분인가",4,"정릉골재개발",1411,2.3,11.6,"5",""),
    ("성북","관리처분인가",4,"신월곡1",2244,10.5,15.7,"5",""),
    ("강북","관리처분인가",4,"미아3측",1037,5.2,11.1,"5.5",""),
    ("강북","사업시행인가",3,"미아9-2",1798,6.1,11.3,"8",""),
    ("중랑","관리처분<분양>",5,"중화1",1057,2.6,11.3,"1.5",""),
    ("노원","사업시행인가",3,"상계1",1388,3.4,10.3,"6",""),
    ("노원","사업시행인가",3,"상계2",2126,3.8,10.5,"6.5",""),
    ("노원","관리처분인가",4,"백사마을",2473,3.7,10.6,"5",""),
]

# ─── DB 초기화 ──────────────────────────────────
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
            location TEXT, stage TEXT, stage_order INTEGER,
            district_name TEXT UNIQUE, households INTEGER,
            initial_investment REAL, total_investment REAL,
            time_required TEXT, notes TEXT,
            last_updated TEXT, source_url TEXT
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
            description TEXT, published_at TEXT, collected_at TEXT
        );
    """)
    # 시드 데이터
    count = conn.execute("SELECT COUNT(*) c FROM districts").fetchone()["c"]
    if count == 0:
        for row in SEED:
            try:
                conn.execute("""INSERT INTO districts 
                    (location,stage,stage_order,district_name,households,
                     initial_investment,total_investment,time_required,notes,last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (*row, datetime.now().isoformat()))
            except sqlite3.IntegrityError:
                pass
        conn.commit()
        print(f"✅ {len(SEED)}개 시드 데이터 투입")
    conn.commit()

# ─── 뉴스 수집기 ────────────────────────────────
async def fetch_naver_news(query: str, days: int = 2):
    if not NAVER_CLIENT_ID:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://openapi.naver.com/v1/search/news.json",
                params={"query": query, "display": 20, "sort": "date"},
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

async def update_single_district(district):
    name = district["district_name"]
    location = district["location"]
    query = f"{name} 재개발"
    
    news_items = await fetch_naver_news(query, days=2)
    new_stage, source_url = detect_stage_change(news_items)
    
    conn = get_db()
    updates = {}
    
    # 단계 업데이트 (더 진행된 단계일 때만)
    if new_stage and new_stage in STAGE_ORDER:
        new_order = STAGE_ORDER[new_stage]
        if new_order > district["stage_order"]:
            updates["stage"] = new_stage
            updates["stage_order"] = new_order
            updates["source_url"] = source_url
    
    # 뉴스 저장
    for item in news_items[:5]:
        try:
            conn.execute("""INSERT OR IGNORE INTO news_items
                (district_id, title, url, description, published_at, collected_at)
                VALUES (?,?,?,?,?,?)""",
                (district["id"], item["title"], item["url"],
                 item["description"], item["published_at"], datetime.now().isoformat()))
        except:
            pass
    
    # 변경 감지 및 DB 업데이트
    if updates:
        for field, new_val in updates.items():
            old_val = district[field] if field in district.keys() else None
            if old_val != new_val:
                conn.execute("""INSERT INTO change_logs
                    (district_id, field, old_value, new_value, source, changed_at)
                    VALUES (?,?,?,?,?,?)""",
                    (district["id"], field, str(old_val or ""), str(new_val),
                     source_url or "auto", datetime.now().isoformat()))
        
        updates["last_updated"] = datetime.now().isoformat()
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE districts SET {set_clause} WHERE id=?",
                    (*updates.values(), district["id"]))
        print(f"✅ {name}: {updates}")
    
    conn.commit()

async def scheduled_update():
    print(f"\n{'='*50}\n🔄 자동 업데이트 시작: {datetime.now()}\n{'='*50}")
    conn = get_db()
    districts = conn.execute("SELECT * FROM districts WHERE stage_order >= 3").fetchall()
    
    sem = asyncio.Semaphore(3)
    async def wrapped(d):
        async with sem:
            await update_single_district(dict(d))
            await asyncio.sleep(1)
    
    await asyncio.gather(*[wrapped(d) for d in districts])
    print(f"\n✅ 완료: {datetime.now()}\n")

# ─── FastAPI 앱 ─────────────────────────────────
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
                          ORDER BY published_at DESC LIMIT 10""", (did,)).fetchall()
    return {"district": dict(d), "history": [dict(l) for l in logs],
            "news": [dict(n) for n in news]}

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
    ws.title = "재개발현황"
    ws.append(["위치","단계","구역명","세대수","초투","총투","소요","메모","최종업데이트"])
    for r in rows:
        d = dict(r)
        ws.append([d["location"], d["stage"], d["district_name"], d["households"],
                  d["initial_investment"], d["total_investment"], d["time_required"],
                  d["notes"], d["last_updated"]])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    fn = f"재개발현황_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return StreamingResponse(buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fn}"})

# ─── 프런트엔드 HTML ────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def index():
    return FRONTEND_HTML

FRONTEND_HTML = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>서울 재개발 현황</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdn.tailwindcss.com"></script>
<style>body{font-family:-apple-system,'Apple SD Gothic Neo','Malgun Gothic',sans-serif}
.stage-사업시행인가{background:#dbeafe;color:#1e40af}
.stage-관리처분인가{background:#fef3c7;color:#92400e}
.stage-관리처분철거{background:#fee2e2;color:#991b1b}
.stage-관리처분분양{background:#dcfce7;color:#166534}
.stage-착공분양{background:#e0e7ff;color:#3730a3}
tr.row:hover{background:#f8fafc;cursor:pointer}
th.sortable{cursor:pointer;user-select:none;position:relative;padding-right:18px!important}
th.sortable:hover{background:#e2e8f0}
th.sortable::after{content:'⇅';position:absolute;right:4px;opacity:0.3;font-size:10px}
th.sort-asc::after{content:'▲';opacity:1;color:#2563eb}
th.sort-desc::after{content:'▼';opacity:1;color:#2563eb}
</style></head>
<body class="bg-slate-50"><div class="max-w-screen-2xl mx-auto p-4">
<header class="flex justify-between items-center mb-4">
<div><h1 class="text-2xl font-bold">서울 재개발 현황 (자동 업데이트)</h1>
<p class="text-sm text-slate-500"><span id="upd"></span> · 매일 06:00 자동 갱신</p></div>
<div class="flex gap-2"><button onclick="refresh()" class="px-3 py-2 bg-blue-600 text-white rounded text-sm">🔄 지금 업데이트</button>
<a href="/api/export/excel" class="px-3 py-2 bg-green-600 text-white rounded text-sm">📥 엑셀</a></div>
</header>
<div id="stats" class="grid grid-cols-2 md:grid-cols-5 gap-3 mb-4"></div>
<div class="bg-white p-3 mb-3 rounded border flex gap-2 flex-wrap">
<select id="f-loc" onchange="render()" class="border rounded px-2 py-1 text-sm"><option value="">전체 위치</option></select>
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
<th class="p-2 text-left sortable" onclick="sortBy('stage_order')">단계</th>
<th class="p-2 text-left sortable" onclick="sortBy('district_name')">구역명</th>
<th class="p-2 text-right sortable" onclick="sortBy('households')">세대수</th>
<th class="p-2 text-right sortable" onclick="sortBy('initial_investment')">초투</th>
<th class="p-2 text-right sortable" onclick="sortBy('total_investment')">총투</th>
<th class="p-2 text-right sortable" onclick="sortBy('time_required')">소요</th>
<th class="p-2 text-center sortable" onclick="sortBy('last_updated')">업데이트</th>
</tr></thead><tbody id="tbody"></tbody></table></div></div>
<div id="modal" class="hidden fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick="this.classList.add('hidden')">
<div class="bg-white rounded-lg max-w-3xl w-full max-h-[90vh] overflow-y-auto p-5" onclick="event.stopPropagation()">
<div class="flex justify-between mb-4"><h2 id="m-title" class="text-xl font-bold"></h2>
<button onclick="document.getElementById('modal').classList.add('hidden')" class="text-2xl">&times;</button></div>
<div id="m-content"></div></div></div>
<script>
let data=[];
let sortState={key:'location',dir:'asc'};

async function load(){
  data=await fetch('/api/districts').then(r=>r.json());
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
  if(sortState.key===key){
    sortState.dir=sortState.dir==='asc'?'desc':'asc';
  } else {
    sortState.key=key;
    sortState.dir='asc';
  }
  render();
}

function compareVal(a,b,key){
  let va=a[key], vb=b[key];
  if(va==null && vb==null) return 0;
  if(va==null) return 1;
  if(vb==null) return -1;
  const numKeys=['households','initial_investment','total_investment','stage_order'];
  if(numKeys.includes(key)){
    return (parseFloat(va)||0)-(parseFloat(vb)||0);
  }
  if(key==='time_required'){
    const na=parseFloat(va), nb=parseFloat(vb);
    if(!isNaN(na) && !isNaN(nb)) return na-nb;
    if(!isNaN(na)) return -1;
    if(!isNaN(nb)) return 1;
    return String(va).localeCompare(String(vb),'ko');
  }
  if(key==='last_updated'){
    return new Date(va)-new Date(vb);
  }
  return String(va).localeCompare(String(vb),'ko');
}

function render(){
  const loc=document.getElementById('f-loc').value;
  const stage=document.getElementById('f-stage').value;
  const q=document.getElementById('f-search').value.toLowerCase();
  let f=data.filter(d=>(!loc||d.location===loc)&&(!stage||d.stage===stage)&&(!q||d.district_name.toLowerCase().includes(q)));
  
  f.sort((a,b)=>{
    const cmp=compareVal(a,b,sortState.key);
    return sortState.dir==='asc'?cmp:-cmp;
  });
  
  document.querySelectorAll('th.sortable').forEach(th=>{
    th.classList.remove('sort-asc','sort-desc');
  });
  const colMap={'location':0,'stage_order':1,'district_name':2,'households':3,
                'initial_investment':4,'total_investment':5,'time_required':6,'last_updated':7};
  const idx=colMap[sortState.key];
  if(idx!==undefined){
    const ths=document.querySelectorAll('th.sortable');
    if(ths[idx]) ths[idx].classList.add(sortState.dir==='asc'?'sort-asc':'sort-desc');
  }
  
  document.getElementById('tbody').innerHTML=f.map(d=>{
    const c='stage-'+(d.stage||'').replace(/[<>\\/]/g,'');
    const upd=d.last_updated?new Date(d.last_updated).toLocaleDateString('ko-KR'):'-';
    return `<tr class="row border-t" onclick="detail(${d.id})"><td class="p-2">${d.location}</td>
<td class="p-2"><span class="${c} px-2 py-0.5 rounded text-xs">${d.stage}</span></td>
<td class="p-2 font-medium">${d.district_name}</td>
<td class="p-2 text-right">${d.households?.toLocaleString()||'-'}</td>
<td class="p-2 text-right">${d.initial_investment??'-'}</td>
<td class="p-2 text-right">${d.total_investment??'-'}</td>
<td class="p-2 text-right text-slate-500">${d.time_required||'-'}</td>
<td class="p-2 text-center text-xs text-slate-500">${upd}</td></tr>`;
  }).join('');
}

async function detail(id){
  const r=await fetch('/api/districts/'+id).then(r=>r.json());
  const d=r.district;
  document.getElementById('m-title').textContent=`${d.location} ${d.district_name}`;
  const nm=encodeURIComponent(d.district_name+' 재개발');
  document.getElementById('m-content').innerHTML=`
<div class="grid grid-cols-2 gap-3 mb-4 text-sm">
<div><span class="text-slate-500">단계:</span> <b>${d.stage}</b></div>
<div><span class="text-slate-500">세대수:</span> <b>${d.households?.toLocaleString()||'-'}</b></div>
<div><span class="text-slate-500">초투:</span> <b>${d.initial_investment??'-'}억</b></div>
<div><span class="text-slate-500">총투:</span> <b>${d.total_investment??'-'}억</b></div>
<div class="col-span-2"><span class="text-slate-500">메모:</span> ${d.notes||'-'}</div></div>
<div class="mb-4"><p class="font-medium mb-2">🔗 외부 링크</p>
<div class="grid grid-cols-3 md:grid-cols-5 gap-2 text-xs">
<a href="https://search.naver.com/search.naver?where=news&query=${nm}&sort=1" target="_blank" class="bg-blue-50 text-blue-700 p-2 rounded text-center">📰 뉴스</a>
<a href="https://search.naver.com/search.naver?where=post&query=${nm}&sort=1" target="_blank" class="bg-green-50 text-green-700 p-2 rounded text-center">📝 블로그</a>
<a href="https://new.land.naver.com/search?sk=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-orange-50 text-orange-700 p-2 rounded text-center">🏠 부동산</a>
<a href="https://hogangnono.com/search?q=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-purple-50 text-purple-700 p-2 rounded text-center">📊 호갱노노</a>
<a href="https://cleanup.seoul.go.kr/search/searchResult.do?searchWord=${encodeURIComponent(d.district_name)}" target="_blank" class="bg-slate-100 text-slate-700 p-2 rounded text-center">🏛️ 정보몽땅</a>
</div></div>
<p class="font-medium mb-2">📰 최근 뉴스 (자동 수집)</p>
${r.news.length===0?'<p class="text-sm text-slate-400 p-2">수집된 뉴스 없음</p>':
'<ul class="space-y-2 mb-4">'+r.news.map(n=>`<li class="text-sm border-l-2 border-blue-400 pl-2"><a href="${n.url}" target="_blank" class="font-medium hover:text-blue-600">${n.title}</a><div class="text-xs text-slate-500">${n.description.slice(0,100)}...</div></li>`).join('')+'</ul>'}
<p class="font-medium mb-2">📜 변경 이력</p>
${r.history.length===0?'<p class="text-sm text-slate-400 p-2">변경 이력 없음</p>':
'<table class="w-full text-xs border"><thead class="bg-slate-50"><tr><th class="p-2 text-left">시각</th><th class="p-2 text-left">필드</th><th class="p-2 text-left">이전</th><th class="p-2 text-left">이후</th></tr></thead><tbody>'+
r.history.map(l=>`<tr class="border-t"><td class="p-2">${new Date(l.changed_at).toLocaleDateString('ko-KR')}</td><td class="p-2">${l.field}</td><td class="p-2 text-red-600">${l.old_value||'-'}</td><td class="p-2 text-green-600">${l.new_value||'-'}</td></tr>`).join('')+'</tbody></table>'}`;
  document.getElementById('modal').classList.remove('hidden');
}

async function refresh(){
  if(!confirm('지금 전체 업데이트를 실행할까요? (약 1-2분 소요)'))return;
  await fetch('/api/refresh-all',{method:'POST'});
  alert('업데이트 시작됨. 1-2분 후 페이지를 새로고침하세요.');
}
load();setInterval(load,60000);
</script></body></html>"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
