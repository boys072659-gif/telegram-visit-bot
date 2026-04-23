"""
결석자 타겟 심방 텔레그램 봇 v4
완전 버튼식 UI + 전체 사용법 + 특별관리 + DB 진단

흐름:
  /start → 메인 메뉴 (3버튼)
    📋 결석자 심방 → 교회 → 부서 → 지역입력 → 결석자 → 8단계 기록
    🚨 특별관리 → 교회 → 부서 → 4회+명단 → 선택(방감지) → 4항목 체크리스트
    ❓ 도움말 → 전체 사용법

Cloud Run (Python 3.11) + python-telegram-bot 20.x + Supabase REST API
"""

import os
import re
import json
import logging
import httpx
from urllib.parse import quote
from datetime import datetime, time as dtime, timezone, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, WebAppInfo, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

# ── 환경변수 ──────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN = os.environ["TELEGRAM_TOKEN"]
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]

KST = timezone(timedelta(hours=9))
WEEKLY_REMINDER_HOUR = int(os.environ.get("WEEKLY_REMINDER_HOUR", "19"))
WEEKLY_REMINDER_MIN  = int(os.environ.get("WEEKLY_REMINDER_MIN", "0"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=representation",
}

CHURCHES = ["서울교회", "포천교회", "구리교회", "동대문교회", "의정부교회"]
DEPTS    = ["자문회", "장년회", "부녀회", "청년회"]

# ── 심방 입력 8단계 ────────────────────────────────────────────────────────────
STEPS = ["shepherd", "date", "plan", "target", "done", "worship", "note", "attendance"]
STEP_LABELS = {
    "shepherd":   "👤 심방자 (예: 홍길동(집사))",
    "date":       "📅 심방날짜 (예: 4/27 또는 2026-04-27)",
    "plan":       "📝 심방계획 (간단히)",
    "target":     "🎯 타겟여부",
    "done":       "✅ 진행여부",
    "worship":    "🙏 예배확답",
    "note":       "📋 진행사항 (없으면 '없음')",
    "attendance": "⛪ 예배참석",
}
STEP_CHOICES = {
    "target":     [["타겟", "미타겟"]],
    "done":       [["완료", "미완료"]],
    "worship":    [["확정", "미정", "불참"]],
    "attendance": [["참석", "불참"]],
}

# ── 특별관리 4항목 ─────────────────────────────────────────────────────────────
# 1번: 최초 1회만 · 2/3/4번: 매주 화요일 19시 초기화
SP_ITEM_LABELS = {
    "item1_chat_invited":  "대책방 초대완료 (구역장·인섬교·강사·전도사·심방부사명자)",
    "item2_feedback_done": "금주 피드백 진행",
    "item3_visit_date":    "금주 심방예정일",
    "item4_visit_plan":    "금주 심방계획",
}

# ── 마크다운 이스케이프 ────────────────────────────────────────────────────────
# Telegram legacy Markdown v1 파서는 * _ ` [ 가 제대로 쌍을 이루지 않으면 에러.
# 이름 안에 * 있으면 (예: "박*준") 파서가 bold 시작으로 인식하고 닫는 * 를 못 찾아 실패.
# 해결: 모든 동적 텍스트의 _ * ` [ ] 를 전부 이스케이프.
_MD_SPECIALS = "_*`[]()"
def md(s) -> str:
    """Markdown v1에서 안전하게 표시되도록 특수문자 이스케이프."""
    if s is None: return ""
    return "".join(("\\" + c) if c in _MD_SPECIALS else c for c in str(s))

def plain(s) -> str:
    """마크다운 없이 사용할 때 - 그냥 반환"""
    if s is None: return ""
    return str(s)


async def safe_send(send_func, text: str, **kwargs):
    """Markdown 파싱 실패 시 자동으로 plain text로 fallback 전송."""
    try:
        return await send_func(text, **kwargs)
    except Exception as e:
        emsg = str(e)
        if "parse" in emsg.lower() or "entity" in emsg.lower() or "markdown" in emsg.lower():
            logger.warning("Markdown parse failed (%s), retrying as plain text", emsg)
            kwargs.pop("parse_mode", None)
            # 마크다운 특수문자 전부 제거한 평문
            plain_text = text
            for ch in ("*", "_", "`"):
                plain_text = plain_text.replace(ch, "")
            plain_text = plain_text.replace("\\[", "[").replace("\\]", "]")
            plain_text = plain_text.replace("\\(", "(").replace("\\)", ")")
            try:
                return await send_func(plain_text, **kwargs)
            except Exception as e2:
                logger.exception("plain fallback also failed: %s", e2)
                raise
        else:
            raise


# ═════════════════════════════════════════════════════════════════════════════
# Supabase 헬퍼 (에러 메시지 친절하게)
# ═════════════════════════════════════════════════════════════════════════════
async def sb_get(path: str):
    """GET /rest/v1/{path} → JSON 반환. 빈 응답은 [] 반환."""
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, timeout=15)
        if r.status_code >= 400:
            logger.error("sb_get %s failed %d: %s", path, r.status_code, r.text[:300])
        r.raise_for_status()
        if not r.content or not r.content.strip():
            return []
        try:
            return r.json()
        except Exception:
            return []

async def sb_rpc(func: str, payload: dict):
    """RPC 호출. 404 (함수 없음) 시 RuntimeError 발생."""
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/rpc/{func}",
            headers=HEADERS,
            content=json.dumps(payload),
            timeout=15,
        )
        if r.status_code == 404:
            raise RuntimeError(f"RPC '{func}' 가 DB에 없습니다. SQL 마이그레이션 필요.")
        if r.status_code >= 400:
            logger.error("RPC %s failed %d: %s", func, r.status_code, r.text[:300])
        r.raise_for_status()
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return None


# ═════════════════════════════════════════════════════════════════════════════
# 이름 마스킹 복구 ("김*영" → 교적에서 "김지영" 찾아 반환)
# ═════════════════════════════════════════════════════════════════════════════
import re as _re_name

def _is_masked_name(s: str) -> bool:
    """이름에 * 같은 마스킹 문자가 있는지"""
    if not s: return False
    return ('*' in s) or ('_' in s and len(s) <= 5)


async def resolve_real_name(
    masked_name: str,
    church: str = None,
    dept: str = None,
    phone_last4: str = None,
) -> str:
    """
    마스킹된 이름(예: 김*영)을 교적에서 역조회하여 실제 이름으로 복구.
    매칭 우선순위:
      1. church + dept + phone_last4 + 이름 패턴
      2. church + phone_last4 + 이름 패턴
      3. phone_last4 + 이름 패턴
    매칭 실패 시 원래 마스킹 이름 반환.
    """
    if not masked_name or not _is_masked_name(masked_name):
        return masked_name

    # 이름 패턴 생성: "김*영" → "^김.영$"
    try:
        pattern = '^' + _re_name.escape(masked_name).replace(r'\*', '.').replace(r'\_', '.') + '$'
    except Exception:
        return masked_name

    # 1차: 교회 + 부서 + 전화뒷4 일치
    params = []
    if church:     params.append(f"church=eq.{quote(church)}")
    if dept:       params.append(f"dept=eq.{quote(dept)}")
    if phone_last4:params.append(f"phone_last4=eq.{quote(phone_last4)}")
    params.append("limit=30")

    if len(params) >= 3:  # 최소 교회+뭔가+전화 또는 뭔가 2개+전화
        try:
            rows = await sb_get(f"church_member_registry?select=name&" + "&".join(params))
            for r in rows or []:
                nm = r.get("name", "")
                if nm and _re_name.match(pattern, nm):
                    return nm
        except Exception:
            pass

    # 2차: 교회 + 전화뒷4
    if church and phone_last4:
        try:
            rows = await sb_get(
                f"church_member_registry?select=name"
                f"&church=eq.{quote(church)}&phone_last4=eq.{quote(phone_last4)}&limit=30"
            )
            for r in rows or []:
                nm = r.get("name", "")
                if nm and _re_name.match(pattern, nm):
                    return nm
        except Exception:
            pass

    # 3차: 전화뒷4만 (마지막 수단)
    if phone_last4:
        try:
            rows = await sb_get(
                f"church_member_registry?select=name,church"
                f"&phone_last4=eq.{quote(phone_last4)}&limit=30"
            )
            # 교회 일치하면 우선 반환
            for r in rows or []:
                nm = r.get("name", "")
                if not nm: continue
                if not _re_name.match(pattern, nm): continue
                if church and r.get("church") == church:
                    return nm
            # 교회 안 맞아도 패턴 일치면 반환
            for r in rows or []:
                nm = r.get("name", "")
                if nm and _re_name.match(pattern, nm):
                    return nm
        except Exception:
            pass

    return masked_name  # 매칭 실패 시 원본 반환


async def enrich_names(rows: list, church_key: str = "church", dept_key: str = "dept",
                       name_key: str = "name", phone_key: str = "phone_last4") -> list:
    """결석자 행 목록을 받아서 각 행의 name을 실제 이름으로 복구."""
    if not rows: return rows
    for r in rows:
        nm = r.get(name_key, "")
        if _is_masked_name(nm):
            real = await resolve_real_name(
                nm,
                church=r.get(church_key),
                dept=r.get(dept_key),
                phone_last4=r.get(phone_key),
            )
            if real and real != nm:
                r[name_key] = real
                r["_original_name"] = nm  # 원본 보존
    return rows


# ═════════════════════════════════════════════════════════════════════════════
# 주차 계산 (화요일 18시 KST 기준)
# ═════════════════════════════════════════════════════════════════════════════
def compute_target_week_key() -> tuple[str, str]:
    """지금 시점 기준 타겟 주일 주차의 (week_key, week_label) 반환."""
    now = datetime.now(KST)
    weekday = now.weekday()  # 0=월 … 6=일
    hour = now.hour

    if weekday == 6:
        diff = 0
    elif weekday == 0 or (weekday == 1 and hour < 18):
        diff = -(weekday + 1)
    else:
        diff = 6 - weekday

    sunday = (now + timedelta(days=diff)).replace(hour=0, minute=0, second=0, microsecond=0)
    year, month = sunday.year, sunday.month

    # 해당 월의 몇 번째 일요일인지
    first = datetime(year, month, 1, tzinfo=KST)
    week_no = 0
    for d in range(1, sunday.day + 1):
        cur = first.replace(day=d)
        if cur.weekday() == 6:
            week_no += 1
            if d == sunday.day:
                break
    if week_no == 0:
        week_no = 1

    week_key = f"{year}-{month:02d}-w{week_no}"
    week_label = f"{year}년 {month}월 {week_no}주차"
    return week_key, week_label


async def get_active_week() -> tuple[str, str]:
    """현재 사용할 week_key, week_label. DB에 기대 주차 있으면 그것, 없으면 최신."""
    expected_key, expected_label = compute_target_week_key()
    try:
        rows = await sb_get(
            f"weekly_target_weeks?select=week_key,week_label&week_key=eq.{quote(expected_key)}&limit=1"
        )
        if rows:
            return rows[0]["week_key"], rows[0].get("week_label", expected_label)
    except Exception as e:
        logger.warning("get_active_week check failed: %s", e)
    try:
        rows = await sb_get("weekly_target_weeks?select=week_key,week_label&order=week_key.desc&limit=1")
        if rows:
            return rows[0]["week_key"], rows[0].get("week_label", rows[0]["week_key"])
    except Exception as e:
        logger.warning("get_active_week fallback failed: %s", e)
    return "", ""


# ═════════════════════════════════════════════════════════════════════════════
# 컨텍스트 (세션)
# ═════════════════════════════════════════════════════════════════════════════
async def get_ctx(chat_id: int):
    try:
        rows = await sb_rpc("get_telegram_visit_context", {"p_chat_id": chat_id})
        if rows:
            return rows[0] if isinstance(rows, list) else rows
    except Exception as e:
        logger.warning("get_ctx failed: %s", e)
    return None

async def save_ctx(chat_id: int, **kwargs):
    payload = {"p_chat_id": chat_id}
    for k, v in kwargs.items():
        payload[f"p_{k}"] = v
    try:
        await sb_rpc("set_telegram_visit_context", payload)
    except Exception as e:
        logger.warning("save_ctx failed: %s", e)

async def clear_tmp(chat_id: int):
    try:
        await sb_rpc("clear_telegram_tmp", {"p_chat_id": chat_id})
    except Exception as e:
        logger.warning("clear_tmp failed: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# 구역 정규화 (2-1 ↔ 2팀1)
# ═════════════════════════════════════════════════════════════════════════════
def normalize_zone(z: str) -> str:
    if not z: return ""
    s = re.sub(r"\s+", "", z.strip())
    m = re.match(r"^(\d+)[-_](\d+)$", s)
    if m: return f"{m.group(1)}팀{m.group(2)}"
    m = re.match(r"^(\d+)팀(\d+)$", s)
    if m: return f"{m.group(1)}팀{m.group(2)}"
    return s

def looks_like_zone(text: str) -> bool:
    s = re.sub(r"\s+", "", text.strip())
    return bool(re.match(r"^\d+[-_팀]\d+$", s))


# ═════════════════════════════════════════════════════════════════════════════
# 결석자 조회 (RPC 시도 → REST 폴백)
# ═════════════════════════════════════════════════════════════════════════════
async def fetch_absentees_by_region(week_key: str, church: str, dept: str, region: str):
    """교회+부서+지역으로 결석자 조회. RPC 실패 시 REST 폴백."""
    # RPC 시도
    try:
        rows = await sb_rpc("get_absentees_by_dept_region", {
            "p_week_key": week_key, "p_dept": dept, "p_region": region
        })
        if rows is None: rows = []
        # RPC는 church 필터가 없으니 클라이언트에서 교회 필터
        rows = [r for r in rows if r.get("church", church) == church or not r.get("church")]
        # dept 정보를 추가 (RPC가 반환 안 할 수 있음)
        for r in rows:
            if not r.get("dept"): r["dept"] = dept
            if not r.get("church"): r["church"] = church
        return await enrich_names(rows)
    except Exception as e:
        logger.info("RPC get_absentees_by_dept_region 폴백: %s", e)

    # REST 폴백
    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{quote(week_key)}"
        f"&church=eq.{quote(church)}"
        f"&dept=eq.{quote(dept)}"
        f"&region_name=eq.{quote(region)}"
        f"&order=zone_name.asc,name.asc"
    )
    rows = await sb_get(path)
    return await enrich_names(rows)


async def fetch_absentees_by_zone(week_key: str, church: str, dept: str, zone: str):
    """교회+부서+구역으로 결석자 조회."""
    normalized = normalize_zone(zone)
    # RPC 시도
    try:
        rows = await sb_rpc("get_absentees_by_dept_zone", {
            "p_week_key": week_key, "p_dept": dept, "p_zone": normalized
        })
        if rows is None: rows = []
        rows = [r for r in rows if r.get("church", church) == church or not r.get("church")]
        for r in rows:
            if not r.get("dept"): r["dept"] = dept
            if not r.get("church"): r["church"] = church
        return await enrich_names(rows)
    except Exception as e:
        logger.info("RPC get_absentees_by_dept_zone 폴백: %s", e)

    # REST 폴백 - 정규화된 구역명으로 시도, 실패 시 원본으로
    for try_zone in [normalized, zone]:
        path = (
            f"weekly_visit_targets"
            f"?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
            f"&week_key=eq.{quote(week_key)}"
            f"&church=eq.{quote(church)}"
            f"&dept=eq.{quote(dept)}"
            f"&zone_name=eq.{quote(try_zone)}"
            f"&order=name.asc"
        )
        rows = await sb_get(path)
        if rows: return await enrich_names(rows)
    return []


async def fetch_absentees_4plus(week_key: str, church: str, dept: str):
    """4회 이상 연속결석자 (교회+부서)."""
    try:
        rows = await sb_rpc("get_absentees_4plus_by_dept", {
            "p_week_key": week_key, "p_dept": dept
        })
        if rows is None: rows = []
        rows = [r for r in rows if r.get("church", church) == church or not r.get("church")]
        for r in rows:
            if not r.get("dept"): r["dept"] = dept
            if not r.get("church"): r["church"] = church
        return await enrich_names(rows)
    except Exception as e:
        logger.info("RPC get_absentees_4plus_by_dept 폴백: %s", e)

    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{quote(week_key)}"
        f"&church=eq.{quote(church)}"
        f"&dept=eq.{quote(dept)}"
        f"&consecutive_absent_count=gte.4"
        f"&order=consecutive_absent_count.desc,name.asc"
    )
    rows = await sb_get(path)
    return await enrich_names(rows)


async def get_progress(week_key: str, row_id: str):
    rows = await sb_get(
        f"weekly_visit_progress?select=*&week_key=eq.{quote(week_key)}&row_id=eq.{quote(row_id)}"
    )
    return rows[0] if rows else None


async def upsert_progress(week_key: str, row_id: str, ctx: dict):
    raw_date = ctx.get("tmp_date") or ""
    date_sort = raw_date if re.match(r"^\d{4}-\d{2}-\d{2}$", raw_date) else None
    await sb_rpc("upsert_weekly_visit_progress", {
        "p_week_key":           week_key,
        "p_row_id":             row_id,
        "p_shepherd":           ctx.get("tmp_shepherd") or "",
        "p_visit_date_display": raw_date,
        "p_visit_date_sort":    date_sort,
        "p_plan_text":          ctx.get("tmp_plan") or "",
        "p_is_target":          ctx.get("tmp_target") == "타겟",
        "p_is_done":            ctx.get("tmp_done") == "완료",
        "p_worship":            ctx.get("tmp_worship") or None,
        "p_attendance":         ctx.get("tmp_attendance") or None,
        "p_note":               ctx.get("tmp_note") or "",
    })


# ═════════════════════════════════════════════════════════════════════════════
# 키보드 빌더
# ═════════════════════════════════════════════════════════════════════════════
# 미니웹앱 URL (환경변수 MINIAPP_URL로 지정. 없으면 WEBHOOK_URL에서 /miniapp 접미사 자동 유도)
MINIAPP_URL = os.environ.get("MINIAPP_URL", "")
if not MINIAPP_URL:
    _webhook = os.environ.get("WEBHOOK_URL", "")
    if _webhook:
        # 예: https://xxx.run.app/webhook → https://xxx.run.app/miniapp
        MINIAPP_URL = _webhook.rsplit("/", 1)[0] + "/miniapp"


def kb_reply_main(is_private: bool = True) -> ReplyKeyboardMarkup:
    """하단에 고정되는 리플라이 키보드. 키보드 아이콘(⌨️) 탭하면 이 버튼들이 나옴.
    
    ⚠️ 웹앱 버튼은 1:1 개인 채팅에서만 작동. 그룹에서는 제외.
    """
    rows = [
        [KeyboardButton("📋 결석자 심방"), KeyboardButton("🚨 특별관리결석자")],
    ]
    # 웹앱 버튼은 개인 채팅에서만 추가 (그룹에서는 "Web app buttons can be used in private chats only" 에러 발생)
    if is_private and MINIAPP_URL.startswith("https://"):
        rows.append([KeyboardButton(
            "📝 결석자 심방 기록 (폼)",
            web_app=WebAppInfo(url=MINIAPP_URL)
        )])
    rows.append([KeyboardButton("❓ 사용법"), KeyboardButton("🏠 메인 메뉴")])
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="메뉴를 선택하세요",
    )


def kb_main_menu(is_private: bool = True) -> InlineKeyboardMarkup:
    """인라인 메인 메뉴. 웹앱 버튼은 개인 채팅에서만."""
    rows = [
        [InlineKeyboardButton("📋 결석자 심방",       callback_data="m:absentee")],
        [InlineKeyboardButton("🚨 특별관리결석자",    callback_data="m:special")],
    ]
    # 웹앱 버튼은 개인 채팅에서만
    if is_private and MINIAPP_URL.startswith("https://"):
        rows.append([InlineKeyboardButton(
            "📝 결석자 심방 기록 (미니웹앱)",
            web_app=WebAppInfo(url=MINIAPP_URL)
        )])
    rows += [
        [InlineKeyboardButton("❓ 사용법 (도움말)",    callback_data="m:help")],
        [InlineKeyboardButton("🔍 DB 진단",            callback_data="m:diagnose")],
    ]
    return InlineKeyboardMarkup(rows)


def kb_cancel_only() -> InlineKeyboardMarkup:
    """입력 중단용 취소 버튼만 있는 키보드."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("❌ 입력 취소", callback_data="flow_cancel")
    ]])


def is_private_chat(update: Update) -> bool:
    """개인 채팅(1:1) 여부 판별. 그룹/수퍼그룹/채널은 False."""
    try:
        chat = update.effective_chat
        return chat is not None and chat.type == "private"
    except Exception:
        return True  # 알 수 없으면 안전하게 private으로

def kb_church_select(flow: str) -> InlineKeyboardMarkup:
    """flow: 'abs' | 'sp'"""
    rows = []
    for i in range(0, len(CHURCHES), 2):
        row = []
        for ch in CHURCHES[i:i+2]:
            row.append(InlineKeyboardButton(ch, callback_data=f"{flow}_ch:{ch}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    return InlineKeyboardMarkup(rows)

def kb_dept_select(flow: str, church: str) -> InlineKeyboardMarkup:
    """flow: 'abs' | 'sp'"""
    rows = [
        [
            InlineKeyboardButton(DEPTS[0], callback_data=f"{flow}_dp:{church}:{DEPTS[0]}"),
            InlineKeyboardButton(DEPTS[1], callback_data=f"{flow}_dp:{church}:{DEPTS[1]}"),
        ],
        [
            InlineKeyboardButton(DEPTS[2], callback_data=f"{flow}_dp:{church}:{DEPTS[2]}"),
            InlineKeyboardButton(DEPTS[3], callback_data=f"{flow}_dp:{church}:{DEPTS[3]}"),
        ],
        [InlineKeyboardButton("◀ 교회 다시 선택", callback_data=f"m:{'absentee' if flow=='abs' else 'special'}")],
    ]
    return InlineKeyboardMarkup(rows)


# ═════════════════════════════════════════════════════════════════════════════
# 명령어 핸들러
# ═════════════════════════════════════════════════════════════════════════════
HELP_TEXT = (
    "📖 *결석자 타겟 심방 봇 — 사용법*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "*1️⃣ 방 설정 (최초 1회)*\n"
    "`/start` 또는 `/setup` 으로 이 방의 담당 범위 설정:\n"
    "   교회 → 부서 → 지역 → 구역\n"
    "교회까지만 설정하면 *교회 전체 결석자* 조회 가능\n"
    "설정 후 `📋 결석자 심방` 에서 해당 범위의 결석자만 표시\n\n"

    "*2️⃣ 결석자 심방 기록 흐름*\n"
    "메인 메뉴 → `📋 결석자 심방` 탭\n"
    "→ 결석자 선택 → *8단계* 순차 입력:\n"
    "   ① 심방자 ② 심방날짜 ③ 심방계획\n"
    "   ④ 타겟여부 ⑤ 진행여부 ⑥ 예배확답\n"
    "   ⑦ 진행사항 ⑧ 예배참석\n"
    "→ 모든 필드 완료 후 확인 → 저장\n"
    "입력 중 `❌ 입력 취소` 버튼 또는 `/cancel` 로 중단 가능\n\n"

    "*3️⃣ 특별관리 결석자 (연속결석 4회 이상)*\n"
    "메인 메뉴 → `🚨 특별관리결석자` 탭\n"
    "→ 교회 → 부서 → 결석자 선택\n"
    "→ 이 방이 *감지방* 으로 등록되고 4항목 체크리스트 표시:\n"
    "   ① 대책방 초대완료 (최초 1회)\n"
    "   ② 금주 피드백 진행 (주간 리셋)\n"
    "   ③ 금주 심방예정일\n"
    "   ④ 금주 심방계획\n"
    f"매주 화요일 {WEEKLY_REMINDER_HOUR:02d}:{WEEKLY_REMINDER_MIN:02d} KST 에 미체크 항목 리마인더 발송\n\n"

    "*4️⃣ 미니앱 (개인 채팅에서만)*\n"
    "`📝 결석자 심방 기록 (폼)` 버튼 탭\n"
    "→ 이름+전화뒷4로 결석자 검색\n"
    "→ 기존 심방 기록 자동 로드 → 보충/수정 후 저장\n"
    "⚠️ 그룹방에서는 미니앱 버튼 안 보임 (텔레그램 정책)\n\n"

    "*5️⃣ 명령어 모음*\n"
    "• `/start` — 방 설정 + 메인 메뉴\n"
    "• `/menu` — 메인 메뉴\n"
    "• `/setup` — 방 범위 재설정 (최초 설정자만)\n"
    "• `/myscope` — 이 방의 현재 범위 확인\n"
    "• `/cancel` — 현재 입력 중단\n"
    "• `/help` — 이 사용법\n"
    "• `/diagnose` — DB 진단\n\n"

    "━━━━━━━━━━━━━━━━━━━━\n"
    "🌐 *상세 분석·통계·CSV 는 웹 대시보드에서.*\n"
    "💬 문제 있으면 `/diagnose` 결과를 관리자에게.\n"
)

# 하위 호환
HELP_TEXT_1 = HELP_TEXT
HELP_TEXT_2 = ""

async def _send_help(update: Update):
    """도움말 전송 (한 메시지, 4096자 이하로 유지)."""
    await safe_reply_text(update.message, HELP_TEXT, parse_mode="Markdown",
                          reply_markup=kb_main_menu(is_private_chat(update)))


async def safe_reply_text(message, text: str, **kwargs):
    """Markdown 파싱 실패 시 plain text로 fallback."""
    try:
        return await message.reply_text(text, **kwargs)
    except Exception as e:
        emsg = str(e).lower()
        if "parse" in emsg or "entity" in emsg or "markdown" in emsg:
            kwargs.pop("parse_mode", None)
            plain = text
            for ch in ("*", "_", "`"):
                plain = plain.replace(ch, "")
            return await message.reply_text(plain, **kwargs)
        raise




# ═════════════════════════════════════════════════════════════════════════════
# 방별 범위(scope) 관리 — 교회/부서/지역/구역 고정
# ═════════════════════════════════════════════════════════════════════════════
async def get_chat_scope(chat_id: int) -> dict | None:
    """이 방의 현재 범위 반환. 미설정이면 None."""
    try:
        rows = await sb_rpc("get_chat_scope", {"p_chat_id": chat_id})
        if rows and len(rows) > 0:
            s = rows[0]
            if s.get("church"):  # 최소 교회는 있어야 설정된 것으로 간주
                return s
        return None
    except Exception as e:
        logger.warning("get_chat_scope 실패: %s", e)
        return None


async def save_chat_scope(
    chat_id: int, chat_title: str,
    church: str = None, dept: str = None,
    region_name: str = None, zone_name: str = None,
    owner_user_id: int = None, owner_name: str = None,
):
    try:
        await sb_rpc("set_chat_scope", {
            "p_chat_id": chat_id,
            "p_chat_title": chat_title or "",
            "p_church": church,
            "p_dept": dept,
            "p_region_name": region_name,
            "p_zone_name": zone_name,
            "p_owner_user_id": owner_user_id,
            "p_owner_name": owner_name,
        })
        return True
    except Exception as e:
        logger.warning("save_chat_scope 실패: %s", e)
        return False


async def check_scope_owner(chat_id: int, user_id: int) -> tuple[bool, str]:
    """
    이 방의 scope를 이 사용자가 변경할 수 있는지 확인.
    반환: (허용여부, 사유메시지)
    """
    s = await get_chat_scope(chat_id)
    if not s:
        return True, ""  # 최초 설정
    owner = s.get("owner_user_id")
    if not owner:
        return True, ""  # owner 미지정 → 누구나
    if int(owner) == int(user_id):
        return True, ""
    owner_name = s.get("owner_name") or "최초 설정자"
    return False, f"이 방의 범위는 *{md(owner_name)}* 님만 변경할 수 있습니다."


def scope_label(s: dict) -> str:
    """범위 설명 텍스트"""
    if not s: return "설정 안 됨"
    parts = []
    if s.get("church"): parts.append(s["church"])
    if s.get("dept"):   parts.append(s["dept"])
    if s.get("region_name"): parts.append(f"{s['region_name']} 지역")
    if s.get("zone_name"):   parts.append(f"{s['zone_name']} 구역")
    return " / ".join(parts) if parts else "설정 안 됨"


async def scope_filter_absentees(chat_id: int, week_key: str) -> list:
    """현재 방의 scope에 맞는 결석자 목록 반환."""
    s = await get_chat_scope(chat_id)
    if not s or not s.get("church"):
        return []
    # scope 범위별 path 구성
    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{quote(week_key)}"
        f"&church=eq.{quote(s['church'])}"
    )
    if s.get("dept"):
        path += f"&dept=eq.{quote(s['dept'])}"
    if s.get("region_name"):
        path += f"&region_name=eq.{quote(s['region_name'])}"
    if s.get("zone_name"):
        path += f"&zone_name=eq.{quote(normalize_zone(s['zone_name']))}"
    path += "&order=dept.asc,region_name.asc,name.asc&limit=5000"
    rows = await sb_get(path)
    return await enrich_names(rows)


# ── Setup(scope 설정) 키보드 빌더 ─────────────────────────────────────────────
def kb_setup_church() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"⛪ {ch}", callback_data=f"scope_ch:{ch}")] for ch in CHURCHES]
    rows.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)

def kb_setup_dept(church: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"🏛 {dp}", callback_data=f"scope_dp:{dp}")] for dp in DEPTS]
    rows.append([InlineKeyboardButton("⏭ 여기까지만 (교회 전체)", callback_data="scope_stop:church")])
    rows.append([InlineKeyboardButton("◀ 교회 다시 선택", callback_data="scope_setup")])
    rows.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)

def kb_setup_region() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ 여기까지만 (부서 전체)", callback_data="scope_stop:dept")],
        [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="scope_setup_back_dept")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])

def kb_setup_zone() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ 여기까지만 (지역 전체)", callback_data="scope_stop:region")],
        [InlineKeyboardButton("◀ 지역 다시 입력", callback_data="scope_setup_back_region")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])


async def setup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """방의 담당 범위 설정 시작 (교회부터)."""
    chat_id = update.effective_chat.id
    user = update.effective_user
    # 이미 설정되어 있고 소유자가 다르면 차단
    ok, reason = await check_scope_owner(chat_id, user.id if user else 0)
    if not ok:
        await safe_reply_text(update.message, f"❌ {reason}", parse_mode="Markdown")
        return

    # 설정 시작
    await save_ctx(chat_id, editing_step="awaiting_scope_church")
    current = await get_chat_scope(chat_id)
    cur_txt = f"\n\n📌 현재 설정: *{md(scope_label(current))}*" if current else ""
    await safe_reply_text(
        update.message,
        f"🔧 *방 담당 범위 설정*{cur_txt}\n\n"
        f"이 방에서 관리할 범위를 순서대로 선택하세요.\n"
        f"*① 교회* 를 먼저 선택하세요 👇\n\n"
        f"💡 교회만 설정해도 되고, 더 상세히 (부서/지역/구역) 설정할 수도 있습니다.",
        parse_mode="Markdown",
        reply_markup=kb_setup_church(),
    )


async def myscope_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """현재 방 범위 조회."""
    chat_id = update.effective_chat.id
    s = await get_chat_scope(chat_id)
    if not s:
        await safe_reply_text(
            update.message,
            "📌 이 방은 아직 범위가 설정되지 않았습니다.\n"
            "`/setup` 으로 먼저 담당 범위를 설정하세요.",
            parse_mode="Markdown",
        )
        return
    owner = s.get("owner_name") or "(미기록)"
    txt = (
        f"📌 *이 방의 담당 범위*\n\n"
        f"{md(scope_label(s))}\n\n"
        f"👤 최초 설정자: *{md(owner)}*\n\n"
        f"변경하려면 `/setup` (최초 설정자만 가능)"
    )
    await safe_reply_text(update.message, txt, parse_mode="Markdown")


# ── Setup 콜백 핸들러들 ───────────────────────────────────────────────────────
async def _on_scope_church(update: Update, chat_id: int, church: str):
    q = update.callback_query
    user = update.effective_user
    ok, reason = await check_scope_owner(chat_id, user.id if user else 0)
    if not ok:
        await q.edit_message_text(f"❌ {reason}", parse_mode="Markdown")
        return
    await save_ctx(chat_id, church_filter=church, editing_step="awaiting_scope_dept")
    await q.edit_message_text(
        f"✅ *① 교회*: {md(church)}\n\n"
        f"*② 부서*를 선택하세요 (또는 교회 전체만 보려면 `⏭ 여기까지만`)",
        parse_mode="Markdown",
        reply_markup=kb_setup_dept(church),
    )


async def _on_scope_dept(update: Update, chat_id: int, dept: str):
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    church = ctx.get("church_filter") or ""
    await save_ctx(chat_id, church_filter=church, dept_filter=dept,
                   editing_step="awaiting_scope_region_text")
    await q.edit_message_text(
        f"✅ *① 교회*: {md(church)}\n"
        f"✅ *② 부서*: {md(dept)}\n\n"
        f"*③ 지역* 이름을 입력하세요.\n"
        f"예) `강북`, `강남`, `노원`, `성북`, `중랑`, `대학`\n\n"
        f"💡 부서 전체만 보려면 아래 `⏭ 여기까지만`",
        parse_mode="Markdown",
        reply_markup=kb_setup_region(),
    )


async def _on_scope_stop(update: Update, chat_id: int, stop_level: str):
    """지정 단계에서 범위 설정 완료"""
    q = update.callback_query
    user = update.effective_user
    ctx = await get_ctx(chat_id)
    church = ctx.get("church_filter")
    dept   = ctx.get("dept_filter") if stop_level in ("dept","region","zone") else None
    region = ctx.get("editing_region") if stop_level in ("region","zone") else None
    zone   = ctx.get("editing_zone") if stop_level == "zone" else None

    if not church:
        await q.edit_message_text("❌ 교회 정보가 없습니다. /setup 다시 시작.")
        return

    owner_name = (user.full_name if user else "") or (user.username if user else "")
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""

    await save_chat_scope(
        chat_id, chat_title,
        church=church, dept=dept, region_name=region, zone_name=zone,
        owner_user_id=user.id if user else None,
        owner_name=owner_name,
    )
    await clear_tmp(chat_id)

    new_scope = {"church": church, "dept": dept, "region_name": region, "zone_name": zone}
    await q.edit_message_text(
        f"🎉 *방 범위 설정 완료*\n\n"
        f"📌 {md(scope_label(new_scope))}\n"
        f"👤 최초 설정자: *{md(owner_name or '(미기록)')}*\n\n"
        f"이제 `📋 결석자 심방` 에서 이 범위의 결석자만 표시됩니다.\n"
        f"범위 확인: `/myscope`\n"
        f"변경(최초 설정자만): `/setup`",
        parse_mode="Markdown",
    )
    # 메뉴 다시 표시
    await q.message.reply_text(
        "🏠 *메인 메뉴*",
        parse_mode="Markdown",
        reply_markup=kb_main_menu(is_private_chat(update)),
    )


async def _on_scope_text_input(update: Update, chat_id: int, text: str):
    """scope 설정 중 텍스트 입력 처리 (지역명 또는 구역명)."""
    ctx = await get_ctx(chat_id)
    step = ctx.get("editing_step")
    user = update.effective_user

    if step == "awaiting_scope_region_text":
        # 지역 입력
        region = text.strip()
        await save_ctx(chat_id, editing_region=region, editing_step="awaiting_scope_zone_text")
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        await safe_reply_text(
            update.message,
            f"✅ *① 교회*: {md(church)}\n"
            f"✅ *② 부서*: {md(dept)}\n"
            f"✅ *③ 지역*: {md(region)}\n\n"
            f"*④ 구역* 이름을 입력하세요.\n"
            f"예) `1-1`, `1팀1`, `2-3`\n\n"
            f"💡 지역 전체만 보려면 아래 `⏭ 여기까지만`",
            parse_mode="Markdown",
            reply_markup=kb_setup_zone(),
        )
        return True

    if step == "awaiting_scope_zone_text":
        zone = text.strip()
        await save_ctx(chat_id, editing_zone=zone)
        # 구역까지 완료 - 바로 저장
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        region = ctx.get("editing_region") or ""
        owner_name = (user.full_name if user else "") or (user.username if user else "")
        chat_title = update.effective_chat.title or update.effective_chat.full_name or ""

        await save_chat_scope(
            chat_id, chat_title,
            church=church, dept=dept, region_name=region, zone_name=zone,
            owner_user_id=user.id if user else None,
            owner_name=owner_name,
        )
        await clear_tmp(chat_id)

        new_scope = {"church": church, "dept": dept, "region_name": region, "zone_name": zone}
        await safe_reply_text(
            update.message,
            f"🎉 *방 범위 설정 완료*\n\n"
            f"📌 {md(scope_label(new_scope))}\n"
            f"👤 최초 설정자: *{md(owner_name or '(미기록)')}*\n\n"
            f"이제 `📋 결석자 심방` 에서 이 범위의 결석자만 표시됩니다.",
            parse_mode="Markdown",
            reply_markup=kb_main_menu(is_private_chat(update)),
        )
        return True

    return False


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    week_key, week_label = await get_active_week()

    # 1) 하단에 고정되는 리플라이 키보드
    await update.message.reply_text(
        f"👋 *결석자 타겟 심방 봇*에 오신 것을 환영합니다\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⌨️ 하단 키보드로 시작하거나, `❓ 사용법` 버튼으로 도움말을 확인하세요 👇",
        parse_mode="Markdown",
        reply_markup=kb_reply_main(is_private_chat(update)),
    )

    # 2) 방 scope 확인 — 미설정이면 setup 유도
    scope = await get_chat_scope(chat_id)
    if not scope:
        # 개인채팅이 아닌 그룹/채널에서만 scope 필요
        if is_private_chat(update):
            # 개인방은 scope 불필요
            await update.message.reply_text(
                "🏠 *메인 메뉴*",
                parse_mode="Markdown",
                reply_markup=kb_main_menu(is_private_chat(update)),
            )
        else:
            # 그룹방: scope 설정 유도
            await update.message.reply_text(
                f"📌 *이 방은 아직 담당 범위가 설정되지 않았습니다.*\n\n"
                f"이 방에서 관리할 *교회 / 부서 / 지역 / 구역*을 설정해야\n"
                f"결석자 목록이 해당 범위로 자동 필터링됩니다.\n\n"
                f"아래 `🔧 방 범위 설정` 버튼을 눌러 시작하세요 👇\n"
                f"💡 교회까지만 설정하면 교회 전체 결석자를 볼 수 있습니다.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
                    [InlineKeyboardButton("❓ 사용법", callback_data="show_help")],
                ]),
            )
    else:
        # 이미 설정됨 - 현재 범위 안내 + 메인 메뉴
        await update.message.reply_text(
            f"📌 *이 방의 담당 범위*: {md(scope_label(scope))}\n\n"
            f"🏠 *메인 메뉴*",
            parse_mode="Markdown",
            reply_markup=kb_main_menu(is_private_chat(update)),
        )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    week_key, week_label = await get_active_week()
    txt = (
        "🏠 *메인 메뉴*\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "아래 버튼에서 원하는 기능을 선택하세요 👇\n\n"
        "💡 사용법은 *❓ 사용법* 버튼 또는 `/help`"
    )
    await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))
    # 리플라이 키보드가 사라져있을 수 있으니 복구
    await update.message.reply_text("⌨️ 하단 키보드 메뉴 활성화", reply_markup=kb_reply_main(is_private_chat(update)))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_help(update)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_tmp(chat_id)
    await update.message.reply_text("🚫 현재 작업을 취소했습니다.\n/menu 로 메인 메뉴로.")


async def diagnose_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """DB 연결 / 데이터 존재 여부 진단"""
    lines = ["🔍 *DB 진단 결과*", "━━━━━━━━━━━━━━━━━━━━"]

    # 1) 주차 목록
    try:
        weeks = await sb_get("weekly_target_weeks?select=week_key,week_label&order=week_key.desc&limit=5")
        if weeks:
            lines.append(f"✅ 주차 {len(weeks)}개 등록됨:")
            for w in weeks:
                lines.append(f"   • `{md(w['week_key'])}` — {md(w.get('week_label',''))}")
        else:
            lines.append("❌ 등록된 주차 없음 → 웹에서 명단 업로드 필요")
    except Exception as e:
        lines.append(f"❌ 주차 조회 실패: {md(str(e))[:100]}")

    # 2) 최신 주차 결석자 수
    try:
        week_key, _ = await get_active_week()
        if week_key:
            cnt_rows = await sb_get(
                f"weekly_visit_targets?select=dept,church&week_key=eq.{quote(week_key)}&limit=1000"
            )
            total = len(cnt_rows)
            by_church = {}
            by_dept   = {}
            for r in cnt_rows:
                c = r.get("church") or "(미지정)"
                d = r.get("dept") or "(미지정)"
                by_church[c] = by_church.get(c, 0) + 1
                by_dept[d]   = by_dept.get(d, 0) + 1
            lines.append(f"\n✅ 주차 `{md(week_key)}` 결석자 {total}명:")
            for c, n in sorted(by_church.items()):
                lines.append(f"   • {md(c)}: {n}명")
            lines.append("   _부서별:_")
            for d, n in sorted(by_dept.items()):
                lines.append(f"   • {md(d)}: {n}명")
    except Exception as e:
        lines.append(f"❌ 결석자 조회 실패: {md(str(e))[:100]}")

    # 3) 필수 RPC 존재 확인
    lines.append("\n*RPC 기능 확인*")
    for fn, payload in [
        ("get_absentees_by_dept_region", {"p_week_key":"_test_","p_dept":"_","p_region":"_"}),
        ("get_absentees_4plus_by_dept",  {"p_week_key":"_test_","p_dept":"_"}),
        ("get_telegram_visit_context",    {"p_chat_id": update.effective_chat.id}),
    ]:
        try:
            await sb_rpc(fn, payload)
            lines.append(f"   ✅ `{fn}`")
        except RuntimeError as e:
            if "없습니다" in str(e):
                lines.append(f"   ❌ `{fn}` — SQL 마이그레이션 필요")
            else:
                lines.append(f"   ⚠️ `{fn}` — {md(str(e))[:80]}")
        except Exception as e:
            # RPC는 있는데 파라미터 에러 등 → 존재함
            lines.append(f"   ✅ `{fn}` (존재)")

    await update.message.reply_text(
        "\n".join(lines), parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update))
    )


# ═════════════════════════════════════════════════════════════════════════════
# 콜백 (버튼) 디스패처
# ═════════════════════════════════════════════════════════════════════════════
async def button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data
    chat_id = update.effective_chat.id

    try:
        # ── 메인 메뉴 ──
        if data == "m:home":
            await _show_home(update)
        elif data == "m:absentee":
            await _show_church_select(update, "abs")
        elif data == "m:special":
            await _show_church_select(update, "sp")
        elif data == "m:help":
            # 사용법은 길어서 2개로 분할 전송. edit_message는 덮어쓰므로 새 메시지로 전송.
            await q.message.reply_text(HELP_TEXT_1, parse_mode="Markdown")
            await q.message.reply_text(HELP_TEXT_2, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))
        elif data == "m:diagnose":
            # 진단은 새 메시지로 전송 (긴 내용)
            class FakeUpd:
                effective_chat = update.effective_chat
                message = q.message
            await diagnose_command(FakeUpd(), context)

        # ── 일반 결석자 흐름 ──
        elif data.startswith("abs_ch:"):
            church = data.split(":", 1)[1]
            await _on_abs_church(update, chat_id, church)
        elif data.startswith("abs_dp:"):
            _, church, dept = data.split(":", 2)
            await _on_abs_dept(update, chat_id, church, dept)
        elif data.startswith("abs_sel:"):
            row_id = data.split(":", 1)[1]
            await _on_abs_select(update, chat_id, row_id)
        elif data.startswith("choice:"):
            _, step, value = data.split(":", 2)
            await _on_choice(update, chat_id, step, value)
        elif data == "confirm_save":
            await _do_save(update, chat_id)
        elif data == "cancel_save":
            await clear_tmp(chat_id)
            await q.message.reply_text("🚫 저장이 취소되었습니다.\n/menu")
        elif data == "flow_cancel":
            # 입력 흐름 전체 취소 (지역/구역 대기, 8단계 입력, 특별관리 3/4번 입력)
            await clear_tmp(chat_id)
            await q.message.reply_text(
                "🚫 입력이 취소되었습니다.",
                reply_markup=kb_main_menu(is_private_chat(update)),
            )

        # ── 방 범위(scope) 설정 흐름 ──
        elif data == "scope_setup":
            # /setup 과 동일하게 시작
            class FakeUpd:
                effective_chat = update.effective_chat
                effective_user = update.effective_user
                message = q.message
            await setup_command(FakeUpd(), context)
        elif data.startswith("scope_ch:"):
            church = data.split(":", 1)[1]
            await _on_scope_church(update, chat_id, church)
        elif data.startswith("scope_dp:"):
            dept = data.split(":", 1)[1]
            await _on_scope_dept(update, chat_id, dept)
        elif data.startswith("scope_stop:"):
            level = data.split(":", 1)[1]
            await _on_scope_stop(update, chat_id, level)
        elif data == "scope_setup_back_dept":
            ctx = await get_ctx(chat_id)
            church = ctx.get("church_filter") or ""
            await save_ctx(chat_id, editing_step="awaiting_scope_dept")
            await q.edit_message_text(
                f"✅ *① 교회*: {md(church)}\n\n"
                f"*② 부서*를 선택하세요.",
                parse_mode="Markdown",
                reply_markup=kb_setup_dept(church),
            )
        elif data == "scope_setup_back_region":
            ctx = await get_ctx(chat_id)
            church = ctx.get("church_filter") or ""
            dept = ctx.get("dept_filter") or ""
            await save_ctx(chat_id, editing_step="awaiting_scope_region_text")
            await q.edit_message_text(
                f"✅ *① 교회*: {md(church)}\n"
                f"✅ *② 부서*: {md(dept)}\n\n"
                f"*③ 지역* 이름을 다시 입력하세요.",
                parse_mode="Markdown",
                reply_markup=kb_setup_region(),
            )
        elif data == "show_help":
            await _send_help(update if update.message else type('X',(),{'message':q.message})())

        # ── 특별관리 흐름 ──
        elif data.startswith("sp_ch:"):
            church = data.split(":", 1)[1]
            await _on_sp_church(update, chat_id, church)
        elif data.startswith("sp_dp:"):
            _, church, dept = data.split(":", 2)
            await _on_sp_dept(update, chat_id, church, dept)
        elif data.startswith("sp_pk:"):
            # sp_pk:{row_id} - row_id 로부터 결석자 정보 조회
            row_id = data.split(":", 1)[1]
            await _on_sp_pick_by_rowid(update, chat_id, row_id)
        elif data in ("sp_t1", "sp_t2"):
            which = "1" if data == "sp_t1" else "2"
            await _on_sp_toggle_ctx(update, chat_id, which)
        elif data in ("sp_e3", "sp_e4"):
            which = "3" if data == "sp_e3" else "4"
            await _on_sp_edit_text_ctx(update, chat_id, which)
        elif data == "sp_del":
            await _on_sp_unregister_ctx(update, chat_id)
        # 하위 호환 (구버전 callback data)
        elif data.startswith("sp_pick:"):
            parts = data.split(":", 4)
            if len(parts) == 5:
                _, church, dept, name, phone = parts
                await _on_sp_pick(update, chat_id, church, dept, name, phone)

    except Exception as e:
        logger.exception("button_cb failed: %s", e)
        try:
            await q.message.reply_text(f"❌ 오류: {e}\n/menu 로 돌아가세요.")
        except Exception:
            pass


async def _show_home(update: Update):
    q = update.callback_query
    week_key, week_label = await get_active_week()
    txt = (
        "🏠 *메인 메뉴*\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "원하는 기능을 선택하세요 👇"
    )
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))


async def _show_church_select(update: Update, flow: str):
    """scope 기반 자동 점프. scope 없으면 교회 선택, 있으면 자동 진행."""
    q = update.callback_query
    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    # 그룹방에서 scope 미설정이면 설정 유도
    if not scope and not is_private_chat(update):
        await q.edit_message_text(
            "📌 이 방은 담당 범위가 설정되지 않았습니다.\n\n"
            "먼저 `/setup` 으로 교회/부서/지역/구역을 설정하세요.\n"
            "설정 후 이 방에서는 해당 범위의 결석자만 표시됩니다.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
                [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
            ]),
        )
        return

    # scope 있음 - 자동 진행
    if scope:
        await _scope_jump(update, chat_id, scope, flow)
        return

    # 개인방 (scope 없을 수 있음) - 기존처럼 교회 선택
    header = {
        "abs": "📋 *결석자 심방*\n\n① *교회* 를 선택하세요 👇",
        "sp":  "🚨 *특별관리결석자*\n\n① *교회* 를 선택하세요 👇\n_(연속결석 4회 이상만 표시)_",
    }[flow]
    await q.edit_message_text(header, parse_mode="Markdown", reply_markup=kb_church_select(flow))


async def _show_church_menu(update: Update, flow: str):
    """리플라이 키보드에서 진입할 때 — scope 자동 반영"""
    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    if not scope and not is_private_chat(update):
        await update.message.reply_text(
            "📌 이 방은 담당 범위가 설정되지 않았습니다.\n\n"
            "먼저 `/setup` 으로 교회/부서/지역/구역을 설정하세요.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
            ]),
        )
        return

    if scope:
        # scope 에 맞춰 결석자 목록 직접 표시
        class FakeQ:
            message = update.message
            async def edit_message_text(self, *a, **kw):
                await update.message.reply_text(*a, **kw)
        class FakeUpd:
            callback_query = FakeQ()
            effective_chat = update.effective_chat
            message = update.message
        # 메시지 타입으로 분기해서 전송
        await _scope_jump_from_message(update, chat_id, scope, flow)
        return

    header = {
        "abs": "📋 *결석자 심방*\n\n① *교회* 를 선택하세요 👇",
        "sp":  "🚨 *특별관리결석자*\n\n① *교회* 를 선택하세요 👇\n_(연속결석 4회 이상만 표시)_",
    }[flow]
    await update.message.reply_text(
        header, parse_mode="Markdown", reply_markup=kb_church_select(flow)
    )


async def _scope_jump(update: Update, chat_id: int, scope: dict, flow: str):
    """scope 기반 결석자 목록 바로 표시 (edit)"""
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.",
                                  reply_markup=kb_main_menu(is_private_chat(update)))
        return

    church = scope.get("church")
    dept   = scope.get("dept")
    region = scope.get("region_name")
    zone   = scope.get("zone_name")

    await save_ctx(chat_id, active_week_key=week_key,
                   church_filter=church, dept_filter=dept)

    rows = await _fetch_scoped(week_key, church, dept, region, zone, flow)
    scope_txt = scope_label(scope)

    if flow == "sp":
        header = f"🚨 <b>특별관리결석자</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"
    else:
        header = f"📋 <b>결석자 심방</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"

    if not rows:
        await q.edit_message_text(
            header + "\n📭 해당 범위의 결석자가 없습니다.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
            ]),
        )
        return

    keyboard = _build_absentee_buttons(rows, flow)
    keyboard.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    await q.edit_message_text(
        header + f"\n총 <b>{len(rows)}</b>명\n결석자를 선택하세요 👇",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


async def _scope_jump_from_message(update: Update, chat_id: int, scope: dict, flow: str):
    """scope 기반 결석자 목록 바로 표시 (new message)"""
    week_key, week_label = await get_active_week()
    if not week_key:
        await update.message.reply_text("❌ 등록된 주차가 없습니다.")
        return

    church = scope.get("church"); dept = scope.get("dept")
    region = scope.get("region_name"); zone = scope.get("zone_name")

    await save_ctx(chat_id, active_week_key=week_key,
                   church_filter=church, dept_filter=dept)

    rows = await _fetch_scoped(week_key, church, dept, region, zone, flow)
    scope_txt = scope_label(scope)

    if flow == "sp":
        header = f"🚨 <b>특별관리결석자</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"
    else:
        header = f"📋 <b>결석자 심방</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"

    if not rows:
        await update.message.reply_text(
            header + "\n📭 해당 범위의 결석자가 없습니다.",
            parse_mode="HTML",
        )
        return

    keyboard = _build_absentee_buttons(rows, flow)
    keyboard.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    await update.message.reply_text(
        header + f"\n총 <b>{len(rows)}</b>명\n결석자를 선택하세요 👇",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(keyboard),
    )


def _escape_html(s) -> str:
    import html as _h
    return _h.escape(str(s)) if s is not None else ""


async def _fetch_scoped(week_key, church, dept, region, zone, flow):
    """scope 범위 + flow (abs/sp) 에 맞춰 결석자 목록 반환"""
    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{quote(week_key)}"
        f"&church=eq.{quote(church)}"
    )
    if dept:   path += f"&dept=eq.{quote(dept)}"
    if region: path += f"&region_name=eq.{quote(region)}"
    if zone:   path += f"&zone_name=eq.{quote(normalize_zone(zone))}"
    if flow == "sp":
        path += "&consecutive_absent_count=gte.4"
        path += "&order=consecutive_absent_count.desc,name.asc"
    else:
        path += "&order=dept.asc,region_name.asc,zone_name.asc,name.asc"
    path += "&limit=5000"

    rows = await sb_get(path)
    return await enrich_names(rows or [])


def _build_absentee_buttons(rows, flow, max_buttons=40):
    """결석자 이름 버튼 목록 생성 (페이지네이션 고려)"""
    keyboard = []
    cb_prefix = "sp_pk" if flow == "sp" else "abs_sel"
    for r in rows[:max_buttons]:
        name = r.get("name", "?")
        zone = r.get("zone_name", "") or r.get("region_name", "") or ""
        streak = r.get("consecutive_absent_count", 0) or 0
        label = f"{name} {zone} · 연속{streak}회" if zone else f"{name} · 연속{streak}회"
        if len(label) > 60: label = label[:57] + "..."
        keyboard.append([InlineKeyboardButton(label, callback_data=f"{cb_prefix}:{r['row_id']}")])
    if len(rows) > max_buttons:
        keyboard.append([InlineKeyboardButton(f"... 외 {len(rows)-max_buttons}명 (범위를 좁혀주세요)", callback_data="noop")])
    return keyboard


# ═════════════════════════════════════════════════════════════════════════════
# 일반 결석자 흐름
# ═════════════════════════════════════════════════════════════════════════════
async def _on_abs_church(update: Update, chat_id: int, church: str):
    q = update.callback_query
    await save_ctx(chat_id, church_filter=church)
    txt = (
        f"📋 *결석자 심방*\n\n"
        f"✅ 교회: *{md(church)}*\n\n"
        f"② *부서* 를 선택하세요 👇"
    )
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_dept_select("abs", church))


async def _on_abs_dept(update: Update, chat_id: int, church: str, dept: str):
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text(
            "❌ 등록된 주차가 없습니다.\n웹 대시보드에서 명단을 먼저 업로드해주세요.",
            reply_markup=kb_main_menu(is_private_chat(update)),
        )
        return

    await save_ctx(chat_id,
        active_week_key=week_key,
        church_filter=church,
        dept_filter=dept,
        editing_step="awaiting_region_or_zone",
    )
    txt = (
        f"📋 *결석자 심방*\n"
        f"✅ {md(church)} / {md(dept)} / `{md(week_label)}`\n\n"
        f"③ *지역 또는 구역명*을 입력하세요 👇\n\n"
        f"• 지역 예) `강북`, `강남`, `강서`, `강동`, `노원`\n"
        f"• 구역 예) `2-1` 또는 `2팀1` (둘 다 동일)"
    )
    await q.edit_message_text(txt, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"abs_ch:{church}")],
            [InlineKeyboardButton("❌ 입력 취소", callback_data="flow_cancel")],
        ]))


async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """텍스트 입력 핸들러 — 리플라이 키보드 / 지역·구역 / 심방 단계 / 특별관리 3·4번"""
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    # ── 0) 리플라이 키보드 (하단 버튼) 라벨 라우팅 ──────────────────────
    #  - 컨텍스트보다 우선하지만, 사용자가 입력 중이면 의도와 다를 수 있으니
    #    컨텍스트의 editing_step 이 비어있을 때만 라우팅
    ctx_pre = await get_ctx(chat_id)
    pre_step = (ctx_pre.get("editing_step", "") if ctx_pre else "") or ""
    if not pre_step:
        if text == "📋 결석자 심방":
            await _show_church_menu(update, "abs")
            return
        if text == "🚨 특별관리결석자":
            await _show_church_menu(update, "sp")
            return
        if text == "❓ 사용법":
            await _send_help(update)
            return
        if text == "🏠 메인 메뉴":
            await menu_command(update, context)
            return
        if text == "📝 결석자 심방 기록 (폼)":
            if not is_private_chat(update):
                await update.message.reply_text(
                    "⚠️ 미니앱 폼은 *개인 채팅*에서만 열 수 있습니다.\n"
                    "봇과 1:1 채팅을 시작한 다음 사용해주세요.",
                    parse_mode="Markdown",
                )
                return
            if MINIAPP_URL.startswith("https://"):
                await update.message.reply_text(
                    "📝 아래 버튼을 탭하면 결석자 심방 기록 폼이 열립니다.\n\n"
                    "폼에서:\n"
                    "1️⃣ 결석자 이름/전화뒷4/교회/부서로 검색\n"
                    "2️⃣ 기존 기록이 있으면 자동으로 불러옴\n"
                    "3️⃣ 부족한 내용 보충하거나 수정 후 저장",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(
                        "📝 폼 열기", web_app=WebAppInfo(url=MINIAPP_URL)
                    )]])
                )
            else:
                await update.message.reply_text(
                    "⚠️ 미니웹앱 URL이 설정되지 않았습니다. (HTTPS 필수)\n"
                    "관리자에게 MINIAPP_URL 환경변수 설정을 요청하세요."
                )
            return

    ctx = ctx_pre
    if not ctx:
        return

    step = pre_step

    # 0.5) 방 범위(scope) 설정 중 — 지역/구역 입력
    if step in ("awaiting_scope_region_text", "awaiting_scope_zone_text"):
        handled = await _on_scope_text_input(update, chat_id, text)
        if handled: return

    # 1) 지역/구역 입력 대기 중
    if step == "awaiting_region_or_zone":
        church = ctx.get("church_filter", "")
        dept   = ctx.get("dept_filter", "")
        week_key = ctx.get("active_week_key", "")
        if not (church and dept and week_key):
            await update.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
            return

        # 주차 라벨 조회
        try:
            wrows = await sb_get(
                f"weekly_target_weeks?select=week_label&week_key=eq.{quote(week_key)}&limit=1"
            )
            week_label = wrows[0]["week_label"] if wrows else week_key
        except Exception:
            week_label = week_key

        # 구역/지역 구분
        if looks_like_zone(text):
            query_kind = "구역"
            normalized = normalize_zone(text)
            absentees = await fetch_absentees_by_zone(week_key, church, dept, normalized)
            query_label = normalized
        else:
            query_kind = "지역"
            absentees = await fetch_absentees_by_region(week_key, church, dept, text)
            query_label = text

        await save_ctx(chat_id, editing_step="")

        if not absentees:
            # 도움말: 같은 교회+부서에서 실제로 존재하는 지역/구역 나열
            hint = ""
            try:
                all_rows = await sb_get(
                    f"weekly_visit_targets?select=region_name,zone_name"
                    f"&week_key=eq.{quote(week_key)}"
                    f"&church=eq.{quote(church)}"
                    f"&dept=eq.{quote(dept)}"
                    f"&limit=500"
                )
                regions = sorted(set(r.get("region_name","") for r in all_rows if r.get("region_name")))
                zones   = sorted(set(r.get("zone_name","")   for r in all_rows if r.get("zone_name")))
                if regions:
                    hint += "\n\n📍 사용 가능한 지역:\n" + ", ".join(f"`{md(r)}`" for r in regions[:20])
                if zones:
                    hint += "\n📍 사용 가능한 구역:\n" + ", ".join(f"`{md(z)}`" for z in zones[:15])
                if not regions and not zones:
                    hint = "\n\n_이 교회/부서에 등록된 결석자가 없습니다._"
            except Exception:
                pass

            await update.message.reply_text(
                f"📭 *{md(church)} / {md(dept)} / {query_kind}: {md(query_label)}*\n"
                f"주차: `{md(week_label)}`\n결석자가 없습니다.{hint}\n\n"
                f"다시 입력하거나 /menu",
                parse_mode="Markdown",
            )
            await save_ctx(chat_id, editing_step="awaiting_region_or_zone")
            return

        # 결석자 버튼 목록 (텔레그램 reply_markup 크기 제한 대비 최대 40명)
        MAX_BUTTONS = 40
        shown = absentees[:MAX_BUTTONS]
        overflow_abs = len(absentees) - MAX_BUTTONS
        buttons = []
        for ab in shown:
            name   = ab.get("name", "?")
            phone  = ab.get("phone_last4", "") or ""
            zone   = ab.get("zone_name", "") or ""
            streak = ab.get("consecutive_absent_count", 0) or 0
            if query_kind == "구역":
                label = f"{name} · 연속{streak}회"
            else:
                label = f"{name} {zone} · 연속{streak}회"
            if len(label) > 60:
                label = label[:57] + "..."
            buttons.append([InlineKeyboardButton(label, callback_data=f"abs_sel:{ab['row_id']}")])
        buttons.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])

        overflow_note = f"\n\n_(+ {overflow_abs}명은 화면 제한으로 생략 — 더 정확한 지역/구역명으로 다시 검색해주세요)_" if overflow_abs > 0 else ""
        await update.message.reply_text(
            f"📋 *{md(church)} / {md(dept)} / {query_kind}: {md(query_label)}*\n"
            f"주차: `{md(week_label)}` | 총 {len(absentees)}명\n\n"
            f"심방 기록할 결석자를 선택하세요 👇"
            f"{overflow_note}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
        return

    # 2) 특별관리 3·4번 텍스트 입력
    if step in ("awaiting_sp3", "awaiting_sp4"):
        church = ctx.get("church_filter", "")
        dept   = ctx.get("dept_filter", "")
        name   = ctx.get("tmp_sp_name", "")
        phone  = ctx.get("tmp_sp_phone", "")
        which  = "3" if step == "awaiting_sp3" else "4"
        fn = "set_special_item3" if which == "3" else "set_special_item4"
        try:
            await sb_rpc(fn, {
                "p_dept": dept, "p_name": name,
                "p_phone_last4": phone, "p_value": text
            })
        except Exception as e:
            await update.message.reply_text(f"❌ 저장 실패: {e}")
            return
        await save_ctx(chat_id, editing_step="")
        label_ko = "심방예정일" if which == "3" else "심방계획"
        await update.message.reply_text(
            f"✅ *금주 {label_ko}* 저장됨: `{md(text)}`",
            parse_mode="Markdown",
        )
        await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=True)
        return

    # 3) 일반 심방 입력 단계
    if step in STEPS:
        tmp_key = f"tmp_{step}"
        await save_ctx(chat_id, **{tmp_key: text})
        step_idx = STEPS.index(step)
        await _next_step(update, chat_id, step_idx, ctx)


async def _on_abs_select(update: Update, chat_id: int, row_id: str):
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await q.message.reply_text("❌ 세션 만료. /menu")
        return

    week_key = ctx.get("active_week_key", "")
    prog = await get_progress(week_key, row_id)
    rows = await sb_get(
        f"weekly_visit_targets?select=name,region_name,zone_name,church,dept,phone_last4"
        f"&row_id=eq.{quote(row_id)}&week_key=eq.{quote(week_key)}"
    )
    if rows:
        # 이름 마스킹 복구 시도
        enriched = await enrich_names(rows)
        name = enriched[0]["name"] if enriched else row_id
    else:
        name = row_id

    await save_ctx(chat_id, editing_row_id=row_id, editing_step="shepherd")

    # 🛡 HTML parse_mode 사용 (Markdown 보다 관대하여 특수문자 이름에도 파싱 에러 없음)
    import html as _html
    existing = ""
    if prog:
        existing = (
            f"\n\n📂 <b>기존 입력값</b>\n"
            f"심방자: {_html.escape(prog.get('shepherd','') or '없음')}\n"
            f"심방날짜: {_html.escape(prog.get('visit_date_display','') or '없음')}\n"
            f"진행여부: {'완료' if prog.get('is_done') else '미완료'}"
        )

    try:
        await q.message.reply_text(
            f"✏️ <b>{_html.escape(str(name))}</b> 님 심방 기록 시작{existing}\n\n"
            f"1️⃣ {_html.escape(STEP_LABELS['shepherd'])}\n입력해주세요:\n\n"
            f"<i>중단하려면 ❌ 취소 버튼 또는 /cancel</i>",
            parse_mode="HTML",
            reply_markup=kb_cancel_only(),
        )
    except Exception as e:
        logger.warning("HTML parse 실패, 평문으로 전송: %s", e)
        await q.message.reply_text(
            f"✏️ {name} 님 심방 기록 시작\n\n"
            f"1️⃣ {STEP_LABELS['shepherd']}\n입력해주세요:",
            reply_markup=kb_cancel_only(),
        )


async def _on_choice(update: Update, chat_id: int, step: str, value: str):
    q = update.callback_query
    tmp_key = f"tmp_{step}"
    await save_ctx(chat_id, **{tmp_key: value})
    ctx = await get_ctx(chat_id)
    step_idx = STEPS.index(step)

    class FakeUpd:
        message = q.message
        effective_chat = update.effective_chat

    await _next_step(FakeUpd(), chat_id, step_idx, ctx)


async def _next_step(update, chat_id: int, current_idx: int, ctx: dict):
    next_idx = current_idx + 1
    if next_idx >= len(STEPS):
        await _show_confirm(update, chat_id, ctx)
        return

    next_step = STEPS[next_idx]
    await save_ctx(chat_id, editing_step=next_step)
    label = STEP_LABELS[next_step]
    step_num = next_idx + 1

    if next_step in STEP_CHOICES:
        choice_rows = STEP_CHOICES[next_step]
        buttons = [
            [InlineKeyboardButton(c, callback_data=f"choice:{next_step}:{c}") for c in row]
            for row in choice_rows
        ]
        await update.message.reply_text(
            f"{step_num}️⃣ {label}",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        await update.message.reply_text(
            f"{step_num}️⃣ {label}\n입력해주세요:",
            reply_markup=kb_cancel_only()
        )


async def _show_confirm(update, chat_id: int, ctx: dict):
    row_id = ctx.get("editing_row_id", "")
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    name = rows[0]["name"] if rows else row_id

    summary = (
        f"📋 *심방 기록 확인* — {md(name)}\n\n"
        f"심방자: {md(ctx.get('tmp_shepherd','') or '-')}\n"
        f"심방날짜: {md(ctx.get('tmp_date','') or '-')}\n"
        f"심방계획: {md(ctx.get('tmp_plan','') or '-')}\n"
        f"타겟여부: {md(ctx.get('tmp_target','') or '-')}\n"
        f"진행여부: {md(ctx.get('tmp_done','') or '-')}\n"
        f"예배확답: {md(ctx.get('tmp_worship','') or '-')}\n"
        f"진행사항: {md(ctx.get('tmp_note','') or '-')}\n"
        f"예배참석: {md(ctx.get('tmp_attendance','') or '-')}\n\n"
        f"저장하시겠습니까?"
    )
    buttons = [[
        InlineKeyboardButton("✅ 저장", callback_data="confirm_save"),
        InlineKeyboardButton("❌ 취소", callback_data="cancel_save"),
    ]]
    await update.message.reply_text(summary, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons))


async def _do_save(update: Update, chat_id: int):
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await q.message.reply_text("❌ 세션 만료.")
        return
    week_key = ctx.get("active_week_key", "")
    row_id   = ctx.get("editing_row_id", "")
    if not week_key or not row_id:
        await q.message.reply_text("❌ 저장 정보 부족.")
        return
    try:
        await upsert_progress(week_key, row_id, ctx)
        await clear_tmp(chat_id)
        rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
        name = rows[0]["name"] if rows else row_id
        await q.message.reply_text(
            f"✅ *{md(name)}* 님 심방 기록 저장 완료!\n\n"
            f"계속하려면 /menu",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.exception(e)
        await q.message.reply_text(f"❌ 저장 실패: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# 특별관리 흐름
# ═════════════════════════════════════════════════════════════════════════════
async def _on_sp_church(update: Update, chat_id: int, church: str):
    q = update.callback_query
    await save_ctx(chat_id, church_filter=church)
    txt = (
        f"🚨 *특별관리결석자*\n\n"
        f"✅ 교회: *{md(church)}*\n\n"
        f"② *부서* 를 선택하세요 👇"
    )
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_dept_select("sp", church))


async def _on_sp_dept(update: Update, chat_id: int, church: str, dept: str):
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.", reply_markup=kb_main_menu(is_private_chat(update)))
        return

    targets = await fetch_absentees_4plus(week_key, church, dept)
    if not targets:
        await q.edit_message_text(
            f"📭 *{md(church)} / {md(dept)}* 의 연속결석 4회 이상 결석자가 없습니다.\n"
            f"(주차: `{md(week_label)}`)",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"sp_ch:{church}")],
                [InlineKeyboardButton("◀ 메인 메뉴",       callback_data="m:home")],
            ]),
        )
        return

    # 기등록(방 감지중) 확인
    try:
        registered = await sb_get(
            f"special_management_targets?select=name,phone_last4,monitor_chat_id"
            f"&dept=eq.{quote(dept)}"
        )
        registered_set = {(r.get("name",""), r.get("phone_last4","") or "") for r in registered}
    except Exception:
        registered_set = set()

    # 🚨 Telegram reply_markup 총 크기 제한 (~4096 bytes) — 최대 30명까지만 표시
    MAX_TARGETS = 30
    targets_shown = targets[:MAX_TARGETS]
    overflow = len(targets) - MAX_TARGETS
    buttons = []
    for t in targets_shown:
        name   = t.get("name", "?")
        phone  = t.get("phone_last4", "") or ""
        region = t.get("region_name", "") or ""
        zone   = t.get("zone_name", "") or ""
        streak = t.get("consecutive_absent_count", 0) or 0
        row_id = t.get("row_id", "")
        is_reg = (name, phone) in registered_set
        mark = "🚨" if is_reg else "⚠️"
        label = f"{mark} {name} ({region} {zone}) · {streak}회"
        # 버튼 텍스트는 64자 제한 (Telegram button label)
        if len(label) > 60:
            label = label[:57] + "..."
        # callback_data는 64 byte 제한 — row_id만 전달 (나중에 DB에서 조회)
        buttons.append([InlineKeyboardButton(
            label,
            callback_data=f"sp_pk:{row_id}"
        )])
    buttons.append([InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"sp_ch:{church}")])
    buttons.append([InlineKeyboardButton("◀ 메인 메뉴",       callback_data="m:home")])

    overflow_note = f"\n\n_(+ {overflow}명은 화면 제한으로 생략 — 연속결석 순 상위 {MAX_TARGETS}명만 표시)_" if overflow > 0 else ""
    txt = (
        f"🚨 *{md(church)} / {md(dept)}* — 4회 이상 {len(targets)}명\n"
        f"주차: `{md(week_label)}`\n\n"
        f"🚨 = 특별관리 등록됨 (방 감지중)\n"
        f"⚠️ = 아직 미등록\n\n"
        f"관리할 결석자를 선택하세요 👇\n"
        f"_(선택 시 이 방이 감지방으로 등록됩니다)_"
        f"{overflow_note}"
    )
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))


async def _on_sp_pick(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    """특별관리 대상 선택 → 방 감지 등록 + 상세 화면"""
    q = update.callback_query
    chat = update.effective_chat

    # 결석자 정보
    rows = await sb_get(
        f"weekly_visit_targets?select=region_name,zone_name"
        f"&dept=eq.{quote(dept)}&name=eq.{quote(name)}"
        + (f"&phone_last4=eq.{quote(phone)}" if phone else "")
        + "&limit=1"
    )
    region = rows[0].get("region_name","") if rows else ""
    zone   = rows[0].get("zone_name","")   if rows else ""

    # 방 감지 등록
    try:
        await sb_rpc("register_special_management", {
            "p_dept":         dept,
            "p_name":         name,
            "p_phone_last4":  phone,
            "p_region_name":  region,
            "p_zone_name":    zone,
            "p_chat_id":      chat.id,
            "p_chat_title":   chat.title or chat.full_name or f"chat_{chat.id}",
        })
    except Exception as e:
        logger.exception(e)
        await q.message.reply_text(f"❌ 등록 실패: {e}")
        return

    await q.edit_message_text(
        f"✅ *{md(name)}* 님을 *특별관리 대상*으로 등록했습니다.\n"
        f"이 방에서 감지를 시작합니다.\n\n"
        f"매주 화요일 {WEEKLY_REMINDER_HOUR:02d}:{WEEKLY_REMINDER_MIN:02d} KST 에 "
        f"미체크 항목 리마인더가 이 방으로 발송됩니다.",
        parse_mode="Markdown",
    )
    await save_ctx(
        chat_id,
        church_filter=church,
        dept_filter=dept,
        tmp_sp_name=name,
        tmp_sp_phone=phone,
    )
    await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=True)


async def _show_sp_detail(update, chat_id: int, church: str, dept: str, name: str, phone: str, send_new: bool = False):
    """특별관리 대상 상세 + 4항목 체크리스트"""
    try:
        detail = await sb_rpc("get_special_detail", {
            "p_dept": dept, "p_name": name, "p_phone_last4": phone
        })
    except Exception as e:
        logger.warning("get_special_detail failed: %s", e)
        detail = None

    if not detail:
        msg = "❌ 특별관리 정보를 찾을 수 없습니다."
        target = update.message if hasattr(update, 'message') and update.message else (
            update.callback_query.message if update.callback_query else None
        )
        if target:
            await target.reply_text(msg)
        return

    d = detail[0] if isinstance(detail, list) else detail
    region = d.get("region_name","") or ""
    zone   = d.get("zone_name","")   or ""

    item1 = bool(d.get("item1_chat_invited"))
    item2 = bool(d.get("item2_feedback_done"))
    item3 = d.get("item3_visit_date") or ""
    item4 = d.get("item4_visit_plan") or ""

    text = (
        f"🚨 *특별관리: {md(name)}*\n"
        f"{md(church)} / {md(dept)} / {md(region)} {md(zone)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'✅' if item1 else '⬜️'} *1. 대책방 초대완료*\n"
        f"   (구역장·인섬교·강사·전도사·심방부사명자)\n"
        f"   _최초 1회만 체크 (주간 리셋 안 됨)_\n\n"
        f"{'✅' if item2 else '⬜️'} *2. 금주 피드백 진행*\n"
        f"   _매주 화요일 19시 초기화_\n\n"
        f"📅 *3. 금주 심방예정일:* {md(item3) if item3 else '_미입력_'}\n\n"
        f"📝 *4. 금주 심방계획:* {md(item4) if item4 else '_미입력_'}"
    )

    # callback_data는 64 byte 제한 — 짧은 명령만, 실제 대상은 ctx에서 읽음
    buttons = [
        [InlineKeyboardButton(
            f"{'✅ 1번 체크됨 (탭:해제)' if item1 else '⬜️ 1번 체크 (대책방 초대완료)'}",
            callback_data="sp_t1"
        )],
        [InlineKeyboardButton(
            f"{'✅ 2번 체크됨 (탭:해제)' if item2 else '⬜️ 2번 체크 (금주 피드백)'}",
            callback_data="sp_t2"
        )],
        [InlineKeyboardButton("📅 3번 심방예정일 입력/수정",
            callback_data="sp_e3")],
        [InlineKeyboardButton("📝 4번 심방계획 입력/수정",
            callback_data="sp_e4")],
        [InlineKeyboardButton("🗑 특별관리 해제",
            callback_data="sp_del")],
        [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
    ]
    kb = InlineKeyboardMarkup(buttons)

    if send_new:
        target = update.message if hasattr(update, 'message') and update.message else (
            update.callback_query.message if update.callback_query else None
        )
        if target:
            await target.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        q = update.callback_query
        if q:
            try:
                await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            except Exception:
                await q.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def _on_sp_toggle(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str, which: str):
    try:
        detail = await sb_rpc("get_special_detail", {
            "p_dept": dept, "p_name": name, "p_phone_last4": phone
        })
    except Exception as e:
        await update.callback_query.message.reply_text(f"❌ 조회 실패: {e}")
        return

    cur = False
    if detail:
        d = detail[0] if isinstance(detail, list) else detail
        cur = bool(d.get(f"item{which}_chat_invited" if which == "1" else "item2_feedback_done"))

    fn = "toggle_special_item1" if which == "1" else "toggle_special_item2"
    await sb_rpc(fn, {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": not cur
    })
    await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=False)


async def _on_sp_edit_text(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str, which: str):
    q = update.callback_query
    step = "awaiting_sp3" if which == "3" else "awaiting_sp4"
    await save_ctx(chat_id,
        church_filter=church, dept_filter=dept,
        tmp_sp_name=name, tmp_sp_phone=phone,
        editing_step=step,
    )
    label = "금주 심방예정일" if which == "3" else "금주 심방계획"
    await q.message.reply_text(
        f"✏️ *{md(name)}* 님의 *{label}* 을 입력해주세요:\n\n"
        f"_취소하려면 /cancel_",
        parse_mode="Markdown",
    )


async def _on_sp_unregister(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    try:
        await sb_rpc("unregister_special_management", {
            "p_dept": dept, "p_name": name, "p_phone_last4": phone
        })
    except Exception as e:
        await update.callback_query.message.reply_text(f"❌ 해제 실패: {e}")
        return
    q = update.callback_query
    await q.edit_message_text(
        f"🗑 *{md(name)}* 님을 특별관리에서 해제했습니다.",
        parse_mode="Markdown",
        reply_markup=kb_main_menu(is_private_chat(update)),
    )


# ── row_id 기반 / 컨텍스트 기반 래퍼 (callback_data 64 byte 한도 대응) ────
async def _on_sp_pick_by_rowid(update: Update, chat_id: int, row_id: str):
    """row_id 로 결석자 조회 후 _on_sp_pick 호출"""
    rows = await sb_get(
        f"weekly_visit_targets?select=name,phone_last4,church,dept,region_name,zone_name"
        f"&row_id=eq.{quote(row_id)}&limit=1"
    )
    if not rows:
        q = update.callback_query
        await q.message.reply_text("❌ 결석자 정보를 찾을 수 없습니다.\n/menu 로 돌아가세요.")
        return
    t = rows[0]
    await _on_sp_pick(
        update, chat_id,
        t.get("church","") or "",
        t.get("dept","") or "",
        t.get("name","") or "",
        t.get("phone_last4","") or "",
    )

async def _get_current_sp(chat_id: int):
    """컨텍스트에서 현재 관리 중인 특별관리 대상 정보 추출"""
    ctx = await get_ctx(chat_id)
    if not ctx:
        return None
    name  = ctx.get("tmp_sp_name") or ""
    phone = ctx.get("tmp_sp_phone") or ""
    dept  = ctx.get("dept_filter") or ""
    church = ctx.get("church_filter") or ""
    if not (name and dept):
        return None
    return {"church": church, "dept": dept, "name": name, "phone": phone}

async def _on_sp_toggle_ctx(update: Update, chat_id: int, which: str):
    info = await _get_current_sp(chat_id)
    if not info:
        q = update.callback_query
        await q.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
        return
    await _on_sp_toggle(update, chat_id, info["church"], info["dept"], info["name"], info["phone"], which)

async def _on_sp_edit_text_ctx(update: Update, chat_id: int, which: str):
    info = await _get_current_sp(chat_id)
    if not info:
        q = update.callback_query
        await q.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
        return
    await _on_sp_edit_text(update, chat_id, info["church"], info["dept"], info["name"], info["phone"], which)

async def _on_sp_unregister_ctx(update: Update, chat_id: int):
    info = await _get_current_sp(chat_id)
    if not info:
        q = update.callback_query
        await q.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
        return
    await _on_sp_unregister(update, chat_id, info["church"], info["dept"], info["name"], info["phone"])


# ═════════════════════════════════════════════════════════════════════════════
# 매주 화요일 19시 KST — 주간 리마인더 + 2/3/4번 리셋
# ═════════════════════════════════════════════════════════════════════════════
async def weekly_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🔔 weekly_reminder_job start")
    try:
        targets = await sb_rpc("get_all_special_targets", {}) or []
        if not targets:
            logger.info("no special targets, reset only")
            try: await sb_rpc("reset_special_weekly_items", {})
            except Exception: pass
            return

        for t in targets:
            chat_id = t.get("monitor_chat_id")
            if not chat_id: continue
            name   = t.get("name", "?")
            dept   = t.get("dept", "")
            region = t.get("region_name","") or ""
            zone   = t.get("zone_name","") or ""

            unchecked = []
            if not t.get("item1_chat_invited"):
                unchecked.append("⬜️ 1. 대책방 초대완료 (최초 1회)")
            if not t.get("item2_feedback_done"):
                unchecked.append("⬜️ 2. 금주 피드백 진행")
            if not (t.get("item3_visit_date") or ""):
                unchecked.append("⬜️ 3. 금주 심방예정일 (미입력)")
            if not (t.get("item4_visit_plan") or ""):
                unchecked.append("⬜️ 4. 금주 심방계획 (미입력)")

            if unchecked:
                msg = (
                    f"🔔 *주간 리마인더* (화요일 {WEEKLY_REMINDER_HOUR}시)\n"
                    f"👤 *{md(name)}* ({md(dept)} / {md(region)} {md(zone)})\n\n"
                    f"미체크 항목:\n" + "\n".join(unchecked) +
                    f"\n\n/menu → 🚨 특별관리결석자 에서 업데이트하세요."
                )
            else:
                msg = (
                    f"🔔 *주간 리마인더*\n"
                    f"👤 {md(name)} ({md(dept)} / {md(region)} {md(zone)})\n\n"
                    f"✅ 모든 항목 체크 완료. 수고하셨습니다!\n"
                    f"_(2~4번은 곧 초기화됩니다)_"
                )

            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning("send failed to %s: %s", chat_id, e)
                try:
                    await context.bot.send_message(chat_id=chat_id,
                        text=msg.replace("*","").replace("`","").replace("_",""))
                except Exception:
                    pass

        try:
            await sb_rpc("reset_special_weekly_items", {})
            logger.info("weekly reset done")
        except Exception as e:
            logger.warning("weekly reset failed: %s", e)
    except Exception as e:
        logger.exception("weekly_reminder_job failed: %s", e)


async def force_weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔔 주간 리마인더 강제 실행 중...")
    await weekly_reminder_job(context)
    await update.message.reply_text("✅ 완료")


# ═════════════════════════════════════════════════════════════════════════════
# 앱 시작
# ═════════════════════════════════════════════════════════════════════════════
MINIAPP_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "miniapp.html")


def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # 🛡 글로벌 에러 핸들러 — Markdown 파싱 실패 자동 감지/재시도
    async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        logger.error("Global error: %s", err, exc_info=True)
        emsg = str(err)
        if ("parse" in emsg.lower() or "entity" in emsg.lower()) and isinstance(update, Update):
            # 사용자에게는 간단한 안내만
            try:
                chat = update.effective_chat
                if chat:
                    await context.bot.send_message(
                        chat_id=chat.id,
                        text=f"⚠️ 일부 특수문자 때문에 표시에 문제가 있었습니다. /menu 로 돌아가세요."
                    )
            except Exception:
                pass

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start",    start_command))
    app.add_handler(CommandHandler("menu",     menu_command))
    app.add_handler(CommandHandler("help",     help_command))
    app.add_handler(CommandHandler("cancel",   cancel_command))
    app.add_handler(CommandHandler("setup",    setup_command))
    app.add_handler(CommandHandler("myscope",  myscope_command))
    app.add_handler(CommandHandler("diagnose", diagnose_command))
    app.add_handler(CommandHandler("weektest", force_weekly_command))

    app.add_handler(CallbackQueryHandler(button_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    # 매주 화요일 19시 KST (python-telegram-bot v20: 월=0, 화=1, ..., 일=6)
    if app.job_queue is not None:
        app.job_queue.run_daily(
            weekly_reminder_job,
            time=dtime(hour=WEEKLY_REMINDER_HOUR, minute=WEEKLY_REMINDER_MIN, tzinfo=KST),
            days=(1,),
            name="weekly_special_reminder",
        )
        logger.info("📅 weekly reminder: 화 %02d:%02d KST", WEEKLY_REMINDER_HOUR, WEEKLY_REMINDER_MIN)
    else:
        logger.warning("⚠ JobQueue unavailable")

    port = int(os.environ.get("PORT", 8080))
    webhook_url = os.environ["WEBHOOK_URL"]
    logger.info(f"Starting integrated server port={port} url={webhook_url}")

    # ─────────────────────────────────────────────────────────────
    # 🔧 PTB 웹훅 + 미니웹앱을 하나의 aiohttp 서버로 통합
    # ─────────────────────────────────────────────────────────────
    import asyncio
    from aiohttp import web
    from telegram import Update as TgUpdate

    async def webhook_handler(request):
        """텔레그램 웹훅 수신 → PTB 큐에 업데이트 투입"""
        try:
            data = await request.json()
        except Exception:
            return web.Response(status=400, text="bad request")
        try:
            update = TgUpdate.de_json(data, app.bot)
            await app.process_update(update)
            return web.Response(text="OK")
        except Exception as e:
            logger.exception("webhook process error: %s", e)
            return web.Response(status=500, text="error")

    async def miniapp_html_handler(request):
        """미니웹앱 HTML 정적 서빙"""
        try:
            with open(MINIAPP_HTML_PATH, "r", encoding="utf-8") as f:
                html = f.read()
            return web.Response(text=html, content_type="text/html", charset="utf-8")
        except FileNotFoundError:
            return web.Response(
                text="<h1>miniapp.html 파일이 배포되지 않았습니다</h1>",
                content_type="text/html", status=404
            )

    async def miniapp_search_handler(request):
        """결석자 검색: 이름+전화뒷4+교회+부서로 target 찾고, 기존 progress 함께 반환."""
        name   = (request.query.get("name") or "").strip()
        phone  = (request.query.get("phone") or "").strip()
        church = (request.query.get("church") or "").strip()
        dept   = (request.query.get("dept") or "").strip()

        if not (name and phone and church and dept):
            return web.json_response({"ok": False, "error": "이름/전화/교회/부서 모두 필요"}, status=400)

        try:
            # 최신 주차 우선
            week_key, _ = await get_active_week()
            if not week_key:
                return web.json_response({"ok": False, "error": "등록된 주차 없음. 명단을 먼저 업로드해주세요."}, status=404)

            # 결석자 찾기 (active week 우선, 없으면 최근 주차로 fallback)
            path = (
                f"weekly_visit_targets"
                f"?select=row_id,week_key,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
                f"&week_key=eq.{quote(week_key)}"
                f"&church=eq.{quote(church)}"
                f"&dept=eq.{quote(dept)}"
                f"&name=eq.{quote(name)}"
                f"&phone_last4=eq.{quote(phone)}"
                f"&limit=1"
            )
            rows = await sb_get(path)

            # 못 찾으면 가장 최근 주차에서 재시도
            if not rows:
                recent_weeks = await sb_get(
                    "weekly_target_weeks?select=week_key&order=week_key.desc&limit=4"
                )
                for w in recent_weeks or []:
                    wk = w.get("week_key")
                    if wk == week_key:
                        continue
                    path2 = (
                        f"weekly_visit_targets"
                        f"?select=row_id,week_key,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
                        f"&week_key=eq.{quote(wk)}"
                        f"&church=eq.{quote(church)}"
                        f"&dept=eq.{quote(dept)}"
                        f"&name=eq.{quote(name)}"
                        f"&phone_last4=eq.{quote(phone)}"
                        f"&limit=1"
                    )
                    rows = await sb_get(path2)
                    if rows:
                        break

            if not rows:
                return web.json_response({"ok": True, "target": None, "progress": None})

            target = rows[0]

            # 기존 심방 기록 로드
            prog_rows = await sb_get(
                f"weekly_visit_progress"
                f"?select=*"
                f"&week_key=eq.{quote(target['week_key'])}"
                f"&row_id=eq.{quote(target['row_id'])}"
                f"&limit=1"
            )
            progress = prog_rows[0] if prog_rows else None

            return web.json_response({"ok": True, "target": target, "progress": progress})
        except Exception as e:
            logger.exception("miniapp search failed")
            return web.json_response({"ok": False, "error": str(e)}, status=500)


    async def miniapp_submit_handler(request):
        """미니웹앱 폼 제출 → weekly_visit_progress 업서트 (심방 기록 저장).

        기존 데이터 보존: upsert_weekly_visit_progress RPC 사용.
        필수: week_key, row_id.
        나머지 필드는 빈 값이면 기존 값 유지하도록 처리.
        """
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        week_key = str(data.get("week_key", "")).strip()
        row_id   = str(data.get("row_id", "")).strip()
        if not (week_key and row_id):
            return web.json_response(
                {"ok": False, "error": "week_key 또는 row_id 누락"}, status=400
            )

        # 기존 progress 로드 (빈 값은 기존 값 유지 용도)
        existing = None
        try:
            prog_rows = await sb_get(
                f"weekly_visit_progress"
                f"?select=*&week_key=eq.{quote(week_key)}&row_id=eq.{quote(row_id)}&limit=1"
            )
            existing = prog_rows[0] if prog_rows else None
        except Exception:
            existing = None

        def pick(new_val, old_val, default=""):
            """새 값이 비어있으면 기존 값 유지. (빈 문자열도 유지)"""
            s = str(new_val or "").strip()
            if s != "":
                return s
            return old_val if old_val is not None else default

        # target 값 해석 (UI는 "타겟"/"미타겟"/"")
        target_str = str(data.get("target", "")).strip()
        if target_str == "타겟":
            is_target = True
        elif target_str == "미타겟":
            is_target = False
        else:
            is_target = bool(existing.get("is_target")) if existing else False

        done_str = str(data.get("done", "")).strip()
        if done_str == "완료":
            is_done = True
        elif done_str == "미완료":
            is_done = False
        else:
            is_done = bool(existing.get("is_done")) if existing else False

        worship_str = str(data.get("worship", "")).strip()
        if worship_str in ("확정", "미정", "불참"):
            worship = worship_str
        else:
            worship = existing.get("worship") if existing else None

        attendance_str = str(data.get("attendance", "")).strip()
        if attendance_str in ("참석", "불참"):
            attendance = attendance_str
        else:
            attendance = existing.get("attendance") if existing else None

        shepherd = pick(data.get("shepherd"), existing.get("shepherd") if existing else "", "")
        visit_date_display = pick(data.get("visit_date_display"), existing.get("visit_date_display") if existing else "", "")
        plan_text = pick(data.get("plan_text"), existing.get("plan_text") if existing else "", "")
        note = pick(data.get("note"), existing.get("note") if existing else "", "")

        # 날짜 파싱 (YYYY-MM-DD 포맷만 sort에 저장)
        import re as _re
        visit_date_sort = None
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", visit_date_display):
            visit_date_sort = visit_date_display

        # 지역/구역 업데이트 (결석자 명단 자체에)
        region_name = str(data.get("region_name", "")).strip()
        zone_name   = normalize_zone(str(data.get("zone_name", "")).strip())
        if region_name or zone_name:
            try:
                update_body = {}
                if region_name: update_body["region_name"] = region_name
                if zone_name:   update_body["zone_name"]   = zone_name
                if update_body:
                    async with httpx.AsyncClient() as client:
                        r = await client.patch(
                            f"{SUPABASE_URL}/rest/v1/weekly_visit_targets"
                            f"?week_key=eq.{quote(week_key)}&row_id=eq.{quote(row_id)}",
                            headers=HEADERS,
                            content=json.dumps(update_body),
                            timeout=10,
                        )
                        if r.status_code >= 400:
                            logger.warning("target region/zone update failed %d: %s", r.status_code, r.text[:200])
            except Exception as e:
                logger.warning("target region/zone update error: %s", e)

        # 심방 기록 UPSERT
        try:
            await sb_rpc("upsert_weekly_visit_progress", {
                "p_week_key":           week_key,
                "p_row_id":             row_id,
                "p_is_target":          is_target,
                "p_is_done":            is_done,
                "p_worship":            worship,
                "p_attendance":         attendance,
                "p_note":               note,
                "p_shepherd":           shepherd,
                "p_visit_date_sort":    visit_date_sort,
                "p_visit_date_display": visit_date_display,
                "p_plan_text":          plan_text,
            })
        except Exception as e:
            logger.exception("miniapp submit failed: %s", e)
            return web.json_response({"ok": False, "error": str(e)}, status=500)

        return web.json_response({"ok": True, "message": "심방 기록 저장 완료"})

    async def health(request):
        return web.Response(text="OK")

    @web.middleware
    async def cors_mw(request, handler):
        if request.method == "OPTIONS":
            return web.Response(headers={
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type",
            })
        resp = await handler(request)
        resp.headers["Access-Control-Allow-Origin"] = "*"
        resp.headers["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return resp

    http_app = web.Application(middlewares=[cors_mw])
    http_app.router.add_post("/webhook", webhook_handler)
    http_app.router.add_get("/miniapp", miniapp_html_handler)
    http_app.router.add_get("/miniapp/", miniapp_html_handler)
    http_app.router.add_get("/miniapp/search", miniapp_search_handler)
    http_app.router.add_post("/miniapp/submit", miniapp_submit_handler)
    http_app.router.add_get("/", health)
    http_app.router.add_get("/health", health)

    async def _run():
        await app.initialize()
        await app.start()
        # 웹훅 등록
        try:
            await app.bot.delete_webhook(drop_pending_updates=True)
            await app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
            logger.info("✅ webhook registered: %s", webhook_url)
        except Exception as e:
            logger.exception("set_webhook failed: %s", e)

        runner = web.AppRunner(http_app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", port)
        await site.start()
        logger.info("✅ aiohttp server running on 0.0.0.0:%d", port)
        # keep alive
        try:
            while True:
                await asyncio.sleep(3600)
        finally:
            await runner.cleanup()
            await app.stop()
            await app.shutdown()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
