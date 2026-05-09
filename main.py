"""
결석자 타겟 심방 텔레그램 봇 v4
완전 버튼식 UI + 전체 사용법 + 특별관리 + DB 진단

흐름:
  /start → 메인 메뉴 (3버튼)
    📋 결석자 심방 → 교회 → 부서 → 지역입력 → 결석자 → 7단계 기록
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

# ── 심방 입력 7단계 ────────────────────────────────────────────────────────────
STEPS = ["shepherd", "date", "plan", "target", "done", "worship", "note"]
STEP_LABELS = {
    "shepherd":   "👤 심방자 (예: 홍길동(집사))",
    "date":       "📅 심방날짜 (예: 4/27 또는 2026-04-27)",
    "plan":       "📝 심방계획 (간단히)",
    "target":     "🎯 타겟여부",
    "done":       "✅ 진행여부",
    "worship":    "🙏 예배확답",
    "note":       "📋 진행사항 (없으면 '없음')",
}
STEP_CHOICES = {
    "target":     [["타겟", "미타겟"]],
    "done":       [["완료", "미완료"]],
    "worship":    [["확정", "미정", "불참"]],
}

# ── 특별관리 4항목 ─────────────────────────────────────────────────────────────
# 1번: 최초 1회만 · 2/3/4번: 매주 수요일 08시 초기화
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


async def sb_post(path: str, payload, extra_headers: dict = None):
    """POST /rest/v1/{path}. payload 는 dict 또는 list. PostgREST upsert 용."""
    headers = dict(HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{path}",
            headers=headers,
            content=json.dumps(payload),
            timeout=15,
        )
        if r.status_code >= 400:
            logger.error("sb_post %s failed %d: %s", path, r.status_code, r.text[:300])
        r.raise_for_status()
        if not r.content or not r.content.strip():
            return None
        try:
            return r.json()
        except Exception:
            return None


async def sb_patch(path: str, payload: dict, extra_headers: dict = None):
    """PATCH /rest/v1/{path} — 기존 row 수정."""
    headers = dict(HEADERS)
    if extra_headers:
        headers.update(extra_headers)
    async with httpx.AsyncClient() as client:
        r = await client.patch(
            f"{SUPABASE_URL}/rest/v1/{path}",
            headers=headers,
            content=json.dumps(payload),
            timeout=15,
        )
        if r.status_code >= 400:
            logger.error("sb_patch %s failed %d: %s", path, r.status_code, r.text[:300])
        r.raise_for_status()
        if not r.content or not r.content.strip():
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
    """결석자 행 목록을 받아서 각 행의 name을 실제 이름으로 복구.
    🆕 v6.0: 사용자 요청 — 마스킹된 이름(김*희)을 그대로 표시 (개인정보 보호)
    """
    return rows or []


# ═════════════════════════════════════════════════════════════════════════════
# 주차 계산 (🆕 v5.1 일요일 기준 — 항상 직전 일요일이 속한 주차)
# ═════════════════════════════════════════════════════════════════════════════
def compute_target_week_key() -> tuple[str, str]:
    """지금 시점 기준 타겟 주일 주차의 (week_key, week_label) 반환.
    
    🆕 v5.1: 일요일(주일) 기준 — 항상 직전(또는 당일) 일요일이 속한 주차
    예시:
      4/19(일) → 4월 3주차 (당일 일요일)
      4/20(월) → 4월 3주차 (어제가 일요일)
      4/22(수) → 4월 3주차 (4/19이 직전 일요일) ← 핵심!
      4/26(일) → 4월 4주차 (당일 일요일)
    """
    now = datetime.now(KST)
    weekday = now.weekday()  # Python: 0=월, 1=화, 2=수, 3=목, 4=금, 5=토, 6=일

    # 🆕 v5.5: 사용자 운영 흐름 정확히 반영
    #   "수요일 00시 KST" 가 새 주차 데이터 업로드 시작점
    #
    #   화요일 23:59 까지: 지난 주 일요일 데이터 처리 중
    #   수요일 00:00 부터: 이번 주 일요일 데이터 처리 시작
    #
    #   예시:
    #     4/22(수) 00시 ~ 4/28(화) 23시 → 4/19 일요일 데이터
    #     4/29(수) 00시 ~ 5/5(화) 23시  → 4/26 일요일 데이터
    if weekday == 6:
        # 일요일 → 그 일요일은 새 주차 시작점이지만,
        # 아직 수요일 전이라 데이터는 지난 주일 처리 중
        diff = -7
    elif weekday == 0 or weekday == 1:
        # 월(0), 화(1) → 지난 주 일요일
        # 직전 일요일 = -(weekday+1), 그 한 주 전 = -(weekday+1) - 7
        diff = -(weekday + 1) - 7
    else:
        # 수(2), 목(3), 금(4), 토(5) → 이번 주 일요일 (직전)
        diff = -(weekday + 1)

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
    # 🆕 v5.5: label = ISO 날짜 (그 주의 일요일)
    week_label = sunday.strftime('%Y-%m-%d')
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
    """🔧 v6.0: 항상 dict 반환 (None 반환 금지) — 호출자의 .get() NoneType 에러 방지"""
    try:
        rows = await sb_rpc("get_telegram_visit_context", {"p_chat_id": chat_id})
        if rows:
            result = rows[0] if isinstance(rows, list) else rows
            if isinstance(result, dict):
                return result
    except Exception as e:
        logger.warning("get_ctx failed: %s", e)
    return {}   # 🆕 None 대신 빈 dict

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


def _parse_visit_date_to_iso(raw: str) -> str | None:
    """🆕 v4.7: 다양한 날짜 형식을 ISO (YYYY-MM-DD) 로 변환 (정렬용).
    
    인식 형식:
    - 4/27, 4-27, 4.27 (월/일)
    - 2026-4-27, 2026/4/27, 2026.4.27 (연-월-일)
    - 4월 27일
    - 2026년 4월 27일
    """
    if not raw:
        return None
    raw = raw.strip()
    now_year = datetime.now(KST).year

    # 1) 이미 YYYY-MM-DD
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m:
        return raw

    # 2) YYYY[-/.]M[-/.]D
    m = re.match(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$", raw)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    # 3) M[/.−]D (현재 연도 사용)
    m = re.match(r"^(\d{1,2})[/.\-](\d{1,2})$", raw)
    if m:
        mo, d = m.groups()
        return f"{now_year:04d}-{int(mo):02d}-{int(d):02d}"

    # 4) "4월 27일"
    m = re.match(r"^(\d{1,2})월\s*(\d{1,2})일$", raw)
    if m:
        mo, d = m.groups()
        return f"{now_year:04d}-{int(mo):02d}-{int(d):02d}"

    # 5) "2026년 4월 27일"
    m = re.match(r"^(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일$", raw)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    return None


async def upsert_progress(week_key: str, row_id: str, ctx: dict):
    raw_date = ctx.get("tmp_date") or ""
    # 🆕 v4.7: 다양한 형식 → ISO 자동 변환
    date_sort = _parse_visit_date_to_iso(raw_date)
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

# 🆕 v4.7: 웹 대시보드 URL (알림 메시지의 버튼 링크)
# 🆕 v5.8: 디폴트 대시보드 URL (환경변수 우선, 없으면 GitHub Pages)
DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "https://boys072659-gif.github.io/absentee-dashboard/").strip()


def kb_reply_main(is_private: bool = True, is_special: bool = False) -> ReplyKeyboardMarkup:
    """하단에 고정되는 리플라이 키보드. 키보드 아이콘(⌨️) 탭하면 이 버튼들이 나옴.
    
    ⚠️ 웹앱 버튼은 1:1 개인 채팅에서만 작동. 그룹에서는 제외.
    🆕 1:1 개인방에선 '특별관리결석자' 버튼 숨김 (그룹방 전용 기능)
    🆕 v6.0: 특별관리 대책방에선 '결석자 심방' 메뉴 숨김 (해당 대상자만 관리)
    """
    if is_special:
        # 🆕 v6.0: 특별관리 대책방 — 특별관리결석자 메뉴만 노출
        rows = [[KeyboardButton("🚨 특별관리결석자")]]
    elif is_private:
        # 개인방: 결석자 심방만
        rows = [[KeyboardButton("📋 결석자 심방")]]
    else:
        # 일반 그룹방: 결석자 심방 + 특별관리결석자
        rows = [[KeyboardButton("📋 결석자 심방"), KeyboardButton("🚨 특별관리결석자")]]
    # 웹앱 버튼은 개인 채팅에서만 추가 (그룹에서는 "Web app buttons can be used in private chats only" 에러 발생)
    if is_private and not is_special and MINIAPP_URL.startswith("https://"):
        rows.append([KeyboardButton(
            "📝 결석자 심방 기록 (폼)",
            web_app=WebAppInfo(url=MINIAPP_URL)
        )])
    rows.append([KeyboardButton("📘 사용법"), KeyboardButton("🏠 메인 메뉴")])
    return ReplyKeyboardMarkup(
        rows,
        resize_keyboard=True,
        is_persistent=True,
        input_field_placeholder="메뉴를 선택하세요",
    )


def kb_main_menu(is_private: bool = True, is_special: bool = False) -> InlineKeyboardMarkup:
    """인라인 메인 메뉴. 웹앱 버튼은 개인 채팅에서만.
    🆕 1:1 개인방에선 '특별관리결석자' 메뉴 숨김 (그룹방 전용 기능)
    🆕 v6.0: 특별관리 대책방에선 '결석자 심방' 메뉴 숨김
    """
    rows = []
    if is_special:
        # 🆕 v6.0: 특별관리 대책방 — 특별관리결석자 메뉴만 노출
        rows.append([InlineKeyboardButton("🚨 특별관리결석자", callback_data="m:special")])
    else:
        rows.append([InlineKeyboardButton("📋 결석자 심방", callback_data="m:absentee")])
        # 특별관리는 그룹방에서만 노출
        if not is_private:
            rows.append([InlineKeyboardButton("🚨 특별관리결석자", callback_data="m:special")])
        # 웹앱 버튼은 개인 채팅에서만
        if is_private and MINIAPP_URL.startswith("https://"):
            rows.append([InlineKeyboardButton(
                "📝 결석자 심방 기록 (미니웹앱)",
                web_app=WebAppInfo(url=MINIAPP_URL)
            )])
        # 개인방엔 웹 대시보드 버튼 자동 추가
        if is_private and DASHBOARD_URL:
            rows.append([InlineKeyboardButton(
                "📊 웹 대시보드 열기",
                url=DASHBOARD_URL,
            )])
    rows += [
        [InlineKeyboardButton("📘 사용법 (도움말)",    callback_data="m:help")],
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


# 🆕 v6.0: helper — chat_id 기반 자동 special 판단해 키보드 반환
async def _kb_main(update: Update) -> InlineKeyboardMarkup:
    """main 메뉴 인라인 키보드 — 특별관리 대책방이면 결석자 심방 메뉴 자동 숨김."""
    is_sp = False
    try:
        is_sp = await is_special_monitor_chat(update.effective_chat.id)
    except Exception:
        pass
    return kb_main_menu(is_private_chat(update), is_special=is_sp)


async def _kb_reply(update: Update):
    """reply 키보드 — 특별관리 대책방이면 결석자 심방 메뉴 자동 숨김.
    🆕 v6.0: 그룹방에서는 ReplyKeyboard 자체를 안 보내고 ReplyKeyboardRemove 반환.
    그룹방의 ReplyKeyboard 는 모든 멤버에게 동일하게 보여 권한 분리가 안 되므로,
    인라인 키보드만 사용하도록 강제.
    """
    if not is_private_chat(update):
        # 그룹방 — 키보드 강제 제거
        return ReplyKeyboardRemove()
    is_sp = False
    try:
        is_sp = await is_special_monitor_chat(update.effective_chat.id)
    except Exception:
        pass
    return kb_reply_main(True, is_special=is_sp)

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
    "📖 <b>결석자 타겟 심방 봇 v4.7 — 사용법</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "<b>🛡 0️⃣ 봇 사용 권한 (최초 1회)</b>\n"
    "이 봇은 <b>승인된 방에서만</b> 동작합니다 (개인방 포함).\n"
    "• 승인 안 된 방 → 🙏 승인 신청 버튼 → 관리자에게 자동 DM\n"
    "• 관리자 승인 후 사용 가능\n"
    "• <code>/chatid</code> — 이 방의 Chat ID 확인 (관리자 전달용)\n\n"

    "<b>📘 1️⃣ 방 설정 (최초 1회)</b>\n"
    "<code>/start</code> 또는 <code>/setup</code> 으로 이 방의 담당 범위 설정:\n"
    "   교회 → 부서 → 지역 (필수) → 구역 (선택)\n"
    "⚠️ 지역까지 필수 — 개인정보 보호\n"
    "설정 후 📋 결석자 심방 에서 해당 범위의 결석자만 표시\n\n"

    "<b>📝 2️⃣ 결석자 심방 기록 흐름</b>\n"
    "메인 메뉴 → 📋 결석자 심방 탭\n"
    "→ 결석자 선택 → <b>7단계</b> 순차 입력:\n"
    "   ① 심방자 ② 심방날짜 ③ 심방계획\n"
    "   ④ 타겟여부 ⑤ 진행여부 ⑥ 예배확답\n"
    "   ⑦ 진행사항\n"
    "→ 모든 필드 완료 후 확인 → 저장\n"
    "입력 중 ❌ 입력 취소 버튼 또는 <code>/cancel</code> 로 중단 가능\n\n"

    "<b>🚨 3️⃣ 특별관리 결석자 (연속결석 4회 이상)</b>\n"
    "메인 메뉴 → 🚨 특별관리결석자 탭\n"
    "→ 교회 → 부서 → 결석자 선택\n"
    "→ 이 방이 <b>이 한 사람 전용 피드백방</b> 으로 등록\n"
    "<b>⚠️ 그룹방당 1명만 등록 가능</b>\n"
    "→ 4항목 체크리스트 표시:\n"
    "   ① 대책방 초대완료 (최초 1회)\n"
    "   ② 금주 피드백 진행 (주간 리셋)\n"
    "   ③ 금주 심방예정일\n"
    "   ④ 금주 심방계획\n"
    "매주 수요일 08:00 KST 미체크 항목 리마인더 자동 발송\n"
    "💡 ③④ 입력 시 결석자 심방 기록과 자동 동기화\n\n"

    "<b>📱 4️⃣ 미니앱 (개인 채팅에서만)</b>\n"
    "📝 결석자 심방 기록 (폼) 버튼 탭\n"
    "→ 5개 필드로 결석자 검색:\n"
    "   이름(필수) · 교회 · 부서 · 지역 · 구역\n"
    "→ 기존 심방 기록 자동 로드 → 보충/수정 후 저장\n"
    "💡 심방날짜는 다양한 형식 입력 가능:\n"
    "   <code>4/27</code> · <code>4-27</code> · <code>4.27</code>\n"
    "   <code>4월 27일</code> · <code>2026-04-27</code> · <code>2026년 4월 27일</code>\n"
    "⚠️ 그룹방에서는 미니앱 버튼 안 보임 (텔레그램 정책)\n\n"

    "<b>📅 5️⃣ 자동 알림 스케줄 (Cloud Scheduler)</b>\n"
    "• <b>매주 수요일 00:00 KST</b> → 새 주차 자동 전환\n"
    "• <b>매주 수요일 08:00 KST</b> → 모든 방에 이번 주 결석자 심방계획 요청\n"
    "• <b>매주 수요일 08:00 KST</b> → 특별관리 대상 미체크 항목 리마인더\n\n"

    "<b>👥 6️⃣ 관리자 권한 3단계</b>\n"
    "• 🛡 <b>지파관리자</b> (1명) — 모든 방 승인/관리\n"
    "• ⛪ <b>교회관리자</b> (교회당 1명) — 자기 교회 방만 승인\n"
    "• 🏛 <b>부서관리자</b> (부서당 다수) — 자기 부서 방만 승인\n\n"

    "<b>⌨️ 7️⃣ 명령어 모음</b> <i>(탭하면 복사)</i>\n"
    "• <code>/start</code> — 방 설정 + 메인 메뉴\n"
    "• <code>/menu</code> — 메인 메뉴\n"
    "• <code>/setup</code> — 방 범위 재설정 (최초 설정자만)\n"
    "• <code>/myscope</code> — 이 방의 현재 범위 확인\n"
    "• <code>/chatid</code> — 이 방의 Chat ID 확인\n"
    "• <code>/cancel</code> — 현재 입력 중단\n"
    "• <code>/help</code> — 이 사용법\n"
    "• <code>/approve &lt;chat_id&gt;</code> — 방 승인 (관리자 전용)\n"
    "• <code>/deny &lt;chat_id&gt;</code> — 방 거부 (관리자 전용)\n"
    "• <code>/allowed</code> — (특별관리 대책방) 봇 사용자 목록\n"
    "• <code>/allow</code> — (특별관리 대책방) 봇 사용자 추가 (reply 필요)\n"
    "• <code>/disallow</code> — (특별관리 대책방) 봇 사용자 제거 (reply 필요)\n\n"

    "<b>🛡 8️⃣ 특별관리 대책방 사용자 제한</b>\n"
    "특별관리 대책방은 <b>지정된 분만</b> 봇 사용 가능:\n"
    "• 첫 봇 사용자 → 자동으로 <b>owner(👑)</b> 등록\n"
    "• owner 가 다른 분을 추가하는 방법:\n"
    "   ① 답장 방식: 추가할 분이 <b>이 그룹방에서</b> 텔레그램 메시지를 보내야 함\n"
    "      → 그 분 메시지에 <b>답장(reply)</b> + <code>/allow</code>\n"
    "   ② 직접 입력: <code>/allow [user_id] [이름]</code>\n"
    "• 등록 안 된 분이 명령 보내면 1회 안내 후 무시\n\n"

    "━━━━━━━━━━━━━━━━━━━━\n"
    "🌐 <b>상세 분석·통계·CSV 는 웹 대시보드에서</b>\n"
    "📊 대시보드 로그인 권한:\n"
    "   • 부서별 비밀번호 (자기 부서만)\n"
    "   • 내무부 비밀번호 (자기 교회 전체 부서)\n"
    "   • ⛪ 교회관리자 비밀번호 (자기 교회 전체)\n"
    "   • 🛡 지파관리자 비밀번호 (전체)\n\n"
    "💬 문제 있으면 <code>/diagnose</code> 결과를 관리자에게"
)

# 하위 호환
HELP_TEXT_1 = HELP_TEXT
HELP_TEXT_2 = ""

# 🆕 v6.0: 특별관리 대책방 전용 사용법 (1, 3, 7, 8 → 1, 2, 3, 4)
HELP_TEXT_SP = (
    "📖 <b>특별관리 대책방 사용법</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "<b>📘 1️⃣ 방 설정 (최초 1회)</b>\n"
    "<code>/start</code> 또는 <code>/setup</code> 으로 이 방의 담당 범위 설정:\n"
    "   교회 → 부서 → 지역 (필수) → 구역 (선택)\n"
    "⚠️ 지역까지 필수 — 개인정보 보호\n"
    "설정 후 🚨 특별관리결석자 에서 해당 범위의 대상자만 표시\n\n"

    "<b>🚨 2️⃣ 특별관리 결석자 (연속결석 4회 이상)</b>\n"
    "메인 메뉴 → 🚨 특별관리결석자 탭\n"
    "→ 교회 → 부서 → 결석자 선택\n"
    "→ 이 방이 <b>이 한 사람 전용 피드백방</b> 으로 등록\n"
    "<b>⚠️ 그룹방당 1명만 등록 가능</b>\n"
    "→ 4항목 체크리스트 표시:\n"
    "   ① 대책방 초대완료 (최초 1회)\n"
    "   ② 금주 피드백 진행 (주간 리셋)\n"
    "   ③ 금주 심방예정일\n"
    "   ④ 금주 심방계획\n"
    "매주 수요일 08:00 KST 미체크 항목 리마인더 자동 발송\n\n"

    "<b>⌨️ 3️⃣ 명령어 모음</b> <i>(탭하면 복사)</i>\n"
    "• <code>/start</code> — 방 설정 + 메인 메뉴\n"
    "• <code>/menu</code> — 메인 메뉴\n"
    "• <code>/setup</code> — 방 범위 재설정 (최초 설정자만)\n"
    "• <code>/myscope</code> — 이 방의 현재 범위 확인\n"
    "• <code>/chatid</code> — 이 방의 Chat ID 확인\n"
    "• <code>/cancel</code> — 현재 입력 중단\n"
    "• <code>/help</code> — 이 사용법\n"
    "• <code>/allowed</code> — 봇 사용자 목록\n"
    "• <code>/allow</code> — 봇 사용자 추가 (reply 필요)\n"
    "• <code>/disallow</code> — 봇 사용자 제거 (reply 필요)\n\n"

    "<b>🛡 4️⃣ 특별관리 대책방 사용자 제한</b>\n"
    "특별관리 대책방은 <b>지정된 분만</b> 봇 사용 가능:\n"
    "• 첫 봇 사용자 → 자동으로 <b>owner(👑)</b> 등록\n"
    "• owner 가 다른 분을 추가하는 방법:\n"
    "   ① 답장 방식: 추가할 분이 <b>이 그룹방에서</b> 텔레그램 메시지를 보내야 함\n"
    "      → 그 분 메시지에 <b>답장(reply)</b> + <code>/allow</code>\n"
    "   ② 직접 입력: <code>/allow [user_id] [이름]</code>\n"
    "• 등록 안 된 분이 명령 보내면 1회 안내 후 무시\n"
)

async def _send_help(update: Update):
    """도움말 전송 — HTML parse_mode (명령어 탭 복사 가능)
    🆕 v6.0: 특별관리 대책방이면 압축 버전 (1·3·7·8 → 1·2·3·4)
    """
    is_sp = False
    try:
        is_sp = await is_special_monitor_chat(update.effective_chat.id)
    except Exception:
        pass
    text = HELP_TEXT_SP if is_sp else HELP_TEXT
    try:
        await safe_reply_text(update.message, text, parse_mode="HTML",
                              reply_markup=await _kb_main(update))
    except Exception as e:
        logger.warning("help HTML 실패, 평문: %s", e)
        plain = (text.replace("<b>","").replace("</b>","")
                     .replace("<i>","").replace("</i>","")
                     .replace("<code>","").replace("</code>",""))
        await safe_reply_text(update.message, plain,
                              reply_markup=await _kb_main(update))


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
# 🛡 봇 방 승인 체크 (보안 레벨 3)
# ═════════════════════════════════════════════════════════════════════════════
async def is_chat_authorized(chat_id: int) -> bool:
    """이 방이 관리자에 의해 사전 승인된 방인지 확인."""
    try:
        result = await sb_rpc("is_chat_authorized", {"p_chat_id": chat_id})
        if isinstance(result, bool):
            return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict):
                return bool(first.get("is_chat_authorized", False))
        return False
    except Exception as e:
        logger.warning("is_chat_authorized 실패: %s", e)
        return False


async def get_chat_status(chat_id: int) -> str:
    """🆕 v4.7: 방의 승인 상태 정확히 판별
    Returns: 'active' (승인+활성), 'blocked' (승인됐지만 차단됨), 'none' (미승인/없음)
    """
    try:
        rows = await sb_get(f"bot_authorized_chats?select=is_active&chat_id=eq.{chat_id}&limit=1")
        if not rows:
            return 'none'
        return 'active' if bool(rows[0].get('is_active')) else 'blocked'
    except Exception as e:
        logger.warning("get_chat_status 실패: %s", e)
        return 'none'


async def record_chat_access(chat_id: int):
    """승인된 방의 접근 기록 (감사 로그)."""
    try:
        await sb_rpc("record_chat_access", {"p_chat_id": chat_id})
    except Exception as e:
        logger.warning("record_chat_access 실패: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# 🆕 특별관리 대책방 화이트리스트 (chat_allowed_users)
#   - 특별관리 대책방(special_management_targets.monitor_chat_id 가 있는 방)은
#     명시적으로 등록된 user_id 만 봇 사용 가능.
#   - 첫 봇 사용자가 자동으로 owner 로 등록됨 (그 후엔 owner 가 추가)
#   - 일반 그룹방·개인방은 영향 없음.
# ═════════════════════════════════════════════════════════════════════════════
async def is_special_monitor_chat(chat_id: int) -> bool:
    """이 방이 특별관리 대책방(monitor_chat_id) 인지 확인."""
    if not chat_id:
        return False
    try:
        rows = await sb_get(
            f"special_management_targets?select=monitor_chat_id&monitor_chat_id=eq.{chat_id}&limit=1"
        )
        return bool(rows)
    except Exception as e:
        logger.warning("is_special_monitor_chat 실패: %s", e)
        return False


async def list_chat_allowed_users(chat_id: int) -> list[dict]:
    """이 방의 화이트리스트 유저 목록 (활성 only)."""
    try:
        rows = await sb_get(
            f"chat_allowed_users?select=chat_id,user_id,user_name,is_owner,added_by,added_at"
            f"&chat_id=eq.{chat_id}&is_active=eq.true&limit=100"
        ) or []
        return rows
    except Exception as e:
        # 🆕 v6.0: 테이블이 없으면 (마이그레이션 안 됨) 화이트리스트 비활성화
        msg = str(e).lower()
        if 'does not exist' in msg or 'relation' in msg or 'pgrst205' in msg or '404' in msg:
            global _chat_allowed_users_table_exists
            _chat_allowed_users_table_exists = False
            logger.info("chat_allowed_users 테이블 없음 — 화이트리스트 비활성화 (v6.0 패치 미적용)")
        else:
            logger.warning("list_chat_allowed_users 실패: %s", e)
        return []


# 🆕 v6.0: 테이블 존재 캐시 (v6.0 패치 SQL 미적용 환경에서 화이트리스트 자동 비활성)
#   - True: 테이블 정상 작동
#   - False: 테이블 미존재 → 화이트리스트 비활성 (10분 후 재확인)
_chat_allowed_users_table_exists: bool = True
_chat_allowed_users_last_check: float = 0.0   # 마지막 체크 timestamp


async def _refresh_table_cache_if_stale():
    """🆕 v6.0: 테이블 캐시가 False(미존재)로 설정된 후 10분 지나면 재확인.
    DB 마이그레이션 후 자동으로 화이트리스트가 다시 활성화되도록.
    """
    global _chat_allowed_users_table_exists, _chat_allowed_users_last_check
    import time
    now = time.time()
    if _chat_allowed_users_table_exists:
        return  # 이미 활성 — 재확인 불필요
    if now - _chat_allowed_users_last_check < 600:  # 10분 이내면 스킵
        return
    _chat_allowed_users_last_check = now
    try:
        # 가벼운 query 로 테이블 존재 확인
        await sb_get("chat_allowed_users?select=chat_id&limit=1")
        _chat_allowed_users_table_exists = True
        logger.info("✅ chat_allowed_users 테이블 다시 활성화됨 (마이그레이션 적용 감지)")
    except Exception as e:
        msg = str(e).lower()
        if 'does not exist' in msg or 'relation' in msg or 'pgrst205' in msg or '404' in msg:
            # 여전히 없음
            pass
        else:
            # 다른 에러 — 일시적일 수 있으니 다음 기회에 재시도
            logger.info("테이블 재확인 일시 실패: %s", e)


async def is_user_allowed_in_chat(chat_id: int, user_id: int) -> bool:
    """이 유저가 이 방에서 봇을 쓸 수 있는지 (화이트리스트 체크)."""
    if not chat_id or not user_id:
        return False
    try:
        rows = await sb_get(
            f"chat_allowed_users?select=user_id"
            f"&chat_id=eq.{chat_id}&user_id=eq.{user_id}&is_active=eq.true&limit=1"
        )
        return bool(rows)
    except Exception as e:
        logger.warning("is_user_allowed_in_chat 실패: %s", e)
        return False


async def add_chat_allowed_user(
    chat_id: int, user_id: int, user_name: str = "",
    is_owner: bool = False, added_by: int = None
) -> bool:
    """화이트리스트에 유저 추가. 이미 존재하면 is_active=true 로 업데이트."""
    if not chat_id or not user_id:
        return False
    try:
        # upsert (chat_id, user_id 유니크 가정)
        await sb_rpc("upsert_chat_allowed_user", {
            "p_chat_id": chat_id,
            "p_user_id": user_id,
            "p_user_name": user_name or "",
            "p_is_owner": is_owner,
            "p_added_by": added_by,
        })
        return True
    except Exception as e:
        # RPC 미존재 시 fallback: 직접 upsert
        logger.warning("upsert_chat_allowed_user RPC 실패: %s — fallback 시도", e)
        try:
            from urllib.parse import quote as _q
            payload = {
                "chat_id": chat_id,
                "user_id": user_id,
                "user_name": user_name or "",
                "is_owner": bool(is_owner),
                "added_by": added_by,
                "is_active": True,
            }
            await sb_post(
                "chat_allowed_users?on_conflict=chat_id,user_id",
                payload,
                extra_headers={"Prefer": "resolution=merge-duplicates,return=minimal"},
            )
            return True
        except Exception as e2:
            logger.warning("chat_allowed_users 직접 upsert 실패: %s", e2)
            return False


async def remove_chat_allowed_user(chat_id: int, user_id: int) -> bool:
    """화이트리스트에서 제거 (soft delete: is_active=false)."""
    if not chat_id or not user_id:
        return False
    try:
        from urllib.parse import quote as _q
        await sb_patch(
            f"chat_allowed_users?chat_id=eq.{chat_id}&user_id=eq.{user_id}",
            {"is_active": False},
        )
        return True
    except Exception as e:
        logger.warning("remove_chat_allowed_user 실패: %s", e)
        return False


# 🆕 한 번 안내 메시지를 보낸 (chat_id, user_id) 쌍은 메모리에 저장 — 같은 사람에게 반복 안내 방지
_unallowed_notified: set[tuple[int, int]] = set()

# 🆕 v6.0: 승인 신청이 보내진 관리자 DM 메시지 추적 (target_chat_id → list of (admin_uid, msg_id))
#   - 한 관리자가 승인하면 다른 관리자들의 DM 메시지에도 "이미 처리됨" 표시
_pending_admin_msgs: dict[int, list[tuple[int, int]]] = {}


async def ensure_user_allowed_in_special_chat(update: Update) -> tuple[bool, bool]:
    """🆕 특별관리 대책방에서 유저 권한 체크.
    
    Returns: (허용여부, 안내_메시지_보냈는지)
    
    동작:
    - 이 방이 특별관리 대책방이 아니면 → (True, False)  (영향 없음)
    - 화이트리스트가 비어있고 (첫 사용자):
        → 이 user를 owner로 자동 등록하고 (True, False) 반환
    - 화이트리스트에 등록된 user_id 면 → (True, False)
    - 등록 안된 user_id 면 → 1회 안내 후 (False, True)
    """
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return True, False  # 정보 없으면 차단하지 않음 (안전 fallback)

    chat_id = chat.id
    user_id = user.id

    # 🆕 v6.0: 캐시가 False면 주기적 재확인 (마이그레이션 적용 감지)
    await _refresh_table_cache_if_stale()

    # 🆕 v6.0: chat_allowed_users 테이블이 없으면 (v6.0 패치 미적용) 화이트리스트 비활성
    if not _chat_allowed_users_table_exists:
        # 특별관리 대책방인지부터 확인 — 그렇지 않은 일반방은 영향 없으니 조용히 통과
        is_sp = await is_special_monitor_chat(chat_id)
        if is_sp:
            # 🚨 매우 심각 — 특별관리 대책방인데 화이트리스트 시스템 작동 안 함
            logger.error(
                "🚨 [화이트리스트 비활성] 특별관리 대책방에서 권한 체크 불가! "
                "chat_id=%s user=%s | chat_allowed_users 테이블이 DB에 없습니다. "
                "yago_patch_v58_to_v60.sql 을 즉시 적용하세요.",
                chat_id, user_id
            )
        return True, False

    # 봇 관리자는 무조건 통과
    if await is_bot_admin_user(user_id):
        return True, False

    # 특별관리 대책방이 아니면 영향 없음
    is_sp_chat = await is_special_monitor_chat(chat_id)
    if not is_sp_chat:
        return True, False

    # 현재 화이트리스트 조회
    allowed = await list_chat_allowed_users(chat_id)

    if not allowed:
        # 🆕 첫 봇 사용자 → 자동으로 owner 등록
        user_display = user.full_name or user.username or f"user_{user_id}"
        ok = await add_chat_allowed_user(
            chat_id=chat_id,
            user_id=user_id,
            user_name=user_display,
            is_owner=True,
            added_by=user_id,
        )
        if ok:
            logger.info(
                "🆕 chat_allowed_users 자동 등록 (owner): chat_id=%s user_id=%s name=%s",
                chat_id, user_id, user_display,
            )
            # 첫 사용자에게 짧게 안내
            try:
                await update.effective_message.reply_text(
                    "🛡 <b>특별관리 대책방 — 사용자 등록 완료</b>\n\n"
                    f"이 방의 봇은 <b>{_html_escape(user_display)}</b> 님(첫 사용자)이\n"
                    f"<b>관리자(owner)</b> 로 자동 등록되었습니다.\n\n"
                    "다른 분이 이 방의 봇을 사용하려면 owner 가\n"
                    "<code>/allow</code> 명령으로 추가해야 합니다.\n"
                    "(다른 사용자가 봇 명령을 보내면 무시됩니다)\n\n"
                    "<i>💡 owner 변경은 관리자 페이지에서 가능합니다.</i>",
                    parse_mode="HTML",
                )
            except Exception:
                pass
        return True, False

    # 화이트리스트가 있는데 이 user 가 등록되어 있나?
    is_allowed = any(int(r.get("user_id", 0)) == int(user_id) for r in allowed)
    if is_allowed:
        return True, False

    # 🆕 v6.0: 텔레그램 그룹 자체의 owner/administrator 도 자동 허용
    #   ⚠️ 단, 청년회 부서는 제외 — owner가 명시적으로 /allow 한 사람만 사용 가능
    #   (청년회는 사용자 명단이 자주 바뀌어 관리자만 통제 필요)
    try:
        # 이 방의 특별관리 대상자 부서 조회
        target_dept = ""
        try:
            sp_rows = await sb_get(
                f"special_management_targets?select=dept&monitor_chat_id=eq.{chat_id}&limit=1"
            )
            if sp_rows:
                target_dept = (sp_rows[0].get("dept") or "").strip()
        except Exception:
            pass

        # 청년회는 자동 허용 스킵 — owner가 /allow 로 명시 등록한 사람만
        if target_dept == "청년회":
            logger.info(
                "[화이트리스트] 청년회 그룹방 — 텔레그램 관리자 자동 허용 스킵 chat=%s user=%s",
                chat_id, user_id
            )
        else:
            from telegram import Bot
            bot: Bot = update.get_bot()
            member = await bot.get_chat_member(chat_id, user_id)
            if member and member.status in ("creator", "administrator"):
                # 자동으로 화이트리스트에도 추가 (다음부터는 캐시 hit)
                user_display = user.full_name or user.username or f"user_{user_id}"
                try:
                    await add_chat_allowed_user(
                        chat_id=chat_id,
                        user_id=user_id,
                        user_name=user_display,
                        is_owner=(member.status == "creator"),
                        added_by=user_id,
                    )
                    logger.info(
                        "👑 [텔레그램 관리자 자동 허용] chat=%s user=%s(%s) status=%s dept=%s",
                        chat_id, user_id, user_display, member.status, target_dept,
                    )
                except Exception as _e:
                    logger.warning("텔레그램 관리자 자동 등록 실패: %s", _e)
                return True, False
    except Exception as e:
        # API 실패 — 안전하게 통과시키지 않음, 일반 화이트리스트 흐름으로
        logger.debug("get_chat_member 실패: %s", e)

    # 등록 안됨 → 1회만 안내
    key = (chat_id, user_id)
    if key not in _unallowed_notified:
        _unallowed_notified.add(key)
        try:
            await update.effective_message.reply_text(
                "🛡 이 방의 봇은 <b>지정된 분만 사용 가능합니다</b>.\n"
                "(다음 명령부터는 응답하지 않습니다)",
                parse_mode="HTML",
            )
        except Exception:
            pass
        return False, True

    # 이미 안내 보낸 사람 → 조용히 무시
    return False, False


def _html_escape(s: str) -> str:
    import html as _h
    return _h.escape(str(s or ""))


def unauthorized_message(chat_id: int, chat_title: str = "") -> str:
    """미승인 방에 표시할 안내 메시지."""
    import html as _html
    return (
        "🔒 <b>승인되지 않은 방입니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"이 방은 관리자에 의해 사전 승인된 방이 아니므로,\n"
        f"결석자 심방 정보를 볼 수 없습니다.\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {_html.escape(chat_title or '(제목없음)')}\n\n"
        f"👇 <b>먼저 소속 교회를 선택해주세요</b>\n"
        f"   (소속 정보 설정 후 → 승인 신청 가능)"
    )


def blocked_message(chat_id: int, chat_title: str = "") -> str:
    """🆕 v4.7: 차단된(비활성화된) 방에 표시할 안내 메시지."""
    import html as _html
    return (
        "🚫 <b>이 방은 관리자에 의해 차단되었습니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"이 방에서는 봇 사용이 일시 중단된 상태입니다.\n"
        f"기존 데이터는 보존되어 있으니, 재활성화 시 즉시 사용 가능합니다.\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {_html.escape(chat_title or '(제목없음)')}\n\n"
        f"👉 <b>아래 🙏 재활성화 신청 버튼을 누르시면</b>\n"
        f"관리자에게 재활성화 요청이 전달됩니다."
    )


def kb_request_approval() -> InlineKeyboardMarkup:
    """승인 신청 버튼 키보드 (이미 scope 설정된 경우)"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🙏 승인 신청하기", callback_data="request_approval")]
    ])


# 🆕 v5.8: 승인 전 scope 선택 — 교회 선택 키보드
def kb_pre_approval_church() -> InlineKeyboardMarkup:
    """승인 신청 전 — 소속 교회 선택"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🏛 서울교회",   callback_data="prereq_church:서울교회"),
            InlineKeyboardButton("🏛 포천교회",   callback_data="prereq_church:포천교회"),
        ],
        [
            InlineKeyboardButton("🏛 구리교회",   callback_data="prereq_church:구리교회"),
            InlineKeyboardButton("🏛 동대문교회", callback_data="prereq_church:동대문교회"),
        ],
        [
            InlineKeyboardButton("🏛 의정부교회", callback_data="prereq_church:의정부교회"),
        ],
    ])


# 🆕 v5.8: 부서 선택 키보드
def kb_pre_approval_dept(church: str) -> InlineKeyboardMarkup:
    """승인 신청 전 — 소속 부서 선택"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("👔 자문회", callback_data=f"prereq_dept:{church}:자문회"),
            InlineKeyboardButton("👨 장년회", callback_data=f"prereq_dept:{church}:장년회"),
        ],
        [
            InlineKeyboardButton("👩 부녀회", callback_data=f"prereq_dept:{church}:부녀회"),
            InlineKeyboardButton("🧑 청년회", callback_data=f"prereq_dept:{church}:청년회"),
        ],
        [
            InlineKeyboardButton("⛪ 교역자", callback_data=f"prereq_dept:{church}:교역자"),
        ],
        [
            InlineKeyboardButton("⬅️ 교회 다시 선택", callback_data="prereq_back_church"),
        ],
    ])


# 🆕 v5.8: 최종 확인 키보드 (승인 신청 버튼)
def kb_final_approval(church: str, dept: str) -> InlineKeyboardMarkup:
    """승인 신청 전 — 최종 확인 후 승인 신청"""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🙏 승인 신청하기", callback_data="request_approval"),
        ],
        [
            InlineKeyboardButton("⬅️ 부서 다시 선택", callback_data=f"prereq_back_dept:{church}"),
        ],
    ])


def kb_dashboard_link() -> InlineKeyboardMarkup | None:
    """🆕 v4.7: 웹 대시보드 링크 버튼 (DASHBOARD_URL 설정된 경우만)"""
    if not DASHBOARD_URL:
        return None
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 웹 대시보드 열기", url=DASHBOARD_URL)]
    ])


async def is_bot_admin_user(user_id: int) -> bool:
    """이 user_id 가 봇 관리자인지 확인."""
    if not user_id:
        return False
    try:
        result = await sb_rpc("is_bot_admin", {"p_user_id": user_id})
        if isinstance(result, bool):
            return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict):
                return bool(first.get("is_bot_admin", False))
        return False
    except Exception as e:
        logger.warning("is_bot_admin 실패: %s", e)
        return False


async def get_active_bot_admins() -> list[dict]:
    """활성 봇 관리자 목록."""
    try:
        rows = await sb_rpc("get_active_bot_admins", {})
        if isinstance(rows, list):
            return [r for r in rows if isinstance(r, dict)]
        return []
    except Exception as e:
        logger.warning("get_active_bot_admins 실패: %s", e)
        return []


async def try_acquire_job_lock(job_name: str, source: str = "unknown") -> bool:
    """🆕 v4.7: 같은 날 중복 실행 방지.
    
    True: 이번이 첫 실행 → 작업 수행 OK
    False: 오늘 이미 실행됨 → 스킵
    """
    try:
        result = await sb_rpc("try_acquire_job_lock", {
            "p_job_name": job_name,
            "p_source": source,
        })
        if isinstance(result, bool):
            return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict):
                return bool(first.get("try_acquire_job_lock", False))
        return False
    except Exception as e:
        logger.warning("try_acquire_job_lock 실패 (lock 없이 실행): %s", e)
        # DB 오류 시 안전을 위해 True 반환 (실행 진행)
        return True


async def ensure_authorized(update: Update) -> bool:
    """
    승인 체크 + 미승인 시 안내 메시지 전송.
    승인되었으면 True, 안 되었으면 False 반환.
    🔧 개인방도 승인 필요 (관리자가 사전 등록한 user_id만 사용 가능).
    """
    chat = update.effective_chat
    if not chat:
        return False

    chat_id = chat.id

    # 🔒 개인방은 user_id 기준으로 승인 체크 (개인방 chat_id == user_id)
    if chat.type == "private":
        # 봇 관리자는 무조건 승인
        if await is_bot_admin_user(chat_id):
            return True
        # 일반 사용자는 chat_id 가 승인되었는지 체크
        if await is_chat_authorized(chat_id):
            await record_chat_access(chat_id)
            return True
    else:
        # 그룹방
        if await is_chat_authorized(chat_id):
            await record_chat_access(chat_id)
            # 🆕 특별관리 대책방이면 화이트리스트도 체크
            allowed, _ = await ensure_user_allowed_in_special_chat(update)
            if not allowed:
                return False
            return True

    # 미승인 또는 차단 — 차단인지 미승인인지 구분해서 다른 메시지
    chat_title = chat.title or chat.full_name or ""
    status = await get_chat_status(chat_id)
    if status == 'blocked':
        # 🚫 차단된 방
        msg = blocked_message(chat_id, chat_title)
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🙏 재활성화 신청", callback_data="request_approval")]
        ])
    else:
        # 🔒 미승인 방
        msg = unauthorized_message(chat_id, chat_title)
        # 🆕 v5.8: scope 이미 있으면 바로 승인 버튼, 없으면 교회 선택부터
        existing_scope = await get_chat_scope(chat_id)
        if existing_scope and existing_scope.get("church"):
            kb = kb_request_approval()
        else:
            kb = kb_pre_approval_church()
    try:
        if update.message:
            await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kb)
        elif update.callback_query:
            await update.callback_query.message.reply_text(msg, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.warning("미승인 메시지 전송 실패: %s", e)
        try:
            plain = msg.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "")
            if update.message:
                await update.message.reply_text(plain, reply_markup=kb)
            elif update.callback_query:
                await update.callback_query.message.reply_text(plain, reply_markup=kb)
        except Exception:
            pass
    return False


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
    """부서 선택 (지역까지 의무 설정 — 교회 전체 스킵 불가)"""
    rows = [[InlineKeyboardButton(f"🏛 {dp}", callback_data=f"scope_dp:{dp}")] for dp in DEPTS]
    rows.append([InlineKeyboardButton("◀ 교회 다시 선택", callback_data="scope_setup")])
    rows.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)

def kb_setup_region() -> InlineKeyboardMarkup:
    """지역 입력 단계 (의무) — '여기까지만' 버튼 제거"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="scope_setup_back_dept")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])

def kb_setup_zone() -> InlineKeyboardMarkup:
    """구역 입력 (선택) — 지역까지만으로도 완료 가능"""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ 구역 없이 완료 (지역까지)", callback_data="scope_stop:region")],
        [InlineKeyboardButton("◀ 지역 다시 입력", callback_data="scope_setup_back_region")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])


async def setup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """방의 담당 범위 설정 시작 (교회부터)."""
    # 🛡 보안 체크
    if not await ensure_authorized(update):
        return

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
        f"*② 부서*를 선택하세요 👇\n\n"
        f"⚠️ _지역까지 설정해야 결석자를 볼 수 있습니다._",
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
        f"📋 *방 범위 설정 (3/4 단계)*\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"1️⃣ ✅ *교회*: {md(church)}\n"
        f"2️⃣ ✅ *부서*: {md(dept)}\n"
        f"3️⃣ ⏳ *지역* (필수) ← 지금 단계\n"
        f"4️⃣ ⏸ *구역* (선택)\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📍 *지역 이름을 입력해주세요*\n\n"
        f"예시: `강북` `강남` `노원` `성북` `중랑` `대학`\n\n"
        f"💡 _지역까지는 반드시 설정해야 메뉴 진입 가능합니다._",
        parse_mode="Markdown",
        reply_markup=kb_setup_region(),
    )


async def _on_scope_stop(update: Update, chat_id: int, stop_level: str):
    """지정 단계에서 범위 설정 완료 (지역까지 의무)"""
    q = update.callback_query
    user = update.effective_user

    # 🛑 지역까지 의무 설정 — church/dept 스킵 차단
    if stop_level in ("church", "dept"):
        await q.edit_message_text(
            "❌ <b>지역까지 설정해야 합니다</b>\n\n"
            "결석자 정보 보호를 위해 최소 <b>지역</b> 단위까지\n"
            "담당 범위를 설정해야 합니다.\n\n"
            "부서를 선택하고 지역을 입력해주세요.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="scope_setup_back_dept")],
                [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
            ]),
        )
        return

    ctx = await get_ctx(chat_id)
    church = ctx.get("church_filter")
    dept   = ctx.get("dept_filter") if stop_level in ("dept","region","zone") else None
    # 🔧 editing_region/editing_zone 대신 region_filter/(없음) 사용
    region = ctx.get("region_filter") if stop_level in ("region","zone") else None
    zone   = None  # 구역은 텍스트 입력으로만 완료되므로 stop 에서는 항상 None

    if not church:
        await q.edit_message_text("❌ 교회 정보가 없습니다. /setup 다시 시작.")
        return
    if not dept:
        await q.edit_message_text("❌ 부서가 설정되지 않았습니다. /setup 다시 시작.")
        return
    if not region:
        await q.edit_message_text("❌ 지역이 설정되지 않았습니다. /setup 다시 시작.")
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
        f"🎉 <b>방 범위 설정 완료</b>\n\n"
        f"📌 {_escape_html(scope_label(new_scope))}\n"
        f"👤 최초 설정자: <b>{_escape_html(owner_name or '(미기록)')}</b>\n\n"
        f"이제 📋 결석자 심방 에서 이 범위의 결석자만 표시됩니다.\n"
        f"범위 확인: /myscope\n"
        f"변경(최초 설정자만): /setup",
        parse_mode="HTML",
    )
    # 메뉴 다시 표시
    await q.message.reply_text(
        "🏠 *메인 메뉴*",
        parse_mode="Markdown",
        reply_markup=await _kb_main(update),
    )


async def _on_scope_text_input(update: Update, chat_id: int, text: str):
    """scope 설정 중 텍스트 입력 처리 (지역명 또는 구역명)."""
    ctx = await get_ctx(chat_id)
    step = ctx.get("editing_step")
    user = update.effective_user

    # 🆕 v6.0: 디버그 로그 — 어느 단계인지 명확히 추적
    logger.info(
        "[scope_text] chat=%s user=%s step=%s text=%r ctx_keys=%s",
        chat_id, user.id if user else '?', step, text,
        list(ctx.keys()) if ctx else []
    )

    if step == "awaiting_scope_region_text":
        region = text.strip()
        if not region:
            await safe_reply_text(
                update.message,
                "⚠️ 지역 이름이 비어있습니다. 다시 입력해주세요.\n예: `강북`, `노원`",
                parse_mode="Markdown",
                reply_markup=kb_setup_region(),
            )
            return True

        await save_ctx(chat_id, region_filter=region, editing_step="awaiting_scope_zone_text")
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        # 🆕 v6.0: 안내 메시지를 더 크고 명확하게 (글씨 잘 보이게)
        await safe_reply_text(
            update.message,
            f"✅ <b>지역 입력 완료: {_html_escape(region)}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📋 <b>방 범위 설정 (3/4 완료)</b>\n\n"
            f"1️⃣ ✅ 교회: <b>{_html_escape(church)}</b>\n"
            f"2️⃣ ✅ 부서: <b>{_html_escape(dept)}</b>\n"
            f"3️⃣ ✅ 지역: <b>{_html_escape(region)}</b>\n"
            f"4️⃣ ⏳ <b>구역 (마지막 단계)</b>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏘 <b>구역명을 입력하세요</b> 👇\n\n"
            f"<b>예시:</b>\n"
            f"   • <code>1-1</code>  • <code>1팀1</code>\n"
            f"   • <code>2-3</code>  • <code>2팀3</code>\n"
            f"   • <code>사랑-1</code>  • <code>노민-2</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏭ <i>구역 없이 마치려면 아래</i>\n"
            f"<i>[지역까지만 완료] 버튼을 누르세요</i>",
            parse_mode="HTML",
            reply_markup=kb_setup_zone(),
        )
        return True

    if step == "awaiting_scope_zone_text":
        zone = text.strip()
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        region = ctx.get("region_filter") or ""

        if not region:
            # 🆕 v6.0: 디버그 강화 — 왜 region 이 사라졌는지 추적
            logger.error(
                "[scope_text] region 누락! chat=%s ctx=%s text=%r — 사용자가 다시 처음으로 가게 됨",
                chat_id, ctx, text
            )
            await safe_reply_text(
                update.message,
                "❌ <b>지역 정보가 사라졌습니다</b>\n\n"
                "잠시 다른 분이 동시에 같은 방에서 봇을 사용했을 수 있습니다.\n"
                "다시 처음부터 시작해주세요.\n\n"
                "👉 <code>/setup</code> 또는 <code>/start</code>",
                parse_mode="HTML",
            )
            return True

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
            f"🎉 <b>방 범위 설정 완료!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📌 {_html_escape(scope_label(new_scope))}\n"
            f"👤 최초 설정자: <b>{_html_escape(owner_name or '(미기록)')}</b>\n\n"
            f"이제 <b>📋 결석자 심방</b>에서 이 범위의 결석자만 표시됩니다.\n\n"
            f"💡 범위 변경(최초 설정자만): /setup\n"
            f"💡 현재 범위 확인: /myscope",
            parse_mode="HTML",
            reply_markup=await _kb_main(update),
        )
        return True

    return False


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🆕 v6.0: 특별관리 대책방 화이트리스트 체크 (가장 먼저)
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            return
    except Exception as _e:
        logger.warning("ensure_user_allowed_in_special_chat (start): %s", _e)

    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    week_key, week_label = await get_active_week()
    import html as _html

    # 🆕 v6.0: 특별관리 대책방이면 하단 키보드를 명시적으로 제거
    is_sp_chat = await is_special_monitor_chat(chat_id)

    if is_sp_chat:
        # 특별관리 대책방 — 하단 키보드 비활성화 + 인라인 메뉴 안내
        await update.message.reply_text(
            f"👋 <b>결석자 타겟 심방 봇</b>에 오신 것을 환영합니다\n"
            f"📅 현재 주차: <b>{_html.escape(week_label) if week_label else '미등록'}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💡 <b>사용 방법</b>:\n"
            f"• <code>시작</code> 또는 <code>/start</code> — 메인 메뉴\n"
            f"• <code>메뉴</code> 또는 <code>/menu</code> — 인라인 메뉴\n"
            f"• <code>도움말</code> 또는 <code>/help</code> — 사용법",
            parse_mode="HTML",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        # 일반 그룹방·개인방 — helper 가 자동으로 그룹방이면 KeyboardRemove 처리
        await update.message.reply_text(
            f"👋 *결석자 타겟 심방 봇*에 오신 것을 환영합니다\n"
            f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
            f"━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⌨️ 하단 키보드로 시작하세요 👇",
            parse_mode="Markdown",
            reply_markup=await _kb_reply(update),
        )

    # 🛡 보안 체크 (개인방·그룹방 모두) — 승인되지 않으면 안내
    is_private = is_private_chat(update)
    user = update.effective_user
    is_admin = user and await is_bot_admin_user(user.id)

    if is_private:
        # 개인방: 봇 관리자는 무조건 통과 / 일반 사용자는 chat_id 승인 체크
        if not is_admin:
            authorized = await is_chat_authorized(chat_id)
            if not authorized:
                # 🆕 v5.8: scope 미설정시 교회 선택부터
                existing_scope = await get_chat_scope(chat_id)
                kb = kb_request_approval() if existing_scope and existing_scope.get("church") else kb_pre_approval_church()
                await update.message.reply_text(
                    unauthorized_message(chat_id, chat_title),
                    parse_mode="HTML",
                    reply_markup=kb,
                )
                return
            await record_chat_access(chat_id)
    else:
        # 그룹방
        authorized = await is_chat_authorized(chat_id)
        if not authorized:
            # 🆕 v5.8: scope 미설정시 교회 선택부터
            existing_scope = await get_chat_scope(chat_id)
            kb = kb_request_approval() if existing_scope and existing_scope.get("church") else kb_pre_approval_church()
            await update.message.reply_text(
                unauthorized_message(chat_id, chat_title),
                parse_mode="HTML",
                reply_markup=kb,
            )
            return
        await record_chat_access(chat_id)

    # 2) 방 scope 확인 — 미설정이면 setup 유도
    scope = await get_chat_scope(chat_id)
    
    # 🆕 v5.8: 개인방 / 그룹방 둘 다 — 지역/구역까지 완전히 설정해야 메뉴 진입
    #   개인방의 경우, 부서까지만 설정되어 있으면 결석자 목록이 너무 많아서
    #   봇이 버벅거림 → 지역/구역까지 의무 설정
    scope_complete = scope and scope.get("church") and scope.get("dept") and scope.get("region_name")
    
    if not scope_complete:
        if scope and scope.get("church") and scope.get("dept"):
            # 부서까지는 설정됨 (승인 전 단계에서 설정한 것) → 지역 추가 안내
            dashboard_btn = []
            if is_private and DASHBOARD_URL:
                dashboard_btn.append([InlineKeyboardButton("📊 웹 대시보드 열기", url=DASHBOARD_URL)])
            
            await update.message.reply_text(
                f"✅ <b>이 방은 승인되었습니다!</b>\n\n"
                f"📋 <b>방 범위 설정 (2/4 완료)</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"1️⃣ ✅ 교회: {scope.get('church')}\n"
                f"2️⃣ ✅ 부서: {scope.get('dept')}\n"
                f"3️⃣ ⏳ <b>지역 (필수)</b> ← 다음 단계\n"
                f"4️⃣ ⏸ 구역 (선택)\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📍 <b>지역 설정이 필요합니다</b>\n\n"
                f"부서 단위로는 결석자가 너무 많아 봇이 느려집니다.\n"
                f"<b>지역</b>까지 설정하면 본인 담당 결석자만 빠르게 표시됩니다.\n"
                f"(구역은 선택 — 더 좁히고 싶으면 입력)\n\n"
                f"👇 아래 [📍 지역 설정 시작] 버튼을 눌러주세요",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📍 지역 설정 시작", callback_data="scope_setup_back_region")],
                    *dashboard_btn,
                    [InlineKeyboardButton("📘 사용법", callback_data="show_help")],
                ]),
            )
        else:
            # scope 자체가 없음 (이런 경우는 거의 없지만 안전장치)
            await update.message.reply_text(
                f"✅ 이 방은 승인되었습니다.\n\n"
                f"📌 <b>담당 범위 설정이 필요합니다.</b>\n\n"
                f"이 방에서 관리할 <b>교회 / 부서 / 지역 / 구역</b>을 설정해야\n"
                f"결석자 목록이 해당 범위로 자동 필터링됩니다.\n\n"
                f"아래 [🔧 방 범위 설정] 버튼을 눌러 시작하세요 👇",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
                    [InlineKeyboardButton("📘 사용법", callback_data="show_help")],
                ]),
            )
    else:
        # scope 완전 설정됨 → 메인 메뉴 (개인방이면 메뉴에 대시보드 버튼 자동 포함)
        await update.message.reply_text(
            f"📌 *이 방의 담당 범위*: {md(scope_label(scope))}\n\n"
            f"🏠 *메인 메뉴*",
            parse_mode="Markdown",
            reply_markup=await _kb_main(update),
        )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🆕 v6.0: 특별관리 대책방 화이트리스트 체크
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            return
    except Exception as _e:
        logger.warning("ensure_user_allowed_in_special_chat (menu): %s", _e)

    week_key, week_label = await get_active_week()
    chat_id = update.effective_chat.id

    # 🆕 v6.0: 특별관리 대책방인지 확인
    is_sp_chat = await is_special_monitor_chat(chat_id)

    txt = (
        "🏠 *메인 메뉴*\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "아래 버튼에서 원하는 기능을 선택하세요 👇\n\n"
        "💡 사용법은 *📘 사용법* 버튼 또는 `/help`"
    )
    if is_sp_chat:
        # 특별관리 대책방 — 인라인 메뉴만 (하단 키보드는 인라인 메시지에 함께 제거)
        await update.message.reply_text(
            txt, parse_mode="Markdown",
            reply_markup=kb_main_menu(is_private_chat(update), is_special=True),
        )
        # 키보드 제거 (안내 문구 없이 조용히)
        try:
            kb_remove_msg = await update.message.reply_text(".", reply_markup=ReplyKeyboardRemove())
            await kb_remove_msg.delete()
        except Exception:
            pass
    else:
        await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=await _kb_main(update))
        # 리플라이 키보드가 사라져있을 수 있으니 복구
        await update.message.reply_text("⌨️ 하단 키보드 메뉴 활성화", reply_markup=await _kb_reply(update))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🆕 v6.0: 화이트리스트 체크
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            return
    except Exception:
        pass
    await _send_help(update)


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 🆕 v6.0: 화이트리스트 체크
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            return
    except Exception:
        pass
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
        "\n".join(lines), parse_mode="Markdown", reply_markup=await _kb_main(update)
    )


# ═════════════════════════════════════════════════════════════════════════════
# 콜백 (버튼) 디스패처
# ═════════════════════════════════════════════════════════════════════════════
async def button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    chat_id = update.effective_chat.id

    # 🆕 승인 관련 콜백은 먼저 체크 (ensure_authorized 전에)
    if data == "request_approval":
        await request_approval_callback(update, context)
        return
    if data.startswith("admin_approve:"):
        await admin_approve_callback(update, context)
        return
    if data.startswith("admin_deny:"):
        await admin_deny_callback(update, context)
        return
    # 🆕 v5.8: 승인 전 교회/부서 선택 콜백
    if data.startswith("prereq_church:"):
        await prereq_church_callback(update, context)
        return
    if data.startswith("prereq_dept:"):
        await prereq_dept_callback(update, context)
        return
    if data == "prereq_back_church":
        await prereq_back_church_callback(update, context)
        return
    if data.startswith("prereq_back_dept:"):
        await prereq_back_dept_callback(update, context)
        return

    # 🆕 v6.0: 특별관리 대책방 화이트리스트 체크 (q.answer 보다 먼저!)
    #   q.answer 가 먼저 실행되면 alert 가 표시되지 않음
    #   특별관리 대책방이 아니면 영향 없음
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            try:
                user = update.effective_user
                logger.info(
                    "🛡 [차단] 콜백 — chat=%s user=%s(%s) data=%s",
                    chat_id, user.id if user else '?', (user.full_name if user else '?'), data
                )
                await q.answer(
                    "🛡 이 방의 봇은 등록된 사용자만 사용 가능합니다.\n관리자(👑 owner)에게 /allow 등록을 요청하세요.",
                    show_alert=True,
                )
            except Exception:
                pass
            return
    except Exception as _e:
        logger.warning("ensure_user_allowed_in_special_chat (callback): %s", _e)

    await q.answer()

    try:
        # ── 메인 메뉴 ──
        if data == "m:home":
            await _show_home(update)
        elif data == "m:absentee":
            # 🆕 v6.0: 특별관리 대책방에서는 결석자 심방 메뉴 차단
            if await is_special_monitor_chat(chat_id):
                await q.message.reply_text(
                    "🛡 <b>이 방은 특별관리 대책방입니다</b>\n\n"
                    "이 방에서는 <b>특별관리 대상자만</b> 관리할 수 있습니다.\n"
                    "결석자 심방 기능은 <b>일반 그룹방 또는 개인 1:1</b> 에서 사용해주세요.\n\n"
                    "👉 <b>🚨 특별관리결석자</b> 버튼을 사용하세요.",
                    parse_mode="HTML",
                )
                return
            # 🔧 일반 결석자 심방 진입 시 이전 특별관리 컨텍스트 완전 제거
            await clear_tmp(chat_id)
            await _show_church_select(update, "abs")
        elif data == "m:special":
            # 🆕 1:1 개인방에선 특별관리 메뉴 차단 (그룹방 전용 기능)
            if is_private_chat(update):
                await q.message.reply_text(
                    "ℹ️ <b>특별관리결석자</b> 메뉴는 그룹방 전용입니다.\n\n"
                    "특별관리 대상자의 <b>대책방(그룹방)</b> 에서만\n"
                    "사용할 수 있습니다. 1:1 개인방에서는 사용할 수 없습니다.\n\n"
                    "💡 결석자 심방 정보는 <code>📋 결석자 심방</code> 메뉴를 이용해주세요.",
                    parse_mode="HTML",
                )
                return
            # 🔧 특별관리 진입 시 이전 결석자 심방 컨텍스트 완전 제거
            await clear_tmp(chat_id)
            # 🆕 v4.7: 이미 등록된 사람 있으면 그 사람 정보로 바로 이동
            try:
                existing = await sb_get(
                    f"special_management_targets?select=name,dept,phone_last4"
                    f"&monitor_chat_id=eq.{chat_id}&limit=1"
                )
            except Exception:
                existing = []
            if existing:
                ex = existing[0]
                ex_name = ex.get("name", "")
                ex_dept = ex.get("dept", "")
                ex_phone = ex.get("phone_last4", "") or ""
                scope = await get_chat_scope(chat_id)
                ex_church = (scope or {}).get("church", "")
                await save_ctx(chat_id, tmp_sp_name=ex_name, tmp_sp_phone=ex_phone)
                await _show_sp_detail(update, chat_id, ex_church, ex_dept, ex_name, ex_phone, send_new=True)
            else:
                await _show_church_select(update, "sp")
        elif data == "m:help":
            # 🆕 v6.0: 특별관리 대책방이면 압축 사용법
            help_text = HELP_TEXT_SP if await is_special_monitor_chat(chat_id) else HELP_TEXT
            try:
                await q.message.reply_text(help_text, parse_mode="HTML",
                                           reply_markup=await _kb_main(update))
            except Exception as he:
                logger.warning("help HTML 실패: %s", he)
                plain = (help_text.replace("<b>","").replace("</b>","")
                                  .replace("<i>","").replace("</i>","")
                                  .replace("<code>","").replace("</code>",""))
                await q.message.reply_text(plain, reply_markup=await _kb_main(update))
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
        # 🆕 개별 항목 수정 흐름
        elif data.startswith("edit_step:"):
            step = data.split(":", 1)[1]
            await _on_edit_step(update, chat_id, step)
        elif data.startswith("edit_full:"):
            row_id = data.split(":", 1)[1]
            await _on_edit_full(update, chat_id, row_id)
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
                reply_markup=await _kb_main(update),
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
                f"📋 <b>방 범위 설정 (2/4 완료)</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"1️⃣ ✅ 교회: <b>{_html_escape(church)}</b>\n"
                f"2️⃣ ✅ 부서: <b>{_html_escape(dept)}</b>\n"
                f"3️⃣ ⏳ <b>지역 (지금 단계)</b> 👈\n"
                f"4️⃣ ⏸️ 구역 (다음 단계)\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📍 <b>본인 담당 지역명을 입력하세요</b> 👇\n\n"
                f"<b>예시:</b>\n"
                f"   • <code>강북</code>  • <code>노원</code>\n"
                f"   • <code>도봉</code>  • <code>성북</code>\n"
                f"   • <code>강남</code>  • <code>서초</code>\n\n"
                f"<i>입력하시면 자동으로 다음 단계(구역)로 넘어갑니다.</i>",
                parse_mode="HTML",
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
        elif data == "sp_show_current":
            # 🆕 v4.7: 이 방의 현재 등록된 특별관리 대상자 정보 보기
            try:
                existing = await sb_get(
                    f"special_management_targets?select=name,dept,phone_last4"
                    f"&monitor_chat_id=eq.{chat_id}&limit=1"
                )
            except Exception:
                existing = []
            if existing:
                ex = existing[0]
                ex_name = ex.get("name", "")
                ex_dept = ex.get("dept", "")
                ex_phone = ex.get("phone_last4", "") or ""
                # scope 에서 church 추정
                scope = await get_chat_scope(chat_id)
                ex_church = (scope or {}).get("church", "")
                await save_ctx(chat_id, tmp_sp_name=ex_name, tmp_sp_phone=ex_phone)
                await _show_sp_detail(update, chat_id, ex_church, ex_dept, ex_name, ex_phone, send_new=True)
            else:
                await q.message.reply_text("❌ 이 방에 등록된 특별관리 대상이 없습니다.")
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
    chat_id = update.effective_chat.id
    is_sp_chat = await is_special_monitor_chat(chat_id)
    week_key, week_label = await get_active_week()
    txt = (
        "🏠 *메인 메뉴*\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "원하는 기능을 선택하세요 👇"
    )
    if is_sp_chat:
        txt += "\n\n🛡 _이 방은 특별관리 대책방 — 특별관리결석자 메뉴만 사용_"
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update), is_special=is_sp_chat))


async def _show_church_select(update: Update, flow: str):
    """scope 기반 자동 점프. scope 없으면 교회 선택, 있으면 자동 진행."""
    # 🛡 보안 체크 (레벨 3): 승인된 방만 접근 허용
    if not await ensure_authorized(update):
        return

    q = update.callback_query
    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    # 🔒 scope 미설정이면 설정 유도 (개인방/그룹방 동일)
    if not scope:
        chat_type_hint = "개인방" if is_private_chat(update) else "그룹방"
        await q.edit_message_text(
            f"📌 <b>{chat_type_hint}에서도 담당 범위 설정이 필요합니다</b>\n\n"
            f"결석자 정보 보호를 위해 이 방에서 볼 수 있는 범위를\n"
            f"미리 설정해야 합니다 (<b>지역까지 필수</b>).\n\n"
            f"• 교회 → 부서 → 지역 (→ 구역)\n\n"
            f"아래 버튼을 눌러 설정하세요 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
                [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
            ]),
        )
        return

    # scope 있음 - 자동 진행
    await _scope_jump(update, chat_id, scope, flow)


async def _show_church_menu(update: Update, flow: str):
    """리플라이 키보드에서 진입할 때 — scope 자동 반영"""
    # 🛡 보안 체크
    if not await ensure_authorized(update):
        return

    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    if not scope:
        chat_type_hint = "개인방" if is_private_chat(update) else "그룹방"
        await update.message.reply_text(
            f"📌 <b>{chat_type_hint}에서도 담당 범위 설정이 필요합니다</b>\n\n"
            f"결석자 정보 보호를 위해 이 방에서 볼 수 있는 범위를\n"
            f"미리 설정해야 합니다 (<b>지역까지 필수</b>).\n\n"
            f"아래 버튼을 눌러 설정하세요 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")],
            ]),
        )
        return

    # scope 에 맞춰 결석자 목록 직접 표시
    await _scope_jump_from_message(update, chat_id, scope, flow)


async def _scope_jump(update: Update, chat_id: int, scope: dict, flow: str):
    """scope 기반 결석자 목록 바로 표시 (edit)"""
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.",
                                  reply_markup=await _kb_main(update))
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
            reply_markup=await _kb_main(update),
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

    # 🆕 v6.0: 봇이 반응할 텍스트인지 먼저 판단
    #   일반 대화에는 봇이 반응하지 않게 — 권한 체크 메시지조차 보내지 않음
    text_low = text.lower()
    BOT_KEYWORDS = (
        "시작", "처음", "메뉴", "도움말", "사용법", "도움", "취소",
        "Start", "start", "Menu", "menu", "Help", "help", "Cancel", "cancel",
        "📋 결석자 심방", "🚨 특별관리결석자", "📘 사용법", "🏠 메인 메뉴",
    )
    is_bot_command = text.startswith("/") or text in BOT_KEYWORDS

    # 컨텍스트 확인 — 입력 대기 중인지 (지역, 구역, 심방계획 등)
    ctx_pre_early = await get_ctx(chat_id)
    pre_step_early = (ctx_pre_early.get("editing_step", "") if ctx_pre_early else "")
    is_awaiting_input = bool(pre_step_early)

    # 봇 키워드도 아니고 입력 대기 중도 아니면 — 일반 대화로 간주, 봇이 반응 안 함
    if not is_bot_command and not is_awaiting_input:
        return

    # 🆕 v6.0: 특별관리 대책방 화이트리스트 체크 (위 필터 통과한 경우에만)
    try:
        allowed, _ = await ensure_user_allowed_in_special_chat(update)
        if not allowed:
            return
    except Exception as _e:
        logger.warning("ensure_user_allowed_in_special_chat (text): %s", _e)

    # 🆕 v6.0: 한국어/일반 텍스트 → 명령어 매핑
    if text in ("시작", "처음", "Start", "start") or text_low == "/start":
        await start_command(update, context)
        return
    if text in ("메뉴", "menu", "Menu") or text_low == "/menu":
        await menu_command(update, context)
        return
    if text in ("도움말", "사용법", "도움", "help", "Help") or text_low == "/help":
        await help_command(update, context)
        return
    if text in ("취소", "Cancel", "cancel") or text_low == "/cancel":
        await cancel_command(update, context)
        return

    # ── 0) 리플라이 키보드 (하단 버튼) 라벨 라우팅 ──────────────────────
    #  - 컨텍스트보다 우선하지만, 사용자가 입력 중이면 의도와 다를 수 있으니
    #    컨텍스트의 editing_step 이 비어있을 때만 라우팅
    ctx_pre = await get_ctx(chat_id)
    pre_step = (ctx_pre.get("editing_step", "") if ctx_pre else "") or ""
    if not pre_step:
        if text == "📋 결석자 심방":
            # 🆕 v6.0: 특별관리 대책방에서는 결석자 심방 메뉴 차단
            if await is_special_monitor_chat(chat_id):
                await update.message.reply_text(
                    "🛡 <b>이 방은 특별관리 대책방입니다</b>\n\n"
                    "이 방에서는 <b>특별관리 대상자만</b> 관리할 수 있습니다.\n"
                    "결석자 심방 기능은 <b>일반 그룹방 또는 개인 1:1</b> 에서 사용해주세요.\n\n"
                    "👉 <b>🚨 특별관리결석자</b> 버튼을 사용하세요.",
                    parse_mode="HTML",
                )
                return
            # 🔧 이전 특별관리 컨텍스트 제거
            await clear_tmp(chat_id)
            await _show_church_menu(update, "abs")
            return
        if text == "🚨 특별관리결석자":
            # 🆕 1:1 개인방에선 특별관리 메뉴 차단 (그룹방 전용 기능)
            if is_private_chat(update):
                await update.message.reply_text(
                    "ℹ️ <b>특별관리결석자</b> 메뉴는 그룹방 전용입니다.\n\n"
                    "특별관리 대상자의 <b>대책방(그룹방)</b> 에서만\n"
                    "사용할 수 있습니다.",
                    parse_mode="HTML",
                )
                return
            # 🔧 이전 결석자 심방 컨텍스트 제거
            await clear_tmp(chat_id)
            # 🆕 v4.7: 이미 이 방에 등록된 특별관리 대상이 있으면 그 사람 정보로 바로 이동
            try:
                existing = await sb_get(
                    f"special_management_targets?select=name,dept,phone_last4"
                    f"&monitor_chat_id=eq.{chat_id}&limit=1"
                )
            except Exception:
                existing = []
            if existing:
                ex = existing[0]
                ex_name = ex.get("name", "")
                ex_dept = ex.get("dept", "")
                ex_phone = ex.get("phone_last4", "") or ""
                scope = await get_chat_scope(chat_id)
                ex_church = (scope or {}).get("church", "")
                await save_ctx(chat_id, tmp_sp_name=ex_name, tmp_sp_phone=ex_phone)
                await update.message.reply_text(
                    f"🚨 이 방은 이미 <b>{ex_name}</b>님의 전용 피드백방입니다.\n"
                    f"등록된 정보를 표시합니다:",
                    parse_mode="HTML"
                )
                await _show_sp_detail(update, chat_id, ex_church, ex_dept, ex_name, ex_phone, send_new=True)
                return
            # 등록된 사람 없으면 평소대로 선택 메뉴
            await _show_church_menu(update, "sp")
            return
        if text == "📘 사용법":
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
        import html as _html
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
        try:
            await update.message.reply_text(
                f"✅ <b>금주 {label_ko}</b> 저장됨: <code>{_html.escape(str(text))}</code>",
                parse_mode="HTML",
            )
        except Exception:
            await update.message.reply_text(f"✅ 금주 {label_ko} 저장됨: {text}")
        await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=True)
        return

    # 3) 일반 심방 입력 단계 (일반 또는 단일 편집 모드 'edit_<step>')
    is_single_edit = step.startswith("edit_")
    if is_single_edit:
        step = step[5:]  # 'edit_shepherd' → 'shepherd'

    if step in STEPS:
        # 🛡 검증 1: 리플라이 키보드 라벨이면 "입력 안 함"으로 간주하고 에러 안내
        RESERVED_LABELS = {
            "📋 결석자 심방", "🚨 특별관리결석자", "📘 사용법",
            "🏠 메인 메뉴", "📝 결석자 심방 기록 (폼)",
        }
        if text in RESERVED_LABELS:
            await update.message.reply_text(
                f"⚠️ 아직 *{md(STEP_LABELS[step])}* 를 입력하지 않으셨습니다.\n\n"
                f"현재 단계 입력을 먼저 완료해주세요.\n"
                f"중단하려면 ❌ 취소 버튼 또는 `/cancel`",
                parse_mode="Markdown",
                reply_markup=kb_cancel_only(),
            )
            return

        # 🛡 검증 2: 선택형 단계 (target/done/worship/attendance) 는 반드시 버튼으로만
        if step in STEP_CHOICES:
            valid_choices = []
            for row in STEP_CHOICES[step]:
                valid_choices.extend(row)
            if text not in valid_choices:
                rows = STEP_CHOICES[step]
                keyboard = [
                    [InlineKeyboardButton(c, callback_data=f"choice:{step}:{c}") for c in row]
                    for row in rows
                ]
                keyboard.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
                await update.message.reply_text(
                    f"⚠️ *{md(STEP_LABELS[step])}* 는 *아래 버튼 중 하나*를 선택해주세요.\n\n"
                    f"직접 입력된 값: `{md(text)}`\n"
                    f"허용 값: {', '.join(f'`{c}`' for c in valid_choices)}",
                    parse_mode="Markdown",
                    reply_markup=InlineKeyboardMarkup(keyboard),
                )
                return

        # 🛡 검증 3: 심방자 이름 형식 (너무 짧거나 이상한 값 거부)
        if step == "shepherd":
            if len(text) < 2:
                await update.message.reply_text(
                    f"⚠️ *심방자 이름*이 너무 짧습니다.\n\n"
                    f"예: `홍길동(집사)`, `김영희/구역장`, `박철수 목사`\n"
                    f"다시 입력해주세요:",
                    parse_mode="Markdown",
                    reply_markup=kb_cancel_only(),
                )
                return

        # 🛡 검증 4: 날짜 형식 (4/27, 2026-04-27, 4.27 등 허용)
        if step == "date":
            import re as _re
            patterns = [
                r"^\d{1,2}[/.\-]\d{1,2}$",                       # 4/27, 4.27, 4-27
                r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}$",              # 2026-04-27, 2026/4/27, 2026.4.27
                r"^\d{1,2}월\s*\d{1,2}일$",                       # 4월 27일
                r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$",             # 2026년 4월 26일
            ]
            if not any(_re.match(p, text) for p in patterns):
                await update.message.reply_text(
                    f"⚠️ *심방날짜 형식*이 올바르지 않습니다.\n\n"
                    f"허용되는 형식:\n"
                    f"• `4/27` 또는 `4-27` 또는 `4.27`\n"
                    f"• `2026-04-27` 또는 `2026/4/27` 또는 `2026.4.27`\n"
                    f"• `4월 27일`\n"
                    f"• `2026년 4월 27일`\n\n"
                    f"입력된 값: `{md(text)}`\n"
                    f"다시 입력해주세요:",
                    parse_mode="Markdown",
                    reply_markup=kb_cancel_only(),
                )
                return

        # 🛡 검증 5: 심방계획·진행사항은 너무 짧으면 거부 ("없음" 은 예외)
        if step == "plan":
            if len(text) < 3:
                await update.message.reply_text(
                    f"⚠️ *심방계획*이 너무 짧습니다 (3자 이상).\n\n"
                    f"예: `생일축하 겸 안부 방문`, `카페에서 말씀 나눔`\n"
                    f"다시 입력해주세요:",
                    parse_mode="Markdown",
                    reply_markup=kb_cancel_only(),
                )
                return
        if step == "note":
            if len(text) < 2 and text != "없음" and text != "-":
                await update.message.reply_text(
                    f"⚠️ *진행사항*이 너무 짧습니다.\n\n"
                    f"내용이 없으면 `없음` 이라고 입력해주세요.\n"
                    f"다시 입력해주세요:",
                    parse_mode="Markdown",
                    reply_markup=kb_cancel_only(),
                )
                return

        # ✅ 검증 통과 - 저장
        tmp_key = f"tmp_{step}"
        await save_ctx(chat_id, **{tmp_key: text})

        # 🔧 단일 편집 모드: DB에 바로 저장 + 수정 메뉴 복귀
        if is_single_edit:
            await _save_single_edit_and_show_menu(update, chat_id)
            return

        # 일반 흐름: 다음 단계로
        step_idx = STEPS.index(step)
        await _next_step(update, chat_id, step_idx, ctx)


async def _save_single_edit_and_show_menu(update, chat_id: int):
    """단일 항목 편집 완료 → DB 저장 + 수정 메뉴 재표시"""
    import html as _html
    ctx = await get_ctx(chat_id)
    if not ctx:
        return
    week_key = ctx.get("active_week_key", "")
    row_id = ctx.get("editing_row_id", "")
    if not week_key or not row_id:
        await update.message.reply_text("❌ 편집 대상 정보 없음. /menu")
        return

    try:
        await upsert_progress(week_key, row_id, ctx)
    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ 저장 실패: {str(e)[:200]}")
        return

    # editing_step 초기화
    await save_ctx(chat_id, editing_step="")

    # 최신 progress 다시 조회 후 메뉴 재표시
    prog = await get_progress(week_key, row_id)
    rows = await sb_get(
        f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}"
    )
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched:
            name = enriched[0].get("name", name) or name

    try:
        await update.message.reply_text(
            f"✅ <b>{_html.escape(str(name))}</b> — 수정 저장 완료",
            parse_mode="HTML",
        )
    except Exception:
        await update.message.reply_text(f"✅ {name} — 수정 저장 완료")

    # 수정 메뉴 재표시
    class FakeQ:
        message = update.message
    class FakeUpd:
        callback_query = FakeQ()
        effective_chat = update.effective_chat
        message = update.message
    if prog:
        await _show_edit_menu(FakeUpd(), chat_id, row_id, name, prog)


async def _on_abs_select(update: Update, chat_id: int, row_id: str):
    import html as _html
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
        enriched = await enrich_names(rows)
        name = enriched[0]["name"] if enriched else row_id
    else:
        name = row_id

    # 🔧 저장된 기록이 있고 모든 필드가 채워져 있으면 → 수정 메뉴 표시
    # 부분 기록이면 → 빠진 곳부터 이어서 입력
    has_record = bool(prog)
    all_filled = has_record and all(
        (prog.get(k) is not None and prog.get(k) != "")
        for k in ["shepherd", "visit_date_display", "plan_text"]
    )

    if all_filled:
        # 저장된 기록 있음 → 수정 메뉴
        await _show_edit_menu(update, chat_id, row_id, name, prog)
        return

    # 신규 기록 또는 부분 기록 → 빈 필드부터 이어서 입력
    # 다음 비어있는 필드 찾기
    start_step = "shepherd"
    if has_record:
        for s in STEPS:
            key_map = {
                "shepherd":"shepherd", "date":"visit_date_display", "plan":"plan_text",
                "target":"is_target", "done":"is_done", "worship":"attend_confirm",
                "note":"note"
            }
            dbkey = key_map.get(s, s)
            val = prog.get(dbkey)
            if val is None or val == "":
                start_step = s
                break
        else:
            start_step = "shepherd"  # 모두 채워진 경우는 위에서 처리됨

        # 기존 값들을 tmp_* 에 채워넣어서 저장 시 덮어쓰기 기반으로
        await save_ctx(
            chat_id,
            tmp_shepherd   = prog.get("shepherd", "") or "",
            tmp_date       = prog.get("visit_date_display", "") or "",
            tmp_plan       = prog.get("plan_text", "") or "",
            tmp_target     = "타겟" if prog.get("is_target") else ("미타겟" if prog.get("is_target") is False else ""),
            tmp_done       = "완료" if prog.get("is_done") else ("미완료" if prog.get("is_done") is False else ""),
            tmp_worship    = prog.get("attend_confirm", "") or "",
            tmp_note       = prog.get("note", "") or "",
        )

    await save_ctx(chat_id, editing_row_id=row_id, editing_step=start_step)

    existing = ""
    if has_record:
        existing = (
            f"\n\n📂 <b>기존 입력값</b>\n"
            f"심방자: {_html.escape(prog.get('shepherd','') or '없음')}\n"
            f"심방날짜: {_html.escape(prog.get('visit_date_display','') or '없음')}\n"
            f"심방계획: {_html.escape((prog.get('plan_text','') or '없음')[:50])}\n"
            f"<i>빠진 부분부터 이어서 입력하세요.</i>"
        )

    step_idx = STEPS.index(start_step) + 1
    try:
        await q.message.reply_text(
            f"✏️ <b>{_html.escape(str(name))}</b> 님 심방 기록{existing}\n\n"
            f"{step_idx}️⃣ {_html.escape(STEP_LABELS[start_step])}\n입력해주세요:\n\n"
            f"<i>중단하려면 ❌ 취소 버튼 또는 /cancel</i>",
            parse_mode="HTML",
            reply_markup=kb_cancel_only() if start_step not in STEP_CHOICES else _kb_choice(start_step),
        )
    except Exception as e:
        logger.warning("HTML parse 실패, 평문으로 전송: %s", e)
        await q.message.reply_text(
            f"✏️ {name} 님 심방 기록\n\n"
            f"{step_idx}️⃣ {STEP_LABELS[start_step]}\n입력해주세요:",
            reply_markup=kb_cancel_only() if start_step not in STEP_CHOICES else _kb_choice(start_step),
        )


def _kb_choice(step: str) -> InlineKeyboardMarkup:
    """선택형 단계의 인라인 버튼 생성"""
    rows = STEP_CHOICES.get(step, [])
    keyboard = [
        [InlineKeyboardButton(c, callback_data=f"choice:{step}:{c}") for c in row]
        for row in rows
    ]
    keyboard.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(keyboard)


async def _show_edit_menu(update, chat_id: int, row_id: str, name: str, prog: dict):
    """저장된 심방 기록의 수정 메뉴 (각 항목별 개별 수정)"""
    import html as _html
    q = update.callback_query

    # 현재 저장된 값 표시
    def fmt(v, true_label="✅", false_label="❌"):
        if v is None or v == "": return "<i>미입력</i>"
        if isinstance(v, bool): return true_label if v else false_label
        return _html.escape(str(v))

    text = (
        f"📝 <b>{_html.escape(str(name))}</b> 님 심방 기록 (저장됨)\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"① 심방자: {fmt(prog.get('shepherd'))}\n"
        f"② 심방날짜: {fmt(prog.get('visit_date_display'))}\n"
        f"③ 심방계획: {fmt(prog.get('plan_text'))}\n"
        f"④ 타겟여부: {fmt(prog.get('is_target'), '타겟', '미타겟')}\n"
        f"⑤ 진행여부: {fmt(prog.get('is_done'), '완료', '미완료')}\n"
        f"⑥ 예배확답: {fmt(prog.get('attend_confirm'))}\n"
        f"⑦ 진행사항: {fmt(prog.get('note'))}\n\n"
        f"<i>수정할 항목을 선택하세요 👇</i>"
    )

    # 항목별 수정 버튼
    buttons = [
        [InlineKeyboardButton("① 심방자 수정",   callback_data="edit_step:shepherd"),
         InlineKeyboardButton("② 심방날짜 수정", callback_data="edit_step:date")],
        [InlineKeyboardButton("③ 심방계획 수정", callback_data="edit_step:plan"),
         InlineKeyboardButton("④ 타겟여부 수정", callback_data="edit_step:target")],
        [InlineKeyboardButton("⑤ 진행여부 수정", callback_data="edit_step:done"),
         InlineKeyboardButton("⑥ 예배확답 수정", callback_data="edit_step:worship")],
        [InlineKeyboardButton("⑦ 진행사항 수정", callback_data="edit_step:note")],
        [InlineKeyboardButton("🔄 전체 다시 입력", callback_data=f"edit_full:{row_id}")],
        [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
    ]

    await save_ctx(chat_id, editing_row_id=row_id, editing_step="")

    try:
        await q.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("edit menu HTML 실패: %s", e)
        plain = (text.replace("<b>","").replace("</b>","")
                     .replace("<i>","").replace("</i>",""))
        await q.message.reply_text(plain, reply_markup=InlineKeyboardMarkup(buttons))


async def _on_edit_step(update: Update, chat_id: int, step: str):
    """개별 항목 수정 시작"""
    import html as _html
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await q.message.reply_text("❌ 세션 만료. /menu")
        return
    row_id = ctx.get("editing_row_id", "")
    if not row_id:
        await q.message.reply_text("❌ 편집 대상이 없습니다. /menu")
        return

    # 단일 항목만 수정 모드로 설정 ('edit_' 프리픽스로 구분)
    await save_ctx(chat_id, editing_step=f"edit_{step}", editing_row_id=row_id)
    # tmp_* 필드에 기존 값들 유지되도록 (이미 _on_abs_select 에서 채움) 확인
    ctx = await get_ctx(chat_id)
    tmp_key = f"tmp_{step}"
    if not ctx.get(tmp_key):
        # 기존 값 없으면 DB 에서 가져와서 채움
        week_key = ctx.get("active_week_key", "")
        prog = await get_progress(week_key, row_id)
        if prog:
            key_map = {
                "shepherd": prog.get("shepherd", ""),
                "date": prog.get("visit_date_display", ""),
                "plan": prog.get("plan_text", ""),
                "target": "타겟" if prog.get("is_target") else ("미타겟" if prog.get("is_target") is False else ""),
                "done": "완료" if prog.get("is_done") else ("미완료" if prog.get("is_done") is False else ""),
                "worship": prog.get("attend_confirm", ""),
                "note": prog.get("note", ""),
            }
            await save_ctx(chat_id, **{tmp_key: key_map.get(step, "") or ""})

    # 이름 조회
    rows = await sb_get(
        f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}"
    )
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched:
            name = enriched[0].get("name", name) or name

    label = STEP_LABELS.get(step, step)
    step_idx = STEPS.index(step) + 1

    if step in STEP_CHOICES:
        await q.message.reply_text(
            f"✏️ <b>{_html.escape(str(name))}</b> 님 — <b>{_html.escape(label)}</b>\n"
            f"{step_idx}번 항목만 수정합니다.\n\n"
            f"아래에서 선택하세요:",
            parse_mode="HTML",
            reply_markup=_kb_choice(step),
        )
    else:
        await q.message.reply_text(
            f"✏️ <b>{_html.escape(str(name))}</b> 님 — <b>{_html.escape(label)}</b>\n"
            f"{step_idx}번 항목만 수정합니다.\n\n"
            f"새 값을 입력해주세요 (취소: /cancel):",
            parse_mode="HTML",
            reply_markup=kb_cancel_only(),
        )


async def _on_edit_full(update: Update, chat_id: int, row_id: str):
    """전체 다시 입력 — 기존 _on_abs_select 의 순차 입력 로직 실행"""
    # tmp_* 초기화 후 shepherd 부터 시작
    await clear_tmp(chat_id)
    await save_ctx(chat_id, editing_row_id=row_id, editing_step="shepherd")

    import html as _html
    q = update.callback_query
    rows = await sb_get(
        f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}"
    )
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched:
            name = enriched[0].get("name", name) or name

    try:
        await q.message.reply_text(
            f"🔄 <b>{_html.escape(str(name))}</b> 님 심방 기록 — 전체 다시 입력\n\n"
            f"1️⃣ {_html.escape(STEP_LABELS['shepherd'])}\n입력해주세요:\n\n"
            f"<i>중단하려면 ❌ 취소 버튼 또는 /cancel</i>",
            parse_mode="HTML",
            reply_markup=kb_cancel_only(),
        )
    except Exception:
        await q.message.reply_text(
            f"🔄 {name} 님 심방 기록 — 전체 다시 입력\n\n"
            f"1️⃣ {STEP_LABELS['shepherd']}\n입력해주세요:",
            reply_markup=kb_cancel_only(),
        )


async def _on_choice(update: Update, chat_id: int, step: str, value: str):
    q = update.callback_query
    tmp_key = f"tmp_{step}"
    await save_ctx(chat_id, **{tmp_key: value})
    ctx = await get_ctx(chat_id)

    # 🔧 단일 편집 모드 체크
    current_editing = ctx.get("editing_step", "") or ""
    if current_editing == f"edit_{step}":
        # 단일 편집 → DB 바로 저장 + 메뉴 복귀
        class FakeUpd:
            message = q.message
            effective_chat = update.effective_chat
        await _save_single_edit_and_show_menu(FakeUpd(), chat_id)
        return

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
    import html as _html
    row_id = ctx.get("editing_row_id", "")
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    if rows:
        enriched = await enrich_names(rows)
        name = enriched[0]["name"] if enriched else row_id
    else:
        name = row_id

    def _e(v):
        return _html.escape(str(v)) if v else "-"

    summary = (
        f"📋 <b>심방 기록 확인</b> — {_e(name)}\n\n"
        f"심방자: {_e(ctx.get('tmp_shepherd',''))}\n"
        f"심방날짜: {_e(ctx.get('tmp_date',''))}\n"
        f"심방계획: {_e(ctx.get('tmp_plan',''))}\n"
        f"타겟여부: {_e(ctx.get('tmp_target',''))}\n"
        f"진행여부: {_e(ctx.get('tmp_done',''))}\n"
        f"예배확답: {_e(ctx.get('tmp_worship',''))}\n"
        f"진행사항: {_e(ctx.get('tmp_note',''))}\n\n"
        f"저장하시겠습니까?"
    )
    buttons = [[
        InlineKeyboardButton("✅ 저장", callback_data="confirm_save"),
        InlineKeyboardButton("❌ 취소", callback_data="cancel_save"),
    ]]
    try:
        await update.message.reply_text(summary, parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("HTML confirm 전송 실패, 평문으로 재시도: %s", e)
        plain_msg = (summary.replace("<b>","").replace("</b>",""))
        await update.message.reply_text(plain_msg,
            reply_markup=InlineKeyboardMarkup(buttons))


async def _do_save(update: Update, chat_id: int):
    import html as _html
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
        if rows:
            enriched = await enrich_names(rows)
            name = enriched[0]["name"] if enriched else row_id
        else:
            name = row_id
        try:
            await q.message.reply_text(
                f"✅ <b>{_html.escape(str(name))}</b> 님 심방 기록 저장 완료!\n\n"
                f"계속하려면 /menu",
                parse_mode="HTML",
            )
        except Exception as pe:
            logger.warning("HTML 완료 메시지 실패, 평문으로: %s", pe)
            await q.message.reply_text(
                f"✅ {name} 님 심방 기록 저장 완료!\n\n계속하려면 /menu"
            )
    except Exception as e:
        logger.exception(e)
        # 에러 메시지도 평문으로 (특수문자 포함 가능)
        try:
            err_txt = str(e)[:200]  # 너무 긴 에러 잘라내기
            await q.message.reply_text(f"❌ 저장 실패: {err_txt}")
        except Exception:
            await q.message.reply_text("❌ 저장 실패 (알 수 없는 오류)")


# ═════════════════════════════════════════════════════════════════════════════
# 특별관리 흐름
# ═════════════════════════════════════════════════════════════════════════════
async def _on_sp_church(update: Update, chat_id: int, church: str):
    import html as _html
    q = update.callback_query
    await save_ctx(chat_id, church_filter=church)
    txt = (
        f"🚨 <b>특별관리결석자</b>\n\n"
        f"✅ 교회: <b>{_html.escape(church)}</b>\n\n"
        f"② <b>부서</b> 를 선택하세요 👇"
    )
    try:
        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=kb_dept_select("sp", church))
    except Exception as e:
        logger.warning("sp_church edit 실패: %s", e)
        await q.message.reply_text(txt.replace("<b>","").replace("</b>",""),
                                   reply_markup=kb_dept_select("sp", church))


async def _on_sp_dept(update: Update, chat_id: int, church: str, dept: str):
    import html as _html
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.", reply_markup=await _kb_main(update))
        return

    targets = await fetch_absentees_4plus(week_key, church, dept)
    # 이름 마스킹 복구
    targets = await enrich_names(targets)
    if not targets:
        await q.edit_message_text(
            f"📭 <b>{_html.escape(church)} / {_html.escape(dept)}</b> 의 연속결석 4회 이상 결석자가 없습니다.\n"
            f"(주차: {_html.escape(week_label or week_key)})",
            parse_mode="HTML",
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
        if len(label) > 60:
            label = label[:57] + "..."
        buttons.append([InlineKeyboardButton(
            label,
            callback_data=f"sp_pk:{row_id}"
        )])
    buttons.append([InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"sp_ch:{church}")])
    buttons.append([InlineKeyboardButton("◀ 메인 메뉴",       callback_data="m:home")])

    overflow_note = f"\n\n<i>(+ {overflow}명은 화면 제한으로 생략 — 연속결석 순 상위 {MAX_TARGETS}명만 표시)</i>" if overflow > 0 else ""
    txt = (
        f"🚨 <b>{_html.escape(church)} / {_html.escape(dept)}</b> — 4회 이상 {len(targets)}명\n"
        f"주차: {_html.escape(week_label or week_key)}\n\n"
        f"🚨 = 특별관리 등록됨 (방 감지중)\n"
        f"⚠️ = 아직 미등록\n\n"
        f"관리할 결석자를 선택하세요 👇\n"
        f"<i>(선택 시 이 방이 감지방으로 등록됩니다)</i>"
        f"{overflow_note}"
    )
    try:
        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("sp_dept edit 실패: %s", e)
        plain = txt.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","")
        await q.message.reply_text(plain, reply_markup=InlineKeyboardMarkup(buttons))


async def _on_sp_pick(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    """특별관리 대상 선택 → 방 감지 등록 + 상세 화면.
    
    🛡 v4.7: 그룹방당 1명만 등록 가능. 이미 등록된 사람 있으면 차단.
    """
    import html as _html
    q = update.callback_query
    chat = update.effective_chat

    # 🛡 v4.7: 이 방에 이미 등록된 특별관리 대상이 있는지 체크
    try:
        existing = await sb_get(
            f"special_management_targets?select=name,dept,phone_last4"
            f"&monitor_chat_id=eq.{chat.id}"
            f"&limit=2"
        )
    except Exception as e:
        logger.warning("기존 특별관리 조회 실패: %s", e)
        existing = []

    # 같은 사람이면 그냥 상세 화면으로 (재진입)
    is_same = False
    if existing:
        for ex in existing:
            if (ex.get("dept") == dept and ex.get("name") == name 
                and (ex.get("phone_last4") or "") == (phone or "")):
                is_same = True
                break

    # 다른 사람이 이미 등록되어 있으면 차단
    if existing and not is_same:
        ex = existing[0]
        ex_name = ex.get("name", "?")
        ex_dept = ex.get("dept", "?")
        try:
            await q.edit_message_text(
                f"⛔ <b>이 방은 이미 특별관리 대상자가 지정되어 있습니다.</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📌 등록된 대상자: <b>{_html.escape(str(ex_name))}</b>\n"
                f"📌 부서: {_html.escape(str(ex_dept))}\n\n"
                f"<b>그룹방당 한 명만 특별관리 가능합니다.</b>\n"
                f"이 방은 <b>{_html.escape(str(ex_name))}</b>님의 전용 피드백방으로 운영됩니다.\n\n"
                f"⚠️ 다른 분을 특별관리 하려면:\n"
                f"   1. 새 그룹방을 만들거나\n"
                f"   2. 기존 대상자를 [🗑 특별관리 해제] 후 새로 등록",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"📋 {ex_name} 님 정보 보기", callback_data="sp_show_current")],
                    [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
                ]),
            )
        except Exception as e:
            logger.warning("sp_pick block 메시지 실패: %s", e)
            await q.message.reply_text(
                f"⛔ 이 방은 이미 {ex_name}님이 특별관리 대상으로 등록되어 있습니다.\n"
                f"그룹방당 1명만 가능합니다."
            )
        return

    # 결석자 정보 (이름 마스킹 복구)
    rows = await sb_get(
        f"weekly_visit_targets?select=name,region_name,zone_name,church,dept,phone_last4"
        f"&dept=eq.{quote(dept)}&name=eq.{quote(name)}"
        + (f"&phone_last4=eq.{quote(phone)}" if phone else "")
        + "&limit=1"
    )
    if rows:
        enriched = await enrich_names(rows)
        if enriched:
            name = enriched[0].get("name", name) or name
    region = rows[0].get("region_name","") if rows else ""
    zone   = rows[0].get("zone_name","")   if rows else ""

    # 방 감지 등록 (같은 사람이면 update, 처음이면 insert)
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

    # 🆕 v6.0: 등록자를 이 방의 owner 로 자동 등록
    #   목적 — 다른 사람이 이 방에서 먼저 봇 메시지를 보내도 권한이 안 옮겨감
    #   등록자(update.effective_user)가 변은지 → 변은지가 owner 로 즉시 등록됨
    try:
        registrant = update.effective_user
        if registrant and _chat_allowed_users_table_exists:
            registrant_name = registrant.full_name or registrant.username or f"user_{registrant.id}"
            await sb_rpc("upsert_chat_allowed_user", {
                "p_chat_id":   chat.id,
                "p_user_id":   registrant.id,
                "p_user_name": registrant_name,
                "p_is_owner":  True,
                "p_added_by":  registrant.id,
            })
            logger.info(
                "👑 [특별관리 owner 자동등록] chat=%s user=%s(%s) name=%s",
                chat.id, registrant.id, registrant_name, name
            )
    except Exception as e:
        # owner 자동 등록 실패는 등록 자체를 막지 않음 (관리자 페이지에서 수동 변경 가능)
        logger.warning("특별관리 owner 자동등록 실패 chat=%s: %s", chat.id, e)

    if is_same:
        # 재진입
        await save_ctx(chat_id, tmp_sp_name=name, tmp_sp_phone=phone)
        await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=False)
        return

    try:
        await q.edit_message_text(
            f"✅ <b>{_html.escape(str(name))}</b> 님을 <b>특별관리 대상</b>으로 등록했습니다.\n"
            f"이 방은 이제 <b>{_html.escape(str(name))}</b>님 전용 피드백방입니다.\n\n"
            f"매주 수요일 08:00 KST 에 "
            f"미체크 항목 리마인더가 이 방으로 발송됩니다.",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("sp_pick edit 실패: %s", e)
        await q.message.reply_text(
            f"✅ {name} 님을 특별관리 대상으로 등록했습니다.\n"
            f"이 방은 {name}님 전용 피드백방입니다.\n"
            f"매주 수요일 08:00 KST 에 리마인더가 발송됩니다."
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
    """특별관리 대상 상세 + 4항목 체크리스트 (HTML parse_mode)"""
    import html as _html
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
        f"🚨 <b>특별관리 대상자 {_html.escape(str(name))}님 피드백방</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 {_html.escape(church)} / {_html.escape(dept)} / {_html.escape(region)} {_html.escape(zone)}\n\n"
        f"<i>이 그룹방은 그룹방이 삭제될 때까지 <b>{_html.escape(str(name))}</b>님 한 분을 위한 피드백 방입니다.</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'✅' if item1 else '⬜️'} <b>1. 대책방 초대완료</b>\n"
        f"   (구역장·인섬교·강사·전도사·심방부사명자)\n"
        f"   <i>최초 1회만 체크 (주간 리셋 안 됨)</i>\n\n"
        f"{'✅' if item2 else '⬜️'} <b>2. 금주 피드백 진행</b>\n"
        f"   <i>매주 수요일 08시 초기화</i>\n\n"
        f"📅 <b>3. 금주 심방예정일:</b> {_html.escape(str(item3)) if item3 else '<i>미입력</i>'}\n\n"
        f"📝 <b>4. 금주 심방계획:</b> {_html.escape(str(item4)) if item4 else '<i>미입력</i>'}"
    )

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

    async def _send_with_fallback(send_fn_html, send_fn_plain):
        try:
            await send_fn_html()
        except Exception as e:
            logger.warning("sp_detail HTML 실패, 평문으로: %s", e)
            try:
                await send_fn_plain()
            except Exception as e2:
                logger.exception("sp_detail 평문도 실패: %s", e2)

    plain_text = (text.replace("<b>","").replace("</b>","")
                      .replace("<i>","").replace("</i>",""))

    if send_new:
        target = update.message if hasattr(update, 'message') and update.message else (
            update.callback_query.message if update.callback_query else None
        )
        if target:
            await _send_with_fallback(
                lambda: target.reply_text(text, parse_mode="HTML", reply_markup=kb),
                lambda: target.reply_text(plain_text, reply_markup=kb),
            )
    else:
        q = update.callback_query
        if q:
            await _send_with_fallback(
                lambda: q.edit_message_text(text, parse_mode="HTML", reply_markup=kb),
                lambda: q.message.reply_text(plain_text, reply_markup=kb),
            )


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
    import html as _html
    q = update.callback_query
    step = "awaiting_sp3" if which == "3" else "awaiting_sp4"
    await save_ctx(chat_id,
        church_filter=church, dept_filter=dept,
        tmp_sp_name=name, tmp_sp_phone=phone,
        editing_step=step,
    )
    label = "금주 심방예정일" if which == "3" else "금주 심방계획"
    try:
        await q.message.reply_text(
            f"✏️ <b>{_html.escape(str(name))}</b> 님의 <b>{label}</b> 을 입력해주세요:\n\n"
            f"<i>취소하려면 /cancel</i>",
            parse_mode="HTML",
            reply_markup=kb_cancel_only(),
        )
    except Exception:
        await q.message.reply_text(
            f"✏️ {name} 님의 {label} 을 입력해주세요:\n\n취소하려면 /cancel",
            reply_markup=kb_cancel_only(),
        )


async def _on_sp_unregister(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    import html as _html
    try:
        await sb_rpc("unregister_special_management", {
            "p_dept": dept, "p_name": name, "p_phone_last4": phone
        })
    except Exception as e:
        await update.callback_query.message.reply_text(f"❌ 해제 실패: {e}")
        return
    q = update.callback_query
    try:
        await q.edit_message_text(
            f"🗑 <b>{_html.escape(str(name))}</b> 님을 특별관리에서 해제했습니다.",
            parse_mode="HTML",
            reply_markup=await _kb_main(update),
        )
    except Exception:
        await q.edit_message_text(
            f"🗑 {name} 님을 특별관리에서 해제했습니다.",
            reply_markup=await _kb_main(update),
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
# 매주 수요일 08시 KST — 주간 리마인더 + 2/3/4번 리셋
# ═════════════════════════════════════════════════════════════════════════════
async def weekly_reminder_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    """🛡 v4.7: 중복 실행 방지."""
    acquired = await try_acquire_job_lock("special_reminder", source)
    if not acquired:
        logger.info("⏭ special_reminder: 오늘 이미 실행됨 (source=%s, 스킵)", source)
        return

    logger.info("🔔 weekly_reminder_job start (source=%s)", source)
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
                # 🆕 메시지 간소화 — 이름·부서·지역만
                msg = (
                    f"📋 *체크리스트 — 미진행 항목*\n"
                    f"👤 *{md(name)}* ({md(dept)})\n\n"
                    + "\n".join(unchecked) +
                    f"\n\n_/menu → 🚨 특별관리결석자 에서 입력_"
                )
            else:
                msg = (
                    f"✅ *모든 항목 완료*\n"
                    f"👤 {md(name)} ({md(dept)})\n\n"
                    f"수고하셨습니다! _(2~4번은 곧 초기화됩니다)_"
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
    await update.message.reply_text("🔔 주간 리마인더 강제 실행 중 (lock 무시)...")
    await weekly_reminder_job(context, source="manual_test")
    await update.message.reply_text("✅ 완료 (이미 발송됐으면 스킵)")


# ═════════════════════════════════════════════════════════════════════════════
# 🆕 매주 수요일 08시 KST — 모든 봇 방에 타겟 결석자 심방계획 요청
# ═════════════════════════════════════════════════════════════════════════════
async def wednesday_visit_plan_request_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    """
    매주 수요일 08시 KST에 실행.
    봇이 등록된 모든 방(telegram_chat_scope) 에 현재 주차 결석자 요약 + 심방계획 요청.
    
    🛡 v4.7: 같은 날 중복 실행 방지 (Cloud Scheduler + JobQueue 동시 실행 안전).
    """
    # 🛡 중복 실행 체크
    acquired = await try_acquire_job_lock("weekly_visit_plan", source)
    if not acquired:
        logger.info("⏭ weekly_visit_plan: 오늘 이미 실행됨 (source=%s, 스킵)", source)
        return

    logger.info("📅 wednesday_visit_plan_request_job start (source=%s)", source)
    try:
        # 1) 현재 주차
        week_key, week_label = await get_active_week()
        if not week_key:
            logger.warning("활성 주차 없음, 수요일 알림 스킵")
            return

        # 2) 봇이 등록된 모든 방 목록
        try:
            scopes = await sb_get(
                "telegram_chat_scope?select=chat_id,chat_title,church,dept,region_name,zone_name&limit=2000"
            ) or []
        except Exception as e:
            logger.exception("scope 목록 로드 실패: %s", e)
            return

        if not scopes:
            logger.info("등록된 방이 없음")
            return

        logger.info("수요일 알림 대상 방: %d개", len(scopes))
        sent = 0
        failed = 0

        for s in scopes:
            chat_id = s.get("chat_id")
            if not chat_id:
                continue

            # 🛡 승인된 방만 알림 발송
            if not await is_chat_authorized(chat_id):
                logger.info("수요일 알림 스킵 (미승인 방): %s", chat_id)
                continue

            church = s.get("church", "")
            dept   = s.get("dept", "")
            region = s.get("region_name", "")
            zone   = s.get("zone_name", "")

            if not church:
                continue  # 교회 없는 방은 스킵

            # 🆕 메시지 간소화: 결석자 현황·목록 조회 제거
            #     이 방이 특별관리 대책방인지에 따라 메시지 분기
            try:
                import html as _html
                scope_txt = " / ".join([x for x in [church, dept, region, zone] if x])
                week_label_safe = _html.escape(week_label or week_key)

                # 🆕 특별관리 대책방 여부 + 대상자 정보
                sp_target = None
                try:
                    sp_rows = await sb_get(
                        f"special_management_targets?select=name,dept,phone_last4,monitor_chat_id"
                        f"&monitor_chat_id=eq.{chat_id}&limit=1"
                    ) or []
                    if sp_rows:
                        sp_target = sp_rows[0]
                except Exception as e:
                    logger.warning("특별관리 조회 실패 chat=%s: %s", chat_id, e)

                if sp_target:
                    # 🚨 특별관리 대책방 — 피드백/심방날짜/심방계획 안내
                    sp_name = _html.escape(sp_target.get("name", "?"))
                    msg = (
                        f"🚨 <b>수요일 알림 — 특별관리 대책방</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"📌 대상자: <b>{sp_name}</b>\n"
                        f"📅 주차: <b>{week_label_safe}</b>\n\n"
                        f"<b>🙏 이번 주 다음 항목을 진행해주세요:</b>\n"
                        f"1️⃣ <b>금주의 피드백</b> 진행\n"
                        f"2️⃣ <b>심방예정일</b> 정하기\n"
                        f"3️⃣ <b>심방계획</b> 세우기\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"하단 <code>🚨 특별관리결석자</code> 버튼으로 입력하세요."
                    )
                else:
                    # 📋 일반 그룹방 / 개인방 — 간단 안내
                    msg = (
                        f"📋 <b>수요일 심방계획 요청</b>\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n\n"
                        f"📌 담당: <b>{_html.escape(scope_txt)}</b>\n"
                        f"📅 주차: <b>{week_label_safe}</b>\n\n"
                        f"<b>🙏 주일까지 다음 작업을 부탁드립니다:</b>\n"
                        f"1️⃣ 결석자 중 <b>타겟 대상 선정</b>\n"
                        f"2️⃣ 각 타겟에 대한 <b>심방계획 작성</b>\n"
                        f"3️⃣ <b>심방 실행 &amp; 기록 업데이트</b>\n\n"
                        f"━━━━━━━━━━━━━━━━━━━━\n"
                        f"하단 <code>📋 결석자 심방</code> 버튼으로 시작하세요."
                    )

                # 5) 전송 (HTML → fallback to plain)
                # 🆕 v6.0: 사용자 요청 — 수요일 알람에는 대시보드 링크 표시 안 함
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML")
                    sent += 1
                except Exception as e1:
                    logger.warning("HTML send 실패 chat_id=%s: %s", chat_id, e1)
                    try:
                        # 평문 재시도 (HTML 태그 제거)
                        plain = (msg.replace("<b>", "").replace("</b>", "")
                                   .replace("<i>", "").replace("</i>", "")
                                   .replace("<code>", "").replace("</code>", ""))
                        await context.bot.send_message(chat_id=chat_id, text=plain)
                        sent += 1
                    except Exception as e2:
                        logger.warning("평문 send 실패 chat_id=%s: %s", chat_id, e2)
                        failed += 1

            except Exception as e:
                logger.exception("방 %s 처리 실패: %s", chat_id, e)
                failed += 1

        logger.info("📅 수요일 알림 완료: 전송 %d / 실패 %d / 총 %d", sent, failed, len(scopes))
    except Exception as e:
        logger.exception("wednesday_visit_plan_request_job failed: %s", e)


async def force_wednesday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """수요일 알림 강제 실행 (테스트용 - 중복 lock 무시)"""
    await update.message.reply_text("📅 수요일 심방계획 요청 강제 실행 중 (lock 무시)...")
    # 강제 실행 시 source='manual_test' → 어차피 같은 날 lock 있으면 스킵됨
    # 테스트시 매번 발송하려면 lock 테이블에서 오늘 row 삭제 필요
    await wednesday_visit_plan_request_job(context, source="manual_test")
    await update.message.reply_text("✅ 완료 (이미 발송됐으면 스킵)")


async def weekly_rollover_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    """매주 수요일 00:00 KST — 주차 자동 전환.
    
    weekly_target_weeks 에 다음주 entry 가 없으면 자동 추가.
    🛡 v4.7: 중복 실행 방지.
    """
    acquired = await try_acquire_job_lock("weekly_rollover", source)
    if not acquired:
        logger.info("⏭ weekly_rollover: 오늘 이미 실행됨 (source=%s, 스킵)", source)
        return

    logger.info("📅 weekly_rollover_job start (source=%s)", source)
    try:
        week_key, week_label = compute_target_week_key()
        # 이미 존재하는지 체크
        existing = await sb_get(
            f"weekly_target_weeks?select=week_key&week_key=eq.{quote(week_key)}&limit=1"
        )
        if existing:
            logger.info("주차 %s 이미 존재", week_key)
            return
        # 새 주차 등록
        await sb_post("weekly_target_weeks", {
            "week_key": week_key,
            "week_label": week_label,
        })
        logger.info("✅ 주차 자동 전환 완료: %s (%s)", week_key, week_label)
    except Exception as e:
        logger.exception("주차 자동 전환 실패: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# 🆕 편의 명령어: /chatid, /approve, /deny + 승인 신청 버튼
# ═════════════════════════════════════════════════════════════════════════════
async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """어느 방에서든 이 방의 Chat ID 표시."""
    import html as _html
    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    chat_title = chat.title or chat.full_name or "(제목없음)"
    chat_type = {
        "private": "개인채팅",
        "group": "일반 그룹",
        "supergroup": "슈퍼그룹",
        "channel": "채널",
    }.get(chat.type, chat.type)

    authorized = True
    if chat.type != "private":
        authorized = await is_chat_authorized(chat_id)

    auth_badge = "✅ 승인됨" if authorized else "❌ 미승인"
    user_line = ""
    if user:
        user_line = f"• 내 User ID: <code>{user.id}</code>\n"

    msg = (
        "📋 <b>방 정보</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {_html.escape(chat_title)}\n"
        f"• 방 유형: {chat_type}\n"
        f"• 승인 상태: {auth_badge}\n"
        f"{user_line}"
    )
    if not authorized and chat.type != "private":
        msg += "\n\n👇 아래 버튼으로 관리자에게 승인 신청하기"
        await update.message.reply_text(
            msg, parse_mode="HTML", reply_markup=kb_request_approval()
        )
    else:
        await update.message.reply_text(msg, parse_mode="HTML")


async def allow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 특별관리 대책방에서 owner 가 다른 사용자를 화이트리스트에 추가.
    
    사용법:
      1. 추가할 사람의 메시지를 인용(답장)하고 → /allow
      2. /allow (단독) → 안내 메시지
    """
    import html as _html
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return

    chat_id = chat.id

    # 개인방에선 의미 없음
    if chat.type == "private":
        await update.message.reply_text(
            "ℹ️ <code>/allow</code> 명령은 그룹방 전용입니다.\n"
            "특별관리 대책방에서 사용하세요.",
            parse_mode="HTML",
        )
        return

    # 특별관리 대책방인지 체크
    if not await is_special_monitor_chat(chat_id):
        await update.message.reply_text(
            "ℹ️ 이 방은 특별관리 대책방이 아니어서\n"
            "<code>/allow</code> 명령은 효과가 없습니다.\n"
            "(일반 그룹방은 누구나 봇을 사용할 수 있습니다.)",
            parse_mode="HTML",
        )
        return

    # 승인된 방인지 체크
    if not await is_chat_authorized(chat_id):
        await update.message.reply_text(
            "❌ 이 방은 아직 봇 사용 승인이 안 된 방입니다.\n"
            "<code>/start</code> 로 먼저 승인 신청을 해주세요.",
            parse_mode="HTML",
        )
        return

    # 권한 체크 — owner 또는 봇 관리자만
    is_admin = await is_bot_admin_user(user.id)
    allowed_list = await list_chat_allowed_users(chat_id)
    is_owner = any(
        int(r.get("user_id", 0)) == int(user.id) and bool(r.get("is_owner"))
        for r in allowed_list
    )

    if not (is_owner or is_admin):
        await update.message.reply_text(
            "❌ <b>권한 없음</b>\n\n"
            "이 방의 봇 사용자 추가는 <b>owner</b> 또는 <b>봇 관리자</b>만 가능합니다.\n"
            "<code>/allowed</code> 명령으로 현재 owner 를 확인할 수 있습니다.",
            parse_mode="HTML",
        )
        return

    # reply 대상 추출
    reply_msg = update.message.reply_to_message
    if not reply_msg or not reply_msg.from_user:
        # 🆕 v6.0: text 인자로 user_id 직접 받기
        #   사용법: /allow 123456789  또는  /allow 123456789 박정호
        args = (update.message.text or '').split()[1:]
        if args and args[0].isdigit():
            target_uid = int(args[0])
            target_name = ' '.join(args[1:]) if len(args) > 1 else f'user_{target_uid}'
            try:
                await sb_rpc("upsert_chat_allowed_user", {
                    "p_chat_id": chat_id,
                    "p_user_id": target_uid,
                    "p_user_name": target_name,
                    "p_is_owner": False,
                    "p_added_by": user.id,
                })
                await update.message.reply_text(
                    f"✅ <b>{_html.escape(target_name)}</b> 님 (user_id: <code>{target_uid}</code>) 을\n"
                    f"이 방의 봇 사용자로 추가했습니다.\n\n"
                    f"이제 그 분이 이 방에서 봇 명령을 사용할 수 있습니다.",
                    parse_mode="HTML",
                )
                return
            except Exception as e:
                logger.exception("allow by user_id failed: %s", e)
                await update.message.reply_text(
                    f"❌ 추가 실패: {_html.escape(str(e))}",
                    parse_mode="HTML",
                )
                return

        await update.message.reply_text(
            "ℹ️ <b>사용법 (두 가지 방법)</b>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<b>방법 1.</b> 답장으로 추가 (권장)\n"
            "1️⃣ 추가할 분이 <b>이 그룹방에서</b> 메시지를 한 번 보내야 합니다.\n"
            "   <i>(외부 카톡·문자 스크린샷은 안 됩니다 — 텔레그램이 user_id 를 모름)</i>\n"
            "2️⃣ 그 분의 텔레그램 메시지를 길게 누름 → <b>답장(Reply)</b>\n"
            "3️⃣ <code>/allow</code> 입력\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<b>방법 2.</b> user_id 로 직접 추가\n"
            "<code>/allow [user_id]</code> 또는 <code>/allow [user_id] [이름]</code>\n"
            "예: <code>/allow 123456789 박정호</code>\n"
            "<i>(user_id 는 그 분이 봇과 1대1 대화 한 번 후 확인 가능)</i>\n\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "<i>현재 등록된 사용자: <code>/allowed</code></i>",
            parse_mode="HTML",
        )
        return

    target = reply_msg.from_user
    if target.is_bot:
        await update.message.reply_text("❌ 봇은 추가할 수 없습니다.")
        return

    target_name = target.full_name or target.username or f"user_{target.id}"
    ok = await add_chat_allowed_user(
        chat_id=chat_id,
        user_id=target.id,
        user_name=target_name,
        is_owner=False,
        added_by=user.id,
    )

    if ok:
        # 안내 메시지 캐시 클리어 (그 사람이 다시 명령 보낼 때 정상 작동)
        _unallowed_notified.discard((chat_id, target.id))
        await update.message.reply_text(
            f"✅ <b>{_html.escape(target_name)}</b> 님이\n"
            f"이 방의 봇 사용자로 등록되었습니다.\n\n"
            f"이제 이 분도 봇을 사용할 수 있습니다.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text(
            "❌ 등록 실패. 잠시 후 다시 시도해주세요.\n"
            "(DB 마이그레이션이 적용됐는지 관리자에게 문의)",
            parse_mode="HTML",
        )


async def disallow_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 owner 가 화이트리스트에서 사용자 제거.
    사용법: 제거할 사람 메시지에 reply 후 /disallow
    """
    import html as _html
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    chat_id = chat.id

    if chat.type == "private":
        await update.message.reply_text(
            "ℹ️ <code>/disallow</code> 명령은 그룹방 전용입니다.",
            parse_mode="HTML",
        )
        return

    if not await is_special_monitor_chat(chat_id):
        await update.message.reply_text(
            "ℹ️ 이 방은 특별관리 대책방이 아닙니다.",
            parse_mode="HTML",
        )
        return

    is_admin = await is_bot_admin_user(user.id)
    allowed_list = await list_chat_allowed_users(chat_id)
    is_owner = any(
        int(r.get("user_id", 0)) == int(user.id) and bool(r.get("is_owner"))
        for r in allowed_list
    )
    if not (is_owner or is_admin):
        await update.message.reply_text(
            "❌ owner 또는 봇 관리자만 제거할 수 있습니다.",
            parse_mode="HTML",
        )
        return

    reply_msg = update.message.reply_to_message
    if not reply_msg or not reply_msg.from_user:
        await update.message.reply_text(
            "ℹ️ 제거할 분의 메시지에 답장(reply)하며 <code>/disallow</code> 입력하세요.",
            parse_mode="HTML",
        )
        return

    target = reply_msg.from_user
    target_name = target.full_name or target.username or f"user_{target.id}"

    # owner 가 자기 자신을 제거하는 건 막음
    if int(target.id) == int(user.id):
        await update.message.reply_text(
            "❌ owner 본인은 제거할 수 없습니다.\n"
            "owner 변경은 관리자 페이지에서 가능합니다.",
            parse_mode="HTML",
        )
        return

    ok = await remove_chat_allowed_user(chat_id, target.id)
    if ok:
        await update.message.reply_text(
            f"✅ <b>{_html.escape(target_name)}</b> 님을 봇 사용자에서 제거했습니다.",
            parse_mode="HTML",
        )
    else:
        await update.message.reply_text("❌ 제거 실패. 잠시 후 다시 시도해주세요.")


async def allowed_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 이 방의 화이트리스트 사용자 목록 표시 (user_id 포함, 비활성도 표시)."""
    import html as _html
    chat = update.effective_chat
    if not chat:
        return
    chat_id = chat.id

    if chat.type == "private":
        await update.message.reply_text(
            "ℹ️ <code>/allowed</code> 명령은 그룹방 전용입니다.",
            parse_mode="HTML",
        )
        return

    if not await is_special_monitor_chat(chat_id):
        await update.message.reply_text(
            "ℹ️ 이 방은 특별관리 대책방이 아니라\n"
            "사용자 제한이 없습니다 (방 멤버 모두 사용 가능).",
            parse_mode="HTML",
        )
        return

    # 🆕 v6.0: 활성 + 비활성 모두 가져오기 (user_id 보여주기 위함)
    try:
        all_rows = await sb_get(
            f"chat_allowed_users?select=user_id,user_name,is_owner,is_active,added_at"
            f"&chat_id=eq.{chat_id}&order=is_active.desc,is_owner.desc,added_at.asc"
        ) or []
    except Exception as e:
        logger.warning("list all users 실패: %s", e)
        all_rows = []

    if not all_rows:
        await update.message.reply_text(
            "📋 이 방에서 봇과 상호작용한 사람이 아직 없습니다.\n\n"
            "💡 <b>tip</b>: 추가하려는 분이 이 그룹방에서 메시지를 한 번 보내면\n"
            "이 목록에 나타나며, 답장 + <code>/allow</code> 또는\n"
            "<code>/allow [user_id]</code> 로 추가할 수 있습니다.",
            parse_mode="HTML",
        )
        return

    lines = ["🛡 <b>이 방에서 봇과 상호작용한 사람들</b>"]
    lines.append("━" * 14)

    active_rows = [r for r in all_rows if r.get('is_active')]
    inactive_rows = [r for r in all_rows if not r.get('is_active')]

    if active_rows:
        lines.append("✅ <b>활성 (봇 사용 가능)</b>")
        for r in active_rows:
            name = _html.escape(r.get("user_name") or f"user_{r.get('user_id')}")
            crown = "👑" if r.get("is_owner") else "  "
            uid = r.get('user_id')
            lines.append(f"{crown} {name}  <code>{uid}</code>")

    if inactive_rows:
        lines.append("")
        lines.append("⚪ <b>비활성 (한때 등록됐던 사람들 — 다시 추가 가능)</b>")
        for r in inactive_rows:
            name = _html.escape(r.get("user_name") or f"user_{r.get('user_id')}")
            uid = r.get('user_id')
            lines.append(f"   {name}  <code>{uid}</code>")

    lines.append("")
    lines.append("<i>👑 = owner (다른 사용자 추가/제거 가능)</i>")
    lines.append("<i>숫자 = user_id (탭하면 복사됨)</i>")
    lines.append("")
    lines.append("<b>💡 사용자 추가 방법:</b>")
    lines.append("• 답장 방식: 추가할 분 메시지에 답장 + <code>/allow</code>")
    lines.append("• ID 방식: <code>/allow [user_id] [이름]</code>")
    lines.append("<i>제거: 제거할 분 메시지에 답장 + <code>/disallow</code></i>")
    await update.message.reply_text("\n".join(lines), parse_mode="HTML")


async def members_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 v6.0: /members — /allowed 별칭. 그룹방의 사용자 목록 + user_id"""
    await allowed_command(update, context)



async def prereq_church_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 v5.8: 승인 전 교회 선택 → 부서 선택 키보드 표시"""
    q = update.callback_query
    await q.answer()
    
    data = q.data  # "prereq_church:서울교회"
    church = data.split(":", 1)[1] if ":" in data else ""
    if not church:
        await q.edit_message_text("❌ 잘못된 선택입니다.")
        return

    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    
    msg = (
        "🔒 <b>승인되지 않은 방입니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {chat_title}\n\n"
        f"✅ <b>1단계: 소속 교회</b> → {church}\n\n"
        f"👇 <b>2단계: 소속 부서를 선택해주세요</b>"
    )
    
    try:
        await q.edit_message_text(
            msg,
            parse_mode="HTML",
            reply_markup=kb_pre_approval_dept(church),
        )
    except Exception as e:
        logger.warning("prereq_church_callback edit 실패: %s", e)


async def prereq_dept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 v5.8: 승인 전 부서 선택 → scope 저장 + 최종 승인 신청 키보드"""
    q = update.callback_query
    await q.answer()
    
    data = q.data  # "prereq_dept:서울교회:청년회"
    parts = data.split(":")
    if len(parts) < 3:
        await q.edit_message_text("❌ 잘못된 선택입니다.")
        return
    church = parts[1]
    dept = parts[2]
    
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    user = update.effective_user
    owner_name = (user.full_name if user else "") or (user.username if user else "")
    
    # 🆕 scope 저장 (지역/구역은 나중에 승인 후 설정)
    saved = await save_chat_scope(
        chat_id=chat_id,
        chat_title=chat_title,
        church=church,
        dept=dept,
        region_name=None,
        zone_name=None,
        owner_user_id=user.id if user else None,
        owner_name=owner_name,
    )
    
    if not saved:
        await q.edit_message_text(
            "❌ scope 저장 실패. 다시 시도해주세요.",
            reply_markup=kb_pre_approval_church(),
        )
        return
    
    msg = (
        "🔒 <b>승인되지 않은 방입니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {chat_title}\n\n"
        f"✅ <b>1단계: 소속 교회</b> → {church}\n"
        f"✅ <b>2단계: 소속 부서</b> → {dept}\n\n"
        f"👇 <b>3단계: 아래 [🙏 승인 신청하기] 버튼을 누르면</b>\n"
        f"   <i>{church} {dept} 관리자</i> 에게 자동 알림이 전달됩니다."
    )
    
    try:
        await q.edit_message_text(
            msg,
            parse_mode="HTML",
            reply_markup=kb_final_approval(church, dept),
        )
    except Exception as e:
        logger.warning("prereq_dept_callback edit 실패: %s", e)


async def prereq_back_church_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 v5.8: 부서 선택 단계에서 ⬅️ 누르면 교회 선택으로 복귀"""
    q = update.callback_query
    await q.answer()
    
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    
    try:
        await q.edit_message_text(
            unauthorized_message(chat_id, chat_title),
            parse_mode="HTML",
            reply_markup=kb_pre_approval_church(),
        )
    except Exception as e:
        logger.warning("prereq_back_church 실패: %s", e)


async def prereq_back_dept_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """🆕 v5.8: 최종 단계에서 ⬅️ 누르면 부서 선택으로 복귀"""
    q = update.callback_query
    await q.answer()
    
    data = q.data  # "prereq_back_dept:서울교회"
    church = data.split(":", 1)[1] if ":" in data else ""
    if not church:
        # 교회 정보 없으면 처음부터
        await prereq_back_church_callback(update, context)
        return
    
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    
    msg = (
        "🔒 <b>승인되지 않은 방입니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {chat_title}\n\n"
        f"✅ <b>1단계: 소속 교회</b> → {church}\n\n"
        f"👇 <b>2단계: 소속 부서를 선택해주세요</b>"
    )
    
    try:
        await q.edit_message_text(
            msg,
            parse_mode="HTML",
            reply_markup=kb_pre_approval_dept(church),
        )
    except Exception as e:
        logger.warning("prereq_back_dept 실패: %s", e)


async def request_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """승인 신청 버튼 눌림 → 관리자들에게 DM 알림."""
    import html as _html
    q = update.callback_query
    await q.answer()

    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    chat_title = chat.title or chat.full_name or "(제목없음)"
    requester_name = user.full_name if user else "(알 수 없음)"
    requester_id = user.id if user else 0

    # 이미 승인되었으면 스킵
    if await is_chat_authorized(chat_id):
        await q.message.reply_text("✅ 이 방은 이미 승인되어 있습니다. /start 로 시작하세요.")
        return

    # 관리자 목록 조회
    admins = await get_active_bot_admins()
    if not admins:
        await q.message.reply_text(
            "⚠️ <b>등록된 관리자가 없습니다.</b>\n\n"
            "관리자가 웹 대시보드에서 이 Chat ID 를 직접 승인해야 합니다:\n"
            f"<code>{chat_id}</code>\n\n"
            f"관리자에게 직접 전달해주세요.",
            parse_mode="HTML",
        )
        return

    # 🆕 v4.6: scope 기반 라우팅
    # 그룹방 scope (church/dept) 가 설정된 경우 → 해당 scope 관리자만 받음
    # 개인방 또는 scope 미설정 → 지파관리자만 받음
    target_scope = await get_chat_scope(chat_id)
    target_church = (target_scope or {}).get("church")
    target_dept = (target_scope or {}).get("dept")

    def _admin_should_receive(admin: dict) -> bool:
        """관리자가 이 신청을 받아야 하는지 판단."""
        atype = admin.get("scope_type", "zipa")
        achurch = admin.get("scope_church")
        adept = admin.get("scope_dept")
        # 지파관리자는 항상 받음
        if atype == "zipa":
            return True
        # 신청 방의 scope 미설정이면 지파관리자만
        if not target_church:
            return False
        # 교회관리자: 자기 교회 방만
        if atype == "church":
            return achurch == target_church
        # 부서관리자: 자기 교회 자기 부서
        if atype == "dept":
            return achurch == target_church and (
                not target_dept or adept == target_dept
            )
        return False

    routed_admins = [a for a in admins if _admin_should_receive(a)]
    if not routed_admins:
        # fallback: 지파관리자에게 (위 로직상 항상 포함되지만, 안전장치)
        routed_admins = [a for a in admins if a.get("scope_type") == "zipa"]
        if not routed_admins:
            routed_admins = admins  # 최후 수단

    # 관리자에게 DM 알림
    admin_msg = (
        "🔔 <b>새 방 승인 신청</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📋 <b>신청 방 정보</b>\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {_html.escape(chat_title)}\n"
        f"• 방 유형: {chat.type}\n\n"
        f"👤 <b>신청자</b>\n"
        f"• 이름: {_html.escape(requester_name)}\n"
        f"• User ID: <code>{requester_id}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"✅ <b>승인 방법</b>\n"
        f"• 아래 ✅ 승인 버튼을 누르거나\n"
        f"• 명령어로: <code>/approve {chat_id}</code>\n\n"
        f"❌ <b>거부 방법</b>\n"
        f"• 아래 ❌ 거부 버튼을 누르거나\n"
        f"• 명령어로: <code>/deny {chat_id}</code>"
    )
    approve_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ 승인", callback_data=f"admin_approve:{chat_id}"),
        InlineKeyboardButton("❌ 거부", callback_data=f"admin_deny:{chat_id}"),
    ]])

    delivered = 0
    failed = 0
    sent_msgs: list[tuple[int, int]] = []   # (admin_uid, msg_id)
    for admin in routed_admins:
        admin_uid = admin.get("user_id")
        if not admin_uid:
            continue
        try:
            sent = await context.bot.send_message(
                chat_id=admin_uid,
                text=admin_msg,
                parse_mode="HTML",
                reply_markup=approve_kb,
            )
            delivered += 1
            try:
                sent_msgs.append((int(admin_uid), int(sent.message_id)))
            except Exception:
                pass
        except Exception as e:
            logger.warning("관리자 %s DM 실패: %s", admin_uid, e)
            failed += 1

    # 🆕 v6.0: 다른 관리자에게도 알릴 수 있도록 메시지 추적
    if sent_msgs:
        _pending_admin_msgs[int(chat_id)] = sent_msgs

    # 신청자에게 결과 안내
    if delivered > 0:
        await q.message.reply_text(
            f"✅ <b>승인 신청 완료</b>\n\n"
            f"{delivered}명의 관리자에게 승인 요청이 전달되었습니다.\n"
            f"관리자 승인 후 이 방에서 <code>/start</code> 재실행하시면 됩니다.\n\n"
            f"<i>Chat ID: {chat_id}</i>",
            parse_mode="HTML",
        )
    else:
        await q.message.reply_text(
            f"⚠️ 관리자 알림 전송 실패.\n\n"
            f"관리자에게 직접 이 Chat ID를 전달해주세요: <code>{chat_id}</code>\n\n"
            f"💡 관리자가 봇에게 개인채팅으로 <code>/start</code> 를 먼저 실행해야 DM 수신 가능합니다.",
            parse_mode="HTML",
        )


async def approve_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """관리자 전용: /approve <chat_id> 로 방 승인."""
    import html as _html
    user = update.effective_user
    if not user or not await is_bot_admin_user(user.id):
        await update.message.reply_text("🔒 이 명령은 관리자만 사용 가능합니다.")
        return

    args = context.args if hasattr(context, 'args') else []
    if not args:
        await update.message.reply_text(
            "ℹ️ 사용법: <code>/approve &lt;chat_id&gt;</code>\n예: <code>/approve -1001234567890</code>",
            parse_mode="HTML"
        )
        return

    try:
        target_chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Chat ID는 숫자여야 합니다.")
        return

    admin_name = user.full_name or user.username or f"user_{user.id}"
    try:
        await sb_rpc("upsert_authorized_chat", {
            "p_chat_id": target_chat_id,
            "p_chat_title": None,
            "p_notes": f"{admin_name} 님이 /approve 로 승인",
            "p_is_active": True,
            "p_authorized_by": admin_name,
        })
    except Exception as e:
        await update.message.reply_text(f"❌ 승인 실패: {e}")
        return

    await update.message.reply_text(
        f"✅ <b>승인 완료</b>\n\n"
        f"Chat ID: <code>{target_chat_id}</code>\n"
        f"승인자: {_html.escape(admin_name)}\n\n"
        f"해당 방에서 <code>/start</code> 재실행하면 정상 작동합니다.",
        parse_mode="HTML"
    )

    # 승인된 방에 알림 발송 시도
    try:
        await context.bot.send_message(
            chat_id=target_chat_id,
            text=(
                f"✅ <b>이 방이 승인되었습니다!</b>\n\n"
                f"승인자: {_html.escape(admin_name)}\n\n"
                f"이제 <code>/start</code> 로 봇 사용을 시작하세요."
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.info("승인된 방에 알림 발송 실패 (봇이 아직 방에 있지 않을 수 있음): %s", e)

    # 🆕 v6.0: 다른 관리자들에게도 "이미 처리됨" 동기화 표시
    try:
        # ① 메모리 추적된 메시지 갱신
        pending = _pending_admin_msgs.pop(int(target_chat_id), [])
        notified_uids = set()
        for (admin_uid, msg_id) in pending:
            try:
                await context.bot.edit_message_text(
                    chat_id=admin_uid,
                    message_id=msg_id,
                    text=(
                        f"✅ <b>승인 완료됨</b>\n\n"
                        f"<i>{_html.escape(admin_name)} 님이 <code>/approve</code> 명령으로 승인했습니다.</i>\n"
                        f"<i>(Chat ID: {target_chat_id})</i>"
                    ),
                    parse_mode="HTML",
                )
                notified_uids.add(int(admin_uid))
            except Exception as e2:
                logger.info("동기화 알림 실패 admin=%s msg=%s: %s", admin_uid, msg_id, e2)

        # ② 모든 활성 관리자에게 새 알림 DM
        try:
            all_admins = await get_active_bot_admins()
            for admin in (all_admins or []):
                aid = admin.get("user_id")
                if not aid or int(aid) in notified_uids:
                    continue
                if int(aid) == int(user.id):
                    continue
                try:
                    await context.bot.send_message(
                        chat_id=aid,
                        text=(
                            f"🔔 <b>승인 처리 알림</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"📋 Chat ID: <code>{target_chat_id}</code>\n"
                            f"✅ <b>{_html.escape(admin_name)}</b> 님이 <code>/approve</code> 명령으로 승인했습니다.\n\n"
                            f"<i>이 방에 대한 추가 처리는 필요하지 않습니다.</i>"
                        ),
                        parse_mode="HTML",
                    )
                except Exception as e3:
                    logger.info("관리자 알림 DM 실패 admin=%s: %s", aid, e3)
        except Exception as e_all:
            logger.info("관리자 목록 조회 실패: %s", e_all)
    except Exception as e:
        logger.info("/approve 동기화 실패: %s", e)


async def deny_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """관리자 전용: /deny <chat_id> 로 방 거부."""
    user = update.effective_user
    if not user or not await is_bot_admin_user(user.id):
        await update.message.reply_text("🔒 이 명령은 관리자만 사용 가능합니다.")
        return

    args = context.args if hasattr(context, 'args') else []
    if not args:
        await update.message.reply_text(
            "ℹ️ 사용법: <code>/deny &lt;chat_id&gt;</code>",
            parse_mode="HTML"
        )
        return

    try:
        target_chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Chat ID는 숫자여야 합니다.")
        return

    await update.message.reply_text(
        f"❌ <b>거부 처리</b>\n\n"
        f"Chat ID: <code>{target_chat_id}</code>\n"
        f"(승인 목록에 추가하지 않음)",
        parse_mode="HTML"
    )


async def admin_approve_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """관리자 DM 에서 ✅ 승인 버튼 클릭 처리."""
    import html as _html
    q = update.callback_query
    user = update.effective_user

    if not user or not await is_bot_admin_user(user.id):
        await q.answer("🔒 관리자만 가능", show_alert=True)
        return

    data = q.data or ""
    try:
        target_chat_id = int(data.split(":", 1)[1])
    except (ValueError, IndexError):
        await q.answer("❌ 잘못된 데이터", show_alert=True)
        return

    await q.answer("처리 중...")
    admin_name = user.full_name or user.username or f"user_{user.id}"

    try:
        await sb_rpc("upsert_authorized_chat", {
            "p_chat_id": target_chat_id,
            "p_chat_title": None,
            "p_notes": f"{admin_name} 님이 DM 승인",
            "p_is_active": True,
            "p_authorized_by": admin_name,
        })
    except Exception as e:
        await q.message.reply_text(f"❌ 승인 실패: {e}")
        return

    await q.edit_message_text(
        q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n✅ <b>{_html.escape(admin_name)}</b> 님이 승인 완료",
        parse_mode="HTML",
    )

    # 🆕 v6.0: 다른 관리자들에게도 동기화 알림
    #   1) 기존 메시지 갱신 시도 (in-memory 추적이 살아있으면)
    #   2) 추가로 모든 활성 관리자에게 새 DM 발송 (재시작에도 강건)
    try:
        # ① 메모리 추적된 메시지 갱신 (있을 때만)
        pending = _pending_admin_msgs.pop(int(target_chat_id), [])
        my_msg_id = q.message.message_id
        notified_uids = set()
        for (admin_uid, msg_id) in pending:
            if int(admin_uid) == int(user.id) and int(msg_id) == int(my_msg_id):
                notified_uids.add(int(admin_uid))
                continue
            try:
                await context.bot.edit_message_text(
                    chat_id=admin_uid,
                    message_id=msg_id,
                    text=q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n✅ <b>{_html.escape(admin_name)}</b> 님이 먼저 승인 처리 완료",
                    parse_mode="HTML",
                )
                notified_uids.add(int(admin_uid))
            except Exception as e2:
                logger.info("기존 메시지 갱신 실패 admin=%s msg=%s: %s", admin_uid, msg_id, e2)

        # ② 모든 활성 관리자에게 새 알림 DM (메모리에 없는 케이스 보완)
        try:
            all_admins = await get_active_bot_admins()
            for admin in (all_admins or []):
                aid = admin.get("user_id")
                if not aid or int(aid) in notified_uids:
                    continue
                if int(aid) == int(user.id):
                    continue  # 본인 제외
                try:
                    await context.bot.send_message(
                        chat_id=aid,
                        text=(
                            f"🔔 <b>승인 처리 알림</b>\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n\n"
                            f"📋 Chat ID: <code>{target_chat_id}</code>\n"
                            f"✅ <b>{_html.escape(admin_name)}</b> 님이 승인 완료했습니다.\n\n"
                            f"<i>이 방에 대한 추가 처리는 필요하지 않습니다.</i>"
                        ),
                        parse_mode="HTML",
                    )
                except Exception as e3:
                    logger.info("관리자 알림 DM 실패 admin=%s: %s", aid, e3)
        except Exception as e_all:
            logger.info("관리자 목록 조회 실패: %s", e_all)
    except Exception as e:
        logger.info("승인 동기화 실패: %s", e)
    try:
        # 🆕 v5.8: 승인 후 scope 상태 따라 다음 단계 안내
        existing_scope = await get_chat_scope(target_chat_id)
        has_partial = existing_scope and existing_scope.get("church") and existing_scope.get("dept")
        has_complete = has_partial and existing_scope.get("region_name")
        
        if has_complete:
            # 이미 모든 설정 완료
            notify_text = (
                f"✅ <b>이 방이 승인되었습니다!</b>\n\n"
                f"승인자: {_html.escape(admin_name)}\n\n"
                f"📌 담당 범위: {_html.escape(scope_label(existing_scope))}\n\n"
                f"이제 <code>/start</code> 로 봇 사용을 시작하세요."
            )
            kb_after = None
        elif has_partial:
            # 교회+부서까지만 설정됨 → 지역/구역 추가 설정 안내
            notify_text = (
                f"✅ <b>이 방이 승인되었습니다!</b>\n\n"
                f"승인자: {_html.escape(admin_name)}\n\n"
                f"📋 <b>현재 진행 상황</b>:\n"
                f"1️⃣ ✅ 교회: {existing_scope.get('church')}\n"
                f"2️⃣ ✅ 부서: {existing_scope.get('dept')}\n"
                f"3️⃣ ⏳ 지역 (필수) ← 다음 단계\n"
                f"4️⃣ ⏸ 구역 (선택)\n\n"
                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📍 <b>지역 설정이 필요합니다</b>\n\n"
                f"부서 단위로는 결석자가 너무 많아 봇이 느려집니다.\n"
                f"<b>지역</b>까지 설정하면 본인 담당 결석자만 빠르게 표시됩니다.\n"
                f"(구역은 선택 — 더 좁히고 싶으면 입력)\n\n"
                f"👇 아래 버튼을 누르거나 <code>/start</code> 로 시작"
            )
            kb_buttons = [[InlineKeyboardButton("📍 지역 설정 시작", callback_data="scope_setup_back_region")]]
            # 🆕 개인방이면 대시보드 버튼도 추가
            if target_chat_id > 0 and DASHBOARD_URL:  # 개인방은 chat_id > 0
                kb_buttons.append([InlineKeyboardButton("📊 웹 대시보드 열기", url=DASHBOARD_URL)])
            kb_after = InlineKeyboardMarkup(kb_buttons)
        else:
            # scope 자체가 없음 (예전에 승인 신청 한 옛 방)
            notify_text = (
                f"✅ <b>이 방이 승인되었습니다!</b>\n\n"
                f"승인자: {_html.escape(admin_name)}\n\n"
                f"📌 <b>방 범위 설정이 필요합니다</b>\n"
                f"교회 / 부서 / 지역 / 구역을 차례로 설정해주세요.\n\n"
                f"<code>/start</code> 로 시작"
            )
            kb_after = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")]
            ])
        
        send_kwargs = {
            "chat_id": target_chat_id,
            "text": notify_text,
            "parse_mode": "HTML",
        }
        if kb_after is not None:
            send_kwargs["reply_markup"] = kb_after
        await context.bot.send_message(**send_kwargs)
    except Exception as e:
        logger.info("승인 방 알림 실패: %s", e)


async def admin_deny_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """관리자 DM 에서 ❌ 거부 버튼 클릭 처리."""
    import html as _html
    q = update.callback_query
    user = update.effective_user

    if not user or not await is_bot_admin_user(user.id):
        await q.answer("🔒 관리자만 가능", show_alert=True)
        return

    await q.answer()
    admin_name = user.full_name or user.username or f"user_{user.id}"

    # 거부 대상 chat_id 추출 (admin_deny:<chat_id>)
    data = q.data or ""
    target_chat_id = None
    try:
        target_chat_id = int(data.split(":", 1)[1])
    except (ValueError, IndexError):
        pass

    await q.edit_message_text(
        q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n❌ <b>{_html.escape(admin_name)}</b> 님이 거부",
        parse_mode="HTML",
    )

    # 🆕 v6.0: 다른 관리자들에게도 동기화 표시
    if target_chat_id is not None:
        try:
            # ① 메모리 추적된 메시지 갱신
            pending = _pending_admin_msgs.pop(int(target_chat_id), [])
            my_msg_id = q.message.message_id
            notified_uids = set()
            for (admin_uid, msg_id) in pending:
                if int(admin_uid) == int(user.id) and int(msg_id) == int(my_msg_id):
                    notified_uids.add(int(admin_uid))
                    continue
                try:
                    await context.bot.edit_message_text(
                        chat_id=admin_uid,
                        message_id=msg_id,
                        text=q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n❌ <b>{_html.escape(admin_name)}</b> 님이 먼저 거부 처리",
                        parse_mode="HTML",
                    )
                    notified_uids.add(int(admin_uid))
                except Exception as e2:
                    logger.info("기존 메시지 갱신 실패 admin=%s msg=%s: %s", admin_uid, msg_id, e2)

            # ② 모든 활성 관리자에게 새 알림 DM
            try:
                all_admins = await get_active_bot_admins()
                for admin in (all_admins or []):
                    aid = admin.get("user_id")
                    if not aid or int(aid) in notified_uids:
                        continue
                    if int(aid) == int(user.id):
                        continue
                    try:
                        await context.bot.send_message(
                            chat_id=aid,
                            text=(
                                f"🔔 <b>거부 처리 알림</b>\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"📋 Chat ID: <code>{target_chat_id}</code>\n"
                                f"❌ <b>{_html.escape(admin_name)}</b> 님이 거부 처리했습니다."
                            ),
                            parse_mode="HTML",
                        )
                    except Exception as e3:
                        logger.info("관리자 알림 DM 실패 admin=%s: %s", aid, e3)
            except Exception as e_all:
                logger.info("관리자 목록 조회 실패: %s", e_all)
        except Exception as e:
            logger.info("거부 동기화 실패: %s", e)


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
    app.add_handler(CommandHandler("wedtest",  force_wednesday_command))
    # 🆕 편의 명령어
    app.add_handler(CommandHandler("chatid",   chatid_command))
    app.add_handler(CommandHandler("approve",  approve_command))
    app.add_handler(CommandHandler("deny",     deny_command))
    # 🆕 특별관리 대책방 화이트리스트 관리
    app.add_handler(CommandHandler("allow",    allow_command))
    app.add_handler(CommandHandler("disallow", disallow_command))
    app.add_handler(CommandHandler("allowed",  allowed_command))
    app.add_handler(CommandHandler("members",  members_command))

    app.add_handler(CallbackQueryHandler(button_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    # 📅 스케줄 (python-telegram-bot v20: 월=0, 화=1, 수=2, 목=3, 금=4, 토=5, 일=6)
    if app.job_queue is not None:
        # 🆕 매주 수요일 08:00 KST — 봇 사용자(개인방) 모두에게 타겟 결석자 심방계획 요청
        app.job_queue.run_daily(
            wednesday_visit_plan_request_job,
            time=dtime(hour=8, minute=0, tzinfo=KST),
            days=(2,),  # 수요일
            name="wednesday_personal_visit_plan",
        )
        logger.info("📅 [매주 수요일 08:00 KST] 개인방 타겟 결석자 심방계획 요청")

        # 🆕 매주 수요일 08:00 KST — 특별관리 그룹방에 피드백/심방계획 요청
        app.job_queue.run_daily(
            weekly_reminder_job,
            time=dtime(hour=8, minute=0, tzinfo=KST),
            days=(2,),  # 수요일
            name="wednesday_special_reminder",
        )
        logger.info("📅 [매주 수요일 08:00 KST] 특별관리 그룹방 피드백 회의 요청")

        # 🆕 매주 수요일 00:00 KST — 주차 자동 전환 (선택적)
        app.job_queue.run_daily(
            weekly_rollover_job,
            time=dtime(hour=0, minute=0, tzinfo=KST),
            days=(2,),  # 수요일
            name="wednesday_weekly_rollover",
        )
        logger.info("📅 [매주 수요일 00:00 KST] 주차 자동 전환")
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

    # 🆕 v4.7: Cloud Scheduler 트리거용 endpoint
    # 보안: SCHEDULER_TOKEN 환경변수와 일치하는 token 만 허용
    SCHEDULER_TOKEN = os.environ.get("SCHEDULER_TOKEN", "")

    def _check_scheduler_auth(request) -> bool:
        """Cloud Scheduler 인증 체크 (?token=xxx 또는 Authorization 헤더)"""
        if not SCHEDULER_TOKEN:
            logger.warning("SCHEDULER_TOKEN 환경변수 미설정 - 모든 요청 거부")
            return False
        # 1) Query parameter
        token = request.query.get("token", "")
        if token == SCHEDULER_TOKEN:
            return True
        # 2) Authorization 헤더
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer ") and auth[7:] == SCHEDULER_TOKEN:
            return True
        return False

    async def trigger_weekly_visit_plan(request):
        """🌅 매주 수요일 08:00 KST — 개인방 사용자에게 타겟 결석자 심방계획 요청.
        Cloud Scheduler 가 호출."""
        if not _check_scheduler_auth(request):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        logger.info("📅 [Cloud Scheduler] trigger_weekly_visit_plan 호출")
        try:
            # JobQueue context 객체 흉내내기 (job_queue 안 쓸 때 대비)
            class FakeContext:
                bot = app.bot
            await wednesday_visit_plan_request_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "weekly_visit_plan 발송 완료"})
        except Exception as e:
            logger.exception("trigger_weekly_visit_plan 실패: %s", e)
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def trigger_special_reminder(request):
        """🚨 매주 수요일 08:00 KST — 특별관리 그룹방에 피드백/심방계획 요청.
        Cloud Scheduler 가 호출."""
        if not _check_scheduler_auth(request):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        logger.info("📅 [Cloud Scheduler] trigger_special_reminder 호출")
        try:
            class FakeContext:
                bot = app.bot
            await weekly_reminder_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "special_reminder 발송 완료"})
        except Exception as e:
            logger.exception("trigger_special_reminder 실패: %s", e)
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def trigger_weekly_rollover(request):
        """🌃 매주 수요일 00:00 KST — 주차 자동 전환.
        Cloud Scheduler 가 호출."""
        if not _check_scheduler_auth(request):
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        logger.info("📅 [Cloud Scheduler] trigger_weekly_rollover 호출")
        try:
            class FakeContext:
                bot = app.bot
            await weekly_rollover_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "weekly_rollover 완료"})
        except Exception as e:
            logger.exception("trigger_weekly_rollover 실패: %s", e)
            return web.json_response({"ok": False, "error": str(e)}, status=500)


    # ─────────────────────────────────────────────────────────────
    # 🆕 v5.7: 관리자 메시지 발송 endpoint
    # ─────────────────────────────────────────────────────────────
    BROADCAST_TOKEN = os.environ.get("BROADCAST_TOKEN", os.environ.get("SCHEDULER_TOKEN", ""))

    async def _resolve_target_chats(scope: dict) -> list:
        """scope 에 따라 발송 대상 활성 그룹방 목록 반환

        지원하는 scope.type:
          - all          : 전체 그룹방
          - church       : 특정 교회 전체 부서
          - dept         : 특정 교회 + 특정 부서
          - all_dept     : 모든 교회의 특정 부서 (지파관리자)
          - special      : 모든 특별관리 대상자 대책방
          - special_church : 특정 교회의 특별관리 대상자 대책방
          - special_dept   : 특정 교회·부서의 특별관리 대상자 대책방
        """
        scope_type = (scope or {}).get("type") or "all"
        scope_church = (scope or {}).get("church")
        scope_dept = (scope or {}).get("dept")

        # 🆕 특별관리 대상자 대책방 발송
        if scope_type in ("special", "special_church", "special_dept"):
            sp_qs = "special_management_targets?select=name,dept,phone_last4,monitor_chat_id,region_name,zone_name&monitor_chat_id=not.is.null&limit=5000"
            sp_rows = await sb_get(sp_qs) or []

            # name+dept+phone_last4 → registry 매칭으로 church 찾기
            reg_rows = await sb_get(
                "church_member_registry?select=name,dept,phone_last4,church&limit=50000"
            ) or []
            # (name, dept, phone) → church 매핑
            reg_map = {}
            for r in reg_rows:
                key = (r.get("name") or "", r.get("dept") or "", r.get("phone_last4") or "")
                reg_map[key] = r.get("church") or ""

            # 결과로 변환
            chats = []
            seen_chat_ids = set()
            for s in sp_rows:
                cid = s.get("monitor_chat_id")
                if not cid or cid in seen_chat_ids:
                    continue
                key = (s.get("name") or "", s.get("dept") or "", s.get("phone_last4") or "")
                church = reg_map.get(key, "")

                # scope 별 필터
                if scope_type == "special_church" and scope_church and church != scope_church:
                    continue
                if scope_type == "special_dept":
                    if scope_church and church != scope_church:
                        continue
                    if scope_dept and s.get("dept") != scope_dept:
                        continue

                seen_chat_ids.add(cid)
                title = f"{s.get('name','?')} 대책방"
                chats.append({
                    "chat_id": cid,
                    "chat_title": title,
                    "church": church,
                    "dept": s.get("dept"),
                    "region_name": s.get("region_name"),
                    "zone_name": s.get("zone_name"),
                })

            if not chats:
                return []

            # 봇 활성 권한 체크
            chat_ids = [c["chat_id"] for c in chats]
            chat_ids_str = ",".join(str(c) for c in chat_ids)
            auth_rows = await sb_get(
                f"bot_authorized_chats?select=chat_id&chat_id=in.({chat_ids_str})&is_active=eq.true&limit=5000"
            ) or []
            authorized = {r["chat_id"] for r in auth_rows}
            return [c for c in chats if c["chat_id"] in authorized]

        # 일반 그룹방 발송
        qs = "telegram_chat_scope?select=chat_id,chat_title,church,dept,region_name,zone_name&limit=5000"
        if scope_type == "church" and scope_church:
            qs += f"&church=eq.{quote(scope_church)}"
        elif scope_type == "dept" and scope_church and scope_dept:
            qs += f"&church=eq.{quote(scope_church)}&dept=eq.{quote(scope_dept)}"
        elif scope_type == "all_dept" and scope_dept:
            # 🆕 모든 교회의 특정 부서
            qs += f"&dept=eq.{quote(scope_dept)}"

        scope_rows = await sb_get(qs) or []
        chat_ids = [r["chat_id"] for r in scope_rows if r.get("chat_id")]
        if not chat_ids:
            return []

        chat_ids_str = ",".join(str(c) for c in chat_ids)
        auth_rows = await sb_get(
            f"bot_authorized_chats?select=chat_id&chat_id=in.({chat_ids_str})&is_active=eq.true&limit=5000"
        ) or []
        authorized = {r["chat_id"] for r in auth_rows}
        return [r for r in scope_rows if r["chat_id"] in authorized]

    def _check_requester_scope(scope: dict, requester: dict) -> tuple:
        """권한 검증. (scope_적용, error_메시지) 튜플 반환"""
        req_role = (requester or {}).get("role") or "zipa"
        req_church = (requester or {}).get("church")
        req_dept = (requester or {}).get("dept")
        scope_type = (scope or {}).get("type") or "all"
        scope_church = (scope or {}).get("church")
        scope_dept = (scope or {}).get("dept")

        if req_role == "zipa":
            return scope, None  # 모두 허용
        if req_role == "church":
            # 자기 교회만 — special/special_church 도 자기 교회 한정
            if scope_type == "all":
                return ({"type": "church", "church": req_church}, None)
            if scope_type == "all_dept":
                # 교회관리자는 모든 교회 부서 발송 불가 → 자기 교회 부서로 강제
                return ({"type": "dept", "church": req_church, "dept": scope_dept}, None)
            if scope_type == "special":
                # 자기 교회 특별관리만
                return ({"type": "special_church", "church": req_church}, None)
            if scope_type in ("special_church", "special_dept"):
                if scope_church != req_church:
                    return None, "교회관리자는 자기 교회만 발송 가능"
                return scope, None
            if scope_church != req_church:
                return None, "교회관리자는 자기 교회만 발송 가능"
            return scope, None
        if req_role == "dept":
            # 자기 교회 + 자기 부서만
            if scope_type == "special_dept":
                if scope_church != req_church or scope_dept != req_dept:
                    return None, "부서관리자는 자기 부서만 발송 가능"
                return scope, None
            if scope_type != "dept" or scope_church != req_church or scope_dept != req_dept:
                return None, "부서관리자는 자기 부서만 발송 가능"
            return scope, None
        return None, "알 수 없는 권한"

    async def broadcast_preview_handler(request):
        """발송 전 대상 미리보기"""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        token = data.get("token") or request.headers.get("X-Broadcast-Token", "")
        if not BROADCAST_TOKEN or token != BROADCAST_TOKEN:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        scope, err = _check_requester_scope(data.get("scope") or {}, data.get("requester") or {})
        if err:
            return web.json_response({"ok": False, "error": err}, status=403)

        try:
            chats = await _resolve_target_chats(scope)
            return web.json_response({
                "ok": True,
                "total": len(chats),
                "chats": [
                    {
                        "chat_id": c["chat_id"],
                        "title": c.get("chat_title", "(제목없음)"),
                        "church": c.get("church"),
                        "dept": c.get("dept"),
                    }
                    for c in chats
                ],
                "applied_scope": scope,
            })
        except Exception as e:
            logger.exception("broadcast_preview 실패: %s", e)
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def broadcast_send_handler(request):
        """관리자 페이지에서 호출 → 그룹방들에 메시지 발송"""
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        token = data.get("token") or request.headers.get("X-Broadcast-Token", "")
        if not BROADCAST_TOKEN or token != BROADCAST_TOKEN:
            return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

        message = (data.get("message") or "").strip()
        if not message:
            return web.json_response({"ok": False, "error": "empty_message"}, status=400)
        if len(message) > 4000:
            return web.json_response({"ok": False, "error": "message_too_long"}, status=400)

        scope, err = _check_requester_scope(data.get("scope") or {}, data.get("requester") or {})
        if err:
            return web.json_response({"ok": False, "error": err}, status=403)

        # 🆕 v5.7: 버튼 (URL 바로가기) 처리
        # buttons: [[{"text":"라벨", "url":"https://..."}, ...], ...]
        buttons = data.get("buttons") or []
        reply_markup = None
        if buttons and isinstance(buttons, list):
            try:
                from telegram import InlineKeyboardButton, InlineKeyboardMarkup
                btn_rows = []
                for row in buttons[:5]:  # 최대 5줄
                    if not isinstance(row, list):
                        continue
                    btn_row = []
                    for b in row[:3]:  # 줄당 최대 3개
                        if not isinstance(b, dict):
                            continue
                        text = (b.get("text") or "").strip()[:64]
                        url = (b.get("url") or "").strip()
                        if not text or not url:
                            continue
                        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("tg://")):
                            continue
                        btn_row.append(InlineKeyboardButton(text=text, url=url))
                    if btn_row:
                        btn_rows.append(btn_row)
                if btn_rows:
                    reply_markup = InlineKeyboardMarkup(btn_rows)
            except Exception as e:
                logger.warning("버튼 파싱 실패: %s", e)

        try:
            target_chats = await _resolve_target_chats(scope)
        except Exception as e:
            logger.exception("broadcast 대상 조회 실패: %s", e)
            return web.json_response({"ok": False, "error": f"db_error: {e}"}, status=500)

        if not target_chats:
            return web.json_response({
                "ok": True,
                "total_targets": 0,
                "sent": 0,
                "failed": 0,
                "failures": [],
                "message": "발송 대상 그룹방이 없습니다",
            })

        # 🆕 v5.7: 안전한 HTML 서식 허용 (Telegram 지원 태그만)
        # 허용 태그: <b>, <strong>, <i>, <em>, <u>, <s>, <strike>, <code>, <pre>, <a>, <br>
        # 다른 태그는 그대로 출력 (Telegram이 무시) — 위험한 것 없음
        # XSS 위험: 봇은 텍스트만 보내고 HTML 렌더는 Telegram 클라이언트에서만 발생 → 안전
        # 🆕 특별관리 대상자 대책방엔 다른 prefix
        sc_type = (scope or {}).get("type") or "all"
        if str(sc_type).startswith("special"):
            prefixed_message = (
                "🚨 <b>특별관리 안내</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{message}\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "<i>본 메시지는 특별관리 대책방으로 발송됨</i>"
            )
        else:
            prefixed_message = (
                "📢 <b>서울야고보지파 안내</b>\n"
                "━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{message}\n\n"
                "━━━━━━━━━━━━━━━━━━━━\n"
                "<i>[총회장님 어록] 하나님이 누군가를 부르신다는 것은 그 사람이 그럴만한 능력이 있어서가 아니라 그럴만한 능력을 주시겠다는 그런 말이다</i>"
            )

        sent = 0
        failed = 0
        failures = []
        for r in target_chats:
            cid = r["chat_id"]
            try:
                send_kwargs = {
                    "chat_id": cid,
                    "text": prefixed_message,
                    "parse_mode": "HTML",
                }
                if reply_markup is not None:
                    send_kwargs["reply_markup"] = reply_markup
                await app.bot.send_message(**send_kwargs)
                sent += 1
                await asyncio.sleep(0.05)  # rate limit 안전장치
            except Exception as e:
                failed += 1
                failures.append({
                    "chat_id": cid,
                    "title": r.get("chat_title"),
                    "error": str(e)[:200],
                })
                logger.warning("broadcast 발송 실패 chat=%s err=%s", cid, e)

        return web.json_response({
            "ok": True,
            "total_targets": len(target_chats),
            "sent": sent,
            "failed": failed,
            "failures": failures[:20],
        })


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
        """결석자 검색: 이름(필수) + 선택 필터 (전화뒷4/교회/부서/지역/구역)"""
        name   = (request.query.get("name") or "").strip()
        phone  = (request.query.get("phone") or "").strip()
        church = (request.query.get("church") or "").strip()
        dept   = (request.query.get("dept") or "").strip()
        region = (request.query.get("region") or "").strip()
        zone   = (request.query.get("zone") or "").strip()

        # 🔧 이름만 필수, 나머지는 선택 (여러 필터 조합 가능)
        if not name:
            return web.json_response({"ok": False, "error": "이름은 필수입니다"}, status=400)

        def _build_path(week_key, zone_value=None):
            p = (
                f"weekly_visit_targets"
                f"?select=row_id,week_key,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count"
                f"&week_key=eq.{quote(week_key)}"
                f"&name=eq.{quote(name)}"
            )
            if phone:  p += f"&phone_last4=eq.{quote(phone)}"
            if church: p += f"&church=eq.{quote(church)}"
            if dept:   p += f"&dept=eq.{quote(dept)}"
            if region: p += f"&region_name=eq.{quote(region)}"
            if zone_value: p += f"&zone_name=eq.{quote(zone_value)}"
            p += "&limit=5"
            return p

        try:
            week_key, _ = await get_active_week()
            if not week_key:
                return web.json_response({"ok": False, "error": "등록된 주차 없음"}, status=404)

            weeks_to_try = [week_key]
            try:
                recent = await sb_get("weekly_target_weeks?select=week_key&order=week_key.desc&limit=4")
                for w in (recent or []):
                    wk = w.get("week_key")
                    if wk and wk not in weeks_to_try:
                        weeks_to_try.append(wk)
            except Exception:
                pass

            rows = []
            for wk in weeks_to_try:
                if zone:
                    # zone 여러 형태 시도 (4-2 / 4팀2)
                    zone_norm = normalize_zone(zone)
                    rows = await sb_get(_build_path(wk, zone_norm))
                    if not rows and zone != zone_norm:
                        rows = await sb_get(_build_path(wk, zone))
                    if not rows:
                        zone_alt = zone.replace("팀", "-") if "팀" in zone else zone.replace("-", "팀")
                        if zone_alt != zone and zone_alt != zone_norm:
                            rows = await sb_get(_build_path(wk, zone_alt))
                else:
                    rows = await sb_get(_build_path(wk, None))
                if rows:
                    break

            if not rows:
                return web.json_response({"ok": True, "target": None, "progress": None})

            # 여러 명 매칭되면 가장 첫 번째 (전화뒷4로 구분되는게 이상적)
            if len(rows) > 1:
                logger.info(f"miniapp search: {len(rows)} matches for {name}, returning first")

            target = rows[0]

            # 이름 마스킹 복구
            enriched = await enrich_names([target])
            if enriched:
                target = enriched[0]

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

        # 🆕 v4.7: 다양한 날짜 형식 모두 ISO로 변환 (4/27, 4-27, 4.27, 4월 27일, 2026-04-27, 2026년 4월 27일)
        visit_date_sort = _parse_visit_date_to_iso(visit_date_display)

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
    # 🆕 Cloud Scheduler 트리거 endpoints (POST 권장, GET도 허용)
    http_app.router.add_post("/trigger/weekly-visit-plan", trigger_weekly_visit_plan)
    http_app.router.add_get("/trigger/weekly-visit-plan",  trigger_weekly_visit_plan)
    http_app.router.add_post("/trigger/special-reminder",  trigger_special_reminder)
    http_app.router.add_get("/trigger/special-reminder",   trigger_special_reminder)
    http_app.router.add_post("/trigger/weekly-rollover",   trigger_weekly_rollover)
    http_app.router.add_get("/trigger/weekly-rollover",    trigger_weekly_rollover)
    # 🆕 v5.7: 관리자 메시지 발송
    http_app.router.add_post("/broadcast/preview", broadcast_preview_handler)
    http_app.router.add_post("/broadcast/send",    broadcast_send_handler)

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

        # 🆕 v6.0: 시작 시 chat_allowed_users 테이블 존재 확인 (큰 경고)
        global _chat_allowed_users_table_exists
        try:
            await sb_get("chat_allowed_users?select=chat_id&limit=1")
            _chat_allowed_users_table_exists = True
            logger.info("✅ chat_allowed_users 테이블 정상 — 화이트리스트 활성")
        except Exception as e:
            msg = str(e).lower()
            if 'does not exist' in msg or 'relation' in msg or 'pgrst205' in msg or '404' in msg:
                _chat_allowed_users_table_exists = False
                logger.error("=" * 70)
                logger.error("🚨 [화이트리스트 비활성] chat_allowed_users 테이블이 DB 에 없습니다!")
                logger.error("🚨 특별관리 대책방의 권한 체크가 작동하지 않습니다.")
                logger.error("🚨 yago_patch_v58_to_v60.sql 또는 yago_master_v60.sql 을 즉시 적용하세요.")
                logger.error("=" * 70)
            else:
                logger.warning("chat_allowed_users 테이블 확인 실패 (일시적): %s", e)

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
