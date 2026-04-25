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
# 1번: 최초 1회만 · 2/3/4번: 매주 수요일 07시 초기화
SP_ITEM_LABELS = {
    "item1_chat_invited":  "대책방 초대완료 (구역장·인섬교·강사·전도사·심방부사명자)",
    "item2_feedback_done": "금주 피드백 진행",
    "item3_visit_date":    "금주 심방예정일",
    "item4_visit_plan":    "금주 심방계획",
}

# ── 마크다운 이스케이프 ────────────────────────────────────────────────────────
_MD_SPECIALS = "_*`[]()"
def md(s) -> str:
    if s is None: return ""
    return "".join(("\\" + c) if c in _MD_SPECIALS else c for c in str(s))

def plain(s) -> str:
    if s is None: return ""
    return str(s)

async def safe_send(send_func, text: str, **kwargs):
    try:
        return await send_func(text, **kwargs)
    except Exception as e:
        emsg = str(e)
        if "parse" in emsg.lower() or "entity" in emsg.lower() or "markdown" in emsg.lower():
            logger.warning("Markdown parse failed (%s), retrying as plain text", emsg)
            kwargs.pop("parse_mode", None)
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
# Supabase 헬퍼
# ═════════════════════════════════════════════════════════════════════════════
async def sb_get(path: str):
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
# 이름 마스킹 복구
# ═════════════════════════════════════════════════════════════════════════════
import re as _re_name

def _is_masked_name(s: str) -> bool:
    if not s: return False
    return ('*' in s) or ('_' in s and len(s) <= 5)

async def resolve_real_name(
    masked_name: str,
    church: str = None,
    dept: str = None,
    phone_last4: str = None,
) -> str:
    if not masked_name or not _is_masked_name(masked_name):
        return masked_name

    try:
        pattern = '^' + _re_name.escape(masked_name).replace(r'\*', '.').replace(r'\_', '.') + '$'
    except Exception:
        return masked_name

    params = []
    if church:     params.append(f"church=eq.{quote(church)}")
    if dept:       params.append(f"dept=eq.{quote(dept)}")
    if phone_last4:params.append(f"phone_last4=eq.{quote(phone_last4)}")
    params.append("limit=30")

    if len(params) >= 3:
        try:
            rows = await sb_get(f"church_member_registry?select=name&" + "&".join(params))
            for r in rows or []:
                nm = r.get("name", "")
                if nm and _re_name.match(pattern, nm):
                    return nm
        except Exception:
            pass

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

    if phone_last4:
        try:
            rows = await sb_get(
                f"church_member_registry?select=name,church"
                f"&phone_last4=eq.{quote(phone_last4)}&limit=30"
            )
            for r in rows or []:
                nm = r.get("name", "")
                if not nm: continue
                if not _re_name.match(pattern, nm): continue
                if church and r.get("church") == church:
                    return nm
            for r in rows or []:
                nm = r.get("name", "")
                if nm and _re_name.match(pattern, nm):
                    return nm
        except Exception:
            pass

    return masked_name

async def enrich_names(rows: list, church_key: str = "church", dept_key: str = "dept",
                       name_key: str = "name", phone_key: str = "phone_last4") -> list:
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
                r["_original_name"] = nm
    return rows

def compute_target_week_key() -> tuple[str, str]:
    now = datetime.now(KST)
    weekday = now.weekday()
    if weekday == 6:
        diff = 0
    elif weekday in (0, 1):
        diff = -(weekday + 1)
    else:
        diff = 6 - weekday

    sunday = (now + timedelta(days=diff)).replace(hour=0, minute=0, second=0, microsecond=0)
    year, month = sunday.year, sunday.month

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

async def fetch_absentees_by_region(week_key: str, church: str, dept: str, region: str):
    try:
        rows = await sb_rpc("get_absentees_by_dept_region", {
            "p_week_key": week_key, "p_dept": dept, "p_region": region
        })
        if rows is None: rows = []
        rows = [r for r in rows if r.get("church", church) == church or not r.get("church")]
        for r in rows:
            if not r.get("dept"): r["dept"] = dept
            if not r.get("church"): r["church"] = church
        return await enrich_names(rows)
    except Exception as e:
        logger.info("RPC get_absentees_by_dept_region 폴백: %s", e)

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
    normalized = normalize_zone(zone)
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
    if not raw:
        return None
    raw = raw.strip()
    now_year = datetime.now(KST).year

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})$", raw)
    if m: return raw

    m = re.match(r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$", raw)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    m = re.match(r"^(\d{1,2})[/.\-](\d{1,2})$", raw)
    if m:
        mo, d = m.groups()
        return f"{now_year:04d}-{int(mo):02d}-{int(d):02d}"

    m = re.match(r"^(\d{1,2})월\s*(\d{1,2})일$", raw)
    if m:
        mo, d = m.groups()
        return f"{now_year:04d}-{int(mo):02d}-{int(d):02d}"

    m = re.match(r"^(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일$", raw)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    return None

async def upsert_progress(week_key: str, row_id: str, ctx: dict):
    raw_date = ctx.get("tmp_date") or ""
    date_sort = _parse_visit_date_to_iso(raw_date)
    plan_text = ctx.get("tmp_plan") or ""
    
    await sb_rpc("upsert_weekly_visit_progress", {
        "p_week_key":           week_key,
        "p_row_id":             row_id,
        "p_shepherd":           ctx.get("tmp_shepherd") or "",
        "p_visit_date_display": raw_date,
        "p_visit_date_sort":    date_sort,
        "p_plan_text":          plan_text,
        "p_is_target":          ctx.get("tmp_target") == "타겟",
        "p_is_done":            ctx.get("tmp_done") == "완료",
        "p_worship":            ctx.get("tmp_worship") or None,
        "p_attendance":         ctx.get("tmp_attendance") or None,
        "p_note":               ctx.get("tmp_note") or "",
    })

    # [추가] 일반 심방 기록 후 특별관리 테이블에도 동시 반영 (동기화)
    try:
        t_rows = await sb_get(f"weekly_visit_targets?select=name,dept,phone_last4&row_id=eq.{quote(row_id)}&limit=1")
        if t_rows:
            enriched = await enrich_names(t_rows)
            target_info = enriched[0]
            if raw_date:
                await sb_rpc("set_special_item3", {
                    "p_dept": target_info.get("dept", ""), 
                    "p_name": target_info.get("name", ""), 
                    "p_phone_last4": target_info.get("phone_last4", ""), 
                    "p_value": raw_date
                })
            if plan_text:
                await sb_rpc("set_special_item4", {
                    "p_dept": target_info.get("dept", ""), 
                    "p_name": target_info.get("name", ""), 
                    "p_phone_last4": target_info.get("phone_last4", ""), 
                    "p_value": plan_text
                })
    except Exception as e:
        logger.warning("특별관리 항목 동시 업데이트 실패 (upsert_progress): %s", e)

# ═════════════════════════════════════════════════════════════════════════════
# 키보드 빌더
# ═════════════════════════════════════════════════════════════════════════════
MINIAPP_URL = os.environ.get("MINIAPP_URL", "")
if not MINIAPP_URL:
    _webhook = os.environ.get("WEBHOOK_URL", "")
    if _webhook:
        MINIAPP_URL = _webhook.rsplit("/", 1)[0] + "/miniapp"

DASHBOARD_URL = os.environ.get("DASHBOARD_URL", "").strip()

def kb_reply_main(is_private: bool = True) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("📋 결석자 심방"), KeyboardButton("🚨 특별관리결석자")],
    ]
    if is_private and MINIAPP_URL.startswith("https://"):
        rows.append([KeyboardButton("📝 결석자 심방 기록 (폼)", web_app=WebAppInfo(url=MINIAPP_URL))])
    rows.append([KeyboardButton("📘 사용법"), KeyboardButton("🏠 메인 메뉴")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True, is_persistent=True, input_field_placeholder="메뉴를 선택하세요")

def kb_main_menu(is_private: bool = True) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("📋 결석자 심방",       callback_data="m:absentee")],
        [InlineKeyboardButton("🚨 특별관리결석자",    callback_data="m:special")],
    ]
    if is_private and MINIAPP_URL.startswith("https://"):
        rows.append([InlineKeyboardButton("📝 결석자 심방 기록 (미니웹앱)", web_app=WebAppInfo(url=MINIAPP_URL))])
    rows += [
        [InlineKeyboardButton("📘 사용법 (도움말)",    callback_data="m:help")],
        [InlineKeyboardButton("🔍 DB 진단",            callback_data="m:diagnose")],
    ]
    return InlineKeyboardMarkup(rows)

def kb_cancel_only() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ 입력 취소", callback_data="flow_cancel")]])

def is_private_chat(update: Update) -> bool:
    try:
        chat = update.effective_chat
        return chat is not None and chat.type == "private"
    except Exception:
        return True

def kb_church_select(flow: str) -> InlineKeyboardMarkup:
    rows = []
    for i in range(0, len(CHURCHES), 2):
        row = []
        for ch in CHURCHES[i:i+2]:
            row.append(InlineKeyboardButton(ch, callback_data=f"{flow}_ch:{ch}"))
        rows.append(row)
    rows.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    return InlineKeyboardMarkup(rows)

def kb_dept_select(flow: str, church: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(DEPTS[0], callback_data=f"{flow}_dp:{church}:{DEPTS[0]}"), InlineKeyboardButton(DEPTS[1], callback_data=f"{flow}_dp:{church}:{DEPTS[1]}")],
        [InlineKeyboardButton(DEPTS[2], callback_data=f"{flow}_dp:{church}:{DEPTS[2]}"), InlineKeyboardButton(DEPTS[3], callback_data=f"{flow}_dp:{church}:{DEPTS[3]}")],
        [InlineKeyboardButton("◀ 교회 다시 선택", callback_data=f"m:{'absentee' if flow=='abs' else 'special'}")],
    ]
    return InlineKeyboardMarkup(rows)


# ═════════════════════════════════════════════════════════════════════════════
# 명령어 핸들러
# ═════════════════════════════════════════════════════════════════════════════
HELP_TEXT = (
    "📖 <b>결석자 타겟 심방 봇 — 사용법</b>\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"
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
    "→ 이 방이 <b>감지방</b> 으로 등록되고 4항목 체크리스트 표시:\n"
    "   ① 대책방 초대완료 (최초 1회)\n"
    "   ② 금주 피드백 진행 (주간 리셋)\n"
    "   ③ 금주 심방예정일\n"
    "   ④ 금주 심방계획\n"
    "매주 수요일 07:00 KST 에 미체크 항목 리마인더 발송\n\n"
    "<b>📱 4️⃣ 미니앱 (개인 채팅에서만)</b>\n"
    "📝 결석자 심방 기록 (폼) 버튼 탭\n"
    "→ 이름+전화뒷4로 결석자 검색\n"
    "→ 기존 심방 기록 자동 로드 → 보충/수정 후 저장\n"
    "⚠️ 그룹방에서는 미니앱 버튼 안 보임 (텔레그램 정책)\n\n"
    "<b>📅 자동 알림 스케줄</b>\n"
    "• <b>수요일 07:00 KST</b> → 모든 방에 이번 주 결석자 심방계획 요청\n"
    "• <b>수요일 07:00 KST</b> → 특별관리 대상 미체크 항목 리마인더 (연속결석자 방)\n\n"
    "<b>⌨️ 5️⃣ 명령어 모음</b> <i>(탭하면 복사)</i>\n"
    "• <code>/start</code> — 방 설정 + 메인 메뉴\n"
    "• <code>/menu</code> — 메인 메뉴\n"
    "• <code>/setup</code> — 방 범위 재설정 (최초 설정자만)\n"
    "• <code>/myscope</code> — 이 방의 현재 범위 확인\n"
    "• <code>/chatid</code> — 이 방의 Chat ID 확인\n"
    "• <code>/cancel</code> — 현재 입력 중단\n"
    "• <code>/help</code> — 이 사용법\n"
    "• <code>/diagnose</code> — DB 진단\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "🌐 <b>상세 분석·통계·CSV 는 웹 대시보드에서</b>\n"
    "💬 문제 있으면 <code>/diagnose</code> 결과를 관리자에게"
)

async def _send_help(update: Update):
    try:
        await safe_reply_text(update.message, HELP_TEXT, parse_mode="HTML",
                              reply_markup=kb_main_menu(is_private_chat(update)))
    except Exception as e:
        logger.warning("help HTML 실패, 평문: %s", e)
        plain = (HELP_TEXT.replace("<b>","").replace("</b>","")
                          .replace("<i>","").replace("</i>","")
                          .replace("<code>","").replace("</code>",""))
        await safe_reply_text(update.message, plain,
                              reply_markup=kb_main_menu(is_private_chat(update)))

async def safe_reply_text(message, text: str, **kwargs):
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
    try:
        result = await sb_rpc("is_chat_authorized", {"p_chat_id": chat_id})
        if isinstance(result, bool): return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict): return bool(first.get("is_chat_authorized", False))
        return False
    except Exception as e:
        logger.warning("is_chat_authorized 실패: %s", e)
        return False

async def record_chat_access(chat_id: int):
    try:
        await sb_rpc("record_chat_access", {"p_chat_id": chat_id})
    except Exception as e:
        logger.warning("record_chat_access 실패: %s", e)

def unauthorized_message(chat_id: int, chat_title: str = "") -> str:
    import html as _html
    return (
        "🔒 <b>승인되지 않은 방입니다</b>\n"
        "━━━━━━━━━━━━━━━━━━━━\n\n"
        f"이 방은 관리자에 의해 사전 승인된 방이 아니므로,\n"
        f"결석자 심방 정보를 볼 수 없습니다.\n\n"
        f"📋 <b>이 방 정보</b>:\n"
        f"• Chat ID: <code>{chat_id}</code>\n"
        f"• 방 이름: {_html.escape(chat_title or '(제목없음)')}\n\n"
        f"👉 <b>아래 🙏 승인 신청하기 버튼을 누르시면</b>\n"
        f"관리자에게 자동으로 승인 요청이 전달됩니다."
    )

def kb_request_approval() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🙏 승인 신청하기", callback_data="request_approval")]])

def kb_dashboard_link() -> InlineKeyboardMarkup | None:
    if not DASHBOARD_URL: return None
    return InlineKeyboardMarkup([[InlineKeyboardButton("📊 웹 대시보드 열기", url=DASHBOARD_URL)]])

async def is_bot_admin_user(user_id: int) -> bool:
    if not user_id: return False
    try:
        result = await sb_rpc("is_bot_admin", {"p_user_id": user_id})
        if isinstance(result, bool): return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict): return bool(first.get("is_bot_admin", False))
        return False
    except Exception as e:
        logger.warning("is_bot_admin 실패: %s", e)
        return False

async def get_active_bot_admins() -> list[dict]:
    try:
        rows = await sb_rpc("get_active_bot_admins", {})
        if isinstance(rows, list): return [r for r in rows if isinstance(r, dict)]
        return []
    except Exception as e:
        logger.warning("get_active_bot_admins 실패: %s", e)
        return []

async def try_acquire_job_lock(job_name: str, source: str = "unknown") -> bool:
    try:
        result = await sb_rpc("try_acquire_job_lock", {"p_job_name": job_name, "p_source": source})
        if isinstance(result, bool): return result
        if isinstance(result, list) and len(result) > 0:
            first = result[0]
            if isinstance(first, bool): return first
            if isinstance(first, dict): return bool(first.get("try_acquire_job_lock", False))
        return False
    except Exception as e:
        logger.warning("try_acquire_job_lock 실패 (lock 없이 실행): %s", e)
        return True

async def ensure_authorized(update: Update) -> bool:
    chat = update.effective_chat
    if not chat: return False
    chat_id = chat.id

    if chat.type == "private":
        if await is_bot_admin_user(chat_id): return True
        if await is_chat_authorized(chat_id):
            await record_chat_access(chat_id)
            return True
    else:
        if await is_chat_authorized(chat_id):
            await record_chat_access(chat_id)
            return True

    chat_title = chat.title or chat.full_name or ""
    msg = unauthorized_message(chat_id, chat_title)
    kb = kb_request_approval()
    try:
        if update.message:
            await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kb)
        elif update.callback_query:
            await update.callback_query.message.reply_text(msg, parse_mode="HTML", reply_markup=kb)
    except Exception as e:
        logger.warning("미승인 메시지 전송 실패: %s", e)
        try:
            plain = msg.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "")
            if update.message: await update.message.reply_text(plain, reply_markup=kb)
            elif update.callback_query: await update.callback_query.message.reply_text(plain, reply_markup=kb)
        except Exception: pass
    return False

# ═════════════════════════════════════════════════════════════════════════════
# 방별 범위(scope) 관리 — 교회/부서/지역/구역 고정
# ═════════════════════════════════════════════════════════════════════════════
async def get_chat_scope(chat_id: int) -> dict | None:
    try:
        rows = await sb_rpc("get_chat_scope", {"p_chat_id": chat_id})
        if rows and len(rows) > 0:
            s = rows[0]
            if s.get("church"): return s
        return None
    except Exception as e:
        logger.warning("get_chat_scope 실패: %s", e)
        return None

async def save_chat_scope(chat_id: int, chat_title: str, church: str = None, dept: str = None, region_name: str = None, zone_name: str = None, owner_user_id: int = None, owner_name: str = None):
    try:
        await sb_rpc("set_chat_scope", {
            "p_chat_id": chat_id, "p_chat_title": chat_title or "",
            "p_church": church, "p_dept": dept, "p_region_name": region_name, "p_zone_name": zone_name,
            "p_owner_user_id": owner_user_id, "p_owner_name": owner_name,
        })
        return True
    except Exception as e:
        logger.warning("save_chat_scope 실패: %s", e)
        return False

async def check_scope_owner(chat_id: int, user_id: int) -> tuple[bool, str]:
    s = await get_chat_scope(chat_id)
    if not s: return True, ""
    owner = s.get("owner_user_id")
    if not owner: return True, ""
    if int(owner) == int(user_id): return True, ""
    owner_name = s.get("owner_name") or "최초 설정자"
    return False, f"이 방의 범위는 *{md(owner_name)}* 님만 변경할 수 있습니다."

def scope_label(s: dict) -> str:
    if not s: return "설정 안 됨"
    parts = []
    if s.get("church"): parts.append(s["church"])
    if s.get("dept"):   parts.append(s["dept"])
    if s.get("region_name"): parts.append(f"{s['region_name']} 지역")
    if s.get("zone_name"):   parts.append(f"{s['zone_name']} 구역")
    return " / ".join(parts) if parts else "설정 안 됨"

# ── Setup(scope 설정) 키보드 빌더 ─────────────────────────────────────────────
def kb_setup_church() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"⛪ {ch}", callback_data=f"scope_ch:{ch}")] for ch in CHURCHES]
    rows.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)

def kb_setup_dept(church: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(f"🏛 {dp}", callback_data=f"scope_dp:{dp}")] for dp in DEPTS]
    rows.append([InlineKeyboardButton("◀ 교회 다시 선택", callback_data="scope_setup")])
    rows.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(rows)

def kb_setup_region() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="scope_setup_back_dept")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])

def kb_setup_zone() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏭ 지역까지만 완료 (구역 없이)", callback_data="scope_stop:region")],
        [InlineKeyboardButton("◀ 지역 다시 입력", callback_data="scope_setup_back_region")],
        [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")],
    ])

async def setup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await ensure_authorized(update): return
    chat_id = update.effective_chat.id
    user = update.effective_user
    ok, reason = await check_scope_owner(chat_id, user.id if user else 0)
    if not ok:
        await safe_reply_text(update.message, f"❌ {reason}", parse_mode="Markdown")
        return
    await save_ctx(chat_id, editing_step="awaiting_scope_church")
    current = await get_chat_scope(chat_id)
    cur_txt = f"\n\n📌 현재 설정: *{md(scope_label(current))}*" if current else ""
    await safe_reply_text(
        update.message,
        f"🔧 *방 담당 범위 설정*{cur_txt}\n\n이 방에서 관리할 범위를 순서대로 선택하세요.\n*① 교회* 를 먼저 선택하세요 👇\n\n💡 교회만 설정해도 되고, 더 상세히 (부서/지역/구역) 설정할 수도 있습니다.",
        parse_mode="Markdown", reply_markup=kb_setup_church()
    )

async def myscope_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    s = await get_chat_scope(chat_id)
    if not s:
        await safe_reply_text(
            update.message,
            "📌 이 방은 아직 범위가 설정되지 않았습니다.\n`/setup` 으로 먼저 담당 범위를 설정하세요.",
            parse_mode="Markdown",
        )
        return
    owner = s.get("owner_name") or "(미기록)"
    txt = f"📌 *이 방의 담당 범위*\n\n{md(scope_label(s))}\n\n👤 최초 설정자: *{md(owner)}*\n\n변경하려면 `/setup` (최초 설정자만 가능)"
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
        f"✅ *① 교회*: {md(church)}\n\n*② 부서*를 선택하세요 👇\n\n⚠️ _지역까지 설정해야 결석자를 볼 수 있습니다._",
        parse_mode="Markdown", reply_markup=kb_setup_dept(church)
    )

async def _on_scope_dept(update: Update, chat_id: int, dept: str):
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    church = ctx.get("church_filter") or ""
    await save_ctx(chat_id, church_filter=church, dept_filter=dept, editing_step="awaiting_scope_region_text")
    await q.edit_message_text(
        f"✅ *① 교회*: {md(church)}\n✅ *② 부서*: {md(dept)}\n\n*③ 지역* 이름을 입력하세요. (필수)\n예) `강북`, `강남`, `노원`, `성북`, `중랑`, `대학`\n\n⚠️ _지역까지는 반드시 설정해야 합니다._\n_구역까지 더 좁히려면 지역 입력 후 다음 화면에서 선택._",
        parse_mode="Markdown", reply_markup=kb_setup_region()
    )

async def _on_scope_stop(update: Update, chat_id: int, stop_level: str):
    q = update.callback_query
    user = update.effective_user
    if stop_level in ("church", "dept"):
        await q.edit_message_text(
            "❌ <b>지역까지 설정해야 합니다</b>\n\n결석자 정보 보호를 위해 최소 <b>지역</b> 단위까지\n담당 범위를 설정해야 합니다.\n\n부서를 선택하고 지역을 입력해주세요.",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ 부서 다시 선택", callback_data="scope_setup_back_dept")], [InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")]]),
        )
        return

    ctx = await get_ctx(chat_id)
    church = ctx.get("church_filter")
    dept   = ctx.get("dept_filter") if stop_level in ("dept","region","zone") else None
    region = ctx.get("region_filter") if stop_level in ("region","zone") else None
    zone   = None 

    if not church: await q.edit_message_text("❌ 교회 정보가 없습니다. /setup 다시 시작."); return
    if not dept: await q.edit_message_text("❌ 부서가 설정되지 않았습니다. /setup 다시 시작."); return
    if not region: await q.edit_message_text("❌ 지역이 설정되지 않았습니다. /setup 다시 시작."); return

    owner_name = (user.full_name if user else "") or (user.username if user else "")
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""

    await save_chat_scope(
        chat_id, chat_title, church=church, dept=dept, region_name=region, zone_name=zone,
        owner_user_id=user.id if user else None, owner_name=owner_name,
    )
    await clear_tmp(chat_id)

    new_scope = {"church": church, "dept": dept, "region_name": region, "zone_name": zone}
    await q.edit_message_text(
        f"🎉 <b>방 범위 설정 완료</b>\n\n📌 {_escape_html(scope_label(new_scope))}\n👤 최초 설정자: <b>{_escape_html(owner_name or '(미기록)')}</b>\n\n이제 📋 결석자 심방 에서 이 범위의 결석자만 표시됩니다.\n범위 확인: /myscope\n변경(최초 설정자만): /setup",
        parse_mode="HTML",
    )
    await q.message.reply_text("🏠 *메인 메뉴*", parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))

async def _on_scope_text_input(update: Update, chat_id: int, text: str):
    ctx = await get_ctx(chat_id)
    step = ctx.get("editing_step")
    user = update.effective_user

    if step == "awaiting_scope_region_text":
        region = text.strip()
        if not region:
            await safe_reply_text(update.message, "⚠️ 지역 이름이 비어있습니다. 다시 입력해주세요.\n예: `강북`, `노원`", parse_mode="Markdown", reply_markup=kb_setup_region())
            return True

        await save_ctx(chat_id, region_filter=region, editing_step="awaiting_scope_zone_text")
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        await safe_reply_text(
            update.message,
            f"✅ *① 교회*: {md(church)}\n✅ *② 부서*: {md(dept)}\n✅ *③ 지역*: {md(region)}\n\n*④ 구역* 이름을 입력하세요. (선택)\n예) `1-1`, `1팀1`, `2-3`\n\n💡 지역까지만 완료하려면 아래 `⏭ 지역까지만 완료` 버튼",
            parse_mode="Markdown", reply_markup=kb_setup_zone()
        )
        return True

    if step == "awaiting_scope_zone_text":
        zone = text.strip()
        church = ctx.get("church_filter") or ""
        dept = ctx.get("dept_filter") or ""
        region = ctx.get("region_filter") or ""

        if not region:
            await safe_reply_text(update.message, "❌ 지역 정보가 세션에서 사라졌습니다.\n/setup 으로 다시 시작해주세요.")
            return True

        owner_name = (user.full_name if user else "") or (user.username if user else "")
        chat_title = update.effective_chat.title or update.effective_chat.full_name or ""

        await save_chat_scope(chat_id, chat_title, church=church, dept=dept, region_name=region, zone_name=zone, owner_user_id=user.id if user else None, owner_name=owner_name)
        await clear_tmp(chat_id)

        new_scope = {"church": church, "dept": dept, "region_name": region, "zone_name": zone}
        await safe_reply_text(
            update.message,
            f"🎉 *방 범위 설정 완료*\n\n📌 {md(scope_label(new_scope))}\n👤 최초 설정자: *{md(owner_name or '(미기록)')}*\n\n이제 `📋 결석자 심방` 에서 이 범위의 결석자만 표시됩니다.",
            parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update))
        )
        return True
    return False

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or update.effective_chat.full_name or ""
    week_key, week_label = await get_active_week()

    await update.message.reply_text(
        f"👋 *결석자 타겟 심방 봇*에 오신 것을 환영합니다\n📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n━━━━━━━━━━━━━━━━━━━━\n\n⌨️ 하단 키보드로 시작하세요 👇",
        parse_mode="Markdown", reply_markup=kb_reply_main(is_private_chat(update))
    )

    is_private = is_private_chat(update)
    user = update.effective_user
    is_admin = user and await is_bot_admin_user(user.id)

    if is_private:
        if not is_admin:
            authorized = await is_chat_authorized(chat_id)
            if not authorized:
                await update.message.reply_text(unauthorized_message(chat_id, chat_title), parse_mode="HTML", reply_markup=kb_request_approval())
                return
            await record_chat_access(chat_id)
    else:
        authorized = await is_chat_authorized(chat_id)
        if not authorized:
            await update.message.reply_text(unauthorized_message(chat_id, chat_title), parse_mode="HTML", reply_markup=kb_request_approval())
            return
        await record_chat_access(chat_id)

    scope = await get_chat_scope(chat_id)
    if not scope:
        if is_private:
            await update.message.reply_text("🏠 *메인 메뉴*", parse_mode="Markdown", reply_markup=kb_main_menu(is_private))
        else:
            await update.message.reply_text(
                f"✅ 이 방은 승인되었습니다.\n\n📌 *담당 범위 설정이 필요합니다.*\n\n이 방에서 관리할 *교회 / 부서 / 지역 / 구역*을 설정해야\n결석자 목록이 해당 범위로 자동 필터링됩니다.\n\n아래 `🔧 방 범위 설정` 버튼을 눌러 시작하세요 👇",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")], [InlineKeyboardButton("📘 사용법", callback_data="show_help")]])
            )
    else:
        await update.message.reply_text(f"📌 *이 방의 담당 범위*: {md(scope_label(scope))}\n\n🏠 *메인 메뉴*", parse_mode="Markdown", reply_markup=kb_main_menu(is_private))

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    week_key, week_label = await get_active_week()
    txt = f"🏠 *메인 메뉴*\n\n📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n아래 버튼에서 원하는 기능을 선택하세요 👇\n\n💡 사용법은 *📘 사용법* 버튼 또는 `/help`"
    await update.message.reply_text(txt, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))
    await update.message.reply_text("⌨️ 하단 키보드 메뉴 활성화", reply_markup=kb_reply_main(is_private_chat(update)))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _send_help(update)

async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_tmp(chat_id)
    await update.message.reply_text("🚫 현재 작업을 취소했습니다.\n/menu 로 메인 메뉴로.")

async def diagnose_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lines = ["🔍 *DB 진단 결과*", "━━━━━━━━━━━━━━━━━━━━"]
    try:
        weeks = await sb_get("weekly_target_weeks?select=week_key,week_label&order=week_key.desc&limit=5")
        if weeks:
            lines.append(f"✅ 주차 {len(weeks)}개 등록됨:")
            for w in weeks: lines.append(f"   • `{md(w['week_key'])}` — {md(w.get('week_label',''))}")
        else:
            lines.append("❌ 등록된 주차 없음 → 웹에서 명단 업로드 필요")
    except Exception as e:
        lines.append(f"❌ 주차 조회 실패: {md(str(e))[:100]}")

    try:
        week_key, _ = await get_active_week()
        if week_key:
            cnt_rows = await sb_get(f"weekly_visit_targets?select=dept,church&week_key=eq.{quote(week_key)}&limit=1000")
            total = len(cnt_rows)
            by_church = {}
            by_dept   = {}
            for r in cnt_rows:
                c = r.get("church") or "(미지정)"
                d = r.get("dept") or "(미지정)"
                by_church[c] = by_church.get(c, 0) + 1
                by_dept[d]   = by_dept.get(d, 0) + 1
            lines.append(f"\n✅ 주차 `{md(week_key)}` 결석자 {total}명:")
            for c, n in sorted(by_church.items()): lines.append(f"   • {md(c)}: {n}명")
            lines.append("   _부서별:_")
            for d, n in sorted(by_dept.items()): lines.append(f"   • {md(d)}: {n}명")
    except Exception as e:
        lines.append(f"❌ 결석자 조회 실패: {md(str(e))[:100]}")

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
            if "없습니다" in str(e): lines.append(f"   ❌ `{fn}` — SQL 마이그레이션 필요")
            else: lines.append(f"   ⚠️ `{fn}` — {md(str(e))[:80]}")
        except Exception as e:
            lines.append(f"   ✅ `{fn}` (존재)")

    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))


# ═════════════════════════════════════════════════════════════════════════════
# 특별관리 그룹당 1명 제한 확인 로직
# ═════════════════════════════════════════════════════════════════════════════
async def _handle_existing_group_sp(update: Update, chat_id: int, send_new: bool) -> bool:
    """그룹방이 이미 특별관리방으로 지정된 경우, 해당 결석자 상세화면으로 즉시 이동"""
    if update.effective_chat.type == "private":
        return False
    try:
        existing = await sb_get(f"special_management_targets?select=dept,name,phone_last4&monitor_chat_id=eq.{chat_id}&limit=1")
        if existing:
            t = existing[0]
            church = ""
            try:
                scope = await get_chat_scope(chat_id)
                if scope: church = scope.get("church", "")
            except Exception:
                pass
            await save_ctx(chat_id, church_filter=church, dept_filter=t.get("dept",""), tmp_sp_name=t.get("name",""), tmp_sp_phone=t.get("phone_last4",""))
            await _show_sp_detail(update, chat_id, church, t.get("dept",""), t.get("name",""), t.get("phone_last4",""), send_new=send_new)
            return True
    except Exception as e:
        logger.error(e)
        pass
    return False


# ═════════════════════════════════════════════════════════════════════════════
# 콜백 (버튼) 디스패처
# ═════════════════════════════════════════════════════════════════════════════
async def button_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    data = q.data
    chat_id = update.effective_chat.id

    if data == "request_approval":
        await request_approval_callback(update, context)
        return
    if data.startswith("admin_approve:"):
        await admin_approve_callback(update, context)
        return
    if data.startswith("admin_deny:"):
        await admin_deny_callback(update, context)
        return

    await q.answer()

    try:
        if data == "m:home":
            await _show_home(update)
        elif data == "m:absentee":
            await clear_tmp(chat_id)
            await _show_church_select(update, "abs")
        elif data == "m:special":
            await clear_tmp(chat_id)
            if not await _handle_existing_group_sp(update, chat_id, send_new=False):
                await _show_church_select(update, "sp")
        elif data == "m:help":
            try:
                await q.message.reply_text(HELP_TEXT, parse_mode="HTML", reply_markup=kb_main_menu(is_private_chat(update)))
            except Exception as he:
                logger.warning("help HTML 실패: %s", he)
                plain = (HELP_TEXT.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","").replace("<code>","").replace("</code>",""))
                await q.message.reply_text(plain, reply_markup=kb_main_menu(is_private_chat(update)))
        elif data == "m:diagnose":
            class FakeUpd:
                effective_chat = update.effective_chat
                message = q.message
            await diagnose_command(FakeUpd(), context)

        elif data.startswith("abs_ch:"):
            church = data.split(":", 1)[1]
            await _on_abs_church(update, chat_id, church)
        elif data.startswith("abs_dp:"):
            _, church, dept = data.split(":", 2)
            await _on_abs_dept(update, chat_id, church, dept)
        elif data.startswith("abs_sel:"):
            row_id = data.split(":", 1)[1]
            await _on_abs_select(update, chat_id, row_id)
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
            await clear_tmp(chat_id)
            await q.message.reply_text("🚫 입력이 취소되었습니다.", reply_markup=kb_main_menu(is_private_chat(update)))

        elif data == "scope_setup":
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
            await q.edit_message_text(f"✅ *① 교회*: {md(church)}\n\n*② 부서*를 선택하세요.", parse_mode="Markdown", reply_markup=kb_setup_dept(church))
        elif data == "scope_setup_back_region":
            ctx = await get_ctx(chat_id)
            church = ctx.get("church_filter") or ""
            dept = ctx.get("dept_filter") or ""
            await save_ctx(chat_id, editing_step="awaiting_scope_region_text")
            await q.edit_message_text(f"✅ *① 교회*: {md(church)}\n✅ *② 부서*: {md(dept)}\n\n*③ 지역* 이름을 다시 입력하세요.", parse_mode="Markdown", reply_markup=kb_setup_region())
        elif data == "show_help":
            await _send_help(update if update.message else type('X',(),{'message':q.message})())

        elif data.startswith("sp_ch:"):
            church = data.split(":", 1)[1]
            await _on_sp_church(update, chat_id, church)
        elif data.startswith("sp_dp:"):
            _, church, dept = data.split(":", 2)
            await _on_sp_dept(update, chat_id, church, dept)
        elif data.startswith("sp_pk:"):
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
    txt = f"🏠 *메인 메뉴*\n\n📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n원하는 기능을 선택하세요 👇"
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_main_menu(is_private_chat(update)))

async def _show_church_select(update: Update, flow: str):
    if not await ensure_authorized(update): return
    q = update.callback_query
    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    if not scope:
        chat_type_hint = "개인방" if is_private_chat(update) else "그룹방"
        await q.edit_message_text(
            f"📌 <b>{chat_type_hint}에서도 담당 범위 설정이 필요합니다</b>\n\n결석자 정보 보호를 위해 이 방에서 볼 수 있는 범위를\n미리 설정해야 합니다 (<b>지역까지 필수</b>).\n\n• 교회 → 부서 → 지역 (→ 구역)\n\n아래 버튼을 눌러 설정하세요 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")], [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")]]),
        )
        return
    await _scope_jump(update, chat_id, scope, flow)

async def _show_church_menu(update: Update, flow: str):
    if not await ensure_authorized(update): return
    chat_id = update.effective_chat.id
    scope = await get_chat_scope(chat_id)

    if not scope:
        chat_type_hint = "개인방" if is_private_chat(update) else "그룹방"
        await update.message.reply_text(
            f"📌 <b>{chat_type_hint}에서도 담당 범위 설정이 필요합니다</b>\n\n결석자 정보 보호를 위해 이 방에서 볼 수 있는 범위를\n미리 설정해야 합니다 (<b>지역까지 필수</b>).\n\n아래 버튼을 눌러 설정하세요 👇",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔧 방 범위 설정", callback_data="scope_setup")]]),
        )
        return
    await _scope_jump_from_message(update, chat_id, scope, flow)

async def _scope_jump(update: Update, chat_id: int, scope: dict, flow: str):
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.", reply_markup=kb_main_menu(is_private_chat(update)))
        return

    church = scope.get("church"); dept = scope.get("dept"); region = scope.get("region_name"); zone = scope.get("zone_name")
    await save_ctx(chat_id, active_week_key=week_key, church_filter=church, dept_filter=dept)

    rows = await _fetch_scoped(week_key, church, dept, region, zone, flow)
    scope_txt = scope_label(scope)

    if flow == "sp": header = f"🚨 <b>특별관리결석자</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"
    else: header = f"📋 <b>결석자 심방</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"

    if not rows:
        await q.edit_message_text(header + "\n📭 해당 범위의 결석자가 없습니다.", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")]]))
        return

    keyboard = _build_absentee_buttons(rows, flow)
    keyboard.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    await q.edit_message_text(header + f"\n총 <b>{len(rows)}</b>명\n결석자를 선택하세요 👇", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

async def _scope_jump_from_message(update: Update, chat_id: int, scope: dict, flow: str):
    week_key, week_label = await get_active_week()
    if not week_key:
        await update.message.reply_text("❌ 등록된 주차가 없습니다.")
        return

    church = scope.get("church"); dept = scope.get("dept"); region = scope.get("region_name"); zone = scope.get("zone_name")
    await save_ctx(chat_id, active_week_key=week_key, church_filter=church, dept_filter=dept)

    rows = await _fetch_scoped(week_key, church, dept, region, zone, flow)
    scope_txt = scope_label(scope)

    if flow == "sp": header = f"🚨 <b>특별관리결석자</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"
    else: header = f"📋 <b>결석자 심방</b>\n📌 {_escape_html(scope_txt)} · {_escape_html(week_label or week_key)}\n"

    if not rows:
        await update.message.reply_text(header + "\n📭 해당 범위의 결석자가 없습니다.", parse_mode="HTML")
        return

    keyboard = _build_absentee_buttons(rows, flow)
    keyboard.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])
    await update.message.reply_text(header + f"\n총 <b>{len(rows)}</b>명\n결석자를 선택하세요 👇", parse_mode="HTML", reply_markup=InlineKeyboardMarkup(keyboard))

def _escape_html(s) -> str:
    import html as _h
    return _h.escape(str(s)) if s is not None else ""

async def _fetch_scoped(week_key, church, dept, region, zone, flow):
    path = f"weekly_visit_targets?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count&week_key=eq.{quote(week_key)}&church=eq.{quote(church)}"
    if dept:   path += f"&dept=eq.{quote(dept)}"
    if region: path += f"&region_name=eq.{quote(region)}"
    if zone:   path += f"&zone_name=eq.{quote(normalize_zone(zone))}"
    if flow == "sp":
        path += "&consecutive_absent_count=gte.4&order=consecutive_absent_count.desc,name.asc"
    else:
        path += "&order=dept.asc,region_name.asc,zone_name.asc,name.asc"
    path += "&limit=5000"

    rows = await sb_get(path)
    return await enrich_names(rows or [])

def _build_absentee_buttons(rows, flow, max_buttons=40):
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

async def _on_abs_church(update: Update, chat_id: int, church: str):
    q = update.callback_query
    await save_ctx(chat_id, church_filter=church)
    txt = f"📋 *결석자 심방*\n\n✅ 교회: *{md(church)}*\n\n② *부서* 를 선택하세요 👇"
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=kb_dept_select("abs", church))

async def _on_abs_dept(update: Update, chat_id: int, church: str, dept: str):
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.\n웹 대시보드에서 명단을 먼저 업로드해주세요.", reply_markup=kb_main_menu(is_private_chat(update)))
        return

    await save_ctx(chat_id, active_week_key=week_key, church_filter=church, dept_filter=dept, editing_step="awaiting_region_or_zone")
    txt = f"📋 *결석자 심방*\n✅ {md(church)} / {md(dept)} / `{md(week_label)}`\n\n③ *지역 또는 구역명*을 입력하세요 👇\n\n• 지역 예) `강북`, `강남`, `강서`, `강동`, `노원`\n• 구역 예) `2-1` 또는 `2팀1` (둘 다 동일)"
    await q.edit_message_text(txt, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"abs_ch:{church}")], [InlineKeyboardButton("❌ 입력 취소", callback_data="flow_cancel")]]))

async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    ctx_pre = await get_ctx(chat_id)
    pre_step = (ctx_pre.get("editing_step", "") if ctx_pre else "") or ""
    if not pre_step:
        if text == "📋 결석자 심방":
            await clear_tmp(chat_id)
            await _show_church_menu(update, "abs")
            return
        if text == "🚨 특별관리결석자":
            await clear_tmp(chat_id)
            if not await _handle_existing_group_sp(update, chat_id, send_new=True):
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
                await update.message.reply_text("⚠️ 미니앱 폼은 *개인 채팅*에서만 열 수 있습니다.\n봇과 1:1 채팅을 시작한 다음 사용해주세요.", parse_mode="Markdown")
                return
            if MINIAPP_URL.startswith("https://"):
                await update.message.reply_text("📝 아래 버튼을 탭하면 결석자 심방 기록 폼이 열립니다.\n\n폼에서:\n1️⃣ 결석자 이름/전화뒷4/교회/부서로 검색\n2️⃣ 기존 기록이 있으면 자동으로 불러옴\n3️⃣ 부족한 내용 보충하거나 수정 후 저장", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📝 폼 열기", web_app=WebAppInfo(url=MINIAPP_URL))]]))
            else:
                await update.message.reply_text("⚠️ 미니웹앱 URL이 설정되지 않았습니다. (HTTPS 필수)\n관리자에게 MINIAPP_URL 환경변수 설정을 요청하세요.")
            return

    ctx = ctx_pre
    if not ctx: return

    step = pre_step

    if step in ("awaiting_scope_region_text", "awaiting_scope_zone_text"):
        handled = await _on_scope_text_input(update, chat_id, text)
        if handled: return

    if step == "awaiting_region_or_zone":
        church = ctx.get("church_filter", "")
        dept   = ctx.get("dept_filter", "")
        week_key = ctx.get("active_week_key", "")
        if not (church and dept and week_key):
            await update.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
            return

        try:
            wrows = await sb_get(f"weekly_target_weeks?select=week_label&week_key=eq.{quote(week_key)}&limit=1")
            week_label = wrows[0]["week_label"] if wrows else week_key
        except Exception:
            week_label = week_key

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
            hint = ""
            try:
                all_rows = await sb_get(f"weekly_visit_targets?select=region_name,zone_name&week_key=eq.{quote(week_key)}&church=eq.{quote(church)}&dept=eq.{quote(dept)}&limit=500")
                regions = sorted(set(r.get("region_name","") for r in all_rows if r.get("region_name")))
                zones   = sorted(set(r.get("zone_name","")   for r in all_rows if r.get("zone_name")))
                if regions: hint += "\n\n📍 사용 가능한 지역:\n" + ", ".join(f"`{md(r)}`" for r in regions[:20])
                if zones: hint += "\n📍 사용 가능한 구역:\n" + ", ".join(f"`{md(z)}`" for z in zones[:15])
                if not regions and not zones: hint = "\n\n_이 교회/부서에 등록된 결석자가 없습니다._"
            except Exception:
                pass

            await update.message.reply_text(f"📭 *{md(church)} / {md(dept)} / {query_kind}: {md(query_label)}*\n주차: `{md(week_label)}`\n결석자가 없습니다.{hint}\n\n다시 입력하거나 /menu", parse_mode="Markdown")
            await save_ctx(chat_id, editing_step="awaiting_region_or_zone")
            return

        MAX_BUTTONS = 40
        shown = absentees[:MAX_BUTTONS]
        overflow_abs = len(absentees) - MAX_BUTTONS
        buttons = []
        for ab in shown:
            name   = ab.get("name", "?")
            zone   = ab.get("zone_name", "") or ""
            streak = ab.get("consecutive_absent_count", 0) or 0
            label = f"{name} · 연속{streak}회" if query_kind == "구역" else f"{name} {zone} · 연속{streak}회"
            if len(label) > 60: label = label[:57] + "..."
            buttons.append([InlineKeyboardButton(label, callback_data=f"abs_sel:{ab['row_id']}")])
        buttons.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])

        overflow_note = f"\n\n_(+ {overflow_abs}명은 화면 제한으로 생략 — 더 정확한 지역/구역명으로 다시 검색해주세요)_" if overflow_abs > 0 else ""
        await update.message.reply_text(f"📋 *{md(church)} / {md(dept)} / {query_kind}: {md(query_label)}*\n주차: `{md(week_label)}` | 총 {len(absentees)}명\n\n심방 기록할 결석자를 선택하세요 👇{overflow_note}", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(buttons))
        return

    # 특별관리 3, 4번 텍스트 입력 시
    if step in ("awaiting_sp3", "awaiting_sp4"):
        import html as _html
        church = ctx.get("church_filter", "")
        dept   = ctx.get("dept_filter", "")
        name   = ctx.get("tmp_sp_name", "")
        phone  = ctx.get("tmp_sp_phone", "")
        which  = "3" if step == "awaiting_sp3" else "4"
        fn = "set_special_item3" if which == "3" else "set_special_item4"
        try:
            # 1) 특별관리 대상자 테이블 업데이트
            await sb_rpc(fn, {"p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": text})
            
            # 2) [추가] 일반 결석자 심방 기록(weekly_visit_progress)에도 동기화
            try:
                week_key, _ = await get_active_week()
                if week_key:
                    search_path = f"weekly_visit_targets?select=row_id,name&week_key=eq.{quote(week_key)}&dept=eq.{quote(dept)}"
                    if phone:
                        search_path += f"&phone_last4=eq.{quote(phone)}"
                    t_rows = await sb_get(search_path + "&limit=30")
                    
                    target_row_id = None
                    if t_rows:
                        enriched_t = await enrich_names(t_rows)
                        for r in enriched_t:
                            if r.get("name") == name:
                                target_row_id = r.get("row_id")
                                break
                    
                    if target_row_id:
                        prog = await get_progress(week_key, target_row_id)
                        if not prog:
                            prog = {}
                        
                        visit_date = text if which == "3" else (prog.get("visit_date_display") or "")
                        plan = text if which == "4" else (prog.get("plan_text") or "")
                        date_sort = _parse_visit_date_to_iso(visit_date)
                        
                        await sb_rpc("upsert_weekly_visit_progress", {
                            "p_week_key":           week_key,
                            "p_row_id":             target_row_id,
                            "p_shepherd":           prog.get("shepherd", ""),
                            "p_visit_date_display": visit_date,
                            "p_visit_date_sort":    date_sort,
                            "p_plan_text":          plan,
                            "p_is_target":          bool(prog.get("is_target")),
                            "p_is_done":            bool(prog.get("is_done")),
                            "p_worship":            prog.get("attend_confirm"),
                            "p_attendance":         prog.get("attendance"),
                            "p_note":               prog.get("note", ""),
                        })
            except Exception as sync_err:
                logger.warning("특별관리 -> 일반기록 동기화 실패: %s", sync_err)

        except Exception as e:
            await update.message.reply_text(f"❌ 저장 실패: {e}")
            return
            
        await save_ctx(chat_id, editing_step="")
        label_ko = "심방예정일" if which == "3" else "심방계획"
        try:
            await update.message.reply_text(f"✅ <b>금주 {label_ko}</b> 저장됨: <code>{_html.escape(str(text))}</code>", parse_mode="HTML")
        except Exception:
            await update.message.reply_text(f"✅ 금주 {label_ko} 저장됨: {text}")
        await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=True)
        return

    is_single_edit = step.startswith("edit_")
    if is_single_edit: step = step[5:]

    if step in STEPS:
        RESERVED_LABELS = {"📋 결석자 심방", "🚨 특별관리결석자", "📘 사용법", "🏠 메인 메뉴", "📝 결석자 심방 기록 (폼)"}
        if text in RESERVED_LABELS:
            await update.message.reply_text(f"⚠️ 아직 *{md(STEP_LABELS[step])}* 를 입력하지 않으셨습니다.\n\n현재 단계 입력을 먼저 완료해주세요.\n중단하려면 ❌ 취소 버튼 또는 `/cancel`", parse_mode="Markdown", reply_markup=kb_cancel_only())
            return

        if step in STEP_CHOICES:
            valid_choices = []
            for row in STEP_CHOICES[step]: valid_choices.extend(row)
            if text not in valid_choices:
                rows = STEP_CHOICES[step]
                keyboard = [[InlineKeyboardButton(c, callback_data=f"choice:{step}:{c}") for c in row] for row in rows]
                keyboard.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
                await update.message.reply_text(f"⚠️ *{md(STEP_LABELS[step])}* 는 *아래 버튼 중 하나*를 선택해주세요.\n\n직접 입력된 값: `{md(text)}`\n허용 값: {', '.join(f'`{c}`' for c in valid_choices)}", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
                return

        if step == "shepherd":
            if len(text) < 2:
                await update.message.reply_text(f"⚠️ *심방자 이름*이 너무 짧습니다.\n\n예: `홍길동(집사)`, `김영희/구역장`, `박철수 목사`\n다시 입력해주세요:", parse_mode="Markdown", reply_markup=kb_cancel_only())
                return
        if step == "date":
            import re as _re
            patterns = [r"^\d{1,2}[/.\-]\d{1,2}$", r"^\d{4}[-/.]\d{1,2}[-/.]\d{1,2}$", r"^\d{1,2}월\s*\d{1,2}일$", r"^\d{4}년\s*\d{1,2}월\s*\d{1,2}일$"]
            if not any(_re.match(p, text) for p in patterns):
                await update.message.reply_text(f"⚠️ *심방날짜 형식*이 올바르지 않습니다.\n\n허용되는 형식:\n• `4/27` 또는 `4-27` 또는 `4.27`\n• `2026-04-27` 또는 `2026/4/27` 또는 `2026.4.27`\n• `4월 27일`\n• `2026년 4월 27일`\n\n입력된 값: `{md(text)}`\n다시 입력해주세요:", parse_mode="Markdown", reply_markup=kb_cancel_only())
                return
        if step == "plan":
            if len(text) < 3:
                await update.message.reply_text(f"⚠️ *심방계획*이 너무 짧습니다 (3자 이상).\n\n예: `생일축하 겸 안부 방문`, `카페에서 말씀 나눔`\n다시 입력해주세요:", parse_mode="Markdown", reply_markup=kb_cancel_only())
                return
        if step == "note":
            if len(text) < 2 and text != "없음" and text != "-":
                await update.message.reply_text(f"⚠️ *진행사항*이 너무 짧습니다.\n\n내용이 없으면 `없음` 이라고 입력해주세요.\n다시 입력해주세요:", parse_mode="Markdown", reply_markup=kb_cancel_only())
                return

        tmp_key = f"tmp_{step}"
        await save_ctx(chat_id, **{tmp_key: text})

        if is_single_edit:
            await _save_single_edit_and_show_menu(update, chat_id)
            return

        step_idx = STEPS.index(step)
        await _next_step(update, chat_id, step_idx, ctx)

async def _save_single_edit_and_show_menu(update, chat_id: int):
    import html as _html
    ctx = await get_ctx(chat_id)
    if not ctx: return
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

    await save_ctx(chat_id, editing_step="")

    prog = await get_progress(week_key, row_id)
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched: name = enriched[0].get("name", name) or name

    try:
        await update.message.reply_text(f"✅ <b>{_html.escape(str(name))}</b> — 수정 저장 완료", parse_mode="HTML")
    except Exception:
        await update.message.reply_text(f"✅ {name} — 수정 저장 완료")

    class FakeQ: message = update.message
    class FakeUpd: callback_query = FakeQ(); effective_chat = update.effective_chat; message = update.message
    if prog: await _show_edit_menu(FakeUpd(), chat_id, row_id, name, prog)

async def _on_abs_select(update: Update, chat_id: int, row_id: str):
    import html as _html
    q = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await q.message.reply_text("❌ 세션 만료. /menu")
        return

    week_key = ctx.get("active_week_key", "")
    prog = await get_progress(week_key, row_id)
    rows = await sb_get(f"weekly_visit_targets?select=name,region_name,zone_name,church,dept,phone_last4&row_id=eq.{quote(row_id)}&week_key=eq.{quote(week_key)}")
    if rows:
        enriched = await enrich_names(rows)
        name = enriched[0]["name"] if enriched else row_id
    else:
        name = row_id

    has_record = bool(prog)
    all_filled = has_record and all((prog.get(k) is not None and prog.get(k) != "") for k in ["shepherd", "visit_date_display", "plan_text"])

    if all_filled:
        await _show_edit_menu(update, chat_id, row_id, name, prog)
        return

    start_step = "shepherd"
    if has_record:
        for s in STEPS:
            key_map = {"shepherd":"shepherd", "date":"visit_date_display", "plan":"plan_text", "target":"is_target", "done":"is_done", "worship":"attend_confirm", "note":"note"}
            val = prog.get(key_map.get(s, s))
            if val is None or val == "":
                start_step = s
                break
        else:
            start_step = "shepherd"

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
        existing = (f"\n\n📂 <b>기존 입력값</b>\n심방자: {_html.escape(prog.get('shepherd','') or '없음')}\n심방날짜: {_html.escape(prog.get('visit_date_display','') or '없음')}\n심방계획: {_html.escape((prog.get('plan_text','') or '없음')[:50])}\n<i>빠진 부분부터 이어서 입력하세요.</i>")

    step_idx = STEPS.index(start_step) + 1
    try:
        await q.message.reply_text(f"✏️ <b>{_html.escape(str(name))}</b> 님 심방 기록{existing}\n\n{step_idx}️⃣ {_html.escape(STEP_LABELS[start_step])}\n입력해주세요:\n\n<i>중단하려면 ❌ 취소 버튼 또는 /cancel</i>", parse_mode="HTML", reply_markup=kb_cancel_only() if start_step not in STEP_CHOICES else _kb_choice(start_step))
    except Exception as e:
        logger.warning("HTML parse 실패, 평문으로 전송: %s", e)
        await q.message.reply_text(f"✏️ {name} 님 심방 기록\n\n{step_idx}️⃣ {STEP_LABELS[start_step]}\n입력해주세요:", reply_markup=kb_cancel_only() if start_step not in STEP_CHOICES else _kb_choice(start_step))

def _kb_choice(step: str) -> InlineKeyboardMarkup:
    rows = STEP_CHOICES.get(step, [])
    keyboard = [[InlineKeyboardButton(c, callback_data=f"choice:{step}:{c}") for c in row] for row in rows]
    keyboard.append([InlineKeyboardButton("❌ 취소", callback_data="flow_cancel")])
    return InlineKeyboardMarkup(keyboard)

async def _show_edit_menu(update, chat_id: int, row_id: str, name: str, prog: dict):
    import html as _html
    q = update.callback_query
    def fmt(v, true_label="✅", false_label="❌"):
        if v is None or v == "": return "<i>미입력</i>"
        if isinstance(v, bool): return true_label if v else false_label
        return _html.escape(str(v))

    text = f"📝 <b>{_html.escape(str(name))}</b> 님 심방 기록 (저장됨)\n━━━━━━━━━━━━━━━━━━━━\n\n① 심방자: {fmt(prog.get('shepherd'))}\n② 심방날짜: {fmt(prog.get('visit_date_display'))}\n③ 심방계획: {fmt(prog.get('plan_text'))}\n④ 타겟여부: {fmt(prog.get('is_target'), '타겟', '미타겟')}\n⑤ 진행여부: {fmt(prog.get('is_done'), '완료', '미완료')}\n⑥ 예배확답: {fmt(prog.get('attend_confirm'))}\n⑦ 진행사항: {fmt(prog.get('note'))}\n\n<i>수정할 항목을 선택하세요 👇</i>"

    buttons = [
        [InlineKeyboardButton("① 심방자 수정",   callback_data="edit_step:shepherd"), InlineKeyboardButton("② 심방날짜 수정", callback_data="edit_step:date")],
        [InlineKeyboardButton("③ 심방계획 수정", callback_data="edit_step:plan"), InlineKeyboardButton("④ 타겟여부 수정", callback_data="edit_step:target")],
        [InlineKeyboardButton("⑤ 진행여부 수정", callback_data="edit_step:done"), InlineKeyboardButton("⑥ 예배확답 수정", callback_data="edit_step:worship")],
        [InlineKeyboardButton("⑦ 진행사항 수정", callback_data="edit_step:note")],
        [InlineKeyboardButton("🔄 전체 다시 입력", callback_data=f"edit_full:{row_id}")],
        [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")],
    ]

    await save_ctx(chat_id, editing_row_id=row_id, editing_step="")

    try:
        await q.message.reply_text(text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("edit menu HTML 실패: %s", e)
        plain = (text.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))
        await q.message.reply_text(plain, reply_markup=InlineKeyboardMarkup(buttons))

async def _on_edit_step(update: Update, chat_id: int, step: str):
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

    await save_ctx(chat_id, editing_step=f"edit_{step}", editing_row_id=row_id)
    ctx = await get_ctx(chat_id)
    tmp_key = f"tmp_{step}"
    if not ctx.get(tmp_key):
        week_key = ctx.get("active_week_key", "")
        prog = await get_progress(week_key, row_id)
        if prog:
            key_map = {"shepherd": prog.get("shepherd", ""), "date": prog.get("visit_date_display", ""), "plan": prog.get("plan_text", ""), "target": "타겟" if prog.get("is_target") else ("미타겟" if prog.get("is_target") is False else ""), "done": "완료" if prog.get("is_done") else ("미완료" if prog.get("is_done") is False else ""), "worship": prog.get("attend_confirm", ""), "note": prog.get("note", "")}
            await save_ctx(chat_id, **{tmp_key: key_map.get(step, "") or ""})

    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched: name = enriched[0].get("name", name) or name

    label = STEP_LABELS.get(step, step)
    step_idx = STEPS.index(step) + 1

    if step in STEP_CHOICES:
        await q.message.reply_text(f"✏️ <b>{_html.escape(str(name))}</b> 님 — <b>{_html.escape(label)}</b>\n{step_idx}번 항목만 수정합니다.\n\n아래에서 선택하세요:", parse_mode="HTML", reply_markup=_kb_choice(step))
    else:
        await q.message.reply_text(f"✏️ <b>{_html.escape(str(name))}</b> 님 — <b>{_html.escape(label)}</b>\n{step_idx}번 항목만 수정합니다.\n\n새 값을 입력해주세요 (취소: /cancel):", parse_mode="HTML", reply_markup=kb_cancel_only())

async def _on_edit_full(update: Update, chat_id: int, row_id: str):
    await clear_tmp(chat_id)
    await save_ctx(chat_id, editing_row_id=row_id, editing_step="shepherd")

    import html as _html
    q = update.callback_query
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    name = rows[0]["name"] if rows else row_id
    if rows:
        enriched = await enrich_names(rows)
        if enriched: name = enriched[0].get("name", name) or name

    try:
        await q.message.reply_text(f"🔄 <b>{_html.escape(str(name))}</b> 님 심방 기록 — 전체 다시 입력\n\n1️⃣ {_html.escape(STEP_LABELS['shepherd'])}\n입력해주세요:\n\n<i>중단하려면 ❌ 취소 버튼 또는 /cancel</i>", parse_mode="HTML", reply_markup=kb_cancel_only())
    except Exception:
        await q.message.reply_text(f"🔄 {name} 님 심방 기록 — 전체 다시 입력\n\n1️⃣ {STEP_LABELS['shepherd']}\n입력해주세요:", reply_markup=kb_cancel_only())

async def _on_choice(update: Update, chat_id: int, step: str, value: str):
    q = update.callback_query
    tmp_key = f"tmp_{step}"
    await save_ctx(chat_id, **{tmp_key: value})
    ctx = await get_ctx(chat_id)

    current_editing = ctx.get("editing_step", "") or ""
    if current_editing == f"edit_{step}":
        class FakeUpd: message = q.message; effective_chat = update.effective_chat
        await _save_single_edit_and_show_menu(FakeUpd(), chat_id)
        return

    step_idx = STEPS.index(step)
    class FakeUpd: message = q.message; effective_chat = update.effective_chat
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
        buttons = [[InlineKeyboardButton(c, callback_data=f"choice:{next_step}:{c}") for c in row] for row in choice_rows]
        await update.message.reply_text(f"{step_num}️⃣ {label}", reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update.message.reply_text(f"{step_num}️⃣ {label}\n입력해주세요:", reply_markup=kb_cancel_only())

async def _show_confirm(update, chat_id: int, ctx: dict):
    import html as _html
    row_id = ctx.get("editing_row_id", "")
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    if rows:
        enriched = await enrich_names(rows)
        name = enriched[0]["name"] if enriched else row_id
    else:
        name = row_id

    def _e(v): return _html.escape(str(v)) if v else "-"

    summary = f"📋 <b>심방 기록 확인</b> — {_e(name)}\n\n심방자: {_e(ctx.get('tmp_shepherd',''))}\n심방날짜: {_e(ctx.get('tmp_date',''))}\n심방계획: {_e(ctx.get('tmp_plan',''))}\n타겟여부: {_e(ctx.get('tmp_target',''))}\n진행여부: {_e(ctx.get('tmp_done',''))}\n예배확답: {_e(ctx.get('tmp_worship',''))}\n진행사항: {_e(ctx.get('tmp_note',''))}\n\n저장하시겠습니까?"
    buttons = [[InlineKeyboardButton("✅ 저장", callback_data="confirm_save"), InlineKeyboardButton("❌ 취소", callback_data="cancel_save")]]
    try:
        await update.message.reply_text(summary, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("HTML confirm 전송 실패, 평문으로 재시도: %s", e)
        plain_msg = (summary.replace("<b>","").replace("</b>",""))
        await update.message.reply_text(plain_msg, reply_markup=InlineKeyboardMarkup(buttons))

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
            await q.message.reply_text(f"✅ <b>{_html.escape(str(name))}</b> 님 심방 기록 저장 완료!\n\n계속하려면 /menu", parse_mode="HTML")
        except Exception as pe:
            logger.warning("HTML 완료 메시지 실패, 평문으로: %s", pe)
            await q.message.reply_text(f"✅ {name} 님 심방 기록 저장 완료!\n\n계속하려면 /menu")
    except Exception as e:
        logger.exception(e)
        try:
            err_txt = str(e)[:200]
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
    txt = f"🚨 <b>특별관리결석자</b>\n\n✅ 교회: <b>{_html.escape(church)}</b>\n\n② <b>부서</b> 를 선택하세요 👇"
    try:
        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=kb_dept_select("sp", church))
    except Exception as e:
        logger.warning("sp_church edit 실패: %s", e)
        await q.message.reply_text(txt.replace("<b>","").replace("</b>",""), reply_markup=kb_dept_select("sp", church))

async def _on_sp_dept(update: Update, chat_id: int, church: str, dept: str):
    import html as _html
    q = update.callback_query
    week_key, week_label = await get_active_week()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.", reply_markup=kb_main_menu(is_private_chat(update)))
        return

    targets = await fetch_absentees_4plus(week_key, church, dept)
    targets = await enrich_names(targets)
    if not targets:
        await q.edit_message_text(f"📭 <b>{_html.escape(church)} / {_html.escape(dept)}</b> 의 연속결석 4회 이상 결석자가 없습니다.\n(주차: {_html.escape(week_label or week_key)})", parse_mode="HTML", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"sp_ch:{church}")], [InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")]]))
        return

    try:
        registered = await sb_get(f"special_management_targets?select=name,phone_last4,monitor_chat_id&dept=eq.{quote(dept)}")
        registered_set = {(r.get("name",""), r.get("phone_last4","") or "") for r in registered}
    except Exception:
        registered_set = set()

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
        if len(label) > 60: label = label[:57] + "..."
        buttons.append([InlineKeyboardButton(label, callback_data=f"sp_pk:{row_id}")])
    buttons.append([InlineKeyboardButton("◀ 부서 다시 선택", callback_data=f"sp_ch:{church}")])
    buttons.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="m:home")])

    overflow_note = f"\n\n<i>(+ {overflow}명은 화면 제한으로 생략 — 연속결석 순 상위 {MAX_TARGETS}명만 표시)</i>" if overflow > 0 else ""
    txt = f"🚨 <b>{_html.escape(church)} / {_html.escape(dept)}</b> — 4회 이상 {len(targets)}명\n주차: {_html.escape(week_label or week_key)}\n\n🚨 = 특별관리 등록됨 (방 감지중)\n⚠️ = 아직 미등록\n\n관리할 결석자를 선택하세요 👇\n<i>(선택 시 이 방이 감지방으로 등록됩니다)</i>{overflow_note}"
    try:
        await q.edit_message_text(txt, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))
    except Exception as e:
        logger.warning("sp_dept edit 실패: %s", e)
        plain = txt.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>","")
        await q.message.reply_text(plain, reply_markup=InlineKeyboardMarkup(buttons))

async def _on_sp_pick(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    import html as _html
    q = update.callback_query
    chat = update.effective_chat

    rows = await sb_get(f"weekly_visit_targets?select=name,region_name,zone_name,church,dept,phone_last4&dept=eq.{quote(dept)}&name=eq.{quote(name)}" + (f"&phone_last4=eq.{quote(phone)}" if phone else "") + "&limit=1")
    if rows:
        enriched = await enrich_names(rows)
        if enriched: name = enriched[0].get("name", name) or name
    region = rows[0].get("region_name","") if rows else ""
    zone   = rows[0].get("zone_name","")   if rows else ""

    try:
        await sb_rpc("register_special_management", {
            "p_dept":         dept, "p_name":         name, "p_phone_last4":  phone,
            "p_region_name":  region, "p_zone_name":    zone, "p_chat_id":      chat.id,
            "p_chat_title":   chat.title or chat.full_name or f"chat_{chat.id}",
        })
    except Exception as e:
        logger.exception(e)
        await q.message.reply_text(f"❌ 등록 실패: {e}")
        return

    try:
        await q.edit_message_text(f"✅ <b>{_html.escape(str(name))}</b> 님을 <b>특별관리 대상</b>으로 등록했습니다.\n이 방에서 감지를 시작합니다.\n\n매주 수요일 07:00 KST 에 미체크 항목 리마인더가 이 방으로 발송됩니다.", parse_mode="HTML")
    except Exception as e:
        logger.warning("sp_pick edit 실패: %s", e)
        await q.message.reply_text(f"✅ {name} 님을 특별관리 대상으로 등록했습니다.\n이 방에서 감지를 시작합니다.\n매주 수요일 07:00 KST 에 리마인더가 발송됩니다.")

    await save_ctx(chat_id, church_filter=church, dept_filter=dept, tmp_sp_name=name, tmp_sp_phone=phone)
    await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=True)

async def _show_sp_detail(update, chat_id: int, church: str, dept: str, name: str, phone: str, send_new: bool = False):
    import html as _html
    try:
        detail = await sb_rpc("get_special_detail", {"p_dept": dept, "p_name": name, "p_phone_last4": phone})
    except Exception as e:
        logger.warning("get_special_detail failed: %s", e)
        detail = None

    if not detail:
        msg = "❌ 특별관리 정보를 찾을 수 없습니다."
        target = update.message if hasattr(update, 'message') and update.message else (update.callback_query.message if update.callback_query else None)
        if target: await target.reply_text(msg)
        return

    d = detail[0] if isinstance(detail, list) else detail
    region = d.get("region_name","") or ""
    zone   = d.get("zone_name","")   or ""

    item1 = bool(d.get("item1_chat_invited"))
    item2 = bool(d.get("item2_feedback_done"))
    item3 = d.get("item3_visit_date") or ""
    item4 = d.get("item4_visit_plan") or ""

    text = f"🚨 <b>특별관리 대상자 {_html.escape(str(name))}님 피드백방</b>\n━━━━━━━━━━━━━━━━━━━━\n📍 {_html.escape(church)} / {_html.escape(dept)} / {_html.escape(region)} {_html.escape(zone)}\n\n<i>이 그룹방은 그룹방이 삭제될 때까지 <b>{_html.escape(str(name))}</b>님 한 분을 위한 피드백 방입니다.</i>\n━━━━━━━━━━━━━━━━━━━━\n\n{'✅' if item1 else '⬜️'} <b>1. 대책방 초대완료</b>\n   (구역장·인섬교·강사·전도사·심방부사명자)\n   <i>최초 1회만 체크 (주간 리셋 안 됨)</i>\n\n{'✅' if item2 else '⬜️'} <b>2. 금주 피드백 진행</b>\n   <i>매주 수요일 07시 초기화</i>\n\n📅 <b>3. 금주 심방예정일:</b> {_html.escape(str(item3)) if item3 else '<i>미입력</i>'}\n\n📝 <b>4. 금주 심방계획:</b> {_html.escape(str(item4)) if item4 else '<i>미입력</i>'}"

    buttons = [
        [InlineKeyboardButton(f"{'✅ 1번 체크됨 (탭:해제)' if item1 else '⬜️ 1번 체크 (대책방 초대완료)'}", callback_data="sp_t1")],
        [InlineKeyboardButton(f"{'✅ 2번 체크됨 (탭:해제)' if item2 else '⬜️ 2번 체크 (금주 피드백)'}", callback_data="sp_t2")],
        [InlineKeyboardButton("📅 3번 심방예정일 입력/수정", callback_data="sp_e3")],
        [InlineKeyboardButton("📝 4번 심방계획 입력/수정", callback_data="sp_e4")],
        [InlineKeyboardButton("🗑 특별관리 해제", callback_data="sp_del")],
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

    plain_text = (text.replace("<b>","").replace("</b>","").replace("<i>","").replace("</i>",""))

    if send_new:
        target = update.message if hasattr(update, 'message') and update.message else (update.callback_query.message if update.callback_query else None)
        if target:
            await _send_with_fallback(lambda: target.reply_text(text, parse_mode="HTML", reply_markup=kb), lambda: target.reply_text(plain_text, reply_markup=kb))
    else:
        q = update.callback_query
        if q:
            await _send_with_fallback(lambda: q.edit_message_text(text, parse_mode="HTML", reply_markup=kb), lambda: q.message.reply_text(plain_text, reply_markup=kb))

async def _on_sp_toggle(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str, which: str):
    try:
        detail = await sb_rpc("get_special_detail", {"p_dept": dept, "p_name": name, "p_phone_last4": phone})
    except Exception as e:
        await update.callback_query.message.reply_text(f"❌ 조회 실패: {e}")
        return

    cur = False
    if detail:
        d = detail[0] if isinstance(detail, list) else detail
        cur = bool(d.get(f"item{which}_chat_invited" if which == "1" else "item2_feedback_done"))

    fn = "toggle_special_item1" if which == "1" else "toggle_special_item2"
    await sb_rpc(fn, {"p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": not cur})
    await _show_sp_detail(update, chat_id, church, dept, name, phone, send_new=False)

async def _on_sp_edit_text(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str, which: str):
    import html as _html
    q = update.callback_query
    step = "awaiting_sp3" if which == "3" else "awaiting_sp4"
    await save_ctx(chat_id, church_filter=church, dept_filter=dept, tmp_sp_name=name, tmp_sp_phone=phone, editing_step=step)
    label = "금주 심방예정일" if which == "3" else "금주 심방계획"
    try:
        await q.message.reply_text(f"✏️ <b>{_html.escape(str(name))}</b> 님의 <b>{label}</b> 을 입력해주세요:\n\n<i>취소하려면 /cancel</i>", parse_mode="HTML", reply_markup=kb_cancel_only())
    except Exception:
        await q.message.reply_text(f"✏️ {name} 님의 {label} 을 입력해주세요:\n\n취소하려면 /cancel", reply_markup=kb_cancel_only())

async def _on_sp_unregister(update: Update, chat_id: int, church: str, dept: str, name: str, phone: str):
    import html as _html
    try:
        await sb_rpc("unregister_special_management", {"p_dept": dept, "p_name": name, "p_phone_last4": phone})
    except Exception as e:
        await update.callback_query.message.reply_text(f"❌ 해제 실패: {e}")
        return
    q = update.callback_query
    try:
        await q.edit_message_text(f"🗑 <b>{_html.escape(str(name))}</b> 님을 특별관리에서 해제했습니다.", parse_mode="HTML", reply_markup=kb_main_menu(is_private_chat(update)))
    except Exception:
        await q.edit_message_text(f"🗑 {name} 님을 특별관리에서 해제했습니다.", reply_markup=kb_main_menu(is_private_chat(update)))

async def _on_sp_pick_by_rowid(update: Update, chat_id: int, row_id: str):
    rows = await sb_get(f"weekly_visit_targets?select=name,phone_last4,church,dept,region_name,zone_name&row_id=eq.{quote(row_id)}&limit=1")
    if not rows:
        q = update.callback_query
        await q.message.reply_text("❌ 결석자 정보를 찾을 수 없습니다.\n/menu 로 돌아가세요.")
        return
    t = rows[0]
    await _on_sp_pick(update, chat_id, t.get("church","") or "", t.get("dept","") or "", t.get("name","") or "", t.get("phone_last4","") or "")

async def _get_current_sp(chat_id: int):
    ctx = await get_ctx(chat_id)
    if not ctx: return None
    name  = ctx.get("tmp_sp_name") or ""
    phone = ctx.get("tmp_sp_phone") or ""
    dept  = ctx.get("dept_filter") or ""
    church = ctx.get("church_filter") or ""
    if not (name and dept): return None
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

async def weekly_reminder_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    acquired = await try_acquire_job_lock("special_reminder", source)
    if not acquired: return

    try:
        targets = await sb_rpc("get_all_special_targets", {}) or []
        if not targets:
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
            if not t.get("item1_chat_invited"): unchecked.append("⬜️ 1. 대책방 초대완료 (최초 1회)")
            if not t.get("item2_feedback_done"): unchecked.append("⬜️ 2. 금주 피드백 진행")
            if not (t.get("item3_visit_date") or ""): unchecked.append("⬜️ 3. 금주 심방예정일 (미입력)")
            if not (t.get("item4_visit_plan") or ""): unchecked.append("⬜️ 4. 금주 심방계획 (미입력)")

            if unchecked:
                msg = f"🔔 *주간 리마인더* (수요일 07시)\n👤 *{md(name)}* ({md(dept)} / {md(region)} {md(zone)})\n\n미체크 항목:\n" + "\n".join(unchecked) + f"\n\n/menu → 🚨 특별관리결석자 에서 업데이트하세요."
            else:
                msg = f"🔔 *주간 리마인더*\n👤 {md(name)} ({md(dept)} / {md(region)} {md(zone)})\n\n✅ 모든 항목 체크 완료. 수고하셨습니다!\n_(2~4번은 곧 초기화됩니다)_"

            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning("send failed to %s: %s", chat_id, e)
                try: await context.bot.send_message(chat_id=chat_id, text=msg.replace("*","").replace("`","").replace("_",""))
                except Exception: pass

        try:
            await sb_rpc("reset_special_weekly_items", {})
        except Exception as e:
            logger.warning("weekly reset failed: %s", e)
    except Exception as e:
        logger.exception("weekly_reminder_job failed: %s", e)

async def force_weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔔 주간 리마인더 강제 실행 중 (lock 무시)...")
    await weekly_reminder_job(context, source="manual_test")
    await update.message.reply_text("✅ 완료 (이미 발송됐으면 스킵)")

async def wednesday_visit_plan_request_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    acquired = await try_acquire_job_lock("weekly_visit_plan", source)
    if not acquired: return

    try:
        week_key, week_label = await get_active_week()
        if not week_key: return

        try:
            scopes = await sb_get("telegram_chat_scope?select=chat_id,chat_title,church,dept,region_name,zone_name&limit=2000") or []
        except Exception as e:
            return

        if not scopes: return

        sent = 0
        failed = 0

        for s in scopes:
            chat_id = s.get("chat_id")
            if not chat_id: continue
            if not await is_chat_authorized(chat_id): continue

            church = s.get("church", ""); dept = s.get("dept", ""); region = s.get("region_name", ""); zone = s.get("zone_name", "")
            if not church: continue

            try:
                path = f"weekly_visit_targets?select=row_id,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count&week_key=eq.{quote(week_key)}&church=eq.{quote(church)}"
                if dept:   path += f"&dept=eq.{quote(dept)}"
                if region: path += f"&region_name=eq.{quote(region)}"
                if zone:   path += f"&zone_name=eq.{quote(normalize_zone(zone))}"
                path += "&order=consecutive_absent_count.desc,name.asc&limit=500"

                rows = await sb_get(path) or []
                rows = await enrich_names(rows)

                target_summary = {"total": 0, "with_plan": 0, "target_set": 0}
                if rows:
                    row_ids = [r.get("row_id") for r in rows if r.get("row_id")]
                    try:
                        in_list = ",".join([quote(x) for x in row_ids])
                        prog_rows = await sb_get(f"weekly_visit_progress?select=row_id,is_target,plan_text&week_key=eq.{quote(week_key)}&row_id=in.({in_list})&limit=500") or []
                        prog_map = {p["row_id"]: p for p in prog_rows}
                        target_summary["total"] = len(rows)
                        for r in rows:
                            p = prog_map.get(r.get("row_id"))
                            if p:
                                if p.get("is_target"): target_summary["target_set"] += 1
                                if p.get("plan_text", ""): target_summary["with_plan"] += 1
                    except Exception as pe:
                        pass

                import html as _html
                scope_txt = " / ".join([x for x in [church, dept, region, zone] if x])
                week_label_safe = _html.escape(week_label or week_key)

                if not rows:
                    msg = f"📋 <b>수요일 심방계획 요청</b>\n━━━━━━━━━━━━━━━━━━━━\n\n📌 담당: <b>{_html.escape(scope_txt)}</b>\n📅 주차: <b>{week_label_safe}</b>\n\n✅ 이번 주 해당 범위 결석자 없습니다. 수고하셨습니다!"
                else:
                    sample_names = []
                    for r in rows[:10]:
                        nm = r.get("name", "?")
                        streak = r.get("consecutive_absent_count", 0) or 0
                        zn = r.get("zone_name", "") or ""
                        sample_names.append(f"• {_html.escape(nm)} {_html.escape(zn)} · 연속{streak}회")
                    more_line = f"\n... 외 {len(rows) - 10}명" if len(rows) > 10 else ""

                    msg = f"📋 <b>수요일 심방계획 요청</b>\n━━━━━━━━━━━━━━━━━━━━\n\n📌 담당: <b>{_html.escape(scope_txt)}</b>\n📅 주차: <b>{week_label_safe}</b>\n\n📊 <b>이번 주 결석자 현황</b>\n   • 총 <b>{target_summary['total']}</b>명\n   • 🎯 타겟 지정: <b>{target_summary['target_set']}</b>명\n   • 📝 심방계획 입력: <b>{target_summary['with_plan']}</b>명\n\n<b>🙏 주일까지 다음 작업을 부탁드립니다:</b>\n1️⃣ 결석자 중 <b>타겟 대상 선정</b>\n2️⃣ 각 타겟에 대한 <b>심방계획 작성</b>\n3️⃣ <b>심방 실행 & 기록 업데이트</b>\n\n<i>결석자 목록 (상위 10명)</i>\n{chr(10).join(sample_names)}{more_line}\n\n━━━━━━━━━━━━━━━━━━━━\n하단 <code>📋 결석자 심방</code> 버튼으로 시작하세요."

                kb_link = kb_dashboard_link()
                try:
                    await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="HTML", reply_markup=kb_link)
                    sent += 1
                except Exception as e1:
                    try:
                        plain = (msg.replace("<b>", "").replace("</b>", "").replace("<i>", "").replace("</i>", "").replace("<code>", "").replace("</code>", ""))
                        await context.bot.send_message(chat_id=chat_id, text=plain, reply_markup=kb_link)
                        sent += 1
                    except Exception as e2:
                        failed += 1

            except Exception as e:
                failed += 1

    except Exception as e:
        logger.exception("wednesday_visit_plan_request_job failed: %s", e)

async def force_wednesday_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📅 수요일 심방계획 요청 강제 실행 중 (lock 무시)...")
    await wednesday_visit_plan_request_job(context, source="manual_test")
    await update.message.reply_text("✅ 완료 (이미 발송됐으면 스킵)")

async def weekly_rollover_job(context: ContextTypes.DEFAULT_TYPE, source: str = "job_queue"):
    acquired = await try_acquire_job_lock("weekly_rollover", source)
    if not acquired: return

    try:
        week_key, week_label = compute_target_week_key()
        existing = await sb_get(f"weekly_target_weeks?select=week_key&week_key=eq.{quote(week_key)}&limit=1")
        if existing: return
        await sb_post("weekly_target_weeks", {"week_key": week_key, "week_label": week_label})
    except Exception as e:
        logger.exception("주차 자동 전환 실패: %s", e)

async def chatid_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import html as _html
    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    chat_title = chat.title or chat.full_name or "(제목없음)"
    chat_type = {"private": "개인채팅", "group": "일반 그룹", "supergroup": "슈퍼그룹", "channel": "채널"}.get(chat.type, chat.type)

    authorized = True
    if chat.type != "private":
        authorized = await is_chat_authorized(chat_id)

    auth_badge = "✅ 승인됨" if authorized else "❌ 미승인"
    user_line = ""
    if user: user_line = f"• 내 User ID: <code>{user.id}</code>\n"

    msg = f"📋 <b>방 정보</b>\n━━━━━━━━━━━━━━━━━━━━\n\n• Chat ID: <code>{chat_id}</code>\n• 방 이름: {_html.escape(chat_title)}\n• 방 유형: {chat_type}\n• 승인 상태: {auth_badge}\n{user_line}"
    if not authorized and chat.type != "private":
        msg += "\n\n👇 아래 버튼으로 관리자에게 승인 신청하기"
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=kb_request_approval())
    else:
        await update.message.reply_text(msg, parse_mode="HTML")

async def request_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import html as _html
    q = update.callback_query
    await q.answer()

    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    chat_title = chat.title or chat.full_name or "(제목없음)"
    requester_name = user.full_name if user else "(알 수 없음)"
    requester_id = user.id if user else 0

    if await is_chat_authorized(chat_id):
        await q.message.reply_text("✅ 이 방은 이미 승인되어 있습니다. /start 로 시작하세요.")
        return

    admins = await get_active_bot_admins()
    if not admins:
        await q.message.reply_text(f"⚠️ <b>등록된 관리자가 없습니다.</b>\n\n관리자가 웹 대시보드에서 이 Chat ID 를 직접 승인해야 합니다:\n<code>{chat_id}</code>\n\n관리자에게 직접 전달해주세요.", parse_mode="HTML")
        return

    target_scope = await get_chat_scope(chat_id)
    target_church = (target_scope or {}).get("church")
    target_dept = (target_scope or {}).get("dept")

    def _admin_should_receive(admin: dict) -> bool:
        atype = admin.get("scope_type", "zipa")
        achurch = admin.get("scope_church")
        adept = admin.get("scope_dept")
        if atype == "zipa": return True
        if not target_church: return False
        if atype == "church": return achurch == target_church
        if atype == "dept": return achurch == target_church and (not target_dept or adept == target_dept)
        return False

    routed_admins = [a for a in admins if _admin_should_receive(a)]
    if not routed_admins:
        routed_admins = [a for a in admins if a.get("scope_type") == "zipa"]
        if not routed_admins: routed_admins = admins

    admin_msg = f"🔔 <b>새 방 승인 신청</b>\n━━━━━━━━━━━━━━━━━━━━\n\n📋 <b>신청 방 정보</b>\n• Chat ID: <code>{chat_id}</code>\n• 방 이름: {_html.escape(chat_title)}\n• 방 유형: {chat.type}\n\n👤 <b>신청자</b>\n• 이름: {_html.escape(requester_name)}\n• User ID: <code>{requester_id}</code>\n\n━━━━━━━━━━━━━━━━━━━━\n✅ <b>승인 방법</b>\n• 아래 ✅ 승인 버튼을 누르거나\n• 명령어로: <code>/approve {chat_id}</code>\n\n❌ <b>거부 방법</b>\n• 아래 ❌ 거부 버튼을 누르거나\n• 명령어로: <code>/deny {chat_id}</code>"
    approve_kb = InlineKeyboardMarkup([[InlineKeyboardButton("✅ 승인", callback_data=f"admin_approve:{chat_id}"), InlineKeyboardButton("❌ 거부", callback_data=f"admin_deny:{chat_id}")]])

    delivered = 0
    for admin in routed_admins:
        admin_uid = admin.get("user_id")
        if not admin_uid: continue
        try:
            await context.bot.send_message(chat_id=admin_uid, text=admin_msg, parse_mode="HTML", reply_markup=approve_kb)
            delivered += 1
        except Exception as e:
            pass

    if delivered > 0:
        await q.message.reply_text(f"✅ <b>승인 신청 완료</b>\n\n{delivered}명의 관리자에게 승인 요청이 전달되었습니다.\n관리자 승인 후 이 방에서 <code>/start</code> 재실행하시면 됩니다.\n\n<i>Chat ID: {chat_id}</i>", parse_mode="HTML")
    else:
        await q.message.reply_text(f"⚠️ 관리자 알림 전송 실패.\n\n관리자에게 직접 이 Chat ID를 전달해주세요: <code>{chat_id}</code>\n\n💡 관리자가 봇에게 개인채팅으로 <code>/start</code> 를 먼저 실행해야 DM 수신 가능합니다.", parse_mode="HTML")

async def approve_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import html as _html
    user = update.effective_user
    if not user or not await is_bot_admin_user(user.id):
        await update.message.reply_text("🔒 이 명령은 관리자만 사용 가능합니다.")
        return

    args = context.args if hasattr(context, 'args') else []
    if not args:
        await update.message.reply_text("ℹ️ 사용법: <code>/approve &lt;chat_id&gt;</code>\n예: <code>/approve -1001234567890</code>", parse_mode="HTML")
        return

    try:
        target_chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Chat ID는 숫자여야 합니다.")
        return

    admin_name = user.full_name or user.username or f"user_{user.id}"
    try:
        await sb_rpc("upsert_authorized_chat", {"p_chat_id": target_chat_id, "p_chat_title": None, "p_notes": f"{admin_name} 님이 /approve 로 승인", "p_is_active": True, "p_authorized_by": admin_name})
    except Exception as e:
        await update.message.reply_text(f"❌ 승인 실패: {e}")
        return

    await update.message.reply_text(f"✅ <b>승인 완료</b>\n\nChat ID: <code>{target_chat_id}</code>\n승인자: {_html.escape(admin_name)}\n\n해당 방에서 <code>/start</code> 재실행하면 정상 작동합니다.", parse_mode="HTML")

    try:
        await context.bot.send_message(chat_id=target_chat_id, text=f"✅ <b>이 방이 승인되었습니다!</b>\n\n승인자: {_html.escape(admin_name)}\n\n이제 <code>/start</code> 로 봇 사용을 시작하세요.", parse_mode="HTML")
    except Exception as e:
        pass

async def deny_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or not await is_bot_admin_user(user.id):
        await update.message.reply_text("🔒 이 명령은 관리자만 사용 가능합니다.")
        return

    args = context.args if hasattr(context, 'args') else []
    if not args:
        await update.message.reply_text("ℹ️ 사용법: <code>/deny &lt;chat_id&gt;</code>", parse_mode="HTML")
        return

    try:
        target_chat_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Chat ID는 숫자여야 합니다.")
        return

    await update.message.reply_text(f"❌ <b>거부 처리</b>\n\nChat ID: <code>{target_chat_id}</code>\n(승인 목록에 추가하지 않음)", parse_mode="HTML")

async def admin_approve_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import html as _html
    q = update.callback_query
    user = update.effective_user

    if not user or not await is_bot_admin_user(user.id):
        await q.answer("🔒 관리자만 가능", show_alert=True)
        return

    data = q.data or ""
    try: target_chat_id = int(data.split(":", 1)[1])
    except (ValueError, IndexError): await q.answer("❌ 잘못된 데이터", show_alert=True); return

    await q.answer("처리 중...")
    admin_name = user.full_name or user.username or f"user_{user.id}"

    try:
        await sb_rpc("upsert_authorized_chat", {"p_chat_id": target_chat_id, "p_chat_title": None, "p_notes": f"{admin_name} 님이 DM 승인", "p_is_active": True, "p_authorized_by": admin_name})
    except Exception as e:
        await q.message.reply_text(f"❌ 승인 실패: {e}")
        return

    await q.edit_message_text(q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n✅ <b>{_html.escape(admin_name)}</b> 님이 승인 완료", parse_mode="HTML")
    try:
        await context.bot.send_message(chat_id=target_chat_id, text=f"✅ <b>이 방이 승인되었습니다!</b>\n\n승인자: {_html.escape(admin_name)}\n\n이제 <code>/start</code> 로 봇 사용을 시작하세요.", parse_mode="HTML")
    except Exception as e:
        pass

async def admin_deny_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    import html as _html
    q = update.callback_query
    user = update.effective_user

    if not user or not await is_bot_admin_user(user.id):
        await q.answer("🔒 관리자만 가능", show_alert=True)
        return

    await q.answer()
    admin_name = user.full_name or user.username or f"user_{user.id}"
    await q.edit_message_text(q.message.text_html + f"\n\n━━━━━━━━━━━━━━━━━━━━\n❌ <b>{_html.escape(admin_name)}</b> 님이 거부", parse_mode="HTML")


# ═════════════════════════════════════════════════════════════════════════════
# 앱 시작
# ═════════════════════════════════════════════════════════════════════════════
MINIAPP_HTML_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "miniapp.html")

def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        err = context.error
        logger.error("Global error: %s", err, exc_info=True)
        emsg = str(err)
        if ("parse" in emsg.lower() or "entity" in emsg.lower()) and isinstance(update, Update):
            try:
                chat = update.effective_chat
                if chat:
                    await context.bot.send_message(chat_id=chat.id, text=f"⚠️ 일부 특수문자 때문에 표시에 문제가 있었습니다. /menu 로 돌아가세요.")
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
    app.add_handler(CommandHandler("chatid",   chatid_command))
    app.add_handler(CommandHandler("approve",  approve_command))
    app.add_handler(CommandHandler("deny",     deny_command))
    app.add_handler(CallbackQueryHandler(button_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    if app.job_queue is not None:
        app.job_queue.run_daily(wednesday_visit_plan_request_job, time=dtime(hour=7, minute=0, tzinfo=KST), days=(2,), name="wednesday_personal_visit_plan")
        app.job_queue.run_daily(weekly_reminder_job, time=dtime(hour=7, minute=0, tzinfo=KST), days=(2,), name="wednesday_special_reminder")
        app.job_queue.run_daily(weekly_rollover_job, time=dtime(hour=0, minute=0, tzinfo=KST), days=(2,), name="wednesday_weekly_rollover")

    port = int(os.environ.get("PORT", 8080))
    webhook_url = os.environ["WEBHOOK_URL"]
    logger.info(f"Starting integrated server port={port} url={webhook_url}")

    import asyncio
    from aiohttp import web
    from telegram import Update as TgUpdate

    async def webhook_handler(request):
        try: data = await request.json()
        except Exception: return web.Response(status=400, text="bad request")
        try:
            update = TgUpdate.de_json(data, app.bot)
            await app.process_update(update)
            return web.Response(text="OK")
        except Exception as e:
            logger.exception("webhook process error: %s", e)
            return web.Response(status=500, text="error")

    SCHEDULER_TOKEN = os.environ.get("SCHEDULER_TOKEN", "")

    def _check_scheduler_auth(request) -> bool:
        if not SCHEDULER_TOKEN: return False
        token = request.query.get("token", "")
        if token == SCHEDULER_TOKEN: return True
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer ") and auth[7:] == SCHEDULER_TOKEN: return True
        return False

    async def trigger_weekly_visit_plan(request):
        if not _check_scheduler_auth(request): return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        try:
            class FakeContext: bot = app.bot
            await wednesday_visit_plan_request_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "weekly_visit_plan 발송 완료"})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def trigger_special_reminder(request):
        if not _check_scheduler_auth(request): return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        try:
            class FakeContext: bot = app.bot
            await weekly_reminder_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "special_reminder 발송 완료"})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def trigger_weekly_rollover(request):
        if not _check_scheduler_auth(request): return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
        try:
            class FakeContext: bot = app.bot
            await weekly_rollover_job(FakeContext(), source="cloud_scheduler")
            return web.json_response({"ok": True, "message": "weekly_rollover 완료"})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

    async def miniapp_html_handler(request):
        try:
            with open(MINIAPP_HTML_PATH, "r", encoding="utf-8") as f:
                html = f.read()
            return web.Response(text=html, content_type="text/html", charset="utf-8")
        except FileNotFoundError:
            return web.Response(text="<h1>miniapp.html 파일이 배포되지 않았습니다</h1>", content_type="text/html", status=404)

    async def miniapp_search_handler(request):
        name   = (request.query.get("name") or "").strip()
        phone  = (request.query.get("phone") or "").strip()
        church = (request.query.get("church") or "").strip()
        dept   = (request.query.get("dept") or "").strip()
        region = (request.query.get("region") or "").strip()
        zone   = (request.query.get("zone") or "").strip()

        if not name: return web.json_response({"ok": False, "error": "이름은 필수입니다"}, status=400)

        def _build_path(week_key, zone_value=None):
            p = f"weekly_visit_targets?select=row_id,week_key,name,phone_last4,church,dept,region_name,zone_name,consecutive_absent_count&week_key=eq.{quote(week_key)}&name=eq.{quote(name)}"
            if phone:  p += f"&phone_last4=eq.{quote(phone)}"
            if church: p += f"&church=eq.{quote(church)}"
            if dept:   p += f"&dept=eq.{quote(dept)}"
            if region: p += f"&region_name=eq.{quote(region)}"
            if zone_value: p += f"&zone_name=eq.{quote(zone_value)}"
            p += "&limit=5"
            return p

        try:
            week_key, _ = await get_active_week()
            if not week_key: return web.json_response({"ok": False, "error": "등록된 주차 없음"}, status=404)

            weeks_to_try = [week_key]
            try:
                recent = await sb_get("weekly_target_weeks?select=week_key&order=week_key.desc&limit=4")
                for w in (recent or []):
                    wk = w.get("week_key")
                    if wk and wk not in weeks_to_try: weeks_to_try.append(wk)
            except Exception: pass

            rows = []
            for wk in weeks_to_try:
                if zone:
                    zone_norm = normalize_zone(zone)
                    rows = await sb_get(_build_path(wk, zone_norm))
                    if not rows and zone != zone_norm: rows = await sb_get(_build_path(wk, zone))
                    if not rows:
                        zone_alt = zone.replace("팀", "-") if "팀" in zone else zone.replace("-", "팀")
                        if zone_alt != zone and zone_alt != zone_norm: rows = await sb_get(_build_path(wk, zone_alt))
                else:
                    rows = await sb_get(_build_path(wk, None))
                if rows: break

            if not rows: return web.json_response({"ok": True, "target": None, "progress": None})

            target = rows[0]
            enriched = await enrich_names([target])
            if enriched: target = enriched[0]

            prog_rows = await sb_get(f"weekly_visit_progress?select=*&week_key=eq.{quote(target['week_key'])}&row_id=eq.{quote(target['row_id'])}&limit=1")
            progress = prog_rows[0] if prog_rows else None

            return web.json_response({"ok": True, "target": target, "progress": progress})
        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)


    async def miniapp_submit_handler(request):
        try: data = await request.json()
        except Exception: return web.json_response({"ok": False, "error": "invalid json"}, status=400)

        week_key = str(data.get("week_key", "")).strip()
        row_id   = str(data.get("row_id", "")).strip()
        if not (week_key and row_id): return web.json_response({"ok": False, "error": "week_key 또는 row_id 누락"}, status=400)

        existing = None
        try:
            prog_rows = await sb_get(f"weekly_visit_progress?select=*&week_key=eq.{quote(week_key)}&row_id=eq.{quote(row_id)}&limit=1")
            existing = prog_rows[0] if prog_rows else None
        except Exception:
            pass

        def pick(new_val, old_val, default=""):
            s = str(new_val or "").strip()
            if s != "": return s
            return old_val if old_val is not None else default

        target_str = str(data.get("target", "")).strip()
        if target_str == "타겟": is_target = True
        elif target_str == "미타겟": is_target = False
        else: is_target = bool(existing.get("is_target")) if existing else False

        done_str = str(data.get("done", "")).strip()
        if done_str == "완료": is_done = True
        elif done_str == "미완료": is_done = False
        else: is_done = bool(existing.get("is_done")) if existing else False

        worship_str = str(data.get("worship", "")).strip()
        if worship_str in ("확정", "미정", "불참"): worship = worship_str
        else: worship = existing.get("worship") if existing else None

        attendance_str = str(data.get("attendance", "")).strip()
        if attendance_str in ("참석", "불참"): attendance = attendance_str
        else: attendance = existing.get("attendance") if existing else None

        shepherd = pick(data.get("shepherd"), existing.get("shepherd") if existing else "", "")
        visit_date_display = pick(data.get("visit_date_display"), existing.get("visit_date_display") if existing else "", "")
        plan_text = pick(data.get("plan_text"), existing.get("plan_text") if existing else "", "")
        note = pick(data.get("note"), existing.get("note") if existing else "", "")

        # 🆕 다양한 날짜 포맷 허용 & ISO 자동변환 (정렬용)
        visit_date_sort = _parse_visit_date_to_iso(visit_date_display)

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
                            f"{SUPABASE_URL}/rest/v1/weekly_visit_targets?week_key=eq.{quote(week_key)}&row_id=eq.{quote(row_id)}",
                            headers=HEADERS, content=json.dumps(update_body), timeout=10,
                        )
            except Exception as e:
                pass

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

            # [추가] 미니앱 입력 시 특별관리 테이블에도 내용 동시 반영
            try:
                t_rows = await sb_get(f"weekly_visit_targets?select=name,dept,phone_last4&row_id=eq.{quote(row_id)}&limit=1")
                if t_rows:
                    enriched = await enrich_names(t_rows)
                    target_info = enriched[0]
                    if visit_date_display:
                        await sb_rpc("set_special_item3", {
                            "p_dept": target_info.get("dept", ""), 
                            "p_name": target_info.get("name", ""), 
                            "p_phone_last4": target_info.get("phone_last4", ""), 
                            "p_value": visit_date_display
                        })
                    if plan_text:
                        await sb_rpc("set_special_item4", {
                            "p_dept": target_info.get("dept", ""), 
                            "p_name": target_info.get("name", ""), 
                            "p_phone_last4": target_info.get("phone_last4", ""), 
                            "p_value": plan_text
                        })
            except Exception as se:
                logger.warning("미니앱 특별관리 항목 동시 업데이트 실패: %s", se)

        except Exception as e:
            return web.json_response({"ok": False, "error": str(e)}, status=500)

        return web.json_response({"ok": True, "message": "심방 기록 저장 완료"})

    async def health(request): return web.Response(text="OK")

    @web.middleware
    async def cors_mw(request, handler):
        if request.method == "OPTIONS":
            return web.Response(headers={"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "POST, GET, OPTIONS", "Access-Control-Allow-Headers": "Content-Type"})
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
    http_app.router.add_post("/trigger/weekly-visit-plan", trigger_weekly_visit_plan)
    http_app.router.add_get("/trigger/weekly-visit-plan",  trigger_weekly_visit_plan)
    http_app.router.add_post("/trigger/special-reminder",  trigger_special_reminder)
    http_app.router.add_get("/trigger/special-reminder",   trigger_special_reminder)
    http_app.router.add_post("/trigger/weekly-rollover",   trigger_weekly_rollover)
    http_app.router.add_get("/trigger/weekly-rollover",    trigger_weekly_rollover)

    async def _run():
        await app.initialize()
        await app.start()
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
        try:
            while True: await asyncio.sleep(3600)
        finally:
            await runner.cleanup()
            await app.stop()
            await app.shutdown()

    asyncio.run(_run())

if __name__ == "__main__":
    main()
