"""
결석자 타겟 심방 텔레그램 봇 v3
- 메뉴식 UI (결석자 / 특별관리결석자 / 도움말)
- 화요일 18시 KST 기준 주차 자동 선택
- 구역명 정규화 (2-1 ↔ 2팀1)
- 지역명 또는 구역명 입력 지원
- 특별관리결석자 시스템 (매주 화요일 19시 KST 리마인더)

Cloud Run (Python 3.11) + python-telegram-bot 20.x + Supabase REST API
"""

import os
import re
import json
import logging
import httpx
from urllib.parse import quote
from datetime import datetime, time as dtime, timezone, timedelta

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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

# 주간 리마인더 실행 시각 (매주 화요일) — 기본 19:00 KST
WEEKLY_REMINDER_HOUR = int(os.environ.get("WEEKLY_REMINDER_HOUR", "19"))
WEEKLY_REMINDER_MIN  = int(os.environ.get("WEEKLY_REMINDER_MIN",  "0"))

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

DEPTS = ["자문회", "장년회", "부녀회", "청년회"]

# ── 심방 입력 8단계 ────────────────────────────────────────────────────────────
STEPS = ["shepherd", "date", "plan", "target", "done", "worship", "note", "attendance"]
STEP_LABELS = {
    "shepherd":   "👤 심방자 (예: 홍길동(집사))",
    "date":       "📅 심방날짜 (예: 4/27 또는 2026-04-27)",
    "plan":       "📝 심방계획",
    "target":     "🎯 타겟여부",
    "done":       "✅ 진행여부",
    "worship":    "🙏 예배확답",
    "note":       "📋 진행사항 (없으면 '없음' 입력)",
    "attendance": "⛪ 예배참석",
}
STEP_CHOICES = {
    "target":     [["타겟", "미타겟"]],
    "done":       [["완료", "미완료"]],
    "worship":    [["확정", "미정", "불참"]],
    "attendance": [["참석", "불참"]],
}

# ── 특별관리 4항목 ─────────────────────────────────────────────────────────────
SP_ITEMS = [
    ("item1_chat_invited",  "대책방 초대완료 (구역장, 인섬교, 강사, 전도사, 심방부사명자)", "one"),
    ("item2_feedback_done", "금주 피드백 진행",                                             "weekly_check"),
    ("item3_visit_date",    "금주 심방예정일",                                              "weekly_text"),
    ("item4_visit_plan",    "금주 심방계획",                                                "weekly_text"),
]

# ── 마크다운 이스케이프 ────────────────────────────────────────────────────────
_MD_SPECIALS = "_*`["
def md(s) -> str:
    if s is None: return ""
    return "".join(("\\" + c) if c in _MD_SPECIALS else c for c in str(s))

TG_MAX_LEN = 3800


# ─────────────────────────────────────────────────────────────────────────────
# Supabase 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
async def sb_get(path: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, timeout=15)
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
        if r.status_code >= 400:
            logger.error("RPC %s failed %s: %s", func, r.status_code, r.text[:500])
        r.raise_for_status()
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return None

async def sb_delete(path: str):
    async with httpx.AsyncClient() as client:
        r = await client.delete(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, timeout=15)
        r.raise_for_status()
        return True


# ─────────────────────────────────────────────────────────────────────────────
# 주차 계산 — 화요일 18시 KST 기준
# ─────────────────────────────────────────────────────────────────────────────
def get_target_sunday(now: datetime | None = None) -> datetime:
    """화요일 18시 KST 이후면 다음 주일, 아니면 지난 주일 반환."""
    if now is None:
        now = datetime.now(KST)
    else:
        now = now.astimezone(KST)
    
    weekday = now.weekday()  # 0=월, 1=화, 2=수, ..., 6=일
    hour = now.hour
    
    # 일요일(6)
    if weekday == 6:
        diff = 0
    # 월요일(0) ~ 화요일(1) 18시 미만 → 지난 주일
    elif weekday == 0 or (weekday == 1 and hour < 18):
        diff = -(weekday + 1)
    # 화요일(1) 18시 이상 ~ 토요일(5) → 다음 주일
    else:
        diff = 6 - weekday

    target = (now + timedelta(days=diff)).replace(hour=0, minute=0, second=0, microsecond=0)
    return target


def get_sunday_week_no(sunday: datetime) -> int:
    """해당 월의 몇 번째 일요일인지 반환 (1~5)."""
    year, month = sunday.year, sunday.month
    count = 0
    cursor = datetime(year, month, 1, tzinfo=KST)
    last_day = (datetime(year, month + 1, 1, tzinfo=KST) - timedelta(days=1)).day if month < 12 \
               else 31
    for d in range(1, last_day + 1):
        cursor = cursor.replace(day=d)
        if cursor.weekday() == 6:  # 일요일
            count += 1
            if d == sunday.day:
                return count
    return 1


def compute_target_week_key() -> tuple[str, str]:
    """화요일 18시 기준 현재 타겟 주차의 (week_key, week_label) 반환."""
    sunday = get_target_sunday()
    year, month = sunday.year, sunday.month
    week_no = get_sunday_week_no(sunday)
    week_key = f"{year}-{month:02d}-w{week_no}"
    week_label = f"{year}년 {month}월 {week_no}주차"
    return week_key, week_label


# ─────────────────────────────────────────────────────────────────────────────
# 최신 주차 — DB에서 가장 최근 등록된 주차를 가져오되,
# 화요일 18시 기준 계산된 주차가 DB에 있으면 그것을 우선 사용
# ─────────────────────────────────────────────────────────────────────────────
async def get_active_week_key() -> tuple[str, str]:
    """현재 사용할 week_key, week_label 반환."""
    expected_key, expected_label = compute_target_week_key()
    try:
        rows = await sb_get(
            f"weekly_target_weeks?select=week_key,week_label&week_key=eq.{quote(expected_key)}&limit=1"
        )
        if rows:
            return rows[0]["week_key"], rows[0].get("week_label", expected_label)
    except Exception as e:
        logger.warning("get_active_week_key DB check failed: %s", e)

    # 기대 주차가 DB에 없으면 가장 최신 주차로 fallback
    try:
        rows = await sb_get("weekly_target_weeks?select=week_key,week_label&order=week_key.desc&limit=1")
        if rows:
            return rows[0]["week_key"], rows[0].get("week_label", rows[0]["week_key"])
    except Exception as e:
        logger.warning("get_active_week_key fallback failed: %s", e)
    
    return "", ""


# ─────────────────────────────────────────────────────────────────────────────
# 컨텍스트 저장
# ─────────────────────────────────────────────────────────────────────────────
async def get_ctx(chat_id: int):
    rows = await sb_rpc("get_telegram_visit_context", {"p_chat_id": chat_id})
    if rows:
        return rows[0] if isinstance(rows, list) else rows
    return None

async def save_ctx(chat_id: int, **kwargs):
    payload = {"p_chat_id": chat_id}
    for k, v in kwargs.items():
        payload[f"p_{k}"] = v
    await sb_rpc("set_telegram_visit_context", payload)

async def clear_tmp(chat_id: int):
    await sb_rpc("clear_telegram_tmp", {"p_chat_id": chat_id})


# ─────────────────────────────────────────────────────────────────────────────
# 구역 정규화 — "2-1" ↔ "2팀1"
# ─────────────────────────────────────────────────────────────────────────────
def normalize_zone_py(z: str) -> str:
    if not z: return ""
    s = re.sub(r"\s+", "", z.strip())
    m = re.match(r"^(\d+)[-_](\d+)$", s)
    if m: return f"{m.group(1)}팀{m.group(2)}"
    m = re.match(r"^(\d+)팀(\d+)$", s)
    if m: return f"{m.group(1)}팀{m.group(2)}"
    return s


def looks_like_zone(text: str) -> bool:
    """입력 문자열이 구역 형식인지 판별 (2-1, 2팀1, 1-3 등)"""
    s = re.sub(r"\s+", "", text.strip())
    return bool(re.match(r"^\d+[-_팀]\d+$", s))


# ─────────────────────────────────────────────────────────────────────────────
# 결석자 조회
# ─────────────────────────────────────────────────────────────────────────────
async def get_absentees_by_region(week_key: str, dept: str, region: str):
    try:
        return await sb_rpc("get_absentees_by_dept_region", {
            "p_week_key": week_key, "p_dept": dept, "p_region": region
        }) or []
    except Exception as e:
        logger.warning("get_absentees_by_dept_region RPC failed, falling back: %s", e)
        # fallback: REST
        return await sb_get(
            f"weekly_visit_targets"
            f"?select=row_id,name,phone_last4,region_name,zone_name,consecutive_absent_count"
            f"&week_key=eq.{quote(week_key)}"
            f"&dept=eq.{quote(dept)}"
            f"&region_name=eq.{quote(region)}"
            f"&order=zone_name.asc,name.asc"
        )

async def get_absentees_by_zone(week_key: str, dept: str, zone: str):
    try:
        return await sb_rpc("get_absentees_by_dept_zone", {
            "p_week_key": week_key, "p_dept": dept, "p_zone": zone
        }) or []
    except Exception as e:
        logger.warning("get_absentees_by_dept_zone RPC failed: %s", e)
        return []

async def get_absentees_4plus(week_key: str, dept: str):
    try:
        return await sb_rpc("get_absentees_4plus_by_dept", {
            "p_week_key": week_key, "p_dept": dept
        }) or []
    except Exception as e:
        logger.warning("get_absentees_4plus_by_dept failed: %s", e)
        return []


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


# ─────────────────────────────────────────────────────────────────────────────
# 표시용 연속결석 수 = 저장된 값 (이미 업로드 시 +1 적용되어 있음)
# 추가 보정 없이 그대로 표시.
# (업로드 시 apply_consecutive_absent_increment 가 호출되어 +1 된 상태)
# ─────────────────────────────────────────────────────────────────────────────
def display_streak(n) -> str:
    """연속결석 횟수 표시용 - 이미 DB 저장 시 +1 되어 있으므로 그대로 표시"""
    try:
        return str(int(n))
    except Exception:
        return str(n or 0)


# ─────────────────────────────────────────────────────────────────────────────
# 메인 메뉴 (인라인 버튼)
# ─────────────────────────────────────────────────────────────────────────────
def build_main_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 결석자",           callback_data="menu:absentee"),
            InlineKeyboardButton("🚨 특별관리결석자",    callback_data="menu:special"),
        ],
        [
            InlineKeyboardButton("❓ 도움말",            callback_data="menu:help"),
        ],
    ])


def build_dept_menu_kb(kind: str) -> InlineKeyboardMarkup:
    """kind: 'absentee' | 'special'"""
    rows = []
    pair = []
    for dept in DEPTS:
        pair.append(InlineKeyboardButton(dept, callback_data=f"{kind}_dept:{dept}"))
        if len(pair) == 2:
            rows.append(pair); pair = []
    if pair: rows.append(pair)
    rows.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="menu:home")])
    return InlineKeyboardMarkup(rows)


# ─────────────────────────────────────────────────────────────────────────────
# /start /menu — 메인 메뉴 표시
# ─────────────────────────────────────────────────────────────────────────────
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    week_key, week_label = await get_active_week_key()
    header = (
        "👋 *결석자 타겟 심방 봇*에 오신 것을 환영합니다\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "아래 메뉴에서 원하는 기능을 선택하세요 👇"
    )
    await update.message.reply_text(header, parse_mode="Markdown", reply_markup=build_main_menu_kb())


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_command(update, context)


# ─────────────────────────────────────────────────────────────────────────────
# 메인 콜백 디스패처
# ─────────────────────────────────────────────────────────────────────────────
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id

    try:
        # ── 메인 메뉴 ──
        if data == "menu:home":
            await _show_main_menu(update)
        elif data == "menu:absentee":
            await _show_dept_select(update, "absentee")
        elif data == "menu:special":
            await _show_dept_select(update, "special")
        elif data == "menu:help":
            await _show_help(update, edit=True)

        # ── 일반 결석자 흐름 ──
        elif data.startswith("absentee_dept:"):
            dept = data.split(":", 1)[1]
            await _on_absentee_dept_selected(update, chat_id, dept)
        elif data.startswith("select:"):
            await _on_select_absentee(update, chat_id, data.split(":", 1)[1])
        elif data.startswith("choice:"):
            _, step, value = data.split(":", 2)
            await _handle_choice(update, chat_id, step, value)
        elif data == "confirm_save":
            await _do_save(update, chat_id)
        elif data == "cancel_save":
            await clear_tmp(chat_id)
            await query.message.reply_text("🚫 저장이 취소됐습니다.\n/menu 로 메인 메뉴로 돌아가세요.")

        # ── 특별관리 흐름 ──
        elif data.startswith("special_dept:"):
            dept = data.split(":", 1)[1]
            await _on_special_dept_selected(update, chat_id, dept)
        elif data.startswith("sp_pick:"):
            # sp_pick:{dept}:{name}:{phone_last4}
            _, dept, name, phone = data.split(":", 3)
            await _on_special_person_selected(update, chat_id, dept, name, phone)
        elif data.startswith("sp_toggle1:"):
            _, dept, name, phone = data.split(":", 3)
            await _on_sp_toggle1(update, chat_id, dept, name, phone)
        elif data.startswith("sp_toggle2:"):
            _, dept, name, phone = data.split(":", 3)
            await _on_sp_toggle2(update, chat_id, dept, name, phone)
        elif data.startswith("sp_edit3:"):
            _, dept, name, phone = data.split(":", 3)
            await _on_sp_edit_text(update, chat_id, dept, name, phone, "3")
        elif data.startswith("sp_edit4:"):
            _, dept, name, phone = data.split(":", 3)
            await _on_sp_edit_text(update, chat_id, dept, name, phone, "4")
        elif data.startswith("sp_unregister:"):
            _, dept, name, phone = data.split(":", 3)
            await _on_sp_unregister(update, chat_id, dept, name, phone)

    except Exception as e:
        logger.exception(e)
        try:
            await query.message.reply_text(f"❌ 오류: {e}")
        except Exception:
            pass


async def _show_main_menu(update: Update):
    week_key, week_label = await get_active_week_key()
    text = (
        "🏠 *메인 메뉴*\n\n"
        f"📅 현재 주차: *{md(week_label) if week_label else '미등록'}*\n"
        "원하는 기능을 선택하세요 👇"
    )
    q = update.callback_query
    await q.edit_message_text(text, parse_mode="Markdown", reply_markup=build_main_menu_kb())


async def _show_dept_select(update: Update, kind: str):
    q = update.callback_query
    header = {
        "absentee": "📋 *결석자 조회*\n\n부서를 선택하세요 👇",
        "special":  "🚨 *특별관리결석자*\n\n부서를 선택하세요 👇\n(연속결석 4회 이상만 표시됩니다)",
    }[kind]
    await q.edit_message_text(header, parse_mode="Markdown", reply_markup=build_dept_menu_kb(kind))


# ─────────────────────────────────────────────────────────────────────────────
# 일반 결석자 흐름
# ─────────────────────────────────────────────────────────────────────────────
async def _on_absentee_dept_selected(update: Update, chat_id: int, dept: str):
    q = update.callback_query
    week_key, week_label = await get_active_week_key()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다. 웹 대시보드에서 명단을 먼저 업로드해주세요.")
        return

    # 컨텍스트에 저장 — 다음 입력은 "지역 또는 구역명"
    await save_ctx(chat_id,
        active_week_key=week_key,
        dept_filter=dept,
        region_filter="",
        editing_step="awaiting_region_or_zone",
    )

    text = (
        f"✅ *{md(dept)}* 선택\n"
        f"📅 주차: `{md(week_label)}`\n\n"
        f"🔍 *지역 또는 구역명을 입력*해주세요.\n\n"
        f"• 지역 예) `강북`, `강남`, `강서`, `강동`\n"
        f"• 구역 예) `2-1` 또는 `2팀1` (둘 다 동일)\n\n"
        f"취소하려면 /취소 를 입력하세요."
    )
    await q.edit_message_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="menu:absentee")]
        ]),
    )


async def _render_absentee_list(update, chat_id: int, dept: str, query: str, 
                                absentees: list, week_label: str, by_zone: bool):
    """조회 결과를 버튼 목록으로 표시"""
    if not absentees:
        msg = (f"📭 [{dept} / {query}] 결석자가 없습니다.\n"
               f"(주차: {week_label})\n\n"
               f"/menu 로 메인 메뉴로 돌아가세요.")
        return await update.message.reply_text(msg)

    buttons = []
    row = []
    for ab in absentees:
        name   = ab.get("name", "?")
        phone  = ab.get("phone_last4", "") or ""
        zone   = ab.get("zone_name", "") or ""
        region = ab.get("region_name", "") or ""
        streak = display_streak(ab.get("consecutive_absent_count"))
        special_mark = "🚨 " if ab.get("is_special_managed") else ""

        # 지역 조회면: "이름 구역 연속X회"
        # 구역 조회면: "이름 연속X회"
        if by_zone:
            label = f"{special_mark}{name} 연속{streak}회"
        else:
            label = f"{special_mark}{name} {zone} 연속{streak}회"
        
        row.append(InlineKeyboardButton(label, callback_data=f"select:{ab['row_id']}"))
        if len(row) == 1:  # 한 줄에 1개씩 (이름 + 구역 + 연속횟수 길이 때문)
            buttons.append(row); row = []
    if row: buttons.append(row)
    buttons.append([InlineKeyboardButton("◀ 메인 메뉴", callback_data="menu:home")])

    header = (
        f"📋 *{md(dept)} / {md(query)}* 결석자 목록\n"
        f"주차: `{md(week_label)}` | 총 {len(absentees)}명\n\n"
        f"심방 기록할 결석자를 선택하세요 👇"
    )
    await update.message.reply_text(header, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons))


# ─────────────────────────────────────────────────────────────────────────────
# 텍스트 메시지 처리 (지역/구역 입력 또는 단계 입력)
# ─────────────────────────────────────────────────────────────────────────────
async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    ctx = await get_ctx(chat_id)
    if not ctx:
        return  # 무시

    step = ctx.get("editing_step", "")

    # ── 1) 지역/구역 입력 대기 ──
    if step == "awaiting_region_or_zone":
        dept = ctx.get("dept_filter", "")
        week_key = ctx.get("active_week_key", "")
        if not dept or not week_key:
            await update.message.reply_text("❌ 세션이 만료됐습니다. /menu 로 다시 시작해주세요.")
            return

        # 주차 라벨 조회
        wrows = await sb_get(
            f"weekly_target_weeks?select=week_label&week_key=eq.{quote(week_key)}&limit=1"
        )
        week_label = wrows[0]["week_label"] if wrows else week_key

        # 구역 형식이면 구역 검색, 아니면 지역 검색
        if looks_like_zone(text):
            normalized = normalize_zone_py(text)
            absentees = await get_absentees_by_zone(week_key, dept, normalized)
            await save_ctx(chat_id, region_filter=normalized, editing_step="")
            await _render_absentee_list(update, chat_id, dept, normalized,
                                        absentees, week_label, by_zone=True)
        else:
            absentees = await get_absentees_by_region(week_key, dept, text)
            await save_ctx(chat_id, region_filter=text, editing_step="")
            await _render_absentee_list(update, chat_id, dept, text,
                                        absentees, week_label, by_zone=False)
        return

    # ── 2) 특별관리 3/4번 텍스트 입력 대기 ──
    if step in ("awaiting_sp_item3", "awaiting_sp_item4"):
        dept  = ctx.get("dept_filter", "")
        name  = ctx.get("tmp_sp_name", "")
        phone = ctx.get("tmp_sp_phone", "")
        if step == "awaiting_sp_item3":
            await sb_rpc("set_special_item3", {
                "p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": text
            })
            field_label = "금주 심방예정일"
        else:
            await sb_rpc("set_special_item4", {
                "p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": text
            })
            field_label = "금주 심방계획"
        
        await save_ctx(chat_id, editing_step="")
        await update.message.reply_text(
            f"✅ *{md(field_label)}* 저장됨:\n`{md(text)}`",
            parse_mode="Markdown"
        )
        # 다시 상세 화면 표시
        await _show_special_detail_for_person(update, dept, name, phone, send_new=True)
        return

    # ── 3) 일반 심방 입력 단계 ──
    if step in STEPS:
        tmp_key = f"tmp_{step}"
        await save_ctx(chat_id, **{tmp_key: text})
        step_idx = STEPS.index(step)
        await _proceed_to_next_step(update, chat_id, step_idx, ctx)


# ─────────────────────────────────────────────────────────────────────────────
# 심방 기록 입력 흐름 (기존)
# ─────────────────────────────────────────────────────────────────────────────
async def _on_select_absentee(update: Update, chat_id: int, row_id: str):
    query = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await query.message.reply_text("❌ 세션 만료. /menu 로 다시 시작해주세요.")
        return

    week_key = ctx.get("active_week_key", "")
    prog = await get_progress(week_key, row_id)
    rows = await sb_get(
        f"weekly_visit_targets?select=name,phone_last4,region_name,zone_name"
        f"&row_id=eq.{quote(row_id)}&week_key=eq.{quote(week_key)}"
    )
    ab = rows[0] if rows else {}
    name = ab.get("name", "?")

    await save_ctx(chat_id, editing_row_id=row_id, editing_step="shepherd")

    existing_info = ""
    if prog:
        existing_info = (
            f"\n\n📂 *기존 입력값*\n"
            f"심방자: {prog.get('shepherd','')}\n"
            f"심방날짜: {prog.get('visit_date_display','')}\n"
            f"진행여부: {'완료' if prog.get('is_done') else '미완료'}"
        )

    await query.message.reply_text(
        f"✏️ *{md(name)}* 님 심방 기록 시작{existing_info}\n\n"
        f"1️⃣ {STEP_LABELS['shepherd']}\n입력해주세요:\n\n"
        f"중단하려면 /취소 를 입력하세요.",
        parse_mode="Markdown",
    )


async def _handle_choice(update: Update, chat_id: int, step: str, value: str):
    query = update.callback_query
    tmp_key = f"tmp_{step}"
    await save_ctx(chat_id, **{tmp_key: value})
    ctx = await get_ctx(chat_id)
    step_idx = STEPS.index(step)

    class FakeUpdate:
        message = query.message
        effective_chat = update.effective_chat

    await _proceed_to_next_step(FakeUpdate(), chat_id, step_idx, ctx)


async def _proceed_to_next_step(update, chat_id: int, current_idx: int, ctx: dict):
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
        await update.message.reply_text(f"{step_num}️⃣ {label}\n입력해주세요:")


async def _show_confirm(update, chat_id: int, ctx: dict = None):
    if ctx is None:
        ctx = await get_ctx(chat_id)
    row_id = ctx.get("editing_row_id", "")
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
    name = rows[0]["name"] if rows else row_id

    summary = (
        f"📋 *심방 기록 확인* — {md(name)}\n\n"
        f"심방자: {ctx.get('tmp_shepherd','')}\n"
        f"심방날짜: {ctx.get('tmp_date','')}\n"
        f"심방계획: {ctx.get('tmp_plan','')}\n"
        f"타겟여부: {ctx.get('tmp_target','')}\n"
        f"진행여부: {ctx.get('tmp_done','')}\n"
        f"예배확답: {ctx.get('tmp_worship','')}\n"
        f"진행사항: {ctx.get('tmp_note','')}\n"
        f"예배참석: {ctx.get('tmp_attendance','')}\n\n"
        f"저장하시겠습니까?"
    )
    buttons = [[
        InlineKeyboardButton("✅ 저장", callback_data="confirm_save"),
        InlineKeyboardButton("❌ 취소", callback_data="cancel_save"),
    ]]
    await update.message.reply_text(summary, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons))


async def _do_save(update: Update, chat_id: int):
    query = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await query.message.reply_text("❌ 세션 만료.")
        return
    week_key = ctx.get("active_week_key", "")
    row_id   = ctx.get("editing_row_id", "")
    if not week_key or not row_id:
        await query.message.reply_text("❌ 저장 정보가 없습니다.")
        return
    try:
        await upsert_progress(week_key, row_id, ctx)
        await clear_tmp(chat_id)
        rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{quote(row_id)}")
        name = rows[0]["name"] if rows else row_id
        await query.message.reply_text(
            f"✅ *{md(name)}* 님 심방 기록 저장 완료!\n\n"
            f"계속하려면 /menu 로 돌아가세요.",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.exception(e)
        await query.message.reply_text(f"❌ 저장 실패: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 특별관리결석자 흐름
# ─────────────────────────────────────────────────────────────────────────────
async def _on_special_dept_selected(update: Update, chat_id: int, dept: str):
    q = update.callback_query
    week_key, week_label = await get_active_week_key()
    if not week_key:
        await q.edit_message_text("❌ 등록된 주차가 없습니다.")
        return

    targets = await get_absentees_4plus(week_key, dept)
    if not targets:
        await q.edit_message_text(
            f"📭 *{md(dept)}* 의 연속결석 4회 이상 결석자가 없습니다.\n"
            f"(주차: `{md(week_label)}`)",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("◀ 부서 다시 선택", callback_data="menu:special")]
            ]),
        )
        return

    buttons = []
    for t in targets:
        name   = t.get("name", "?")
        phone  = t.get("phone_last4", "") or ""
        region = t.get("region_name", "") or ""
        zone   = t.get("zone_name", "") or ""
        streak = display_streak(t.get("consecutive_absent_count"))
        mark = "🚨" if t.get("is_special_managed") else "⚠️"
        label = f"{mark} {name} ({region} {zone}) 연속{streak}회"
        buttons.append([InlineKeyboardButton(label,
            callback_data=f"sp_pick:{dept}:{name}:{phone}")])

    buttons.append([InlineKeyboardButton("◀ 부서 다시 선택", callback_data="menu:special")])
    
    text = (
        f"🚨 *{md(dept)} 특별관리 대상자*\n"
        f"주차: `{md(week_label)}` | 4회 이상 {len(targets)}명\n\n"
        f"🚨 = 이미 특별관리 등록됨 (방 감지중)\n"
        f"⚠️ = 아직 미등록\n\n"
        f"관리할 결석자를 선택하세요 👇\n"
        f"(선택 시 *현재 이 방*이 감지 방으로 등록됩니다)"
    )
    await q.edit_message_text(text, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons))


async def _on_special_person_selected(update: Update, chat_id: int,
                                       dept: str, name: str, phone: str):
    """특별관리 대상 선택 → 방 감지 등록 + 상세 화면 표시"""
    q = update.callback_query
    chat = update.effective_chat

    # 먼저 결석자 정보 조회 (region, zone)
    rows = await sb_get(
        f"weekly_visit_targets?select=region_name,zone_name"
        f"&dept=eq.{quote(dept)}&name=eq.{quote(name)}"
        f"&phone_last4=eq.{quote(phone)}"
        f"&limit=1"
    )
    region = rows[0].get("region_name", "") if rows else ""
    zone   = rows[0].get("zone_name", "")   if rows else ""

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
        logger.exception("register_special_management failed: %s", e)
        await q.message.reply_text(f"❌ 등록 실패: {e}")
        return

    await q.edit_message_text(
        f"✅ *{md(name)}* 님을 *특별관리 대상*으로 등록했습니다.\n"
        f"이 방에서 감지를 시작합니다.\n\n"
        f"매주 화요일 {WEEKLY_REMINDER_HOUR:02d}:{WEEKLY_REMINDER_MIN:02d}(KST) 에 "
        f"미체크 항목 리마인더가 이 방으로 발송됩니다.",
        parse_mode="Markdown",
    )
    # 상세 화면 이어서 표시
    await _show_special_detail_for_person(update, dept, name, phone, send_new=True)


async def _show_special_detail_for_person(update, dept: str, name: str, phone: str,
                                            send_new: bool = False):
    """특별관리 한 명의 상세 체크리스트 표시"""
    detail_rows = await sb_rpc("get_special_detail", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone
    })
    if not detail_rows:
        msg = "❌ 특별관리 정보를 찾을 수 없습니다."
        target = update.message or (update.callback_query.message if update.callback_query else None)
        if target:
            await target.reply_text(msg)
        return

    d = detail_rows[0] if isinstance(detail_rows, list) else detail_rows
    region = d.get("region_name", "") or ""
    zone   = d.get("zone_name", "")   or ""

    item1 = bool(d.get("item1_chat_invited"))
    item2 = bool(d.get("item2_feedback_done"))
    item3 = d.get("item3_visit_date") or ""
    item4 = d.get("item4_visit_plan") or ""

    text = (
        f"🚨 *특별관리: {md(name)}*\n"
        f"{md(dept)} / {md(region)} {md(zone)}\n"
        f"━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{'✅' if item1 else '⬜️'} *1. 대책방 초대완료*\n"
        f"   (구역장, 인섬교, 강사, 전도사, 심방부사명자)\n"
        f"   _최초 한 번만 체크_\n\n"
        f"{'✅' if item2 else '⬜️'} *2. 금주 피드백 진행*\n"
        f"   _매주 화요일 {WEEKLY_REMINDER_HOUR}시 초기화_\n\n"
        f"📅 *3. 금주 심방예정일:* {md(item3) if item3 else '_미입력_'}\n\n"
        f"📝 *4. 금주 심방계획:* {md(item4) if item4 else '_미입력_'}"
    )

    buttons = [
        [InlineKeyboardButton(
            f"{'✅ 1번 체크됨 (탭하여 해제)' if item1 else '⬜️ 1번 체크하기 (대책방 초대완료)'}",
            callback_data=f"sp_toggle1:{dept}:{name}:{phone}")],
        [InlineKeyboardButton(
            f"{'✅ 2번 체크됨 (탭하여 해제)' if item2 else '⬜️ 2번 체크하기 (금주 피드백 진행)'}",
            callback_data=f"sp_toggle2:{dept}:{name}:{phone}")],
        [InlineKeyboardButton("📅 3번 심방예정일 입력/수정",
            callback_data=f"sp_edit3:{dept}:{name}:{phone}")],
        [InlineKeyboardButton("📝 4번 심방계획 입력/수정",
            callback_data=f"sp_edit4:{dept}:{name}:{phone}")],
        [InlineKeyboardButton("🗑 특별관리 해제",
            callback_data=f"sp_unregister:{dept}:{name}:{phone}")],
        [InlineKeyboardButton("◀ 메인 메뉴", callback_data="menu:home")],
    ]

    kb = InlineKeyboardMarkup(buttons)
    if send_new:
        target = update.message or (update.callback_query.message if update.callback_query else None)
        if target:
            await target.reply_text(text, parse_mode="Markdown", reply_markup=kb)
    else:
        q = update.callback_query
        if q:
            try:
                await q.edit_message_text(text, parse_mode="Markdown", reply_markup=kb)
            except Exception:
                await q.message.reply_text(text, parse_mode="Markdown", reply_markup=kb)


async def _on_sp_toggle1(update: Update, chat_id: int, dept: str, name: str, phone: str):
    detail_rows = await sb_rpc("get_special_detail", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone
    })
    cur = False
    if detail_rows:
        d = detail_rows[0] if isinstance(detail_rows, list) else detail_rows
        cur = bool(d.get("item1_chat_invited"))
    await sb_rpc("toggle_special_item1", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": not cur
    })
    await _show_special_detail_for_person(update, dept, name, phone, send_new=False)


async def _on_sp_toggle2(update: Update, chat_id: int, dept: str, name: str, phone: str):
    detail_rows = await sb_rpc("get_special_detail", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone
    })
    cur = False
    if detail_rows:
        d = detail_rows[0] if isinstance(detail_rows, list) else detail_rows
        cur = bool(d.get("item2_feedback_done"))
    await sb_rpc("toggle_special_item2", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone, "p_value": not cur
    })
    await _show_special_detail_for_person(update, dept, name, phone, send_new=False)


async def _on_sp_edit_text(update: Update, chat_id: int, dept: str, name: str, phone: str, which: str):
    """which: '3' or '4'"""
    q = update.callback_query
    step = "awaiting_sp_item3" if which == "3" else "awaiting_sp_item4"
    await save_ctx(chat_id,
        dept_filter=dept,
        tmp_sp_name=name,
        tmp_sp_phone=phone,
        editing_step=step,
    )
    label = "금주 심방예정일" if which == "3" else "금주 심방계획"
    await q.message.reply_text(
        f"✏️ *{md(name)}* 님의 *{label}* 을 입력해주세요:\n\n"
        f"(예: 4월 27일 / 주일예배 후 가정심방 예정)\n\n"
        f"취소하려면 /취소 입력",
        parse_mode="Markdown",
    )


async def _on_sp_unregister(update: Update, chat_id: int, dept: str, name: str, phone: str):
    await sb_rpc("unregister_special_management", {
        "p_dept": dept, "p_name": name, "p_phone_last4": phone
    })
    q = update.callback_query
    await q.edit_message_text(
        f"🗑 *{md(name)}* 님을 특별관리에서 해제했습니다.",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("◀ 메인 메뉴", callback_data="menu:home")]
        ]),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 매주 화요일 19시 KST — 주간 항목 리셋 + 리마인더 전송
# ─────────────────────────────────────────────────────────────────────────────
async def weekly_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    logger.info("🔔 weekly_reminder_job start (Tuesday 19:00 KST)")
    try:
        # 1) 모든 대상 조회
        targets = await sb_rpc("get_all_special_targets", {}) or []
        if not targets:
            logger.info("no special targets; nothing to do")
            # 그래도 주간 리셋은 수행
            await sb_rpc("reset_special_weekly_items", {})
            return

        # 2) 리마인더 전송 (리셋 *이전*에 — 금주 정보로 알림)
        for t in targets:
            chat_id = t.get("monitor_chat_id")
            if not chat_id:
                continue
            name   = t.get("name", "?")
            dept   = t.get("dept", "")
            region = t.get("region_name", "") or ""
            zone   = t.get("zone_name", "") or ""

            unchecked = []
            if not t.get("item1_chat_invited"):
                unchecked.append("⬜️ 1. 대책방 초대완료 (최초 1회)")
            if not t.get("item2_feedback_done"):
                unchecked.append("⬜️ 2. 금주 피드백 진행")
            if not (t.get("item3_visit_date") or ""):
                unchecked.append("⬜️ 3. 금주 심방예정일 (미입력)")
            if not (t.get("item4_visit_plan") or ""):
                unchecked.append("⬜️ 4. 금주 심방계획 (미입력)")

            if not unchecked:
                msg = (
                    f"🔔 *주간 리마인더*\n"
                    f"👤 {md(name)} ({md(dept)} / {md(region)} {md(zone)})\n\n"
                    f"✅ 모든 항목이 체크되어 있습니다. 수고하셨습니다!\n\n"
                    f"_내일부터 2~4번 항목은 초기화되어 다시 작성해야 합니다._"
                )
            else:
                msg = (
                    f"🔔 *주간 리마인더* (화요일 {WEEKLY_REMINDER_HOUR}시)\n"
                    f"👤 *{md(name)}* ({md(dept)} / {md(region)} {md(zone)})\n\n"
                    f"미체크 항목:\n" + "\n".join(unchecked) +
                    f"\n\n/menu → 🚨 특별관리결석자 → {md(dept)} → 선택하여 업데이트하세요."
                )

            try:
                await context.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                logger.warning("send to chat %s failed: %s", chat_id, e)
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=msg.replace("*", "").replace("`", "").replace("_", "")
                    )
                except Exception as e2:
                    logger.warning("plain send also failed: %s", e2)

            # 기록
            try:
                await sb_rpc("mark_special_reminder_sent", {
                    "p_dept": dept, "p_name": name, "p_phone_last4": t.get("phone_last4", "") or ""
                })
            except Exception:
                pass

        # 3) 주간 항목 리셋 (2/3/4번 초기화, 1번은 유지)
        await sb_rpc("reset_special_weekly_items", {})
        logger.info("weekly reset done")

    except Exception as e:
        logger.exception("weekly_reminder_job failed: %s", e)


async def force_weekly_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """테스트용 — 즉시 주간 리마인더 실행"""
    await update.message.reply_text("🔔 주간 리마인더 강제 실행 중...")
    await weekly_reminder_job(context)
    await update.message.reply_text("✅ 완료")


# ─────────────────────────────────────────────────────────────────────────────
# /취소, /도움말
# ─────────────────────────────────────────────────────────────────────────────
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_tmp(chat_id)
    await update.message.reply_text("🚫 현재 작업이 취소됐습니다.\n/menu 로 메인 메뉴.")


HELP_TEXT = (
    "📖 *결석자 타겟 심방 봇 사용법*\n"
    "━━━━━━━━━━━━━━━━━━━━\n\n"

    "*🔹 기본 명령어*\n"
    "• /start 또는 /menu — 메인 메뉴 열기\n"
    "• /도움말 — 이 안내 보기\n"
    "• /취소 — 현재 입력 취소\n\n"

    "*🔹 메인 메뉴 3가지 기능*\n\n"

    "📋 *1) 결석자 — 일반 심방 기록*\n"
    "① `📋 결석자` 버튼 탭\n"
    "② 부서 선택 (자문회/장년회/부녀회/청년회)\n"
    "③ *지역* 또는 *구역*을 입력\n"
    "   • 지역 예: `강북`, `강남`, `강서`, `강동`\n"
    "   • 구역 예: `2-1` 또는 `2팀1` (둘 다 동일하게 인식)\n"
    "④ 나온 결석자 버튼 선택\n"
    "⑤ 8단계 입력: 심방자 → 심방날짜 → 심방계획 → 타겟여부 → 진행여부 → 예배확답 → 진행사항 → 예배참석\n"
    "⑥ 확인 후 ✅ 저장\n\n"

    "🚨 *2) 특별관리결석자 — 4회 이상 관리*\n"
    "① `🚨 특별관리결석자` 버튼 탭\n"
    "② 부서 선택\n"
    "③ 연속결석 4회 이상 명단에서 대상 선택\n"
    "   → *현재 이 방이 감지방으로 등록됨*\n"
    "④ 4항목 체크리스트 관리:\n"
    "   1️⃣ 대책방 초대완료 (최초 1회)\n"
    "   2️⃣ 금주 피드백 진행 (매주 리셋)\n"
    "   3️⃣ 금주 심방예정일 (매주 리셋)\n"
    "   4️⃣ 금주 심방계획 (매주 리셋)\n"
    f"⑤ 매주 화요일 {WEEKLY_REMINDER_HOUR:02d}:{WEEKLY_REMINDER_MIN:02d}(KST) 에 미체크 항목 리마인더 자동 발송\n\n"

    "*🔹 자동 주차 계산*\n"
    "화요일 18시 KST 기준으로 자동 전환됩니다.\n"
    "• 화요일 18시 이전 → 지난 주일 주차\n"
    "• 화요일 18시 이후 → 이번 주일 주차\n\n"

    "*🔹 연속결석 횟수 표시*\n"
    "업로드된 숫자에 이번 주 결석 1회가 이미 더해진 값입니다.\n"
    "(화요일 18시 업로드 시 자동 +1 처리)\n\n"

    "*🔹 문제 해결*\n"
    "• '결석자가 없습니다' → 지역/구역명 확인 or 명단 업로드 확인\n"
    "• 구역 형식: `N-M` 또는 `N팀M` 둘 다 OK\n"
    "• 언제든 /menu 로 처음부터 다시 시작 가능"
)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(HELP_TEXT, parse_mode="Markdown")


async def _show_help(update: Update, edit: bool = False):
    q = update.callback_query
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("◀ 메인 메뉴", callback_data="menu:home")]
    ])
    if edit and q:
        try:
            await q.edit_message_text(HELP_TEXT, parse_mode="Markdown", reply_markup=kb)
            return
        except Exception:
            pass
    target = update.message or (q.message if q else None)
    if target:
        await target.reply_text(HELP_TEXT, parse_mode="Markdown", reply_markup=kb)


# ─────────────────────────────────────────────────────────────────────────────
# 앱 시작
# ─────────────────────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # 기본 명령어
    app.add_handler(CommandHandler("start",    start_command))
    app.add_handler(CommandHandler("menu",     menu_command))
    app.add_handler(CommandHandler("도움말",   help_command))
    app.add_handler(CommandHandler("help",     help_command))
    app.add_handler(CommandHandler("취소",     cancel_command))
    app.add_handler(CommandHandler("cancel",   cancel_command))
    app.add_handler(CommandHandler("주간알림테스트", force_weekly_command))

    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    # 매주 화요일 19시 KST 리마인더
    if app.job_queue is not None:
        app.job_queue.run_daily(
            weekly_reminder_job,
            time=dtime(hour=WEEKLY_REMINDER_HOUR, minute=WEEKLY_REMINDER_MIN, tzinfo=KST),
            days=(1,),  # 1 = 화요일 (월=0, 화=1, ..., 일=6 in python-telegram-bot v20)
            name="weekly_special_reminder",
        )
        logger.info("📅 weekly reminder scheduled: Tuesday %02d:%02d KST",
                    WEEKLY_REMINDER_HOUR, WEEKLY_REMINDER_MIN)
    else:
        logger.warning("⚠ JobQueue is not available.")

    port = int(os.environ.get("PORT", 8080))
    webhook_url = os.environ["WEBHOOK_URL"]
    logger.info(f"Starting webhook on port={port}, url={webhook_url}")
    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path="webhook",
        webhook_url=webhook_url,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
