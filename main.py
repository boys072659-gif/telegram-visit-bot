"""
결석자 타겟 심방 텔레그램 봇 (관리단계 + 일일 리마인더 포함)
Cloud Run (Python 3.11) + python-telegram-bot 20.x + Supabase REST API

기본 명령어:
  /자문회 강북   → 자문회 강북 지역 결석자 버튼 목록
  /장년회 강남   → 장년회 강남 지역 결석자 버튼 목록
  /부녀회 강서   → 부녀회 강서 지역 결석자 버튼 목록
  /청년회 강동   → 청년회 강동 지역 결석자 버튼 목록
  /취소          → 현재 입력 중인 작업 취소
  /도움말        → 사용법 안내

관리단계 명령어 (4회 이상 연속결석자):
  /관리대책방등록 → 현재 채팅방을 관리대책방으로 등록 (매일 리마인더 수신)
  /관리대상       → 4회 이상 연속결석자 목록 표시 (단계 체크리스트 관리)
  /관리확인       → 일일 리마인더 즉시 실행 (테스트용)
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
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]
SUPABASE_URL    = os.environ["SUPABASE_URL"]
SUPABASE_KEY    = os.environ["SUPABASE_KEY"]
# 일일 리마인더 실행 시각 (한국 시간 기준) — 기본 09:00
REMINDER_HOUR   = int(os.environ.get("REMINDER_HOUR", "9"))
REMINDER_MIN    = int(os.environ.get("REMINDER_MIN", "0"))

KST = timezone(timedelta(hours=9))

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

# ── 입력 단계 순서 ─────────────────────────────────────────────────────────────
STEPS = ["shepherd", "date", "plan", "target", "done", "worship", "note", "attendance"]
STEP_LABELS = {
    "shepherd":   "👤 심방자 (예: 홍길동(집사))",
    "date":       "📅 심방날짜 (예: 4/23 또는 2026-04-23)",
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

# ── 관리단계 체크리스트 정의 ──────────────────────────────────────────────────
STAGE_ITEMS = {
    1: [
        ("s1_chat_opened",    "구역장 창개설 및 초대완료"),
        ("s1_1st_strategy",   "담당 강사 1차 전략 모임"),
        ("s1_1st_visit",      "인섬교/친분자 1차 심방 (결석 사유파악)"),
        ("s1_prayer",         "기도문 올리기"),
    ],
    2: [
        ("s2_2nd_strategy",   "담당 강사 2차 전략 모임"),
        ("s2_2nd_visit",      "2차 심방 (신앙우위자 선정)"),
        ("s2_solution",       "결석 사유에 대한 해결 방안 제시"),
        ("s2_prayer_rotation","인섬교 로테이션 기도문 올리기"),
    ],
    3: [
        ("s3_3rd_strategy",   "담당 강사 3차 전략 모임"),
        ("s3_video_letter",   "구역장의 회장/지파장 영상편지 심방"),
        ("s3_sharing",        "인섬교 친교 후 창에 공유"),
    ],
    4: [
        ("s4_4th_strategy",   "담당 강사 4차 전략 모임"),
        ("s4_pastor_visit",   "강사/전도사 심방 진행"),
    ],
}
STAGE_TITLES = {1: "1단계", 2: "2단계", 3: "3단계", 4: "4단계"}

# ── 마크다운 이스케이프 ──────────────────────────────────────────────────────
# 이름·지역·구역 등 사용자 데이터에 *_[]() 가 들어가면 Markdown 파싱이 깨짐
_MD_SPECIALS = "_*`["
def md(s) -> str:
    if s is None:
        return ""
    out = []
    for ch in str(s):
        if ch in _MD_SPECIALS:
            out.append("\\" + ch)
        else:
            out.append(ch)
    return "".join(out)

# 텔레그램 메시지 최대 길이 (여유 포함)
TG_MAX_LEN = 3800


# ─────────────────────────────────────────────────────────────────────────────
# Supabase 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
async def sb_get(path: str):
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, timeout=15)
        r.raise_for_status()
        if not r.content or not r.content.strip():   # 빈 응답 방어
            return []
        return r.json()

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
        # 일부 RPC는 void 반환 → 빈 body
        if not r.content:
            return None
        try:
            return r.json()
        except Exception:
            return None

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

async def get_latest_week_key() -> str:
    try:
        rows = await sb_get("weekly_target_weeks?select=week_key&order=week_key.desc&limit=1")
        if rows and isinstance(rows, list):
            return rows[0].get("week_key", "")
    except Exception as e:
        logger.warning("get_latest_week_key failed: %s", e)
    return ""

async def get_recent_weeks(limit: int = 6):
    """최근 주차 목록 (week_key, week_label) 반환."""
    try:
        rows = await sb_get(
            f"weekly_target_weeks?select=week_key,week_label&order=week_key.desc&limit={limit}"
        )
        return rows or []
    except Exception as e:
        logger.warning("get_recent_weeks failed: %s", e)
        return []

async def get_absentees(week_key: str, dept: str, region: str):
    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{quote(week_key, safe='')}"
        f"&dept=eq.{quote(dept, safe='')}"
        f"&region_name=eq.{quote(region, safe='')}"
        f"&order=row_order.asc"
    )
    return await sb_get(path)

async def get_progress(week_key: str, row_id: str):
    rows = await sb_get(
        f"weekly_visit_progress?select=*&week_key=eq.{week_key}&row_id=eq.{row_id}"
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
# 관리단계 헬퍼
# ─────────────────────────────────────────────────────────────────────────────
async def get_management_targets(week_key: str):
    return await sb_rpc("get_management_targets", {"p_week_key": week_key}) or []

async def toggle_management_item(week_key: str, row_id: str, field: str, value: bool):
    await sb_rpc("toggle_management_item", {
        "p_week_key": week_key,
        "p_row_id":   row_id,
        "p_field":    field,
        "p_value":    value,
    })

async def set_management_stage(week_key: str, row_id: str, stage: int):
    await sb_rpc("set_management_stage", {
        "p_week_key": week_key,
        "p_row_id":   row_id,
        "p_stage":    stage,
    })

async def mark_management_reminder(week_key: str, row_id: str):
    await sb_rpc("mark_management_reminder", {"p_week_key": week_key, "p_row_id": row_id})

async def register_admin_chat(chat_id: int, chat_title: str, note: str = ""):
    await sb_rpc("register_admin_chat", {
        "p_chat_id":    chat_id,
        "p_chat_title": chat_title or "",
        "p_note":       note,
    })

async def get_admin_chats():
    rows = await sb_get("telegram_admin_chats?select=chat_id,chat_title")
    return rows or []


# ─────────────────────────────────────────────────────────────────────────────
# /부서 지역 명령어 → 주차 확인 메뉴 먼저 표시
# ─────────────────────────────────────────────────────────────────────────────
async def dept_region_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cmd_text = update.message.text.strip()
    parts = cmd_text.split(None, 1)
    dept = parts[0].lstrip("/")
    region = parts[1].strip() if len(parts) > 1 else ""

    if not region:
        await update.message.reply_text(f"❗ 지역을 함께 입력해주세요.\n예) /{dept} 강북")
        return

    try:
        # 최신 주차 + 최근 주차 목록 동시 조회
        latest_key = await get_latest_week_key()
        if not latest_key:
            await update.message.reply_text("❌ 등록된 주차 데이터가 없습니다.\n웹 대시보드에서 명단을 먼저 업로드해주세요.")
            return

        recent_weeks = await get_recent_weeks(6)

        # dept / region 을 컨텍스트에 임시 저장 (week 미확정)
        chat_id = update.effective_chat.id
        await save_ctx(chat_id, dept_filter=dept, region_filter=region)

        # 최신 주차 라벨 찾기
        latest_label = next(
            (w.get("week_label", latest_key) for w in recent_weeks if w.get("week_key") == latest_key),
            latest_key,
        )

        # ── 버튼 구성 ──────────────────────────────────────────────────────
        buttons = [
            [InlineKeyboardButton(
                f"✅ 이 주차로 진행  ({latest_label})",
                callback_data=f"week_confirm:{latest_key}",
            )]
        ]
        # 이전 주차가 있으면 '다른 주차 선택' 버튼 추가
        if len(recent_weeks) > 1:
            buttons.append([
                InlineKeyboardButton("🔄 다른 주차 선택", callback_data="week_select")
            ])

        await update.message.reply_text(
            f"📋 *{md(dept)} / {md(region)}*\n\n"
            f"📅 현재 최신 주차: *{md(latest_label)}*\n"
            f"어느 주차 결석자를 조회할까요?",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(buttons),
        )

    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ 오류: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 콜백 (버튼 클릭) — select / choice / mgmt_*
# ─────────────────────────────────────────────────────────────────────────────
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id

    try:
        if data.startswith("select:"):
            await _on_select_absentee(update, chat_id, data.split(":", 1)[1])
        elif data.startswith("choice:"):
            _, step, value = data.split(":", 2)
            await _handle_choice(update, chat_id, step, value)
        elif data == "confirm_save":
            await _do_save(update, chat_id)
        elif data == "cancel_save":
            await clear_tmp(chat_id)
            await query.message.reply_text("🚫 저장이 취소됐습니다.")
        # ── 주차 확인 ──
        elif data.startswith("week_confirm:"):
            week_key = data.split(":", 1)[1]
            await _on_week_confirmed(update, chat_id, week_key)
        elif data == "week_select":
            await _on_week_select(update, chat_id)
        elif data.startswith("week_pick:"):
            week_key = data.split(":", 1)[1]
            await _on_week_confirmed(update, chat_id, week_key)
        # ── 관리단계 ──
        elif data == "mgmt_back":
            await _show_management_list(update, chat_id, edit=True)
        elif data.startswith("mgmt_view:"):
            row_id = data.split(":", 1)[1]
            await _show_management_detail(update, chat_id, row_id, edit=True)
        elif data.startswith("mgmt_toggle:"):
            _, row_id, field = data.split(":", 2)
            await _on_mgmt_toggle(update, chat_id, row_id, field)
        elif data.startswith("mgmt_stage:"):
            _, row_id, stage_s = data.split(":", 2)
            await _on_mgmt_stage(update, chat_id, row_id, int(stage_s))
    except Exception as e:
        logger.exception(e)
        try:
            await query.message.reply_text(f"❌ 오류: {e}")
        except Exception:
            pass


async def _on_week_confirmed(update: Update, chat_id: int, week_key: str):
    """주차 확정 → 결석자 목록 표시."""
    query = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await query.message.reply_text("❌ 세션이 만료됐습니다. 다시 명령어를 입력해주세요.")
        return

    dept   = ctx.get("dept_filter", "")
    region = ctx.get("region_filter", "")

    # week_key 를 컨텍스트에 저장
    await save_ctx(chat_id, active_week_key=week_key)

    # 주차 라벨 조회
    try:
        wrows = await sb_get(
            f"weekly_target_weeks?select=week_label&week_key=eq.{quote(week_key, safe='')}&limit=1"
        )
        week_label = wrows[0].get("week_label", week_key) if wrows else week_key
    except Exception:
        week_label = week_key

    await query.edit_message_text(f"🔍 *{md(dept)} / {md(region)}* 결석자 불러오는 중…", parse_mode="Markdown")

    absentees = await get_absentees(week_key, dept, region)
    if not absentees:
        await query.edit_message_text(
            f"📭 [{dept} / {region}] 결석자가 없습니다.\n(주차: {week_label})"
        )
        return

    buttons = []
    row = []
    for ab in absentees:
        name   = ab.get("name", "?")
        phone  = ab.get("phone_last4", "")
        streak = ab.get("consecutive_absent_count", "")
        label  = f"{name}({phone}) 연속{streak}회"
        row.append(InlineKeyboardButton(label, callback_data=f"select:{ab['row_id']}"))
        if len(row) == 2:
            buttons.append(row); row = []
    if row:
        buttons.append(row)

    await query.edit_message_text(
        f"📋 *{md(dept)} / {md(region)}* 결석자 목록\n"
        f"주차: `{week_label}` | 총 {len(absentees)}명\n\n"
        f"심방 기록할 결석자를 선택하세요 👇",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _on_week_select(update: Update, chat_id: int):
    """주차 목록 버튼 메뉴 표시."""
    query = update.callback_query
    recent_weeks = await get_recent_weeks(6)
    if not recent_weeks:
        await query.answer("조회 가능한 주차가 없습니다.", show_alert=True)
        return

    buttons = [
        [InlineKeyboardButton(
            f"📅 {w.get('week_label', w.get('week_key', '?'))}",
            callback_data=f"week_pick:{w.get('week_key', '')}",
        )]
        for w in recent_weeks
    ]

    await query.edit_message_text(
        "🗓 조회할 주차를 선택하세요:",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def _on_select_absentee(update: Update, chat_id: int, row_id: str):
    query = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await query.message.reply_text("❌ 세션이 만료됐습니다. 다시 명령어를 입력해주세요.")
        return

    week_key = ctx.get("active_week_key", "")
    prog = await get_progress(week_key, row_id)

    rows = await sb_get(
        f"weekly_visit_targets?select=name,phone_last4,region_name,zone_name&row_id=eq.{row_id}&week_key=eq.{week_key}"
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
        f"1️⃣ {STEP_LABELS['shepherd']}\n입력해주세요:",
        parse_mode="Markdown",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 단계별 입력 흐름 (기존 로직 유지)
# ─────────────────────────────────────────────────────────────────────────────
async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    ctx = await get_ctx(chat_id)
    if not ctx or not ctx.get("editing_step"):
        return

    step = ctx["editing_step"]
    tmp_key = f"tmp_{step}"
    await save_ctx(chat_id, **{tmp_key: text})
    step_idx = STEPS.index(step)
    await _proceed_to_next_step(update, chat_id, step_idx, ctx)


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
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{row_id}")
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
        InlineKeyboardButton("✅ 보내기 (저장)", callback_data="confirm_save"),
        InlineKeyboardButton("❌ 취소",          callback_data="cancel_save"),
    ]]
    await update.message.reply_text(summary, parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons))


async def _do_save(update: Update, chat_id: int):
    query = update.callback_query
    ctx = await get_ctx(chat_id)
    if not ctx:
        await query.message.reply_text("❌ 세션 만료. 다시 시작해주세요.")
        return
    week_key = ctx.get("active_week_key", "")
    row_id   = ctx.get("editing_row_id", "")
    if not week_key or not row_id:
        await query.message.reply_text("❌ 저장 정보가 없습니다.")
        return

    try:
        await upsert_progress(week_key, row_id, ctx)
        await clear_tmp(chat_id)
        rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{row_id}")
        name = rows[0]["name"] if rows else row_id
        await query.message.reply_text(
            f"✅ *{md(name)}* 님의 심방 기록이 저장됐습니다!\n\n"
            f"계속하려면 명령어를 다시 입력하세요.\n"
            f"예) /{ctx.get('dept_filter','')} {ctx.get('region_filter','')}",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.exception(e)
        await query.message.reply_text(f"❌ 저장 실패: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 관리단계 — 명령어와 뷰
# ─────────────────────────────────────────────────────────────────────────────
async def register_admin_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    title = chat.title or chat.full_name or f"chat_{chat.id}"
    try:
        await register_admin_chat(chat.id, title, note="registered via /관리대책방등록")
        await update.message.reply_text(
            f"✅ 이 채팅방(`{md(title)}`)을 관리대책방으로 등록했습니다.\n"
            f"매일 {REMINDER_HOUR:02d}:{REMINDER_MIN:02d}(KST)에 미체크 항목 리마인더를 보냅니다.\n\n"
            f"해제: /관리대책방해제",
            parse_mode="Markdown",
        )
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(
                "❌ `register_admin_chat` RPC가 DB에 아직 없습니다.\n"
                "Supabase SQL Editor에서 `supabase_additions.sql` 을 먼저 실행해주세요.",
                parse_mode="Markdown",
            )
        else:
            logger.exception(e)
            await update.message.reply_text(f"❌ 등록 실패: HTTP {e.response.status_code}")
    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ 등록 실패: {e}")


async def unregister_admin_chat_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """현재 채팅방을 관리대책방 목록에서 제거"""
    chat_id = update.effective_chat.id
    try:
        async with httpx.AsyncClient() as client:
            r = await client.delete(
                f"{SUPABASE_URL}/rest/v1/telegram_admin_chats?chat_id=eq.{chat_id}",
                headers=HEADERS,
                timeout=15,
            )
            r.raise_for_status()
        await update.message.reply_text("🗑️ 이 채팅방을 관리대책방에서 해제했습니다.")
    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ 해제 실패: {e}")


async def management_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await _show_management_list(update, update.effective_chat.id, edit=False)
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 404:
            await update.message.reply_text(
                "❌ `get_management_targets` RPC가 DB에 아직 없습니다.\n"
                "Supabase SQL Editor에서 `supabase_additions.sql` 을 먼저 실행해주세요.",
                parse_mode="Markdown",
            )
        else:
            raise


async def _show_management_list(update: Update, chat_id: int, edit: bool = False):
    week_key = await get_latest_week_key()
    if not week_key:
        await _send_or_edit(update, edit, "❌ 등록된 주차가 없습니다.")
        return

    targets = await get_management_targets(week_key)
    if not targets:
        await _send_or_edit(update, edit,
            f"📭 연속결석 4회 이상 결석자가 없습니다.\n(주차: {week_key})")
        return

    buttons = []
    for t in targets:
        name   = t.get("name", "?")
        dept   = t.get("dept", "")
        region = t.get("region_name", "")
        streak = t.get("consecutive_absent_count", "")
        stage  = int(t.get("current_stage") or 1)
        if stage not in STAGE_ITEMS:
            stage = 1
        # 해당 단계 미체크 개수
        remaining = _count_remaining(t, stage)
        mark = "🟢" if remaining == 0 else "🟡"
        # 버튼 라벨은 Markdown 파싱 대상이 아니므로 이스케이프 불필요
        label = f"{mark} {stage}단계 · {dept} {region} · {name}({streak}회) · 미{remaining}"
        buttons.append([InlineKeyboardButton(label, callback_data=f"mgmt_view:{t['row_id']}")])

    text = (
        f"📌 *연속결석 4회 이상 관리대상*\n"
        f"주차: `{week_key}` | 총 {len(targets)}명\n\n"
        f"🟢 해당 단계 전부 완료  🟡 미체크 있음\n\n"
        f"관리할 대상을 선택하세요 👇"
    )
    await _send_or_edit(update, edit, text,
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="Markdown")


async def _show_management_detail(update: Update, chat_id: int, row_id: str, edit: bool = False):
    week_key = await get_latest_week_key()
    targets = await get_management_targets(week_key)
    t = next((x for x in targets if x.get("row_id") == row_id), None)
    if not t:
        await _send_or_edit(update, edit, "❌ 해당 대상을 찾을 수 없습니다.")
        return

    stage = int(t.get("current_stage") or 1)
    if stage not in STAGE_ITEMS:
        stage = 1
    name   = t.get("name", "?")
    dept   = t.get("dept", "")
    region = t.get("region_name", "")
    zone   = t.get("zone_name", "")
    streak = t.get("consecutive_absent_count", "")

    header = (
        f"👤 *{md(name)}* ({md(dept)} / {md(region)} {md(zone)})\n"
        f"연속결석 {streak}회 · 현재 *{STAGE_TITLES[stage]}*\n"
        f"━━━━━━━━━━━━━━━━━━━━"
    )

    # 단계 전환 버튼
    stage_buttons = []
    row = []
    for s in [1, 2, 3, 4]:
        prefix = "✅" if s == stage else "  "
        row.append(InlineKeyboardButton(f"{prefix} {s}단계",
            callback_data=f"mgmt_stage:{row_id}:{s}"))
    stage_buttons.append(row)

    # 체크리스트 — 현재 단계의 항목만
    items = STAGE_ITEMS[stage]
    item_buttons = []
    for field, label in items:
        checked = bool(t.get(field))
        mark = "✅" if checked else "⬜️"
        item_buttons.append([
            InlineKeyboardButton(f"{mark} {label}",
                callback_data=f"mgmt_toggle:{row_id}:{field}")
        ])

    nav = [[InlineKeyboardButton("◀ 목록으로", callback_data="mgmt_back")]]

    body_lines = [header, f"\n📝 *{STAGE_TITLES[stage]} 체크리스트*"]
    remaining = _count_remaining(t, stage)
    if remaining == 0:
        body_lines.append("🎉 이 단계는 전부 완료됐습니다. 다음 단계로 이동을 고려하세요.")
    else:
        body_lines.append(f"미체크 {remaining}개 남음")

    await _send_or_edit(update, edit,
        "\n".join(body_lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(stage_buttons + item_buttons + nav),
    )


async def _on_mgmt_toggle(update: Update, chat_id: int, row_id: str, field: str):
    week_key = await get_latest_week_key()
    # 현재 값 조회 후 반전 (목록 전체 대신 해당 한 명만 필요하지만
    # 관리대상 수가 많지 않으니 현 구조 유지)
    targets = await get_management_targets(week_key)
    t = next((x for x in targets if x.get("row_id") == row_id), None)
    current = bool(t.get(field)) if t else False
    await toggle_management_item(week_key, row_id, field, not current)
    await _show_management_detail(update, chat_id, row_id, edit=True)


async def _on_mgmt_stage(update: Update, chat_id: int, row_id: str, stage: int):
    if stage not in (1, 2, 3, 4): return
    week_key = await get_latest_week_key()
    await set_management_stage(week_key, row_id, stage)
    await _show_management_detail(update, chat_id, row_id, edit=True)


def _count_remaining(row: dict, stage: int) -> int:
    items = STAGE_ITEMS.get(stage, [])
    return sum(1 for field, _ in items if not row.get(field))


async def _send_or_edit(update: Update, edit: bool, text: str,
                        reply_markup=None, parse_mode=None):
    if edit and update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, reply_markup=reply_markup, parse_mode=parse_mode
            )
            return
        except Exception as e:
            # "Message is not modified" 같은 경우 그냥 무시
            if "not modified" in str(e).lower():
                return
            # 그 외 실패 시 새 메시지로 전송 (아래)
            logger.debug("edit_message_text failed, falling back to send: %s", e)
    target_msg = update.message or (update.callback_query.message if update.callback_query else None)
    if target_msg:
        await target_msg.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)


# ─────────────────────────────────────────────────────────────────────────────
# 일일 리마인더 (JobQueue)
# ─────────────────────────────────────────────────────────────────────────────
async def daily_reminder_job(context: ContextTypes.DEFAULT_TYPE):
    """매일 등록된 관리대책방들로 미체크 항목 요약 전송."""
    logger.info("🔔 daily_reminder_job start")
    try:
        week_key = await get_latest_week_key()
        if not week_key:
            logger.info("no week_key; skipping reminder")
            return
        targets = await get_management_targets(week_key)
        admin_chats = await get_admin_chats()
        if not admin_chats:
            logger.info("no admin chats registered; skipping reminder")
            return

        today = datetime.now(KST).date().isoformat()
        pending = []
        for t in targets:
            stage = int(t.get("current_stage") or 1)
            if stage not in STAGE_ITEMS:
                stage = 1
            items = STAGE_ITEMS[stage]
            unchecked = [(field, label) for field, label in items if not t.get(field)]
            if not unchecked:
                continue
            last = t.get("last_reminder_date")
            if last and str(last) == today:
                continue
            pending.append((t, stage, unchecked))

        if not pending:
            logger.info("nothing pending; skipping reminder")
            return

        # ── 메시지 빌드 ────────────────────────────────────────────────
        header = (
            f"🔔 *일일 관리 리마인더* ({datetime.now(KST).strftime('%Y-%m-%d %H:%M')})\n"
            f"주차: `{week_key}`\n"
            f"미체크 항목이 있는 관리대상 {len(pending)}명\n"
            "━━━━━━━━━━━━━━━━━━━━"
        )
        footer = "\n/관리대상 으로 체크하러 가기"

        # 사람별 블록들을 만들고, 길이에 맞춰 묶어서 여러 메시지로 분할
        blocks = []
        for t, stage, unchecked in pending:
            name   = t.get("name", "?")
            dept   = t.get("dept", "")
            region = t.get("region_name", "")
            streak = t.get("consecutive_absent_count", "")
            block_lines = [
                f"\n👤 *{md(name)}* ({md(dept)}/{md(region)}) · 연속{streak}회 · *{STAGE_TITLES[stage]}*"
            ]
            for _, label in unchecked:
                block_lines.append(f"  ⬜️ {label}")
            blocks.append("\n".join(block_lines))

        # 분할 묶기
        messages = []
        current = header
        for block in blocks:
            if len(current) + len(block) + len(footer) > TG_MAX_LEN:
                messages.append(current + footer)
                current = header + block
            else:
                current += "\n" + block
        messages.append(current + footer)

        # ── 전송 ──────────────────────────────────────────────────────
        for ch in admin_chats:
            for idx, text in enumerate(messages):
                try:
                    await context.bot.send_message(
                        chat_id=ch["chat_id"],
                        text=text,
                        parse_mode="Markdown",
                    )
                except Exception as e:
                    # Markdown 파싱 실패 시 일반 텍스트로 재시도
                    logger.warning("send markdown failed (%s), retry plain: %s",
                                   ch.get("chat_id"), e)
                    try:
                        await context.bot.send_message(
                            chat_id=ch["chat_id"],
                            text=text.replace("*", "").replace("`", ""),
                        )
                    except Exception as e2:
                        logger.warning("send plain also failed: %s", e2)

        # 중복 전송 방지 기록
        for t, *_ in pending:
            try:
                await mark_management_reminder(week_key, t["row_id"])
            except Exception as e:
                logger.warning("mark reminder failed: %s", e)

    except Exception as e:
        logger.exception("daily_reminder_job failed: %s", e)


async def force_reminder_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔔 리마인더 강제 실행 중...")
    await daily_reminder_job(context)
    await update.message.reply_text("✅ 완료")


# ─────────────────────────────────────────────────────────────────────────────
# /취소, /도움말
# ─────────────────────────────────────────────────────────────────────────────
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_tmp(chat_id)
    await update.message.reply_text("🚫 현재 작업이 취소됐습니다.")


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *사용법*\n\n"
        "*심방 기록*\n"
        "/자문회 강북 — 자문회 강북 결석자 목록\n"
        "/장년회 강남 — 장년회 강남 결석자 목록\n"
        "/부녀회 강서 — 부녀회 강서 결석자 목록\n"
        "/청년회 강동 — 청년회 강동 결석자 목록\n"
        "/취소 — 현재 입력 취소\n\n"
        "*연속결석 4회 이상 관리*\n"
        "/관리대책방등록 — 이 방을 관리대책방으로 등록\n"
        "/관리대책방해제 — 이 방의 등록 해제\n"
        "/관리대상 — 4회 이상 결석자 단계 체크리스트\n"
        "/관리확인 — 일일 리마인더 즉시 실행 (테스트)\n\n"
        "결석자 선택 → 안내 따라 입력 → Supabase DB 자동 저장\n"
        f"일일 리마인더: 매일 {REMINDER_HOUR:02d}:{REMINDER_MIN:02d}(KST)",
        parse_mode="Markdown",
    )


# ─────────────────────────────────────────────────────────────────────────────
# 앱 시작
# ─────────────────────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # 부서 명령어
    DEPT_NAMES = ["자문회", "장년회", "부녀회", "청년회"]
    for dept in DEPT_NAMES:
        app.add_handler(CommandHandler(dept, dept_region_command))

    app.add_handler(CommandHandler("취소",            cancel_command))
    app.add_handler(CommandHandler("도움말",          help_command))
    app.add_handler(CommandHandler("관리대책방등록",   register_admin_chat_command))
    app.add_handler(CommandHandler("관리대책방해제",   unregister_admin_chat_command))
    app.add_handler(CommandHandler("관리대상",        management_list_command))
    app.add_handler(CommandHandler("관리확인",        force_reminder_command))

    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    # ── 일일 리마인더 스케줄 등록 (KST) ──
    if app.job_queue is not None:
        app.job_queue.run_daily(
            daily_reminder_job,
            time=dtime(hour=REMINDER_HOUR, minute=REMINDER_MIN, tzinfo=KST),
            name="daily_reminder",
        )
        logger.info("📅 daily reminder scheduled at %02d:%02d KST",
                    REMINDER_HOUR, REMINDER_MIN)
    else:
        logger.warning("⚠ JobQueue is not available. "
                       "pip install 'python-telegram-bot[job-queue]' 필요")

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
