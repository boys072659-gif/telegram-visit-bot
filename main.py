"""
결석자 타겟 심방 텔레그램 봇
Cloud Run (Python 3.11) + python-telegram-bot 20.x + Supabase REST API

명령어:
  /자문회 강북   → 자문회 강북 지역 결석자 버튼 목록
  /장년회 강남   → 장년회 강남 지역 결석자 버튼 목록
  /취소           → 현재 입력 중인 작업 취소
"""

import os
import re
import json
import logging
import httpx
from datetime import datetime

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
SUPABASE_URL    = os.environ["SUPABASE_URL"]       # https://xxx.supabase.co
SUPABASE_KEY    = os.environ["SUPABASE_KEY"]        # anon public key

logging.basicConfig(level=logging.INFO)
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
# 버튼 선택형 단계
STEP_CHOICES = {
    "target":     [["타겟", "미타겟"]],
    "done":       [["완료", "미완료"]],
    "worship":    [["확정", "미정", "불참"]],
    "attendance": [["참석", "불참"]],
}

# ── Supabase 헬퍼 ──────────────────────────────────────────────────────────────
async def sb_get(path: str) -> list | dict:
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{SUPABASE_URL}/rest/v1/{path}", headers=HEADERS, timeout=10)
        r.raise_for_status()
        return r.json()

async def sb_rpc(func: str, payload: dict):
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/rpc/{func}",
            headers=HEADERS,
            content=json.dumps(payload),
            timeout=10,
        )
        r.raise_for_status()
        return r.json()

async def get_ctx(chat_id: int) -> dict | None:
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
    """weekly_target_weeks에서 가장 최근 week_key 조회"""
    try:
        rows = await sb_get("weekly_target_weeks?select=week_key&order=week_key.desc&limit=1")
        if rows and isinstance(rows, list):
            return rows[0].get("week_key", "")
    except Exception:
        pass
    return ""

async def get_absentees(week_key: str, dept: str, region: str) -> list:
    """weekly_visit_targets에서 부서+지역 필터로 결석자 조회"""
    # region_name 컬럼 사용 (org_name의 첫 단어가 아닌 실제 region_name)
    path = (
        f"weekly_visit_targets"
        f"?select=row_id,name,phone_last4,region_name,zone_name,consecutive_absent_count"
        f"&week_key=eq.{week_key}"
        f"&dept=eq.{dept}"
        f"&region_name=eq.{region}"
        f"&order=row_order.asc"
    )
    return await sb_get(path)

async def get_progress(week_key: str, row_id: str) -> dict | None:
    rows = await sb_get(
        f"weekly_visit_progress?select=*&week_key=eq.{week_key}&row_id=eq.{row_id}"
    )
    return rows[0] if rows else None

async def upsert_progress(week_key: str, row_id: str, ctx: dict):
    raw_date = ctx.get("tmp_date") or ""
    # visit_date_sort: YYYY-MM-DD 형식이면 그대로, 아니면 None
    import re
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

# ── /부서 지역 명령어 ───────────────────────────────────────────────────────────
async def dept_region_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    /자문회 강북  or  /장년회 강남  등의 커맨드 처리
    커맨드 이름 자체가 부서명 (슬래시 뒤)
    """
    cmd_text = update.message.text.strip()  # 예: /자문회 강북
    parts = cmd_text.split(None, 1)
    dept = parts[0].lstrip("/")             # 자문회
    region = parts[1].strip() if len(parts) > 1 else ""

    if not region:
        await update.message.reply_text(
            f"❗ 지역을 함께 입력해주세요.\n예) /{dept} 강북"
        )
        return

    await update.message.reply_text("🔍 결석자 목록을 불러오는 중...")

    try:
        week_key = await get_latest_week_key()
        if not week_key:
            await update.message.reply_text("❌ 등록된 주차 데이터가 없습니다.")
            return

        absentees = await get_absentees(week_key, dept, region)
        if not absentees:
            await update.message.reply_text(
                f"📭 [{dept} / {region}] 결석자가 없습니다.\n(주차: {week_key})"
            )
            return

        # 컨텍스트 저장
        chat_id = update.effective_chat.id
        await save_ctx(
            chat_id,
            active_week_key=week_key,
            dept_filter=dept,
            region_filter=region,
        )

        # 버튼 생성 (한 줄에 2명씩)
        buttons = []
        row = []
        for ab in absentees:
            name = ab.get("name", "?")
            phone = ab.get("phone_last4", "")
            streak = ab.get("consecutive_absent_count", "")
            label = f"{name}({phone}) 연속{streak}회"
            row.append(InlineKeyboardButton(label, callback_data=f"select:{ab['row_id']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        reply_markup = InlineKeyboardMarkup(buttons)
        await update.message.reply_text(
            f"📋 *{dept} / {region}* 결석자 목록\n"
            f"주차: `{week_key}` | 총 {len(absentees)}명\n\n"
            f"심방 기록할 결석자를 선택하세요 👇",
            parse_mode="Markdown",
            reply_markup=reply_markup,
        )

    except Exception as e:
        logger.exception(e)
        await update.message.reply_text(f"❌ 오류: {e}")


# ── 결석자 버튼 클릭 ────────────────────────────────────────────────────────────
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = update.effective_chat.id

    # ── 결석자 선택 ──
    if data.startswith("select:"):
        row_id = data.split(":", 1)[1]
        ctx = await get_ctx(chat_id)
        if not ctx:
            await query.message.reply_text("❌ 세션이 만료됐습니다. 다시 명령어를 입력해주세요.")
            return

        week_key = ctx.get("active_week_key", "")

        # 기존 심방 정보 미리 불러오기
        prog = await get_progress(week_key, row_id)

        # row 기본정보 가져오기
        rows = await sb_get(
            f"weekly_visit_targets?select=name,phone_last4,region_name,zone_name&row_id=eq.{row_id}&week_key=eq.{week_key}"
        )
        ab = rows[0] if rows else {}
        name = ab.get("name", "?")

        # 컨텍스트에 선택된 row_id와 첫 단계 저장
        await save_ctx(chat_id, editing_row_id=row_id, editing_step="shepherd")

        # 기존 값 안내
        existing_info = ""
        if prog:
            existing_info = (
                f"\n\n📂 *기존 입력값*\n"
                f"심방자: {prog.get('shepherd','')}\n"
                f"심방날짜: {prog.get('visit_date_display','')}\n"
                f"진행여부: {'완료' if prog.get('is_done') else '미완료'}"
            )

        await query.message.reply_text(
            f"✏️ *{name}* 님 심방 기록 시작{existing_info}\n\n"
            f"1️⃣ {STEP_LABELS['shepherd']}\n"
            f"입력해주세요:",
            parse_mode="Markdown",
        )

    # ── 버튼 선택형 답변 ──
    elif data.startswith("choice:"):
        _, step, value = data.split(":", 2)
        await _handle_choice(update, chat_id, step, value)

    # ── 저장 확인 ──
    elif data == "confirm_save":
        await _do_save(update, chat_id)

    # ── 취소 ──
    elif data == "cancel_save":
        await clear_tmp(chat_id)
        await query.message.reply_text("🚫 저장이 취소됐습니다.")


# ── 텍스트 메시지 처리 (단계별 입력) ──────────────────────────────────────────
async def text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    text = update.message.text.strip()

    ctx = await get_ctx(chat_id)
    if not ctx or not ctx.get("editing_step"):
        return  # 진행 중인 입력 없으면 무시

    step = ctx["editing_step"]
    tmp_key = f"tmp_{step}"

    # 현재 단계 값 저장
    await save_ctx(chat_id, **{tmp_key: text})

    # 다음 단계로
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
        # 모든 단계 완료 → 확인 메시지
        await _show_confirm(update, chat_id, ctx)
        return

    next_step = STEPS[next_idx]
    await save_ctx(chat_id, editing_step=next_step)
    label = STEP_LABELS[next_step]
    step_num = next_idx + 1

    if next_step in STEP_CHOICES:
        # 버튼 선택형
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

    # row 이름 조회
    row_id = ctx.get("editing_row_id", "")
    rows = await sb_get(f"weekly_visit_targets?select=name&row_id=eq.{row_id}")
    name = rows[0]["name"] if rows else row_id

    summary = (
        f"📋 *심방 기록 확인* — {name}\n\n"
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
        InlineKeyboardButton("❌ 취소", callback_data="cancel_save"),
    ]]
    await update.message.reply_text(
        summary,
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


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
            f"✅ *{name}* 님의 심방 기록이 저장됐습니다!\n\n"
            f"계속하려면 명령어를 다시 입력하세요.\n"
            f"예) /{ctx.get('dept_filter','')} {ctx.get('region_filter','')}",
            parse_mode="Markdown",
        )
    except Exception as e:
        logger.exception(e)
        await query.message.reply_text(f"❌ 저장 실패: {e}")


# ── /취소 명령어 ────────────────────────────────────────────────────────────────
async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await clear_tmp(chat_id)
    await update.message.reply_text("🚫 현재 작업이 취소됐습니다.")


# ── /도움말 ────────────────────────────────────────────────────────────────────
async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 *사용법*\n\n"
        "/자문회 강북 — 자문회 강북 결석자 목록\n"
        "/장년회 강남 — 장년회 강남 결석자 목록\n"
        "/부녀회 강서 — 부녀회 강서 결석자 목록\n"
        "/청년회 강동 — 청년회 강동 결석자 목록\n"
        "/취소 — 현재 입력 취소\n\n"
        "결석자 버튼 클릭 후 안내에 따라 입력하면\n"
        "Supabase DB에 자동 저장됩니다.",
        parse_mode="Markdown",
    )


# ── 앱 시작 ─────────────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # 부서 명령어 등록
    DEPT_NAMES = ["자문회", "장년회", "부녀회", "청년회"]
    for dept in DEPT_NAMES:
        app.add_handler(CommandHandler(dept, dept_region_command))

    app.add_handler(CommandHandler("취소", cancel_command))
    app.add_handler(CommandHandler("도움말", help_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message))

    port = int(os.environ.get("PORT", 8080))
    webhook_url = os.environ["WEBHOOK_URL"]  # https://your-cloud-run-url/webhook

    logger.info(f"Starting webhook on port={port}, url={webhook_url}")

    # Cloud Run: listen 0.0.0.0, url_path는 /webhook 고정
    app.run_webhook(
        listen="0.0.0.0",
        port=port,
        url_path="webhook",          # ← 슬래시 없이 (PTB가 자동으로 붙임)
        webhook_url=webhook_url,
        drop_pending_updates=True,   # 재시작 시 쌓인 메시지 무시
    )


if __name__ == "__main__":
    main()
