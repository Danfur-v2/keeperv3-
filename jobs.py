import random
import logging
import calendar
from datetime import datetime, timedelta
import pytz

logger = logging.getLogger(__name__)
TZ = pytz.timezone('America/Guatemala')


async def _send(context, text):
    db = context.bot_data['db']
    chat_id = db.get_chat_id()
    if not chat_id:
        logger.warning("No chat_id stored — skipping scheduled message")
        return
    await context.bot.send_message(chat_id=chat_id, text=text)


async def _ai(context, message_type, extra=''):
    ai = context.bot_data['ai']
    try:
        return ai.generate_scheduled_message(message_type, extra)
    except Exception as e:
        logger.error(f"AI error for {message_type}: {e}")
        return None


# --- Daily ---

async def wake_up_check(context):
    msg = await _ai(context, 'wake_up')
    if msg:
        await _send(context, msg)


async def daily_briefing(context):
    now = datetime.now(TZ)
    # Monday gets the weekly preview instead
    if now.weekday() == 0:
        msg = await _ai(context, 'weekly_preview')
    else:
        msg = await _ai(context, 'daily_briefing')
    if msg:
        await _send(context, msg)


async def bcblurrr_reminder(context):
    if datetime.now(TZ).weekday() >= 5:
        return
    msg = await _ai(context, 'bcblurrr_reminder')
    if msg:
        await _send(context, msg)


async def bcblurrr_wrapup(context):
    if datetime.now(TZ).weekday() >= 5:
        return
    msg = await _ai(context, 'bcblurrr_wrapup')
    if msg:
        await _send(context, msg)


async def reading_nudge(context):
    db = context.bot_data['db']
    already_read = any(cat == 'reading' for cat, _, _ in db.get_today_logs())
    if already_read:
        return
    msg = await _ai(context, 'reading_nudge')
    if msg:
        await _send(context, msg)


async def schedule_daily_reading_nudge(context):
    now = datetime.now(TZ)
    hour = random.randint(9, 20)
    minute = random.randint(0, 59)
    nudge_time = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if nudge_time <= now:
        nudge_time += timedelta(days=1)
    context.job_queue.run_once(reading_nudge, when=nudge_time, name='reading_nudge_today')


async def evening_recap(context):
    now = datetime.now(TZ)
    # Sunday evening gets the weekly recap
    if now.weekday() == 6:
        msg = await _ai(context, 'weekly_recap')
    else:
        msg = await _ai(context, 'evening_recap')
    if msg:
        await _send(context, msg)


async def bedtime_reminder(context):
    msg = await _ai(context, 'bedtime')
    if msg:
        await _send(context, msg)


# --- Weekly recap (Sunday 5pm) ---

async def weekly_recap(context):
    msg = await _ai(context, 'weekly_recap')
    if msg:
        await _send(context, msg)


# --- Monthly recap (last day of month, 5pm) ---

async def monthly_recap(context):
    now = datetime.now(TZ)
    last_day = calendar.monthrange(now.year, now.month)[1]
    if now.day != last_day:
        return
    msg = await _ai(context, 'monthly_recap')
    if msg:
        await _send(context, msg)


# --- Monthly financial/admin reminders ---

async def invoice_bcblurrr(context):
    if datetime.now(TZ).day != 1:
        return
    msg = await _ai(context, 'invoice_bcblurrr')
    if msg:
        await _send(context, msg)


async def factura_and_taxes(context):
    if datetime.now(TZ).day != 25:
        return
    ai = context.bot_data['ai']
    for t in ('factura_made', 'taxes'):
        try:
            msg = ai.generate_scheduled_message(t)
            await _send(context, msg)
        except Exception as e:
            logger.error(f"Error in {t}: {e}")


async def credit_card_reminder(context):
    if datetime.now(TZ).day != 10:
        return
    msg = await _ai(context, 'credit_card')
    if msg:
        await _send(context, msg)


async def content_reminders(context):
    now = datetime.now(TZ)
    if now.weekday() != 4:  # Fridays only
        return
    last_sat = _last_saturday_of_month(now)
    second_last_sat = last_sat - timedelta(days=7)
    ai = context.bot_data['ai']
    if now.date() == (last_sat - timedelta(days=1)).date():
        try:
            await _send(context, ai.generate_scheduled_message('crypto_content'))
        except Exception as e:
            logger.error(f"crypto_content error: {e}")
    if now.date() == (second_last_sat - timedelta(days=1)).date():
        try:
            await _send(context, ai.generate_scheduled_message('kasemal_content'))
        except Exception as e:
            logger.error(f"kasemal_content error: {e}")


async def dental_reminder(context):
    now = datetime.now(TZ)
    if now.month == 2 and now.day == 1:
        msg = await _ai(context, 'dental')
        if msg:
            await _send(context, msg)


async def unpaid_clients_check(context):
    """Mid-month (15th) check for unpaid clients."""
    now = datetime.now(TZ)
    if now.day != 15:
        return
    db = context.bot_data['db']
    unpaid = db.get_unpaid_clients()
    if unpaid:
        msg = await _ai(context, 'unpaid_clients', ', '.join(unpaid))
        if msg:
            await _send(context, msg)


# --- One-time reminders poller ---

async def check_pending_reminders(context):
    db = context.bot_data['db']
    for reminder_id, message in db.get_pending_reminders():
        await _send(context, f"⏰ {message}")
        db.mark_reminder_sent(reminder_id)


# --- Helper ---

def _last_saturday_of_month(dt):
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    last_date = dt.replace(day=last_day)
    days_back = (last_date.weekday() - 5) % 7
    return last_date - timedelta(days=days_back)
