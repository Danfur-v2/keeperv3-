import json
import logging
from datetime import datetime
import pytz
from telegram import Update
from telegram.ext import ContextTypes

logger = logging.getLogger(__name__)
TZ = pytz.timezone('America/Guatemala')


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data['db']
    db.set_chat_id(update.effective_chat.id)
    await update.message.reply_text(
        "Hey, I'm Keeper.\n\n"
        "I'll check in throughout the day and track your habits, reading, finances, and more.\n\n"
        "Things you can tell me anytime:\n"
        "• \"spent Q150 on groceries\"\n"
        "• \"Made Studio paid me Q3000\"\n"
        "• \"BcBlurrr paid this month\"\n"
        "• \"read 30 min\"\n"
        "• \"finished Atomic Habits\"\n"
        "• \"gave 5 units of [product] to the store\" (Casa Fantasma)\n"
        "• \"remind me to call the dentist Friday at 10am\"\n"
        "• \"how's my month going?\"\n\n"
        "What book are you reading right now?"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    db = context.bot_data['db']
    ai = context.bot_data['ai']

    if not db.get_chat_id():
        db.set_chat_id(update.effective_chat.id)

    user_message = update.message.text
    db.add_conversation('user', user_message)

    try:
        response = ai.chat(user_message)
        message = response.get('message', 'Got it.')
        actions = response.get('actions', [])

        for action in actions:
            _process_action(action, db, context)

        db.add_conversation('assistant', json.dumps(response))
        await update.message.reply_text(message)

    except Exception as e:
        logger.error(f"Error handling message: {e}", exc_info=True)
        await update.message.reply_text("Something went wrong on my end. Try again in a moment.")


def _process_action(action, db, context):
    t = action.get('type')
    try:
        if t == 'log_expense':
            db.log_expense(
                amount=float(action['amount']),
                currency=action.get('currency', 'GTQ'),
                category=action.get('category', 'Other'),
                description=action.get('description', ''),
                wallet=action.get('wallet', 'personal')
            )

        elif t == 'log_income':
            db.log_income(
                amount=float(action['amount']),
                currency=action.get('currency', 'GTQ'),
                source=action.get('source', ''),
                client=action.get('client', ''),
                wallet=action.get('wallet', 'personal')
            )

        elif t == 'mark_client_paid':
            db.mark_client_paid(
                client=action['client'],
                month=action.get('month'),
                amount=action.get('amount'),
                currency=action.get('currency', 'GTQ')
            )

        elif t == 'log_reading':
            mins = int(action.get('duration_minutes', 0))
            db.log_reading_session(duration_minutes=mins, notes=action.get('notes', ''))
            db.log_habit('reading', f'{mins}min', action.get('notes', ''))

        elif t == 'finish_book':
            db.finish_book(notes=action.get('notes', ''))

        elif t == 'start_book':
            db.start_book(title=action['title'], author=action.get('author', ''))

        elif t == 'log_habit':
            db.log_habit(
                category=action['habit'],
                value=action.get('value', ''),
                notes=action.get('notes', '')
            )
            xp = action.get('xp', 0)
            if xp:
                db.award_xp(xp, reason=action['habit'])

        elif t == 'award_xp':
            db.award_xp(int(action['amount']), reason=action.get('reason', ''))

        elif t == 'log_journal':
            db.add_journal_entry(
                text=action.get('text', ''),
                mood=action.get('mood'),
                mood_score=action.get('mood_score'),
                major_event=action.get('major_event')
            )

        elif t == 'add_reminder':
            remind_at = action['remind_at']
            dt = datetime.fromisoformat(remind_at)
            if dt.tzinfo is None:
                dt = TZ.localize(dt)
            reminder_id = db.add_reminder(message=action['message'], remind_at=dt.isoformat())
            if context and context.job_queue:
                context.job_queue.run_once(
                    _fire_reminder,
                    when=dt,
                    data={'id': reminder_id, 'message': action['message']},
                    name=f'reminder_{reminder_id}'
                )

        elif t == 'add_habit':
            with db.conn() as c:
                c.execute(
                    "INSERT INTO custom_habits (name, frequency, check_in_time) VALUES (?,?,?)",
                    (action['name'], action.get('frequency', 'daily'), action.get('check_in_time', '21:00'))
                )

        elif t == 'cf_inventory':
            db.cf_log_inventory(
                action=action['action'],
                product=action['product'],
                quantity=int(action.get('quantity', 0)),
                price_per_unit=action.get('price_per_unit'),
            )

    except Exception as e:
        logger.error(f"Error processing action '{t}': {e}", exc_info=True)


async def _fire_reminder(context):
    db = context.bot_data['db']
    chat_id = db.get_chat_id()
    if not chat_id:
        return
    data = context.job.data
    await context.bot.send_message(chat_id=chat_id, text=f"⏰ {data['message']}")
    db.mark_reminder_sent(data['id'])
