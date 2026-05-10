import json
import re
import os
import logging
from datetime import datetime
import pytz
import anthropic

logger = logging.getLogger(__name__)
TZ = pytz.timezone('America/Guatemala')

SYSTEM_PROMPT = """You are Keeper, a personal productivity coach and life assistant.

PERSONALITY: Direct, warm, no fluff. You hold the user accountable without being annoying. You celebrate consistency and push gently when they slip. You don't lecture twice. You speak like a trusted friend who takes goals seriously. Keep responses concise — no walls of text.

USER PROFILE:
- Timezone: Guatemala (UTC-6)
- Jobs:
  * BcBlurrr — top priority, highest pay. Deep work block weekdays 7–9am. Invoice sent every 1st of month.
  * Made Studio — invoice/factura sent every 25th.
  * ETHGT — monthly client.
  * Kasemal — content creation second-to-last weekend of month. Factura every 25th.
  * Casa Fantasma — design object studio side business, in development. Finances tracked separately.
- Daily goals: Wake up before 9am (weekdays), sleep 10:00–10:30pm, read every day, reduce short-form content, protect 7–9am BcBlurrr block, no phone/social media right after waking up
- Short-form content scale: 1=none consumed, 5=way too much

GAMIFICATION — XP SYSTEM:
- BcBlurrr block done: +50 XP
- Woke up on time (before 9am): +20 XP
- Read today: +30 XP
- In bed by 10:30pm: +20 XP
- Short-form score 1–2: +20 XP
- Short-form score 4–5: -10 XP
- No phone in the morning: +15 XP
Level thresholds: 0=Rookie, 500=Focused, 1500=Consistent, 3000=Disciplined, 6000=Unstoppable
Mention XP gains/losses naturally when logging habits. Surface level-ups enthusiastically.

CURRENT CONTEXT:
{context}

RESPONSE FORMAT — reply with valid JSON only, no markdown, no extra text:
{"message": "your response", "actions": []}

AVAILABLE ACTIONS (include when user's message implies logging or scheduling):

{"type":"log_expense","amount":number,"currency":"GTQ|USD","category":"Food|Transport|Entertainment|Shopping|Health|Subscriptions|Work|CasaFantasma|Other","description":"string","wallet":"personal|casa_fantasma"}

{"type":"log_income","amount":number,"currency":"GTQ|USD","source":"string","client":"string","wallet":"personal|casa_fantasma"}

{"type":"mark_client_paid","client":"string","month":"YYYY-MM"}

{"type":"log_reading","duration_minutes":number,"notes":"string"}

{"type":"finish_book","notes":"string"}

{"type":"start_book","title":"string","author":"string"}

{"type":"log_habit","habit":"wake_up|bcblurrr|bedtime|short_form_content|no_phone_morning","value":"string","notes":"string","xp":number}

{"type":"add_reminder","message":"string","remind_at":"YYYY-MM-DDTHH:MM:SS"}

{"type":"add_habit","name":"string","frequency":"daily|weekdays|weekends|weekly","check_in_time":"HH:MM"}

{"type":"log_journal","text":"string","mood":"string","mood_score":1-5,"major_event":"string or null"}

{"type":"cf_inventory","action":"add|give_to_store|record_sale","product":"string","quantity":number,"price_per_unit":number}

{"type":"award_xp","amount":number,"reason":"string"}

Actions array can be empty []. Always reference real numbers from context (streaks, XP, spending) when relevant."""


class KeeperAI:
    def __init__(self, db):
        self.db = db
        self.client = anthropic.Anthropic(api_key=os.getenv('ANTHROPIC_API_KEY'))
        self.model = os.getenv('CLAUDE_MODEL', 'claude-haiku-4-5-20251001')

    def _build_context(self):
        now = datetime.now(TZ)

        logs = self.db.get_logs_last_days(7)
        logs_text = "\n".join(
            f"  {d} | {cat}: {val}{(' — ' + notes) if notes else ''}"
            for d, cat, val, notes in logs
        ) or "  No recent logs"

        book = self.db.get_current_book()
        book_text = f"{book[1]} by {book[2]} (started {book[3]})" if book else "None"

        reading_streak = self.db.get_reading_streak()
        books_this_year = self.db.get_yearly_book_count()

        _, monthly_total = self.db.get_monthly_spending(wallet='personal')
        _, cf_total = self.db.get_monthly_spending(wallet='casa_fantasma')
        monthly_income = self.db.get_monthly_income()

        xp = self.db.get_total_xp()
        level = self._xp_to_level(xp)
        unpaid = self.db.get_unpaid_clients()

        return (
            f"Date/time: {now.strftime('%A %B %d, %Y %H:%M')}\n\n"
            f"Habit logs last 7 days:\n{logs_text}\n\n"
            f"Current book: {book_text}\n"
            f"Reading streak: {reading_streak} days | Books this year: {books_this_year}\n\n"
            f"Personal finances this month: Q{monthly_total:.0f} spent / Q{monthly_income:.0f} income\n"
            f"Casa Fantasma this month: Q{cf_total:.0f} spent\n\n"
            f"Unpaid clients: {', '.join(unpaid) if unpaid else 'All paid'}\n\n"
            f"XP: {xp} | Level: {level}"
        )

    def _xp_to_level(self, xp):
        if xp >= 6000: return "Unstoppable"
        if xp >= 3000: return "Disciplined"
        if xp >= 1500: return "Consistent"
        if xp >= 500:  return "Focused"
        return "Rookie"

    def _parse_response(self, text):
        text = text.strip()
        match = re.search(r'```(?:json)?\s*(.*?)\s*```', text, re.DOTALL)
        if match:
            text = match.group(1)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            match = re.search(r'\{.*\}', text, re.DOTALL)
            if match:
                try:
                    return json.loads(match.group(0))
                except Exception:
                    pass
        return {"message": text, "actions": []}

    def _build_messages(self, user_message):
        history = self.db.get_recent_conversation(20)
        messages = []
        for role, content in history:
            api_role = 'assistant' if role == 'assistant' else 'user'
            if role == 'assistant':
                try:
                    content = json.loads(content).get('message', content)
                except Exception:
                    pass
            messages.append({"role": api_role, "content": content})
        messages.append({"role": "user", "content": user_message})
        return messages

    def chat(self, user_message):
        context = self._build_context()
        system = SYSTEM_PROMPT.replace('{context}', context)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=512,
            system=[{"type": "text", "text": system, "cache_control": {"type": "ephemeral"}}],
            messages=self._build_messages(user_message)
        )
        return self._parse_response(response.content[0].text)

    def generate_scheduled_message(self, message_type, extra=''):
        context = self._build_context()
        system = SYSTEM_PROMPT.replace('{context}', context)

        tasks = {
            'wake_up':           "It's 6:30am. Send a short wake-up check. Remind them: don't grab the phone or open social media yet.",
            'bcblurrr_reminder': "It's 6:50am weekday. BcBlurrr deep work block in 10 min. Brief motivating nudge.",
            'daily_briefing':    "It's 7am. Give a quick briefing of what they should focus on today based on their goals and schedule. Check if any clients are unpaid.",
            'weekly_preview':    "It's Sunday 5pm. Give a quick preview of the week ahead — what's important, any deadlines, content weekends coming up. Make it feel like a Monday prep.",
            'bcblurrr_wrapup':   "It's 9am weekday. BcBlurrr block just ended. Ask what they got done. Brief.",
            'reading_nudge':     "Casual one-line nudge to read today.",
            'evening_recap':     "It's 9:30pm. Ask them to share how the day went — you'll track mood and highlights from what they write.",
            'bedtime':           "It's 10pm. Gentle wind-down reminder. Ask for short-form content score (1–5).",
            'weekly_recap':      "It's Sunday 5pm — end of week. Give a recap of this week: habits completed, XP earned, streaks, reading progress, spending. Be direct about what went well and what didn't.",
            'monthly_recap':     "Last day of the month, 5pm. Full monthly recap: habit consistency %, XP earned, books read, income vs spending, client payments. Celebrate wins, flag what needs improvement next month.",
            'invoice_bcblurrr':  "1st of the month. Remind them to send their invoice to BcBlurrr.",
            'factura_made':      "25th. Remind them to send Factura to Made Studio.",
            'taxes':             "25th. Remind them to pay taxes.",
            'credit_card':       "10th. Remind them to pay credit card.",
            'crypto_content':    "Last weekend of month coming up. Remind them to create content for the Crypto social account.",
            'kasemal_content':   "Second-to-last weekend coming up. Remind them to create content for Kasemal.",
            'dental':            "February 1st. Remind them to schedule dental cleaning.",
            'unpaid_clients':    f"Mid-month check: these clients haven't been marked as paid yet: {extra}. Send a reminder.",
        }

        task = tasks.get(message_type, extra or "Send a helpful check-in.")
        prompt = f"{system}\n\nTASK: {task}\n\nJSON only: {{\"message\": \"...\", \"actions\": []}}"

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=512,
                messages=[{"role": "user", "content": prompt}]
            )
            parsed = self._parse_response(response.content[0].text)
            return parsed.get('message', response.content[0].text)
        except Exception as e:
            logger.error(f"Claude error for {message_type}: {e}")
            return self._fallback(message_type)

    def _fallback(self, message_type):
        fallbacks = {
            'wake_up':           "Good morning! Don't grab the phone yet — are you up?",
            'bcblurrr_reminder': "BcBlurrr block in 10 minutes.",
            'daily_briefing':    "Good morning! Focus on your BcBlurrr block first today.",
            'weekly_preview':    "New week. Review your priorities and protect the BcBlurrr blocks.",
            'bcblurrr_wrapup':   "BcBlurrr block done — what did you accomplish?",
            'reading_nudge':     "Have you read today?",
            'evening_recap':     "How was your day? Share a few sentences.",
            'bedtime':           "Time to wind down. Short-form content score? (1–5)",
            'weekly_recap':      "End of week — how did it go overall?",
            'monthly_recap':     "End of month — let's review how it went.",
            'invoice_bcblurrr':  "Reminder: send your BcBlurrr invoice today.",
            'factura_made':      "Reminder: send Factura to Made Studio + pay taxes.",
            'taxes':             "Reminder: pay taxes today.",
            'credit_card':       "Reminder: pay credit card today.",
            'crypto_content':    "Crypto content weekend is coming up.",
            'kasemal_content':   "Kasemal content weekend is coming up.",
            'dental':            "Schedule your dental cleaning today.",
        }
        return fallbacks.get(message_type, "Hey, checking in!")
