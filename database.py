import sqlite3
import os
import logging
from datetime import datetime, date, timedelta
import pytz

logger = logging.getLogger(__name__)
TZ = pytz.timezone('America/Guatemala')


class Database:
    def __init__(self):
        self.path = os.getenv('DB_PATH', 'keeper.db')
        dir_path = os.path.dirname(self.path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        logger.info(f"Database path: {self.path}")

    def conn(self):
        return sqlite3.connect(self.path)

    def init(self):
        with self.conn() as c:
            c.executescript("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS daily_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    category TEXT NOT NULL,
                    value TEXT,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS books (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author TEXT,
                    status TEXT DEFAULT 'reading',
                    started_date TEXT,
                    completed_date TEXT,
                    notes TEXT
                );
                CREATE TABLE IF NOT EXISTS reading_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    book_id INTEGER REFERENCES books(id),
                    date TEXT NOT NULL,
                    duration_minutes INTEGER,
                    notes TEXT
                );
                CREATE TABLE IF NOT EXISTS journal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    text TEXT,
                    mood TEXT,
                    mood_score INTEGER,
                    major_event TEXT
                );
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    amount_gtq REAL,
                    category TEXT,
                    description TEXT,
                    wallet TEXT DEFAULT 'personal'
                );
                CREATE TABLE IF NOT EXISTS income (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT NOT NULL,
                    amount_gtq REAL,
                    source TEXT,
                    client TEXT,
                    wallet TEXT DEFAULT 'personal'
                );
                CREATE TABLE IF NOT EXISTS client_payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client TEXT NOT NULL,
                    month TEXT NOT NULL,
                    paid INTEGER DEFAULT 0,
                    paid_date TEXT,
                    amount REAL,
                    currency TEXT DEFAULT 'GTQ',
                    notes TEXT,
                    UNIQUE(client, month)
                );
                CREATE TABLE IF NOT EXISTS gastos_fijos (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    amount REAL,
                    currency TEXT DEFAULT 'GTQ',
                    wallet TEXT DEFAULT 'personal',
                    active INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS cf_products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    unit_cost REAL,
                    sale_price REAL,
                    units_made INTEGER DEFAULT 0,
                    units_at_store INTEGER DEFAULT 0,
                    units_sold INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS cf_inventory_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    product TEXT NOT NULL,
                    action TEXT NOT NULL,
                    quantity INTEGER,
                    price_per_unit REAL,
                    notes TEXT
                );
                CREATE TABLE IF NOT EXISTS xp_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    amount INTEGER NOT NULL,
                    reason TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS reminders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT NOT NULL,
                    remind_at TEXT NOT NULL,
                    sent INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS custom_habits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    frequency TEXT,
                    check_in_time TEXT,
                    active INTEGER DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS conversation_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
            """)

    # --- Config ---

    def get_config(self, key, default=None):
        with self.conn() as c:
            row = c.execute("SELECT value FROM config WHERE key=?", (key,)).fetchone()
        return row[0] if row else default

    def set_config(self, key, value):
        with self.conn() as c:
            c.execute("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", (key, str(value)))

    def get_chat_id(self):
        val = self.get_config('chat_id')
        return int(val) if val else None

    def set_chat_id(self, chat_id):
        self.set_config('chat_id', chat_id)

    # --- Daily logs ---

    def log_habit(self, category, value, notes=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO daily_logs (date, category, value, notes) VALUES (?, ?, ?, ?)",
                (today, category, str(value), notes)
            )

    def get_today_logs(self):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            return c.execute(
                "SELECT category, value, notes FROM daily_logs WHERE date=?", (today,)
            ).fetchall()

    def get_logs_last_days(self, days=7):
        with self.conn() as c:
            return c.execute(
                "SELECT date, category, value, notes FROM daily_logs "
                "WHERE date >= date('now', ?) ORDER BY date DESC, id DESC",
                (f'-{days} days',)
            ).fetchall()

    def get_habit_streak(self, category):
        with self.conn() as c:
            rows = c.execute(
                "SELECT DISTINCT date FROM daily_logs "
                "WHERE category=? AND value NOT IN ('skipped','no','0') "
                "ORDER BY date DESC",
                (category,)
            ).fetchall()
        today = datetime.now(TZ).date()
        streak = 0
        for i, (d,) in enumerate(rows):
            if date.fromisoformat(d) == today - timedelta(days=i):
                streak += 1
            else:
                break
        return streak

    def get_weekly_habit_summary(self):
        """Returns habit completion counts for the current week (Mon–Sun)."""
        with self.conn() as c:
            return c.execute(
                "SELECT category, COUNT(*) FROM daily_logs "
                "WHERE date >= date('now', 'weekday 0', '-7 days') "
                "GROUP BY category"
            ).fetchall()

    def get_monthly_habit_summary(self):
        """Returns habit completion counts for the current month."""
        now = datetime.now(TZ)
        month_start = now.strftime('%Y-%m-01')
        with self.conn() as c:
            return c.execute(
                "SELECT category, COUNT(*) FROM daily_logs "
                "WHERE date >= ? GROUP BY category",
                (month_start,)
            ).fetchall()

    # --- Books ---

    def get_current_book(self):
        with self.conn() as c:
            return c.execute(
                "SELECT id, title, author, started_date FROM books "
                "WHERE status='reading' ORDER BY id DESC LIMIT 1"
            ).fetchone()

    def start_book(self, title, author=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO books (title, author, status, started_date) VALUES (?, ?, 'reading', ?)",
                (title, author, today)
            )

    def finish_book(self, notes=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        book = self.get_current_book()
        if book:
            with self.conn() as c:
                c.execute(
                    "UPDATE books SET status='completed', completed_date=?, notes=? WHERE id=?",
                    (today, notes, book[0])
                )
        return book

    def log_reading_session(self, duration_minutes, notes=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        book = self.get_current_book()
        with self.conn() as c:
            c.execute(
                "INSERT INTO reading_sessions (book_id, date, duration_minutes, notes) VALUES (?, ?, ?, ?)",
                (book[0] if book else None, today, duration_minutes, notes)
            )

    def get_reading_streak(self):
        with self.conn() as c:
            rows = c.execute(
                "SELECT DISTINCT date FROM reading_sessions ORDER BY date DESC"
            ).fetchall()
        today = datetime.now(TZ).date()
        streak = 0
        for i, (d,) in enumerate(rows):
            if date.fromisoformat(d) == today - timedelta(days=i):
                streak += 1
            else:
                break
        return streak

    def get_completed_books(self):
        with self.conn() as c:
            return c.execute(
                "SELECT title, author, started_date, completed_date FROM books "
                "WHERE status='completed' ORDER BY completed_date DESC"
            ).fetchall()

    def get_yearly_book_count(self):
        year = str(datetime.now(TZ).year)
        with self.conn() as c:
            row = c.execute(
                "SELECT COUNT(*) FROM books WHERE status='completed' AND completed_date LIKE ?",
                (f'{year}%',)
            ).fetchone()
        return row[0] if row else 0

    def get_monthly_reading_minutes(self):
        now = datetime.now(TZ)
        with self.conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(duration_minutes), 0) FROM reading_sessions "
                "WHERE strftime('%Y-%m', date) = ?",
                (now.strftime('%Y-%m'),)
            ).fetchone()
        return row[0] if row else 0

    # --- Finances ---

    def _to_gtq(self, amount, currency):
        rate = float(self.get_config('usd_to_gtq', '7.75'))
        return amount if currency == 'GTQ' else round(amount * rate, 2)

    def log_expense(self, amount, currency, category, description='', wallet='personal'):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        amount_gtq = self._to_gtq(amount, currency)
        with self.conn() as c:
            c.execute(
                "INSERT INTO expenses (date, amount, currency, amount_gtq, category, description, wallet) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (today, amount, currency.upper(), amount_gtq, category, description, wallet)
            )
        return amount_gtq

    def log_income(self, amount, currency, source='', client='', wallet='personal'):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        amount_gtq = self._to_gtq(amount, currency)
        with self.conn() as c:
            c.execute(
                "INSERT INTO income (date, amount, currency, amount_gtq, source, client, wallet) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (today, amount, currency.upper(), amount_gtq, source, client, wallet)
            )
        return amount_gtq

    def get_monthly_spending(self, year=None, month=None, wallet=None):
        now = datetime.now(TZ)
        year = str(year or now.year)
        month = f'{month or now.month:02d}'
        query = ("SELECT category, SUM(amount_gtq) FROM expenses "
                 "WHERE strftime('%Y',date)=? AND strftime('%m',date)=?")
        total_query = ("SELECT COALESCE(SUM(amount_gtq), 0) FROM expenses "
                       "WHERE strftime('%Y',date)=? AND strftime('%m',date)=?")
        params = [year, month]
        if wallet:
            query += " AND wallet=?"
            total_query += " AND wallet=?"
            params.append(wallet)
        query += " GROUP BY category ORDER BY SUM(amount_gtq) DESC"
        with self.conn() as c:
            breakdown = c.execute(query, params).fetchall()
            total = c.execute(total_query, params).fetchone()[0]
        return breakdown, total

    def get_monthly_income(self, year=None, month=None, wallet='personal'):
        now = datetime.now(TZ)
        year = str(year or now.year)
        month = f'{month or now.month:02d}'
        with self.conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(amount_gtq), 0) FROM income "
                "WHERE strftime('%Y',date)=? AND strftime('%m',date)=? AND wallet=?",
                (year, month, wallet)
            ).fetchone()
        return row[0] if row else 0

    # --- Client payments ---

    def mark_client_paid(self, client, month=None, amount=None, currency='GTQ'):
        if month is None:
            month = datetime.now(TZ).strftime('%Y-%m')
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO client_payments (client, month, paid, paid_date, amount, currency) "
                "VALUES (?, ?, 1, ?, ?, ?) "
                "ON CONFLICT(client, month) DO UPDATE SET paid=1, paid_date=?, amount=?, currency=?",
                (client, month, today, amount, currency, today, amount, currency)
            )

    def get_unpaid_clients(self, month=None):
        if month is None:
            month = datetime.now(TZ).strftime('%Y-%m')
        all_clients = ['BcBlurrr', 'Made Studio', 'ETHGT', 'Kasemal']
        with self.conn() as c:
            paid = [r[0] for r in c.execute(
                "SELECT client FROM client_payments WHERE month=? AND paid=1", (month,)
            ).fetchall()]
        return [c for c in all_clients if c not in paid]

    def get_client_payment_history(self, client, months=6):
        with self.conn() as c:
            return c.execute(
                "SELECT month, paid, paid_date, amount, currency FROM client_payments "
                "WHERE client=? ORDER BY month DESC LIMIT ?",
                (client, months)
            ).fetchall()

    # --- Gastos fijos ---

    def add_gasto_fijo(self, name, amount, currency='GTQ', wallet='personal'):
        with self.conn() as c:
            c.execute(
                "INSERT INTO gastos_fijos (name, amount, currency, wallet) VALUES (?, ?, ?, ?)",
                (name, amount, currency, wallet)
            )

    def get_gastos_fijos(self, wallet=None):
        query = "SELECT name, amount, currency, wallet FROM gastos_fijos WHERE active=1"
        params = []
        if wallet:
            query += " AND wallet=?"
            params.append(wallet)
        with self.conn() as c:
            return c.execute(query, params).fetchall()

    # --- Casa Fantasma inventory ---

    def cf_log_inventory(self, action, product, quantity, price_per_unit=None, notes=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO cf_inventory_log (date, product, action, quantity, price_per_unit, notes) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (today, product, action, quantity, price_per_unit, notes)
            )
            # Update product totals
            if action == 'add':
                c.execute(
                    "INSERT INTO cf_products (name, units_made) VALUES (?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET units_made = units_made + ?",
                    (product, quantity, quantity)
                )
            elif action == 'give_to_store':
                c.execute(
                    "INSERT INTO cf_products (name, units_at_store) VALUES (?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET units_at_store = units_at_store + ?",
                    (product, quantity, quantity)
                )
            elif action == 'record_sale':
                c.execute(
                    "INSERT INTO cf_products (name, units_sold) VALUES (?, ?) "
                    "ON CONFLICT(name) DO UPDATE SET units_sold = units_sold + ?, units_at_store = MAX(0, units_at_store - ?)",
                    (product, quantity, quantity, quantity)
                )

    def get_cf_inventory(self):
        with self.conn() as c:
            return c.execute(
                "SELECT name, units_made, units_at_store, units_sold, "
                "(units_made - units_at_store - units_sold) as units_home, "
                "sale_price FROM cf_products"
            ).fetchall()

    # --- XP / Gamification ---

    def award_xp(self, amount, reason=''):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO xp_log (date, amount, reason) VALUES (?, ?, ?)",
                (today, amount, reason)
            )

    def get_total_xp(self):
        with self.conn() as c:
            row = c.execute("SELECT COALESCE(SUM(amount), 0) FROM xp_log").fetchone()
        return row[0] if row else 0

    def get_weekly_xp(self):
        with self.conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM xp_log "
                "WHERE date >= date('now', '-7 days')"
            ).fetchone()
        return row[0] if row else 0

    def get_monthly_xp(self):
        now = datetime.now(TZ)
        with self.conn() as c:
            row = c.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM xp_log "
                "WHERE strftime('%Y-%m', date) = ?",
                (now.strftime('%Y-%m'),)
            ).fetchone()
        return row[0] if row else 0

    # --- Journal ---

    def add_journal_entry(self, text, mood=None, mood_score=None, major_event=None):
        today = datetime.now(TZ).strftime('%Y-%m-%d')
        with self.conn() as c:
            c.execute(
                "INSERT INTO journal_entries (date, text, mood, mood_score, major_event) VALUES (?,?,?,?,?)",
                (today, text, mood, mood_score, major_event)
            )

    # --- Reminders ---

    def add_reminder(self, message, remind_at):
        with self.conn() as c:
            cur = c.execute(
                "INSERT INTO reminders (message, remind_at) VALUES (?, ?)", (message, remind_at)
            )
            return cur.lastrowid

    def get_pending_reminders(self):
        now = datetime.now(TZ).isoformat()
        with self.conn() as c:
            return c.execute(
                "SELECT id, message FROM reminders WHERE sent=0 AND remind_at <= ? ORDER BY remind_at",
                (now,)
            ).fetchall()

    def get_unsent_reminders(self):
        with self.conn() as c:
            return c.execute(
                "SELECT id, message, remind_at FROM reminders WHERE sent=0 ORDER BY remind_at"
            ).fetchall()

    def mark_reminder_sent(self, reminder_id):
        with self.conn() as c:
            c.execute("UPDATE reminders SET sent=1 WHERE id=?", (reminder_id,))

    # --- Conversation history ---

    def add_conversation(self, role, content):
        with self.conn() as c:
            c.execute(
                "INSERT INTO conversation_history (role, content) VALUES (?, ?)", (role, content)
            )

    def get_recent_conversation(self, limit=20):
        with self.conn() as c:
            rows = c.execute(
                "SELECT role, content FROM conversation_history ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return list(reversed(rows))
