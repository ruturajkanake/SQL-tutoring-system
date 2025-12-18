import sqlite3

DB_PATH = "app.db"

def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.executescript("""
    CREATE TABLE IF NOT EXISTS user_progress (
        user_id TEXT,
        question_number INTEGER,
        PRIMARY KEY (user_id)
    );

    CREATE TABLE IF NOT EXISTS hint_feedback (
        user_id TEXT,
        question_number INTEGER,
        hint_level INTEGER,
        feedback BOOLEAN
    );
    """)

    conn.commit()
    conn.close()
