import sqlite3

DB_PATH = './customer_details.db'


def create_table_item():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    c.execute('CREATE TABLE "customer_details" ("contact" TEXT NOT NULL,"status" TEXT NOT NULL,PRIMARY KEY("contact"));')

    conn.commit()

def fetch_all_items():
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute('select * from customer_details')
        items = c.fetchall()

    except Exception as e:
        return None


def insert_content(contact, status):
    try:
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()

        c.execute('insert into customer_details(content, status) values(?,?,?)', (contact, status))
        conn.commit()
        return True

    except Exception as e:
        return None


if not fetch_all_items():
    create_table_item()
