import sqlite3

def count_missing_values(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(events);")
    columns = cursor.fetchall()
    for column in columns:
        column_name = column[1]
        # Count NULL values in the column
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM events
            WHERE {column_name} IS NULL;
        """)
        missing_count = cursor.fetchone()[0]
        print(f"Column: {column_name}, Missing Count: {missing_count}")
    conn.close()


count_missing_values('health_events_data.db')