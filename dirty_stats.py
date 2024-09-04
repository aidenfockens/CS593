import sqlite3

def count_missing_values(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info(events);")
    columns = cursor.fetchall()
    count =0
    for column in columns:
        count+=1
        column_name = column[1]
        quoted_column_name = f'"{column_name}"'

        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM events
            WHERE {quoted_column_name} IS NULL OR {quoted_column_name} = '';
        """)
        missing_count = cursor.fetchone()[0]
        print(f"Column: {column_name}, Missing Count: {missing_count}")
        
        if (count >1 and count <5) or count ==6:
            cursor.execute(f"""
            SELECT COUNT(DISTINCT {quoted_column_name}) 
            FROM events;
            """)
            unique_count = cursor.fetchone()[0]
            print(f"Column: {column_name}, Unique Count: {unique_count}")
    conn.close()


#count_missing_values('health_events_data.db')


count_missing_values('health_events_data.db')