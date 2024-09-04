import sqlite3
import csv

database_file = "health_events_data.db"
csv_file = 'funny_epidemiological_events.csv'
table_name = 'events'

conn = sqlite3.connect(database_file)
cur = conn.cursor()

cur.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        "Event ID" TEXT,
        "Condition" TEXT,
        "Agent" TEXT,
        "Reporting Agency" TEXT,
        "Affected Population" INTEGER,
        "City" TEXT,
        "Event Start Date" TEXT,
        "Event End Date" TEXT,
        "Outcome" TEXT,
        "Cost of Damages ($)" REAL
    )
""")

with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    headers = next(reader)
    insert_query = f"""
        INSERT INTO {table_name} (
            "Event ID",
            "Condition",
            "Agent",
            "Reporting Agency",
            "Affected Population",
            "City",
            "Event Start Date",
            "Event End Date",
            "Outcome",
            "Cost of Damages ($)"
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    # Insert rows into the table
    for row in reader:
        cur.execute(insert_query, row)

# Commit the changes and close the connection
conn.commit()
conn.close()
