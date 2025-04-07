import sqlite3

# Connect to the SQLite DB
conn = sqlite3.connect("shipment_database.db")
cursor = conn.cursor()

# List all tables in the database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables in the database:")
for table in tables:
    print(f"- {table[0]}")

# Optionally, inspect the 'shipment' table schema (if it exists)
if ('shipment',) in tables:
    print("\nSchema for 'shipment' table:")
    cursor.execute("PRAGMA table_info(shipment);")
    for col in cursor.fetchall():
        print(col)
else:
    print("\nNo 'shipment' table found.")

conn.close()
