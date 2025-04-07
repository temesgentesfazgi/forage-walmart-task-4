import pandas as pd
import sqlite3
import os

# === Paths ===
DB_PATH = "shipment_database.db"
DATA_DIR = "data"

# === Read CSV files ===
df0 = pd.read_csv(os.path.join(DATA_DIR, "shipping_data_0.csv"))
df1 = pd.read_csv(os.path.join(DATA_DIR, "shipping_data_1.csv"))
df2 = pd.read_csv(os.path.join(DATA_DIR, "shipping_data_2.csv"))

# === Connect to database ===
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# === STEP 0: Insert all unique product names into product table ===
all_products = pd.concat([df0["product"], df1["product"]]).dropna().unique()
for name in all_products:
    cursor.execute("INSERT OR IGNORE INTO product (name) VALUES (?)", (name,))
conn.commit()
print("✅ Populated product table with unique product names.")

# === Helper: get product_id from name ===
def get_product_id(product_name):
    cursor.execute("SELECT id FROM product WHERE name = ?", (product_name,))
    result = cursor.fetchone()
    return result[0] if result else None

# === STEP 1: Insert data from shipping_data_0.csv ===
for _, row in df0.iterrows():
    product_id = get_product_id(row["product"])
    if product_id:
        cursor.execute(
            "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
            (product_id, row["product_quantity"], row["origin_warehouse"], row["destination_store"])
        )
conn.commit()
print("✅ Inserted data from shipping_data_0.csv")

# === STEP 2: Merge shipping_data_1 + shipping_data_2 ===
merged_df = pd.merge(df1, df2, on="shipment_identifier", how="left")

# Group and count to calculate quantity
grouped = merged_df.groupby(
    ["shipment_identifier", "product", "origin_warehouse", "destination_store"]
).size().reset_index(name="quantity")

# Insert grouped records into shipment table
for _, row in grouped.iterrows():
    product_id = get_product_id(row["product"])
    if product_id:
        cursor.execute(
            "INSERT INTO shipment (product_id, quantity, origin, destination) VALUES (?, ?, ?, ?)",
            (product_id, row["quantity"], row["origin_warehouse"], row["destination_store"])
        )
conn.commit()
print("✅ Inserted data from shipping_data_1.csv and shipping_data_2.csv")

# === Optional: Verify output ===
print("\n📦 Sample data from shipment table (joined with product):")
cursor.execute("""
    SELECT s.id, p.name, s.quantity, s.origin, s.destination
    FROM shipment s
    JOIN product p ON s.product_id = p.id
    LIMIT 10;
""")
rows = cursor.fetchall()
for row in rows:
    print(row)

conn.close()
print("🔒 Database connection closed")



