import sqlite3

conn = sqlite3.connect('inventory.db')

cursor = conn.cursor()

sql1 = '''
CREATE TABLE IF NOT EXISTS products (
    product_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    price REAL NOT NULL,
    quantity INTEGER NOT NULL
);
'''

cursor.execute(sql1)
conn.commit()

products_data = [
    (1, "Laptop_A100", 999.99, 10),
    (2, "Smartphone_B200", 499.49, 25),
    (3, "Tablet_C300", 299.99, 15),
    (4, "Headphones_D400", 89.99, 50),
]

sql2 = '''
INSERT INTO products (product_id, name, price, quantity)
VALUES (?, ?, ?, ?);
'''

cursor.executemany(sql2, products_data)
conn.commit()

sql3 = '''
SELECT * FROM products;
'''
cursor.execute(sql3)
rows = cursor.fetchall()
# Print the title row
print(("product_id", "name", "price", "quantity"))
for row in rows:
    print(f"{row[0]:<11} {row[1]:<15} {row[2]:<7} {row[3]:<8}")

# Delete a product with product_id = 2
sql4 = '''
DELETE FROM products
WHERE product_id = ?;
'''
cursor.execute(sql4, (2,))
conn.commit()
# Verify deletion
cursor.execute(sql3)
rows = cursor.fetchall()
print("\nAfter deletion of product_id 2:")
print(("product_id", "name", "price", "quantity"))
for row in rows:
    print(f"{row[0]:<11} {row[1]:<15} {row[2]:<7} {row[3]:<8}")

    
