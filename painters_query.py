import sqlite3

conn = sqlite3.connect('Painters_Data.db')
cursor = conn.cursor()

sql1 = '''
SeLECT * FROM painters_info;
'''
cursor.execute(sql1)
rows = cursor.fetchall()
# Print the title row
print(("name", "birth", "death", "nationality"))
for row in rows:
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<10} {row[3]:<15}")
conn.close()