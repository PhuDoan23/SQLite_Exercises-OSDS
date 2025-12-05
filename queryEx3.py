import sqlite3
from pathlib import Path
import os
import time
import pandas as pd

#III Truy van

DB_FILE = 'Medicine_Data.db'
TABLE_NAME = 'Medicine_info'

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()



q1 = f"""
SELECT name, COUNT(*) AS so_luong_san_pham
FROM {TABLE_NAME}
GROUP BY name
HAVING COUNT(*) > 1;
"""

df = pd.read_sql_query(q1, conn)
print(df.to_string(index = False))

q2 = f"""
SELECT name, COUNT(*) AS so_san_pham_loi_gia
FROM {TABLE_NAME}
WHERE price IS NULL OR price = 0 OR price = '';
"""

df2 = pd.read_sql_query(q2, conn)
print(df2.to_string(index = False))



# q4 = f"""
# SELECT DISTINCT unit FROM {TABLE_NAME};
# """

# df4 = pd.read_sql_query(q4, conn)
# print(df4.to_string(index = False))


q5 = f"""
SELECT COUNT(*) AS so_luong_san_phan
FROM {TABLE_NAME};
"""
df5 = pd.read_sql_query(q5, conn)
print(df5.to_string(index= False))

q6 = f"""
SELECT name, price, orgin_price, 
       (CAST(orgin_price AS INTEGER) - CAST(price AS INTEGER)) as muc_giam
FROM {TABLE_NAME}
ORDER BY muc_giam DESC
LIMIT 10;
"""
df6 = pd.read_sql_query(q6, conn)
print(df6.to_string(index = False))

# 2.2 Sản phẩm đắt nhất
q7 = f"""
SELECT name, price 
FROM {TABLE_NAME} 
ORDER BY CAST(price AS INTEGER) DESC 
LIMIT 1;
"""
df7 = pd.read_sql_query(q7, conn)
print(df7.to_string(index = False))

# 2.3 Thống kê theo đơn vị tính
# q8 = f"""
# SELECT unit, COUNT(*) as so_luong 
# FROM {TABLE_NAME} 
# GROUP BY unit;
# """
# df8 = pd.read_sql_query(q8, conn)
# print(df8.to_string(index = False))

# 2.4 Tìm kiếm sản phẩm "Vitamin C"
q9 = f"""
SELECT name, price 
FROM {TABLE_NAME} 
WHERE name LIKE '%Vitamin C%';
"""
df9 = pd.read_sql_query(q9, conn)
print(df9.to_string(index=False))

# 2.5 Lọc theo khoảng giá (100k - 200k)
q10 = f"""
SELECT name, price 
FROM {TABLE_NAME} 
WHERE CAST(price AS INTEGER) BETWEEN 100000 AND 200000;
"""

df10 = pd.read_sql_query(q10, conn)
print(df10.to_string(index = False))


# ======================================================
# NHÓM 3: TRUY VẤN NÂNG CAO (TÙY CHỌN)
# ======================================================

# 3.1 Sắp xếp giá từ thấp đến cao
q11 = f"""
SELECT name, price 
FROM {TABLE_NAME} 
ORDER BY CAST(price AS INTEGER) ASC 
LIMIT 5; -- Chỉ hiện 5 cái đầu cho gọn
"""

df11 = pd.read_sql_query(q11, conn)
print(df11.to_string(index=False))

# 3.2 Top 5 phần trăm giảm giá cao nhất
# Công thức: ((Gốc - Bán) / Gốc) * 100
q12 = f"""
SELECT name, price, orgin_price,
       ROUND(((CAST(orgin_price AS FLOAT) - CAST(price AS FLOAT)) / CAST(orgin_price AS FLOAT)) * 100, 2) as phan_tram_giam
FROM {TABLE_NAME}
WHERE orgin_price > 0
ORDER BY phan_tram_giam DESC
LIMIT 5;
"""
df12 = pd.read_sql_query(q12, conn)
print(df12.to_string(index = False))

# 3.3 Phân tích nhóm giá (CASE WHEN)
q13 = f"""
SELECT 
    CASE 
        WHEN CAST(price AS INTEGER) < 50000 THEN 'Dưới 50k'
        WHEN CAST(price AS INTEGER) BETWEEN 50000 AND 100000 THEN '50k - 100k'
        ELSE 'Trên 100k'
    END as nhom_gia,
    COUNT(*) as so_luong
FROM {TABLE_NAME}
GROUP BY nhom_gia;
"""

df13 = pd.read_sql_query(q13, conn)
print(df13.to_string(index = False))

# 3.4 URL không hợp lệ
# Lưu ý: Cột product_url cần tồn tại
# q14 = f"""
# SELECT stt, name 
# FROM {TABLE_NAME} 
# WHERE  IS NULL OR product_url = '';
# """

# df14 = pd.read_sql_query(q14, conn)
# print(df14.to_string(index=False))

# 3.5 Xóa bản ghi trùng lặp (Giữ lại dòng có ID nhỏ nhất)
# SQLite sử dụng ROWID để định danh
q15 = f"""
DELETE FROM {TABLE_NAME}
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM {TABLE_NAME}
    GROUP BY name
);
"""

cursor.execute(q15)
conn.commit()
print("Da xoa cac ban ghi trung lap.")
