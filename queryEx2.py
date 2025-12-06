import sqlite3
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import time
import pandas as pd


DB_FILE = 'Painters_Data.db'
TABLE_NAME = 'painters_info'


conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

"""
A. Yêu Cầu Thống Kê và Toàn Cục
1. Đếm tổng số họa sĩ đã được lưu trữ trong bảng.
2. Hiển thị 5 dòng dữ liệu đầu tiên để kiểm tra cấu trúc và nội dung bảng.
3. Liệt kê danh sách các quốc tịch duy nhất có trong tập dữ liệu.

B. Yêu Cầu Lọc và Tìm Kiếm
4. Tìm và hiển thị tên của các họa sĩ có tên bắt đầu bằng ký tự 'F'.
5. Tìm và hiển thị tên và quốc tịch của những họa sĩ có quốc tịch chứa từ khóa 'French' (ví dụ: French, French-American).
6. Hiển thị tên của các họa sĩ không có thông tin quốc tịch (hoặc để trống, hoặc NULL).
7. Tìm và hiển thị tên của những họa sĩ có cả thông tin ngày sinh và ngày mất (không rỗng).
8. Hiển thị tất cả thông tin của họa sĩ có tên chứa từ khóa '%Fales%' (ví dụ: George Fales Baker).

C. Yêu Cầu Nhóm và Sắp Xếp
9. Sắp xếp và hiển thị tên của tất cả họa sĩ theo thứ tự bảng chữ cái (A-Z).
10. Nhóm và đếm số lượng họa sĩ theo từng quốc tịch.
"""


sqlA = [
    # 1. Đếm tổng số họa sĩ
    f"SELECT COUNT(*) FROM {TABLE_NAME};",
    
    # 2. Hiển thị 5 dòng dữ liệu đầu tiên
    f"SELECT * FROM {TABLE_NAME} LIMIT 5;",
    
    # 3. Liệt kê quốc tịch duy nhất
    f"SELECT DISTINCT nationality FROM {TABLE_NAME} WHERE nationality IS NOT NULL AND nationality != '';"
]

cursor.execute(sqlA[0])
total_painters = cursor.fetchone()[0]
print(f"\nTổng số họa sĩ trong bảng: {total_painters}")


cursor.execute(sqlA[1])
rows = cursor.fetchall()
print("\n5 dòng dữ liệu đầu tiên:")
print(("name", "birth", "death", "nationality"))
for row in rows:
    print(f"{row[0]:<20} {row[1]:<10} {row[2]:<10} {row[3]:<15}")


cursor.execute(sqlA[2])
unique_nationalities = cursor.fetchall()
print("\nDanh sách quốc tịch duy nhất:")
for nat in unique_nationalities:
    print(f"- {nat[0]}")


sqlB = [
    # 4. Họa sĩ có tên bắt đầu bằng 'F'
    f"SELECT name FROM {TABLE_NAME} WHERE name LIKE 'F%';",
    # 5. Họa sĩ có quốc tịch chứa 'French'
    f"SELECT name,nationality FROM {TABLE_NAME} WHERE nationality LIKE '%French%';",
    # 6. Họa sĩ không có thông tin quốc tịch
    f"SELECT name FROM {TABLE_NAME} WHERE nationality IS NULL OR nationality = '';",
    # 7. Họa sĩ có cả ngày sinh và ngày mất
    f"SELECT name FROM {TABLE_NAME} WHERE birth IS NOT NULL AND birth != '' AND death IS NOT NULL AND death != '';",
    # 8. Họa sĩ có tên chứa 'Fales'
    f"SELECT * FROM {TABLE_NAME} WHERE name LIKE '%Fales%';"
]

cursor.execute(sqlB[0])
painters_starting_F = cursor.fetchall()
print("\nHọa sĩ có tên bắt đầu bằng 'F':")
for painter in painters_starting_F:
    print(f"- {painter[0]}")
cursor.execute(sqlB[1])
french_painters = cursor.fetchall()
print("\nHọa sĩ có quốc tịch chứa 'French':")
for painter in french_painters:
    print(f"- {painter[0]} ({painter[1]})")
cursor.execute(sqlB[2])
painters_no_nationality = cursor.fetchall()
print("\nHọa sĩ không có thông tin quốc tịch:")
for painter in painters_no_nationality:
    print(f"- {painter[0]}")
cursor.execute(sqlB[3])
painters_with_birth_death = cursor.fetchall()
print("\nHọa sĩ có cả ngày sinh và ngày mất:")
for painter in painters_with_birth_death:
    print(f"- {painter[0]}")
cursor.execute(sqlB[4])
fales_painters = cursor.fetchall()
print("\nHọa sĩ có tên chứa 'Fales':")
for painter in fales_painters:
    print(f"- {painter[0]}, Born: {painter[1]}, Died: {painter[2]}, Nationality: {painter[3]}")



sqlC = [
    #9 Nhom ten cac hoa sy
    f"SELECT name FROM {TABLE_NAME} ORDER BY name ASC;",
    #10 Nhóm và đếm số lượng họa sĩ theo từng quốc tịch.
    f"""
    SELECT nationality, COUNT(*) as so_luong 
    FROM {TABLE_NAME}
    GROUP BY nationality
    ORDER BY so_luong DESC;
    """
]

cursor.execute(sqlC[0])
painter_names = cursor.fetchall()
print("\nHọa sĩ có tên bắt đầu ")
for painter in painter_names:
    print(f"- {painter[0]}")

cursor.execute(sqlC[1])
painter_home = cursor.fetchall()
print("\nHọa sĩ quê hương ")
for painter in painter_home:
    # painter is (nationality_norm, so_luong)
    print(f"- {painter[0]}: {painter[1]}")

