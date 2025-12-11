import sqlite3
from pathlib import Path
import os
import time
import pandas as pd

#III Truy van

"""
Đề Bài Thực Hành: Cào Dữ Liệu Long Châu và Quản Lý SQLite
I. Mục Tiêu
    Thực hiện cào dữ liệu sản phẩm từ trang web chính thức của chuỗi nhà thuốc Long Châu bằng công cụ Selenium, lưu trữ dữ liệu thu thập được một cách tức thời vào cơ sở dữ liệu SQLite, và kiểm tra chất lượng dữ liệu.

II. Yêu Cầu Kỹ Thuật (Scraping & Lưu trữ)
    Công cụ: Sử dụng thư viện Selenium kết hợp với Python và Pandas (cho việc quản lý DataFrame tạm thời và lưu vào DB).

    Phạm vi Cào: Chọn một danh mục sản phẩm cụ thể trên trang Long Châu (ví dụ: "Thực phẩm chức năng", "Chăm sóc da", hoặc "Thuốc") và cào ít nhất 50 sản phẩm (có thể cào nhiều trang/URL khác nhau).

    Dữ liệu cần cào: Đối với mỗi sản phẩm, cần thu thập ít nhất các thông tin sau (table phải có các cột bên dưới):

        Mã sản phẩm (id): cố gắng phân tích và lấy mã sản phẩm gốc từ trang web, nếu không được thì dùng mã tự tăng.

        Tên sản phẩm (product_name)

        Giá bán (price)

        Giá gốc/Giá niêm yết (nếu có, original_price)

        Đơn vị tính (ví dụ: Hộp, Chai, Vỉ, unit)

        Link URL sản phẩm (product_url) (Dùng làm định danh duy nhất)

    Lưu trữ Tức thời:

        Sử dụng thư viện sqlite3 để tạo cơ sở dữ liệu (longchau_db.sqlite).

        Thực hiện lưu trữ dữ liệu ngay lập tức sau khi cào xong thông tin của mỗi sản phẩm (sử dụng conn.cursor().execute() hoặc DataFrame.to_sql(if_exists='append')) thay vì lưu trữ toàn bộ sau khi kết thúc quá trình cào.

        Sử dụng product_url hoặc một trường định danh khác làm PRIMARY KEY (hoặc kết hợp với lệnh INSERT OR IGNORE) để tránh ghi đè nếu chạy lại code.

III. Yêu Cầu Phân Tích Dữ Liệu (Query/Truy Vấn)
    Sau khi dữ liệu được thu thập, tạo và thực thi ít nhất 15 câu lệnh SQL (queries) để khảo sát chất lượng và nội dung dữ liệu.

    Nhóm 1: Kiểm Tra Chất Lượng Dữ Liệu (Bắt buộc)
        Kiểm tra trùng lặp (Duplicate Check): Kiểm tra và hiển thị tất cả các bản ghi có sự trùng lặp dựa trên trường product_url hoặc product_name.

        Kiểm tra dữ liệu thiếu (Missing Data): Đếm số lượng sản phẩm không có thông tin Giá bán (price là NULL hoặc 0).

        Kiểm tra giá: Tìm và hiển thị các sản phẩm có Giá bán lớn hơn Giá gốc/Giá niêm yết (logic bất thường).

        Kiểm tra định dạng: Liệt kê các unit (đơn vị tính) duy nhất để kiểm tra sự nhất quán trong dữ liệu.

        Tổng số lượng bản ghi: Đếm tổng số sản phẩm đã được cào.

    Nhóm 2: Khảo sát và Phân Tích (Bổ sung)
        Sản phẩm có giảm giá: Hiển thị 10 sản phẩm có mức giá giảm (chênh lệch giữa original_price và price) lớn nhất.

        Sản phẩm đắt nhất: Tìm và hiển thị sản phẩm có giá bán cao nhất.

        Thống kê theo đơn vị: Đếm số lượng sản phẩm theo từng Đơn vị tính (unit).

        Sản phẩm cụ thể: Tìm kiếm và hiển thị tất cả thông tin của các sản phẩm có tên chứa từ khóa "Vitamin C".

        Lọc theo giá: Liệt kê các sản phẩm có giá bán nằm trong khoảng từ 100.000 VNĐ đến 200.000 VNĐ.

    Nhóm 3: Các Truy vấn Nâng cao (Tùy chọn)
        Sắp xếp: Sắp xếp tất cả sản phẩm theo Giá bán từ thấp đến cao.

        Phần trăm giảm giá: Tính phần trăm giảm giá cho mỗi sản phẩm và hiển thị 5 sản phẩm có phần trăm giảm giá cao nhất (Yêu cầu tính toán trong query hoặc sau khi lấy data).

        Xóa bản ghi trùng lặp: Viết câu lệnh SQL để xóa các bản ghi bị trùng lặp, chỉ giữ lại một bản ghi (sử dụng Subquery hoặc Common Table Expression - CTE).

        Phân tích nhóm giá: Đếm số lượng sản phẩm trong từng nhóm giá (ví dụ: dưới 50k, 50k-100k, trên 100k).

        URL không hợp lệ: Liệt kê các bản ghi mà trường product_url bị NULL hoặc rỗng.
"""

DB_FILE = 'longchau_db.sqlite'
TABLE_NAME = 'Medicine_info'

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()



q1 = f"""
SELECT product_name, COUNT(*) AS so_luong_san_pham
FROM {TABLE_NAME}
GROUP BY product_name
HAVING COUNT(*) > 1;
"""

df = pd.read_sql_query(q1, conn)
print(df.to_string(index = False))

q2 = f"""
SELECT COUNT(*) AS so_san_pham_loi_gia
FROM {TABLE_NAME}
WHERE price IS NULL OR price = 0;
"""

df2 = pd.read_sql_query(q2, conn)
print(df2.to_string(index = False))



q4 = f"""
SELECT DISTINCT unit FROM {TABLE_NAME};
"""

df4 = pd.read_sql_query(q4, conn)
print(df4.to_string(index = False))


q5 = f"""
SELECT COUNT(*) AS so_luong_san_phan
FROM {TABLE_NAME};
"""
df5 = pd.read_sql_query(q5, conn)
print(df5.to_string(index= False))

q6 = f"""
SELECT product_name, price, original_price, 
    (CAST(original_price AS INTEGER) - CAST(price AS INTEGER)) as muc_giam
FROM {TABLE_NAME}
ORDER BY muc_giam DESC
LIMIT 10;
"""
df6 = pd.read_sql_query(q6, conn)
print(df6.to_string(index = False))

# 2.2 Sản phẩm đắt nhất
q7 = f"""
SELECT product_name, price 
FROM {TABLE_NAME} 
ORDER BY CAST(price AS INTEGER) DESC 
LIMIT 1;
"""
df7 = pd.read_sql_query(q7, conn)
print(df7.to_string(index = False))

# 2.3 Thống kê theo đơn vị tính
q8 = f"""
SELECT unit, COUNT(*) as so_luong 
FROM {TABLE_NAME} 
GROUP BY unit;
"""
df8 = pd.read_sql_query(q8, conn)
print(df8.to_string(index = False))

# 2.4 Tìm kiếm sản phẩm "Vitamin C"
q9 = f"""
SELECT product_name, price 
FROM {TABLE_NAME} 
WHERE product_name LIKE '%Vitamin C%';
"""
df9 = pd.read_sql_query(q9, conn)
print(df9.to_string(index=False))

# 2.5 Lọc theo khoảng giá (100k - 200k)
q10 = f"""
SELECT product_name, price 
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
SELECT product_name, price 
FROM {TABLE_NAME} 
ORDER BY CAST(price AS INTEGER) ASC 
LIMIT 5; -- Chỉ hiện 5 cái đầu cho gọn
"""

df11 = pd.read_sql_query(q11, conn)
print(df11.to_string(index=False))

# 3.2 Top 5 phần trăm giảm giá cao nhất
# Công thức: ((Gốc - Bán) / Gốc) * 100
q12 = f"""
SELECT product_name, price, original_price,
       ROUND(((CAST(original_price AS FLOAT) - CAST(price AS FLOAT)) / CAST(original_price AS FLOAT)) * 100, 2) as phan_tram_giam
FROM {TABLE_NAME}
WHERE original_price > 0
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
q14 = f"""
SELECT product_name, product_url 
FROM {TABLE_NAME} 
WHERE product_url IS NULL OR product_url = '';
"""

df14 = pd.read_sql_query(q14, conn)
print(df14.to_string(index=False))

# 3.5 Xóa bản ghi trùng lặp (Giữ lại dòng có ID nhỏ nhất)
# SQLite sử dụng ROWID để định danh
q15 = f"""
DELETE FROM {TABLE_NAME}
WHERE rowid NOT IN (
    SELECT MIN(rowid)
    FROM {TABLE_NAME}
    GROUP BY product_name
);
"""

cursor.execute(q15)
conn.commit()
print("Da xoa cac ban ghi trung lap.")
