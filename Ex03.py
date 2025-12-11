import sqlite3
from selenium import webdriver
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from pathlib import Path
import os
import time
import pandas as pd
import re


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



# --- SETUP DATABASE ---
DB_FILE = 'longchau_db.sqlite'
TABLE_NAME = 'Medicine_info'

if os.path.exists(DB_FILE):
    try:
        os.remove(DB_FILE)
        print(f'Da xoa file DB cu: {DB_FILE}')
    except PermissionError:
        print("Khong the xoa file DB (dang duoc mo).")

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# Fix 1: Added 'S' to EXISTS and ensured column names are consistent
create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
    stt INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id TEXT,
    product_name TEXT,
    price REAL,
    original_price REAL,
    unit TEXT,
    product_url TEXT UNIQUE,
    img TEXT
);
"""
cursor.execute(create_table_sql)
conn.commit()
print(f"Da ket noi va chuan bi bang {TABLE_NAME} trong {DB_FILE}")

print("\n --- Bat dau lay duong dan ---")

# --- SETUP SELENIUM ---
geckodriver_path = Path(__file__).parent / "geckodriver" # Better path handling
service = Service(executable_path=str(geckodriver_path))

option = webdriver.FirefoxOptions()
# option.binary_location = "/Applications/Firefox.app/Contents/MacOS/firefox" # Only use if Firefox is not in standard Applications
option.add_argument("--width=1200")
option.add_argument("--height=800")

driver = webdriver.Firefox(service=service, options=option)

try:
    url = "https://nhathuoclongchau.com.vn/thuc-pham-chuc-nang/vitamin-khoang-chat"
    driver.get(url)
    time.sleep(2)

    body = driver.find_element(By.TAG_NAME, "body")
    time.sleep(2)

    # Click "Xem thêm" logic
    print("Dang tai them san pham...")
    for _ in range(3): # Reduced range for testing, increase to 10 for full run
        try:
            buttons = driver.find_elements(By.TAG_NAME, "button")
            clicked = False
            for button in buttons:
                if "Xem thêm" in button.text and "sản phẩm" in button.text:
                    driver.execute_script("arguments[0].click();", button) # JavaScript click is often more reliable
                    clicked = True
                    time.sleep(2)
                    break
            if not clicked:
                break
        except Exception as e:
            print(f"Loi click xem them: {e}")

    # Scroll logic
    for _ in range(10):
        body.send_keys(Keys.PAGE_DOWN)
        time.sleep(0.5)

    time.sleep(1)

    # Get all "Buy" buttons
    buttons = driver.find_elements(By.XPATH, "//button[text()='Chọn mua']")
    print(f"Tim thay {len(buttons)} san pham.")

    # --- EXTRACTION LOOP ---
    # helper functions
    def parse_price(text: str) -> float:
        if not text:
            return 0.0
        # remove non-digit chars except dot and comma, then normalize
        num = re.sub(r"[^0-9,\.]", "", text)
        num = num.replace('.', '').replace(',', '')
        try:
            return float(num) if num else 0.0
        except:
            return 0.0

    def extract_unit(sp) -> str:
        # Look for common Vietnamese unit words nearby
        try:
            text = sp.text
            # simple heuristics
            m = re.search(r"\b(Hộp|Chai|Vỉ|Viên|Gói)\b", text)
            if m:
                return m.group(1)
        except:
            pass
        # look for explicit span elements that might contain unit
        try:
            candidates = sp.find_elements(By.XPATH, ".//span|.//p|.//div")
            for c in candidates:
                t = c.text
                if t and re.search(r"\b(Hộp|Chai|Vỉ|Viên|Gói)\b", t):
                    m = re.search(r"\b(Hộp|Chai|Vỉ|Viên|Gói)\b", t)
                    return m.group(1)
        except:
            pass
        return ""

    def extract_product_id_from_url(url: str) -> str:
        if not url:
            return ""
        # try to find numeric id segments in URL
        # examples: .../some-product-123456, .../p/123456, query id=123
        m = re.search(r"(?:-|/)(\d{4,})(?:$|/|\D)", url)
        if m:
            return m.group(1)
        m2 = re.search(r"id=(\d+)", url)
        if m2:
            return m2.group(1)
        return ""

    count = 0
    for i, bt in enumerate(buttons, 1):
        try:
            # Traversal logic (User's original logic)
            parent_div = bt
            for _ in range(3):
                parent_div = parent_div.find_element(By.XPATH, "..")
            
            sp = parent_div

            # Extract Data
            try:
                name_text = sp.find_element(By.TAG_NAME, "h3").text
            except:
                name_text = "Unknown"

            try:
                price_text = sp.find_element(By.CLASS_NAME, "text-blue-5").text
            except:
                price_text = "0"
                
            try:
                # css_selector finding a div that has the class 'line-through'
                origin_price_elm = sp.find_element(By.CSS_SELECTOR, "div.line-through")
                original_price_text = origin_price_elm.text
            except:
                # If there is no discount, original price might be empty or same as current price
                original_price_text = "0"

            try:
                img_src = sp.find_element(By.TAG_NAME, "img").get_attribute("src")
            except:
                img_src = ""

            # product_url extraction: try to find anchor inside the product container
            product_url = ""
            try:
                a = sp.find_element(By.TAG_NAME, 'a')
                product_url = a.get_attribute('href') or ''
            except:
                # fallback: look for any href in descendants
                try:
                    a2 = sp.find_element(By.XPATH, ".//a[@href]")
                    product_url = a2.get_attribute('href') or ''
                except:
                    product_url = ''

            # product_id extraction heuristics
            product_id = ""
            try:
                # check data attributes on button or container
                data_id = bt.get_attribute('data-product-id') or bt.get_attribute('data-id')
                if data_id:
                    product_id = data_id
                else:
                    product_id = extract_product_id_from_url(product_url)
            except:
                product_id = extract_product_id_from_url(product_url)

            # parse numeric prices
            price_val = parse_price(price_text)
            original_price_val = parse_price(original_price_text)

            # extract unit
            unit_val = extract_unit(sp)

            if name_text:
                # Fix 2: Insert INDIVIDUAL values, not the list variables
                insert_sql = f"""
                INSERT OR IGNORE INTO {TABLE_NAME} (product_id, product_name, price, original_price, unit, product_url, img)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """
                cursor.execute(insert_sql, (product_id, name_text, price_val, original_price_val, unit_val, product_url, img_src))
                if cursor.rowcount != 0:
                    count += 1
                    print(f"Saved: {name_text[:40]} | id={product_id} | url={product_url}")
                else:
                    print(f"Skipped (exists): {name_text[:40]} | url={product_url}")
                # commit after each insert to satisfy 'save immediately' requirement
                conn.commit()

        except Exception as e:
            print(f"Error extracting item {i}: {e}")
            continue

    conn.commit()
    print(f"Tong cong da luu {count} san pham.")

except Exception as e:
    print(f"General Error: {e}")

finally:
    # Fix 3: Quit driver OUTSIDE the loop
    driver.quit()
    conn.close()
    
    


