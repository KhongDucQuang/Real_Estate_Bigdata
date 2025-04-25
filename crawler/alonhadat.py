import os
import re
import json
import time
import random
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

STATE_FILE = "/app/crawler_state/crawler_Alonhadat_state.json"

# ========== XỬ LÝ NGÀY ========== #
def parse_post_date(raw_date: str) -> str:
    raw_date = raw_date.lower().strip()
    try:
        if "hôm nay" in raw_date:
            date_obj = datetime.today()
        elif "hôm qua" in raw_date:
            date_obj = datetime.today() - timedelta(days=1)
        else:
            date_obj = datetime.strptime(raw_date, "%d/%m/%Y")
        return date_obj.strftime("%Y-%m-%d")
    except:
        return datetime.today().strftime("%Y-%m-%d")

# ========== GHI/ĐỌC TRẠNG THÁI ========== #
def load_last_page():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
        return state.get("alonhadat", {}).get("last_page", 0)
    return 0

def save_last_page(last_page):
    state = {}
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
    state["alonhadat"] = {"last_page": last_page}
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=4)

# ========== CRAWLER CHÍNH ========== #
def crawl_alonhadat(pages_per_run=25, new_pages=5):
    user_agent_list = [
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.5615.49 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.5563.111 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0',  # Firefox on Ubuntu
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/111.0',  # Firefox on Windows
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/111.0',  # Firefox on Mac
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
    ]

    base_url = "https://alonhadat.com.vn/can-ban-nha"
    data_list = []
    pages_per_ua = 5
    current_ua_index = -1
    driver = None

    start_page = load_last_page() + 1
    end_page = start_page + pages_per_run - 1
    important_pages = list(range(1, new_pages + 1))
    full_page_list = sorted(set(important_pages + list(range(start_page, end_page + 1))))

    for page in full_page_list:
        target_ua_index = ((page - 1) // pages_per_ua) % len(user_agent_list)

        if target_ua_index != current_ua_index or driver is None:
            if driver:
                driver.quit()
            current_ua_index = target_ua_index
            ua = user_agent_list[current_ua_index]

            chrome_options = Options()
            chrome_options.add_argument(f"user-agent={ua}")
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            driver = webdriver.Chrome(options=chrome_options)
            wait = WebDriverWait(driver, 15)

        url = base_url + ".htm" if page == 1 else f"{base_url}/trang-{page}.htm"
        print(f"🕸️ Đang cào trang {page}: {url}")

        try:
            driver.get(url)
            posts = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "content-item")))
        except TimeoutException:
            continue
        except:
            continue

        for post in posts:
            try:

                raw_date = post.find_element(By.CLASS_NAME, "ct_date").text.strip()
                post_date = parse_post_date(raw_date)

                diachi_element = post.find_element(By.CLASS_NAME, "ct_dis")
                diachi = diachi_element.text.strip()
                lists = diachi.split(", ")

                if "Đường" in lists[0]:
                    lists[0] = lists[0].replace("Đường ", "")
                if "Phố" in lists[0]:
                    lists[0] = lists[0].replace("Phố ", "")
                if "Phường" in lists[1]:
                    lists[1] = lists[1].replace("Phường ", "")
                if "Xã" in lists[1]:
                    lists[1] = lists[1].replace("Xã ", "")
                if "Quận" in lists[2]:
                    lists[2] = lists[2].replace("Quận ", "")
                if "Huyện" in lists[2]:
                    lists[2] = lists[2].replace("Huyện ", "")

                price = "Không rõ"
                try:
                    price_text = post.find_element(By.CLASS_NAME, "ct_price").text.strip().lower()
                    if "thỏa thuận" in price_text:
                        price = "Thỏa thuận"
                    else:
                        match_ty = re.search(r"([\d.,]+)\s*tỷ", price_text)
                        match_trieu = re.search(r"([\d.,]+)\s*triệu", price_text)
                        if match_ty:
                            price = float(match_ty.group(1).replace(',', '.'))
                        elif match_trieu:
                            price = float(match_trieu.group(1).replace(',', '.')) / 1000
                except: pass

                area = None
                try:
                    area_text = post.find_element(By.CLASS_NAME, "ct_dt").text.strip()
                    area_match = re.search(r"([\d.,]+)\s*m", area_text)
                    area = float(area_match.group(1).replace(',', '.')) if area_match else None
                except: pass

                try:
                    kthuoc = post.find_element(By.CLASS_NAME, "ct_kt").text.strip().replace("Kích thước: ", "").replace(
                        "m", "")
                    kthuoc_list = kthuoc.split("x")
                    cngang = kthuoc_list[0].replace(",", ".")
                    cdai = kthuoc_list[1].replace(",", ".")
                    if kthuoc == "---":
                        cngang = None
                        cdai = None
                except:
                    cngang = None
                    cdai = None
                try:
                    dorongduong = post.find_element(By.CLASS_NAME, "road-width").text.strip().replace("m", "")
                except:
                    dorongduong = None

                try:
                    sotang = post.find_element(By.CLASS_NAME, "floors").text.strip().replace(" lầu", "")
                except:
                    sotang = None

                try:
                    sophongngu = post.find_element(By.CLASS_NAME, "bedroom").text.strip().replace(" phòng ngủ", "")
                except:
                    sophongngu = None
                try:
                    post.find_element(By.CLASS_NAME, "parking")
                    chodexe = "Có"
                except:
                    chodexe = None
                data_list.append({
                    "post_date": post_date,
                    "duong_pho": lists[0],
                    "phuong_xa": lists[1],
                    "quan_huyen": lists[2],
                    "thanh_pho": lists[3],
                    "loai_bds": "Nhà đất",
                    "area": area,
                    "chieu_ngang": cngang,
                    "chieu_dai": cdai,
                    "duong_truoc_nha": dorongduong,
                    "so_tang": sotang,
                    "so_phong_ngu": sophongngu,
                    "cho_de_xe": chodexe,
                    "price": price,
                    "source": "alonhadat",
                })
            except:
                continue

        time.sleep(random.uniform(1.5, 3.0))

    if driver:
        driver.quit()

    save_last_page(end_page)
    print(f"✅ Cào xong {len(full_page_list)} trang, tổng {len(data_list)} tin.")
    return data_list
