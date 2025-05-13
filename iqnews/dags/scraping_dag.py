from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re
import logging

# Default arguments untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,  # Menambah jumlah retry
    'retry_delay': timedelta(minutes=2),
}

# Definisi DAG
dag = DAG(
    'market_scraping',
    default_args=default_args,
    description='DAG untuk scraping berita dari IQPlus',
    schedule='*/30 * * * *',  # Berjalan setiap hari pukul 7 pagi
    catchup=False,
    max_active_runs=5,  # Jumlah maksimum instance DAG yang dapat berjalan secara bersamaan
    max_active_tasks=3,  # Jumlah task yang dapat berjalan secara bersamaan dalam satu DAG
)

# Fungsi untuk melakukan scraping
def scrape_iqnews_market():
    # Koneksi ke MongoDB menggunakan Airflow Hook
    mongo_hook = MongoHook(conn_id='mongodb_conn')
    client = mongo_hook.get_conn()
    db = client["news_db"]
    collection = db["articles2"]
    
    # URL dan konfigurasi
    base_url = "http://www.iqplus.info"
    start_url = base_url + "/news/market_news/go-to-page,1.html"
    
    # Konfigurasi Chrome untuk environment container dengan pengaturan yang lebih stabil
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--disable-notifications")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--disable-breakpad")
    chrome_options.add_argument("--disable-features=NetworkService")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--single-process")  # Mencoba single-process
    chrome_options.add_argument("--memory-pressure-off")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")
    
    # Fungsi untuk membuat instance webdriver baru
    def create_driver():
        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.set_page_load_timeout(30)  # Menambah timeout
            return driver, WebDriverWait(driver, 15)  # Menambah timeout untuk wait
        except Exception as e:
            logging.error(f"Error saat membuat driver: {e}")
            raise
    
    # Fungsi mendapatkan jumlah halaman
    def get_last_page(driver, wait):
        try:
            driver.get(start_url)
            time.sleep(2)  # Waktu tambahan untuk memastikan halaman dimuat
            
            # Coba mendapatkan elemen navigasi
            try:
                wait.until(EC.presence_of_element_located((By.CLASS_NAME, "nav")))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                nav_span = soup.find("span", class_="nav")
                
                if nav_span:
                    last_page_links = nav_span.find_all("a")
                    if last_page_links and len(last_page_links) >= 2:
                        last_page_link = last_page_links[-2]
                        if last_page_link and last_page_link.text.isdigit():
                            return int(last_page_link.text)
            except Exception as e:
                logging.warning(f"Error mendapatkan halaman terakhir: {e}")
            
            # Fallback ke metode alternatif jika metode utama gagal
            soup = BeautifulSoup(driver.page_source, "html.parser")
            nav_span = soup.find("span", class_="nav")
            if nav_span:
                text = nav_span.get_text()
                matches = re.findall(r'Page \d+ of (\d+)', text)
                if matches:
                    return int(matches[0])
            
            logging.warning("Tidak dapat menentukan jumlah halaman, defaulting ke 1")
            return 5  # Default ke 5 halaman jika tidak dapat menentukan
        except Exception as e:
            logging.error(f"Error fatal saat mendapatkan jumlah halaman: {e}")
            return 5  # Default ke 5 halaman
    
    # Fungsi untuk scraping konten artikel dengan penanganan kesalahan yang lebih baik
    def scrape_article_content(article_url, retry=0):
        if retry >= 3:  # Batas maksimum retry
            logging.warning(f"Menyerah setelah 3 kali percobaan untuk {article_url}")
            return None, None
            
        full_url = base_url + article_url if not article_url.startswith("http") else article_url
        
        try:
            # Buat driver baru untuk setiap artikel untuk menghindari masalah crash
            article_driver, article_wait = create_driver()
            
            try:
                article_driver.get(full_url)
                time.sleep(2)  # Berikan waktu untuk halaman dimuat sepenuhnya
                
                # Coba mendapatkan konten artikel
                try:
                    article_wait.until(EC.presence_of_element_located((By.ID, "zoomthis")))
                except:
                    logging.warning(f"Timeout menunggu elemen zoomthis di {full_url}")
                
                soup = BeautifulSoup(article_driver.page_source, "html.parser")
                zoom_div = soup.find("div", id="zoomthis")
                
                if not zoom_div:
                    logging.warning(f"Konten artikel tidak ditemukan di {full_url}")
                    return None, None
                
                date_element = zoom_div.find("small")
                date_text = date_element.text.strip() if date_element else "Tanggal tidak tersedia"
                
                if date_element:
                    date_element.extract()
                
                title_element = zoom_div.find("h3")
                if title_element:
                    title_element.extract()
                
                zoom_control = zoom_div.find("div", attrs={"align": "right"})
                if zoom_control:
                    zoom_control.extract()
                
                content = zoom_div.get_text(strip=True)
                content = re.sub(r'\s+', ' ', content).strip()
                
                return date_text, content
            except Exception as e:
                logging.error(f"Error saat scraping artikel {full_url}: {e}")
                # Coba lagi dengan pendekatan yang lebih sederhana
                return retry_scrape_article(full_url, retry + 1)
            finally:
                # Pastikan driver ditutup
                article_driver.quit()
        except Exception as e:
            logging.error(f"Error fatal saat scraping artikel {full_url}: {e}")
            return None, None
    
    # Fungsi retry untuk artikel
    def retry_scrape_article(url, retry_count):
        try:
            import requests
            from requests.packages.urllib3.exceptions import InsecureRequestWarning
            requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, verify=False, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                zoom_div = soup.find("div", id="zoomthis")
                
                if not zoom_div:
                    return None, None
                
                date_element = zoom_div.find("small")
                date_text = date_element.text.strip() if date_element else "Tanggal tidak tersedia"
                
                if date_element:
                    date_element.extract()
                
                title_element = zoom_div.find("h3")
                if title_element:
                    title_element.extract()
                
                zoom_control = zoom_div.find("div", attrs={"align": "right"})
                if zoom_control:
                    zoom_control.extract()
                
                content = zoom_div.get_text(strip=True)
                content = re.sub(r'\s+', ' ', content).strip()
                
                return date_text, content
            else:
                logging.warning(f"Gagal mengambil {url}, status code: {response.status_code}")
                return None, None
        except Exception as e:
            logging.error(f"Error saat retry scraping artikel {url}: {e}")
            return scrape_article_content(url, retry_count)  # Mencoba lagi dengan fungsi utama
    
    # Fungsi untuk scraping satu halaman
    def scrape_page(url, page_number):
        try:
            # Buat driver baru untuk setiap halaman
            page_driver, page_wait = create_driver()
            
            try:
                page_driver.get(url)
                time.sleep(3)  # Berikan waktu lebih untuk halaman dimuat
                
                try:
                    page_wait.until(EC.presence_of_element_located((By.ID, "load_news")))
                except:
                    logging.warning(f"Timeout menunggu elemen load_news di halaman {page_number}")
                
                # Tambahan scroll untuk memastikan konten dimuat
                page_driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                
                soup = BeautifulSoup(page_driver.page_source, "html.parser")
                news_list = soup.select_one("#load_news .box ul.news")
                
                if not news_list:
                    logging.warning(f"Elemen berita tidak ditemukan di halaman {page_number}.")
                    return
                
                news_items = news_list.find_all("li")
                logging.info(f"Ditemukan {len(news_items)} berita di halaman {page_number}")
                
                for index, item in enumerate(news_items):
                    try:
                        time_text = item.find("b").text.strip() if item.find("b") else "Tidak ada waktu"
                        title_tag = item.find("a")
                        
                        if title_tag and title_tag.has_attr("href"):
                            title = title_tag.text.strip()
                            link = title_tag["href"]
                            full_link = base_url + link if not link.startswith("http") else link
                            
                            logging.info(f"Scraping artikel {index+1}/{len(news_items)} di halaman {page_number}: {title}")
                            
                            # Cek apakah artikel sudah ada di database
                            if not collection.find_one({"judul": title}):
                                article_date, article_content = scrape_article_content(link)
                                
                                if article_content:
                                    article_data = {
                                        "judul": title, 
                                        "waktu": time_text, 
                                        "link": full_link,
                                        "tanggal_artikel": article_date,
                                        "konten": article_content,
                                        "halaman_sumber": page_number,
                                        "timestamp_scraping": datetime.now()
                                    }
                                    collection.insert_one(article_data)
                                    logging.info(f"Artikel berhasil disimpan: {title}")
                                else:
                                    logging.warning(f"Konten artikel kosong: {title}")
                            else:
                                logging.info(f"Artikel sudah ada di database: {title}")
                                
                            # Tambah jeda untuk menghindari rate limiting
                            time.sleep(2)
                    except Exception as e:
                        logging.error(f"Error saat memproses berita {index} di halaman {page_number}: {e}")
            finally:
                # Pastikan driver ditutup
                page_driver.quit()
        except Exception as e:
            logging.error(f"Error fatal saat scraping halaman {page_number}: {e}")
    
    # Eksekusi scraping
    try:
        # Buat driver untuk mendapatkan jumlah halaman
        main_driver, main_wait = create_driver()
        try:
            last_page = get_last_page(main_driver, main_wait)
            logging.info(f"Total halaman yang terdeteksi: {last_page}")
        finally:
            main_driver.quit()
        
        # Loop untuk setiap halaman dengan penanganan kesalahan yang lebih baik
        for page in range(1, min(last_page + 1, 20)):  # Batasi maksimum 20 halaman untuk keamanan
            page_url = f"{base_url}/news/market_news/go-to-page,{page}.html"
            logging.info(f"Scraping halaman {page}/{last_page}: {page_url}")
            
            try:
                scrape_page(page_url, page)
                logging.info(f"Selesai scraping halaman {page}")
            except Exception as e:
                logging.error(f"Gagal scraping halaman {page}: {e}")
                continue  # Lanjut ke halaman berikutnya meskipun ada error
                
    except Exception as e:
        logging.error(f"Error fatal saat menjalankan scraping: {e}")
    finally:
        logging.info("✅ Scraping selesai.")
        
# Task untuk menjalankan scraping
scrape_task = PythonOperator(
    task_id='scrape_market',
    python_callable=scrape_iqnews_market,
    dag=dag,
    execution_timeout=timedelta(minutes=25),
)

# Task ID dapat digunakan untuk dependency di kemudian hari