from airflow import DAG
from airflow.operators.python import PythonOperator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from selenium.common.exceptions import TimeoutException, WebDriverException
from datetime import datetime
import time
import os
import re
import requests
import urllib.parse
import json
import zipfile
from lxml import etree
import xml.etree.ElementTree as ET   # pakai ET, bukan lxml
import zipfile, json, os, re
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 3,
    'retry_delay': 60,  # 1 minute between retries
}

def get_cookies_and_download_files(company_names, download_dir):
    """
    Get authenticated cookies first, then download instance.zip files using those cookies
    """
    print(f"\nStarting downloads with cookie authentication for {len(company_names)} companies...")
    
    # Set up Chrome for getting cookies
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    cookies_file = os.path.join(download_dir, "idx_cookies.json")
    cookies = None
    
    # Get fresh cookies using Selenium
    print("Getting fresh cookies from IDX website...")
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Apply stealth settings
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.", 
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)
        
        # Navigate to the IDX website to get cookies
        url = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan"
        print(f"Accessing URL for cookies: {url}")
        
        try:
            driver.set_page_load_timeout(60)  # 60 seconds timeout
            driver.get(url)
            
            # Wait for page to fully load
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
            
            # Additional wait to ensure all cookies are set
            time.sleep(5)
            
            # Extract all cookies
            cookies = driver.get_cookies()
            print(f"Retrieved {len(cookies)} cookies from IDX website")
            
            # Save cookies to file for later use
            with open(cookies_file, 'w') as f:
                json.dump(cookies, f)
                
        except Exception as e:
            print(f"Error getting cookies: {str(e)}")
            driver.save_screenshot(f"{download_dir}/cookie_error.png")
            if os.path.exists(cookies_file):
                print("Using previously saved cookies...")
                with open(cookies_file, 'r') as f:
                    cookies = json.load(f)
    finally:
        driver.quit()
    
    if not cookies:
        print("No cookies available. Download will likely fail.")
        return
    
    # Base URL template for zip files
    base_url = "https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%202025/TW1/{company_code}/instance.zip"
    
    # Set up session with headers and cookies
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan",
        "sec-ch-ua": '"Chromium";v="120", "Google Chrome";v="120", "Not=A?Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0"
    }
    session.headers.update(headers)
    
    # Add cookies to session
    for cookie in cookies:
        session.cookies.set(cookie['name'], cookie['value'], domain=cookie['domain'])
    
    # Create a log file for download results
    log_file_path = os.path.join(download_dir, "download_results.log")
    
    successful_downloads = 0
    failed_downloads = 0
    
    # Try to download with referral and proper session
    with open(log_file_path, 'w') as log_file:
        log_file.write(f"Download started at: {datetime.now()}\n")
        log_file.write(f"Total companies to process: {len(company_names)}\n\n")
        
        for idx, company_code in enumerate(company_names, 1):
            try:
                company_code = company_code.strip()
                if not company_code:
                    continue
                    
                # Create URL for this company
                download_url = base_url.format(company_code=company_code)
                
                print(f"[{idx}/{len(company_names)}] Downloading for {company_code}: {download_url}")
                log_file.write(f"[{idx}/{len(company_names)}] Company: {company_code}\n")
                log_file.write(f"URL: {download_url}\n")
                
                # Create company directory if it doesn't exist
                company_dir = os.path.join(download_dir, company_code)
                if not os.path.exists(company_dir):
                    os.makedirs(company_dir)
                
                # First visit the IDX page to establish referrer and session
                session.get("https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan")
                
                # Then download the file
                try:
                    response = session.get(download_url, stream=True, timeout=60)
                    
                    if response.status_code == 200:
                        # Check if it's actually a zip file
                        content_type = response.headers.get('Content-Type', '')
                        content_disp = response.headers.get('Content-Disposition', '')
                        content_length = response.headers.get('Content-Length', 0)
                        
                        # Save the HTTP headers for debugging
                        headers_file = os.path.join(company_dir, f"{company_code}_headers.txt")
                        with open(headers_file, 'w') as f:
                            for key, value in response.headers.items():
                                f.write(f"{key}: {value}\n")
                        
                        if 'application/zip' in content_type or 'zip' in content_type or 'instance.zip' in content_disp or int(content_length) > 1000:
                            # Save the file
                            file_path = os.path.join(company_dir, f"{company_code}_instance.zip")
                            with open(file_path, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            
                            file_size = os.path.getsize(file_path)
                            if file_size > 1000:  # Assuming a valid ZIP is at least 1KB
                                print(f"✓ Downloaded {company_code}_instance.zip - {file_size} bytes")
                                log_file.write(f"Status: Success - {file_size} bytes\n")
                                successful_downloads += 1
                            else:
                                print(f"✗ Warning: Small file size for {company_code}_instance.zip - {file_size} bytes")
                                log_file.write(f"Status: Warning - Small file size {file_size} bytes\n")
                                
                                # Save response content for inspection
                                error_content_file = os.path.join(company_dir, f"{company_code}_small_response.txt")
                                with open(error_content_file, 'wb') as f:
                                    f.write(response.content)
                                
                                failed_downloads += 1
                        else:
                            # Might be an HTML page (error) instead of a zip
                            error_file = os.path.join(company_dir, f"{company_code}_error.html")
                            with open(error_file, 'wb') as f:
                                for chunk in response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                            
                            print(f"✗ Error: Received non-zip content for {company_code}")
                            log_file.write(f"Status: Error - Received non-zip content (Content-Type: {content_type})\n")
                            failed_downloads += 1
                    else:
                        # Try direct download with Selenium as fallback
                        print(f"✗ HTTP {response.status_code} for {company_code}. Trying Selenium fallback...")
                        log_file.write(f"Status: Failed initial HTTP {response.status_code}. Trying Selenium fallback...\n")
                        
                        # Save error response for debugging
                        error_resp_file = os.path.join(company_dir, f"{company_code}_error_response.html")
                        with open(error_resp_file, 'wb') as f:
                            f.write(response.content)
                        
                        # Try Selenium direct download
                        selenium_download_success = download_with_selenium(company_code, download_url, company_dir)
                        
                        if selenium_download_success:
                            print(f"✓ Selenium fallback successful for {company_code}")
                            log_file.write(f"Status: Selenium fallback successful\n")
                            successful_downloads += 1
                        else:
                            print(f"✗ Both download methods failed for {company_code}")
                            log_file.write(f"Status: All download methods failed\n")
                            failed_downloads += 1
                        
                except requests.exceptions.RequestException as e:
                    print(f"✗ Download error for {company_code}: {str(e)}")
                    log_file.write(f"Status: Exception - {str(e)}\n")
                    failed_downloads += 1
                
                # Add a separator between companies in the log
                log_file.write("-" * 50 + "\n")
                
                # Sleep between requests to avoid overwhelming the server
                time.sleep(2)
                
            except Exception as e:
                print(f"✗ Error processing {company_code}: {str(e)}")
                log_file.write(f"Status: Processing Error - {str(e)}\n")
                log_file.write("-" * 50 + "\n")
                failed_downloads += 1
        
        # Write summary
        summary = f"""
Download Summary:
----------------
Total companies: {len(company_names)}
Successful downloads: {successful_downloads}
Failed downloads: {failed_downloads}
Completion time: {datetime.now()}
"""
        print(summary)
        log_file.write(summary)
    
    print(f"Download process completed. See {log_file_path} for details.")

def download_with_selenium(company_code, download_url, company_dir):
    """
    Fallback method to download files using Selenium directly
    """
    print(f"Attempting Selenium direct download for {company_code}")
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Set download directory
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": company_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": False
    })
    
    driver = webdriver.Chrome(options=chrome_options)
    try:
        # Apply stealth settings
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True)
        
        # First visit the main page to establish session
        main_url = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan"
        driver.get(main_url)
        time.sleep(5)  # Let the page load and set cookies
        
        # Take screenshot for debugging
        driver.save_screenshot(f"{company_dir}/{company_code}_main_page.png")
        
        # Now navigate to download URL
        print(f"Navigating to download URL: {download_url}")
        driver.get(download_url)
        time.sleep(10)  # Wait for download to start and complete
        
        # Check if file downloaded
        expected_zip = os.path.join(company_dir, "instance.zip")
        if os.path.exists(expected_zip):
            file_size = os.path.getsize(expected_zip)
            print(f"✓ Selenium downloaded instance.zip - {file_size} bytes")
            
            # Rename to company code format
            new_path = os.path.join(company_dir, f"{company_code}_instance.zip")
            os.rename(expected_zip, new_path)
            return True
        else:
            # Take screenshot of the download page for debugging
            driver.save_screenshot(f"{company_dir}/{company_code}_download_page.png")
            print(f"✗ Selenium download failed for {company_code}")
            return False
            
    except Exception as e:
        print(f"✗ Selenium download error for {company_code}: {str(e)}")
        return False
    finally:
        driver.quit()

def scrape_idx_emiten_names():
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Set download directory
    download_dir = "/opt/airflow/downloads"
    if not os.path.exists(download_dir):
        os.makedirs(download_dir)
    
    driver = webdriver.Chrome(options=chrome_options)
    
    try:
        # Apply stealth settings
        stealth(driver,
                languages=["en-US", "en"],
                vendor="Google Inc.",
                platform="Win32",
                webgl_vendor="Intel Inc.",
                renderer="Intel Iris OpenGL Engine",
                fix_hairline=True,
                )
        
        # Save cookies to file for later use
        cookies_file = os.path.join(download_dir, "idx_cookies.json")
        
        # Navigate to the target URL
        url = "https://www.idx.co.id/id/perusahaan-tercatat/laporan-keuangan-dan-tahunan"
        print(f"Accessing URL: {url}")
        
        try:
            driver.set_page_load_timeout(60)  # 60 seconds timeout
            driver.get(url)
        except TimeoutException:
            print("Page load timed out, continuing with current content")
        
        # Wait for page to load with explicit waits
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
            print("Page loaded successfully")
            
            # Save cookies
            cookies = driver.get_cookies()
            with open(cookies_file, 'w') as f:
                json.dump(cookies, f)
            print(f"Saved {len(cookies)} cookies to {cookies_file}")
            
        except TimeoutException:
            print("Timed out waiting for page to load")
            raise
        
        # Human-like behavior
        print("Simulating human behavior...")
        time.sleep(5)
        
        # Select year 2025
        try:
            print("\nSelecting year 2025...")
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)
            
            # Find and click the 2025 radio button
            year_radio = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input[value='2025']"))
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", year_radio)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", year_radio)
            print("Selected year 2025")
            
            # Click "Terapkan" button
            apply_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'btn--primary') and contains(text(), 'Terapkan')]"))
            )
            driver.execute_script("arguments[0].click();", apply_button)
            print("Clicked 'Terapkan' button")
            
            # Wait for the page to refresh with 2025 data
            time.sleep(5)
            
            # Update cookies after interaction
            cookies = driver.get_cookies()
            with open(cookies_file, 'w') as f:
                json.dump(cookies, f)
            print(f"Updated cookies after year selection")
            
        except Exception as e:
            print(f"Error selecting year 2025: {str(e)}")
            driver.save_screenshot(f"{download_dir}/year_selection_error.png")
            raise
        
        # Extract company names (emiten)
        company_names = []
        try:
            # Wait for the companies to load
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".box-title"))
            )
            
            # Find all company sections
            company_sections = driver.find_elements(By.XPATH, "//div[contains(@class, 'box-title')]")
            print(f"Found {len(company_sections)} company sections")
            
            # Take a screenshot of the loaded page
            driver.save_screenshot(f"{download_dir}/idx_companies_2025.png")
            
            # Extract company names
            for section in company_sections:
                try:
                    # Extract the company code (like AADI, AALI, etc.)
                    company_name_element = section.find_element(By.CSS_SELECTOR, "span.f-20")
                    company_name = company_name_element.text.strip()
                    if company_name:
                        company_names.append(company_name)
                        print(f"Found company: {company_name}")
                except Exception as e:
                    print(f"Error extracting company name from section: {str(e)}")
                    continue
            
            # If pagination exists, we should iterate through all pages
            try:
                pagination = driver.find_elements(By.CSS_SELECTOR, ".pagination li a")
                if len(pagination) > 1:
                    print(f"Found pagination with {len(pagination)} elements")
                    # Determine the last page number
                    last_page = 1
                    for page_link in pagination:
                        try:
                            page_num = int(page_link.text.strip())
                            if page_num > last_page:
                                last_page = page_num
                        except ValueError:
                            continue
                    
                    print(f"Total pages detected: {last_page}")
                    
                    # We've already processed page 1, so start from page 2
                    for page in range(2, last_page + 1):
                        try:
                            # Find and click the page link
                            next_page_link = WebDriverWait(driver, 10).until(
                                EC.element_to_be_clickable((By.XPATH, f"//div[contains(@class, 'pagination')]//a[text()='{page}']"))
                            )
                            driver.execute_script("arguments[0].click();", next_page_link)
                            print(f"Clicked to navigate to page {page}")
                            
                            # Wait for the page to load
                            time.sleep(3)
                            
                            # Update cookies after pagination
                            cookies = driver.get_cookies()
                            with open(cookies_file, 'w') as f:
                                json.dump(cookies, f)
                            
                            # Find all company sections on this page
                            company_sections = driver.find_elements(By.XPATH, "//div[contains(@class, 'box-title')]")
                            print(f"Found {len(company_sections)} company sections on page {page}")
                            
                            # Take a screenshot of each page
                            driver.save_screenshot(f"{download_dir}/idx_companies_2025_page_{page}.png")
                            
                            # Extract company names
                            for section in company_sections:
                                try:
                                    company_name_element = section.find_element(By.CSS_SELECTOR, "span.f-20")
                                    company_name = company_name_element.text.strip()
                                    if company_name:
                                        company_names.append(company_name)
                                        print(f"Found company on page {page}: {company_name}")
                                except Exception as e:
                                    print(f"Error extracting company name from section on page {page}: {str(e)}")
                                    continue
                        except Exception as e:
                            print(f"Error processing page {page}: {str(e)}")
                            driver.save_screenshot(f"{download_dir}/page_{page}_error.png")
                            continue
            except Exception as e:
                print(f"Error processing pagination: {str(e)}")
                
        except Exception as e:
            print(f"Error extracting company names: {str(e)}")
            driver.save_screenshot(f"{download_dir}/extraction_error.png")
            raise
        
        # Save company names to a file
        if company_names:
            output_file = os.path.join(download_dir, "idx_company_names_2025.txt")
            with open(output_file, 'w') as f:
                for name in company_names:
                    f.write(f"{name}\n")
            print(f"Saved {len(company_names)} company names to {output_file}")
            
            # Download instance.zip for each company
            get_cookies_and_download_files(company_names, download_dir)
        else:
            print("No company names found!")
        
        return company_names
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        # Save error screenshot
        error_path = "/opt/airflow/downloads/idx_error.png"
        driver.save_screenshot(error_path)
        print(f"Error screenshot saved to {error_path}")
        raise
    finally:
        driver.quit()
        print("Browser closed")

def download_instance_zip_only():
    """
    Function to only download instance.zip files using an existing company names file.
    This can be used if you already have the idx_company_names_2025.txt file and
    just want to download the zip files without re-scraping.
    """
    download_dir = "/opt/airflow/downloads"
    company_names_file = os.path.join(download_dir, "idx_company_names_2025.txt")
    
    if os.path.exists(company_names_file):
        with open(company_names_file, 'r') as f:
            company_names = [line.strip() for line in f if line.strip()]
        
        print(f"Loaded {len(company_names)} company names from {company_names_file}")
        get_cookies_and_download_files(company_names, download_dir)
        return company_names
    else:
        print(f"Error: Company names file not found at {company_names_file}")
        return []



def convert_xbrl_to_json(download_dir):
    log_path = os.path.join(download_dir, "xbrl_conversion_results.log")
    ok, fail = 0, 0

    # Koneksi MongoDB
    client = MongoClient("mongodb://mongo:27017")  
    db = client["docker_airflow_idx_db"]  # nama database
    collection = db["docker_airflow_idx_laporan_keuangan"]  # nama collection

    with open(log_path, "w") as log:
        log.write(f"XBRL → JSON mulai: {datetime.now()}\n")

        for root, _, files in os.walk(download_dir):
            if root == download_dir:
                continue

            company = os.path.basename(root)
            zips = [f for f in files if f.endswith("_instance.zip")]
            if not zips:
                continue

            for z in zips:
                zpath = os.path.join(root, z)
                log.write(f"Proses: {zpath}\n")
                print(f"Proses {zpath} …")

                try:
                    with zipfile.ZipFile(zpath, "r") as zf:
                        if "instance.xbrl" not in zf.namelist():
                            raise ValueError("instance.xbrl tidak ada")

                        with zf.open("instance.xbrl") as xf:
                            tree = ET.parse(xf)
                            root_el = tree.getroot()

                            kv = {}
                            for elem in root_el.iter():
                                tag = elem.tag.split("}")[-1]
                                txt = (elem.text or "").strip()
                                if txt:
                                    kv[tag] = txt

                            emiten = kv.get("EntityName", company)
                            out = {
                                "emiten": emiten,
                                "laporan_keuangan": kv
                            }

                            # Simpan ke file JSON
                            out_name = f"{company}_instance.json"
                            out_path = os.path.join(root, out_name)
                            with open(out_path, "w", encoding="utf-8") as f:
                                json.dump(out, f, indent=2, ensure_ascii=False)

                            # Simpan ke MongoDB
                            collection.insert_one(out)

                            print(f"✓ disimpan → {out_name} & MongoDB")
                            log.write("Status: SUCCESS\n")
                            ok += 1

                except Exception as e:
                    print(f"✗ gagal: {e}")
                    log.write(f"Status: ERROR – {e}\n")
                    fail += 1

                log.write("-"*50 + "\n")

        print(f"Selesai. Berhasil: {ok}, Gagal: {fail}")
        log.write(f"Selesai {datetime.now()} | OK={ok} FAIL={fail}\n")

     

def process_xbrl_files():
    """
    Task function for Airflow to convert XBRL files to JSON
    """
    download_dir = "/opt/airflow/downloads"
    convert_xbrl_to_json(download_dir)

with DAG(
    dag_id='idx_emiten_scraper',
    default_args=default_args,
    schedule='0 0 * * *',  # 00:00 UTC = 07:00 WIB
    start_date=datetime(2025, 5, 12),
    catchup=False,
    tags=['scraping', 'idx', 'emiten'],
) as dag:

    # Task to scrape company names and download instance.zip files
    scrape_task = PythonOperator(
        task_id='scrape_idx_emiten_names',
        python_callable=scrape_idx_emiten_names,
    )
    
    # Optional task to only download instance.zip files (if you already have the company names)
    download_task = PythonOperator(
        task_id='download_instance_zip_only',
        python_callable=download_instance_zip_only,
    )
    
    # New task to convert XBRL to JSON
    convert_task = PythonOperator(
        task_id='convert_xbrl_to_json',
        python_callable=process_xbrl_files,
    )
    
    # Set task dependencies - convert after download or scrape
    scrape_task >> convert_task
    download_task>>convert_task
