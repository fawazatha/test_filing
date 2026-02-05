import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from datetime import datetime

import json
import time
import random
import subprocess
import platform
import logging
import shutil  


LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_chrome_info():
    """
    Detects the installed Google Chrome version AND path.
    Returns a tuple: (major_version, executable_path)
    """
    if platform.system() == "Linux":
        try:
            for binary in ["chrome", "google-chrome", "chromium", "chromium-browser"]:
                binary_path = shutil.which(binary)
                if not binary_path: 
                    continue
                
                try:
                    output = subprocess.check_output([binary_path, "--version"], text=True)
                    if not output: continue
                    
                    version_str = output.strip().split()[-1] 
                    major_version = int(version_str.split('.')[0])
                    
                    LOGGER.info(f"Detected {binary} at {binary_path} (Version: {major_version})")
                    return major_version, binary_path
                except:
                    continue
            return None, None
        
        except Exception as error:
            LOGGER.error(f"Could not detect Chrome version: {error}")
            return None, None
    
    # Windows fallback
    return 143, None


def extract_json_objects(text: str, target_key='"data":'):
    """
    Generator that finds ALL occurrences of `target_key` and extracts 
    the valid JSON structure (Object or Array) immediately following it.
    """
    start_search = 0

    while True:
        # Find the next occurrence of "data":
        start_idx = text.find(target_key, start_search)
        if start_idx == -1:
            break
            
        # Move past the marker
        structure_start = start_idx + len(target_key)
        
        # Find the first opening bracket [ or {
        open_idx = -1
        stack = []
        
        # Scan forward to find start of structure
        for i in range(structure_start, min(structure_start + 50, len(text))):
            char = text[i]
            if char in ['[', '{']:
                open_idx = i
                stack.append(char)
                break
        
        # If no bracket found near marker, skip this occurrence
        if open_idx == -1:
            start_search = structure_start
            continue

        # Count brackets to find the end
        for i in range(open_idx + 1, len(text)):
            char = text[i]
            
            if char == '[': stack.append('[')
            elif char == '{': stack.append('{')
            elif char == ']':
                if stack and stack[-1] == '[': stack.pop()
            elif char == '}':
                if stack and stack[-1] == '{': stack.pop()
            
            if not stack:
                # Found the closing bracket
                json_str = text[open_idx : i+1]
                yield json_str
                # Continue searching after this object
                start_search = i + 1
                break
        else:
            # If loop finishes without empty stack, structure is malformed/incomplete
            start_search = structure_start


def format_iso_date(iso_str: str) -> str:
    if not iso_str: return ""
    try:
        dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    except ValueError:
        return iso_str


def scrape_bca(page_num: int) -> list[dict[str, any]]:
    LOGGER.info(f"Initializing BCA Scraper for page {page_num}...")

    options = uc.ChromeOptions()
    options.add_argument('--headless=new') 
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    
    # --- NEW ANTI-BLOCKING ARGUMENTS ---
    # Set a standard window size (Headless defaults to 800x600 which is suspicious)
    options.add_argument('--window-size=1920,1080')
    
    # Force a standard User-Agent (Don't let it say "Headless")
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
    
    # Explicitly disable automation flags (Double protection)
    options.add_argument('--disable-blink-features=AutomationControlled')
    
    # Ignore SSL errors (Sometimes WAFs reset connections on SSL handshake)
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--allow-running-insecure-content')

    chrome_version, chrome_path = get_chrome_info()
    driver_path = shutil.which("chromedriver")

    try:
        driver = uc.Chrome(
            options=options, 
            use_subprocess=True, 
            version_main=chrome_version,
            browser_executable_path=chrome_path,
            driver_executable_path=driver_path if platform.system() == "Linux" else None
        )

    except Exception as error:
        LOGGER.error(f"Failed to initialize driver: {error}")
        return []
    
    results = []

    try:
        LOGGER.info("Priming session")
        driver.get("https://bcasekuritas.co.id/")
        time.sleep(random.uniform(3, 5))

        target_url = f"https://bcasekuritas.co.id/en/latest-news/news?page={page_num}"
        LOGGER.info(f"Navigating to {target_url}...")
        driver.get(target_url)
        time.sleep(8) 
        
        # Scan ALL "data" blocks 
        LOGGER.info("Scanning for valid News Data List")
        
        scripts = driver.find_elements(By.TAG_NAME, "script")
        
        for script in scripts:
            content = script.get_attribute("innerHTML")
            if 'self.__next_f.push' in content and 'current_page' in content:
                
                clean_content = content.replace('\\"', '"').replace('\\\\', '\\')
                
                # Iterate through ALL "data": blocks in this script
                for json_str in extract_json_objects(clean_content, '"data":'):
                    try:
                        parsed_data = json.loads(json_str)
                        
                        # CRITICAL CHECK: We only want the LIST, not the single object
                        if isinstance(parsed_data, list):
                            LOGGER.info(f"Found a LIST with {len(parsed_data)} items. Verifying content")
                            
                            # Verify it contains news items (check for 'slug' or 'title_id')
                            if len(parsed_data) > 0 and ('slug' in parsed_data[0] or 'title_id' in parsed_data[0]):
                                LOGGER.info("Target Data Found!")
                                
                                for item in parsed_data:
                                    # Safe extraction
                                    slug = item.get("slug", "")
                                    title = item.get("title_en") or item.get("title_id")
                                    
                                    # Stock Code Extraction
                                    stock_code = None
                                    stock_info = item.get("meta", {}).get("cover", {}).get("stock_information_data", [])
                                    if stock_info and isinstance(stock_info, list) and len(stock_info) > 0:
                                        stock_code = stock_info[0].get("code")

                                    results.append({
                                        "title": title,
                                        "timestamp": format_iso_date(item.get("published_at")),
                                        "source": f"https://bcasekuritas.co.id/en/latest-news/news/{slug}",
                                        # "category": "News",
                                        # "stock_code": stock_code
                                    })
                                break 

                    except json.JSONDecodeError:
                        continue
                
                if results: 
                    break 

        # DOM Fallback 
        if not results:
            LOGGER.info("JSON extraction failed. Falling back to DOM.")
            cards = driver.find_elements(By.CSS_SELECTOR, "div.bg-card")
            
            for card in cards:
                try:
                    try:
                        link_el = card.find_element(By.XPATH, "./..")
                        if link_el.tag_name != "a": link_el = card.find_element(By.TAG_NAME, "a")
                        link = link_el.get_attribute("href")
                    except: link = driver.current_url

                    title = card.find_element(By.TAG_NAME, "h6").get_attribute("textContent").strip()
                    date_text = card.find_element(By.CSS_SELECTOR, "span.text-xs").get_attribute("textContent").strip()
                    if "|" in date_text: date_text = date_text.split("|")[-1].strip()
                    
                    try:
                        dt = datetime.strptime(date_text, "%d %b %Y")
                        date_formatted = dt.strftime("%Y-%m-%d 00:00:00")
                    except:
                        date_formatted = date_text

                    results.append({
                        "title": title,
                        "timestamp": date_formatted,
                        "source": link,
                        # "category": "News",
                        # "stock_code": None
                    })
                except: continue

        # LOGGER.info(json.dumps(results, indent=2))
        LOGGER.info(f'Data parsed total: {len(results)}')
        return results

    except Exception as error:
        LOGGER.error(f"Fatal error: {error}")
        return []
    
    finally:
        try: driver.quit()
        except: pass


def run_scrape_bca_news(num_page: int) -> list[dict[str, any]]:
    all_articles = []

    for page in range(1, num_page + 1):
        article = scrape_bca(page)
        all_articles.extend(article)
        time.sleep(1.5)
    
    return all_articles


if __name__ == "__main__":
    result = scrape_bca(2)
    print(json.dumps(result, indent=2))
