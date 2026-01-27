from curl_cffi import requests
from goose3 import Goose

def check_advanced(url):
    print(f"Attempting to scrape: {url}")
    
   
    response = requests.get(
        url, 
        impersonate="chrome110", 
        timeout=30
    )
    
    # Check if we got through
    if response.status_code == 200:
        print("[SUCCESS] Bypass successful!")
        
        # Feed the raw HTML to Goose
        g = Goose()
        article = g.extract(url=url, raw_html=response.text)
        return (article.cleaned_text or "").strip()
    else:
        print(f"[FAILED] Status Code: {response.status_code}")
        return None

if __name__ == '__main__':
    test = 'https://jakartaglobe.id/business/indonesia-offers-new-retail-bonds-with-up-to-58-annual-returns'
    res = check_advanced(test)
    if res:
        print(res[:200] + "...") 
    else:
        print("Scraping failed.")