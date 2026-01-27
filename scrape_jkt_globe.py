import cloudscraper
from goose3 import Goose

USER_AGENT = "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Mobile Safari/537.36"

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
    "x-test": "true",

}

def check(url):
    scraper = cloudscraper.create_scraper()
    scraper.trust_env = False  
    scraper.proxies = {}       
    
    response = scraper.get(url, headers=HEADERS, timeout=20)
    response.raise_for_status() 

    goose_extractor = Goose()
    article = goose_extractor.extract(url=url, raw_html=response.text)  

    return (article.cleaned_text or "").strip()

if __name__ == '__main__':
    test = 'https://jakartaglobe.id/business/indonesia-offers-new-retail-bonds-with-up-to-58-annual-returns'
    res = check(test)
    print(res)
