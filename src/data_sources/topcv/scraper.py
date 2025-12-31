"""TopCV Scraper - Crawl HTML pages using Playwright with anti-detection"""
import asyncio
import random
import re
import logging
import functools
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from playwright.async_api import async_playwright

from src.config.crawler_config import (
    BASE_URL, BACKUP_DIR, PAGE_TIMEOUT, SELECTOR_TIMEOUT,
    MIN_DELAY, MAX_DELAY, CONCURRENT_PAGES,
    DESKTOP_AGENTS, VIEWPORTS, MOBILE_AGENTS, MOBILE_VIEWPORTS,
    ANTI_DETECTION_SCRIPT, BLOCK_PATTERNS
)

logger = logging.getLogger(__name__)


def get_random_ua() -> tuple:
    """Get random user agent and viewport (80% desktop, 20% mobile)"""
    if random.random() < 0.8:
        return random.choice(DESKTOP_AGENTS), random.choice(VIEWPORTS)
    else:
        return random.choice(MOBILE_AGENTS), random.choice(MOBILE_VIEWPORTS)


def async_retry(max_tries=3, delay_seconds=1.0, backoff_factor=2.0, jitter=True):
    """Async retry decorator with exponential backoff and jitter"""
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            tries = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries >= max_tries:
                        logger.warning(f"{func.__name__} failed after {tries} tries: {e}")
                        raise
                    delay = delay_seconds * (backoff_factor ** (tries - 1))
                    if jitter:
                        delay = delay * (0.5 + random.random())
                    logger.info(f"Retry {tries}/{max_tries-1} for {func.__name__} after {delay:.2f}s: {e}")
                    await asyncio.sleep(delay)
        return wrapper
    return decorator


def detect_captcha(html: str) -> bool:
    """Check if page contains captcha/block"""
    if not html:
        return True
    if len(html) < 5000:
        logger.warning(f"HTML too short ({len(html)} bytes), possibly blocked")
        return True
    
    if "job-item-2" in html:
        job_count = html.count("job-item-2")
        if job_count >= 5:
            logger.info(f"Found {job_count} job items - page OK")
            return False
    
    for pattern in BLOCK_PATTERNS:
        if re.search(pattern, html, re.IGNORECASE):
            logger.warning(f"Block pattern detected: {pattern}")
            return True
    
    if "job-item-2" not in html:
        logger.warning("No job-item-2 found, possibly blocked")
        return True
    
    return False


async def apply_anti_detection(page):
    """Apply anti-bot detection techniques"""
    try:
        await page.add_init_script(ANTI_DETECTION_SCRIPT)
    except Exception as e:
        logger.error(f"Error applying anti-detection: {e}")


async def human_like_scroll(page):
    """Scroll page like a human would"""
    await page.evaluate("""
        () => {
            return new Promise((resolve) => {
                const scrollAmount = window.innerHeight / 2;
                const scrollSteps = 5;
                const scrollDelay = 300;
                let currentStep = 0;
                
                const scrollStep = () => {
                    if (currentStep >= scrollSteps) {
                        window.scrollTo(0, document.body.scrollHeight);
                        setTimeout(resolve, 1000);
                        return;
                    }
                    window.scrollBy(0, scrollAmount / scrollSteps);
                    currentStep += 1;
                    setTimeout(scrollStep, scrollDelay + Math.random() * 200);
                };
                scrollStep();
            });
        }
    """)


async def random_mouse_movement(page):
    """Simulate random mouse movements"""
    try:
        for _ in range(random.randint(2, 5)):
            x = random.randint(100, 800)
            y = random.randint(100, 600)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.1, 0.3))
    except:
        pass



@async_retry(max_tries=2, delay_seconds=5.0, backoff_factor=2.0)
async def scrape_page_impl(page_num: int, user_agent: str, viewport: dict) -> Dict[str, Any]:
    """Implementation of page scraping with retry decorator"""
    url = f"{BASE_URL}?page={page_num}" if page_num > 1 else BASE_URL
    
    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-infobars',
                    '--window-position=0,0',
                    '--ignore-certifcate-errors',
                    '--ignore-certifcate-errors-spki-list',
                ]
            )
            
            context = await browser.new_context(
                user_agent=user_agent,
                viewport=viewport,
                locale='vi-VN',
                timezone_id='Asia/Ho_Chi_Minh',
                java_script_enabled=True,
                bypass_csp=True,
            )
            
            await context.set_extra_http_headers({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Cache-Control': 'max-age=0',
            })
            
            page = await context.new_page()
            await apply_anti_detection(page)
            
            logger.info(f"Scraping page {page_num} with UA: {user_agent[:50]}...")
            
            await asyncio.sleep(random.uniform(1, 2))
            await page.goto(url, wait_until='domcontentloaded', timeout=PAGE_TIMEOUT)
            await asyncio.sleep(random.uniform(2, 4))
            await random_mouse_movement(page)
            
            try:
                await page.wait_for_selector('div.job-item-2', timeout=SELECTOR_TIMEOUT)
            except Exception as e:
                logger.warning(f"Selector not found on page {page_num}: {e}")
                html = await page.content()
                if detect_captcha(html):
                    raise Exception(f"Captcha/block detected on page {page_num}")
            
            await human_like_scroll(page)
            await asyncio.sleep(random.uniform(1, 2))
            
            html = await page.content()
            
            if detect_captcha(html):
                raise Exception(f"Captcha/block detected after scroll on page {page_num}")
            
            logger.info(f"Successfully scraped page {page_num} ({len(html)} bytes)")
            
            return {
                "success": True,
                "page": page_num,
                "html": html,
                "timestamp": datetime.now().isoformat(),
                "size": len(html)
            }
            
        finally:
            if browser:
                await browser.close()


async def scrape_page(page_num: int) -> Dict[str, Any]:
    """Scrape a single page with random delay and user agent"""
    if page_num > 1:
        pre_delay = random.uniform(1, 3)
        await asyncio.sleep(pre_delay)
    
    user_agent, viewport = get_random_ua()
    
    try:
        return await scrape_page_impl(page_num, user_agent, viewport)
    except Exception as e:
        logger.error(f"Failed to scrape page {page_num}: {e}")
        return {"success": False, "page": page_num, "error": str(e)}


async def scrape_pages(num_pages: int = 5, parallel: bool = True) -> List[Dict[str, Any]]:
    """Scrape multiple pages from TopCV"""
    logger.info(f"Starting scrape of {num_pages} pages (parallel={parallel})")
    
    if parallel:
        semaphore = asyncio.Semaphore(CONCURRENT_PAGES)
        
        async def scrape_with_limit(page_num):
            async with semaphore:
                return await scrape_page(page_num)
        
        tasks = [scrape_with_limit(i) for i in range(1, num_pages + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed = []
        for i, r in enumerate(results):
            if isinstance(r, Exception):
                processed.append({"success": False, "page": i + 1, "error": str(r)})
            else:
                processed.append(r)
        
        failed = sum(1 for r in processed if not r.get("success"))
        if failed >= 3 and num_pages >= 4:
            logger.warning(f"Circuit breaker: {failed}/{num_pages} failed. Pausing 60s...")
            await asyncio.sleep(60)
        
        return processed
    else:
        results = []
        for page_num in range(1, num_pages + 1):
            result = await scrape_page(page_num)
            results.append(result)
            
            if page_num < num_pages:
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                logger.info(f"Waiting {delay:.1f}s before next page...")
                await asyncio.sleep(delay)
        
        return results


def save_html_local(html: str, page_num: int) -> Path:
    """Save HTML to local backup directory"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    filename = BACKUP_DIR / f"it_p{page_num}_{timestamp}.html"
    filename.write_text(html, encoding='utf-8')
    logger.info(f"Saved HTML to {filename}")
    return filename
