"""TopCV Parser - Parse HTML to extract job data with multi-selector resilience"""
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
from collections import defaultdict

from bs4 import BeautifulSoup
import pandas as pd

from src.config.parser_config import SELECTORS, FIELD_VALIDATORS, EXTRACTION_STATS

logger = logging.getLogger(__name__)

# Global extraction stats
_stats = defaultdict(lambda: {"success": 0, "fail": 0})


def _record_stat(field: str, success: bool):
    """Record extraction success/fail for a field"""
    _stats[field]["success" if success else "fail"] += 1


def _get_rate(field: str) -> float:
    """Get extraction success rate for a field"""
    s = _stats[field]
    total = s["success"] + s["fail"]
    return s["success"] / total if total > 0 else 0.0


def get_extraction_stats() -> Dict[str, Any]:
    """Get current extraction statistics"""
    rates = {f: _get_rate(f) for f in _stats}
    threshold = EXTRACTION_STATS.get("warn_threshold", 0.7)
    warnings = [f"{f}: {r:.1%}" for f, r in rates.items() if r < threshold]
    return {"rates": rates, "warnings": warnings}


def reset_extraction_stats():
    """Reset extraction statistics"""
    global _stats
    _stats = defaultdict(lambda: {"success": 0, "fail": 0})


def _try_selectors(item, selectors: List[str]):
    """Try multiple selectors until one works"""
    for selector in selectors:
        try:
            elem = item.select_one(selector)
            if elem:
                return elem
        except Exception:
            continue
    return None


def _validate_field(field: str, value: Any) -> bool:
    """Validate a field value against rules"""
    if field not in FIELD_VALIDATORS:
        return True
    
    rules = FIELD_VALIDATORS[field]
    
    if rules.get("required") and not value:
        return False
    if not value:
        return True
    
    str_val = str(value)
    
    if "pattern" in rules and not re.match(rules["pattern"], str_val):
        return False
    if "min_length" in rules and len(str_val) < rules["min_length"]:
        return False
    if "max_length" in rules and len(str_val) > rules["max_length"]:
        return False
    
    return True


def _parse_last_update(text: str) -> int:
    """Convert Vietnamese time string to seconds"""
    if not text:
        return 0
    try:
        text = text.replace("Cập nhật", "").strip()
        match = re.search(r'(\d+)', text)
        if not match:
            return 0
        num = int(match.group(1))
        
        if "tháng" in text:
            return num * 30 * 24 * 3600
        elif "tuần" in text:
            return num * 7 * 24 * 3600
        elif "ngày" in text:
            return num * 24 * 3600
        elif "giờ" in text:
            return num * 3600
        elif "phút" in text:
            return num * 60
        return num
    except:
        return 0


def _extract_job(item) -> Optional[Dict[str, Any]]:
    """Extract job data from a job item element"""
    try:
        job = {
            'job_id': None, 'title': None, 'job_url': None,
            'company_name': None, 'company_url': None,
            'salary': None, 'skills': [], 'location': None,
            'deadline': None, 'verified_employer': False,
            'last_update': None, 'logo_url': None,
            'posted_time': None, 'crawled_at': datetime.now().isoformat()
        }
        
        # Job ID
        job['job_id'] = item.get(SELECTORS["job_id_attr"])
        if not job['job_id']:
            link = item.find('a', href=True)
            if link:
                match = re.search(r'-(\d+)\.html', link['href'])
                if match:
                    job['job_id'] = match.group(1)
        _record_stat('job_id', bool(job['job_id']))
        
        # Title
        elem = _try_selectors(item, SELECTORS["title"])
        if elem:
            job['title'] = elem.get(SELECTORS.get("title_attr")) or elem.get_text(strip=True)
        _record_stat('title', bool(job['title']))
        
        # Job URL
        elem = _try_selectors(item, SELECTORS["job_url"])
        if elem and elem.get('href'):
            href = elem['href']
            job['job_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
        _record_stat('job_url', bool(job['job_url']))
        
        # Company
        elem = _try_selectors(item, SELECTORS["company"])
        if elem:
            job['company_name'] = elem.get_text(strip=True)
            href = elem.get('href')
            if href:
                job['company_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
        _record_stat('company_name', bool(job['company_name']))
        
        # Location
        elem = _try_selectors(item, SELECTORS["location"])
        if elem:
            job['location'] = elem.get_text(strip=True)
        _record_stat('location', bool(job['location']))
        
        # Salary
        elem = _try_selectors(item, SELECTORS["salary"])
        if elem:
            job['salary'] = elem.get_text(strip=True)
        _record_stat('salary', bool(job['salary']))
        
        # Skills
        extra_attr = SELECTORS.get("skills_extra_attr", "data-original-title")
        for selector in SELECTORS["skills_container"]:
            elements = item.select(selector)
            if elements:
                for el in elements:
                    text = el.get_text(strip=True)
                    if text.endswith('+') and el.get(extra_attr):
                        extra = el[extra_attr]
                        if extra and not extra.startswith('<'):
                            job['skills'].extend([s.strip() for s in extra.split(',')])
                    elif text:
                        job['skills'].append(text)
                break
        _record_stat('skills', len(job['skills']) > 0)
        
        # Deadline
        elem = _try_selectors(item, SELECTORS["deadline"])
        if elem:
            job['deadline'] = elem.get_text(strip=True)
        _record_stat('deadline', bool(job['deadline']))
        
        # Verified
        for selector in SELECTORS["verified_badge"]:
            if item.select_one(selector):
                job['verified_employer'] = True
                break
        
        # Last update
        elem = _try_selectors(item, SELECTORS["last_update"])
        if elem:
            job['last_update'] = elem.get_text(strip=True)
        _record_stat('last_update', bool(job['last_update']))
        
        # Logo
        elem = _try_selectors(item, SELECTORS["logo"])
        if elem and elem.get('src'):
            job['logo_url'] = elem['src']
        _record_stat('logo_url', bool(job['logo_url']))
        
        # Posted time
        if job['last_update']:
            seconds = _parse_last_update(job['last_update'])
            job['posted_time'] = datetime.fromtimestamp(
                datetime.now().timestamp() - seconds
            ).isoformat()
        
        # Validate required fields
        if not _validate_field('job_id', job['job_id']) or not _validate_field('title', job['title']):
            return None
        
        return job
        
    except Exception as e:
        logger.error(f"Error extracting job: {e}")
        return None


def parse_html(html: str) -> List[Dict[str, Any]]:
    """Parse HTML content and extract all jobs"""
    soup = BeautifulSoup(html, 'html.parser')
    
    # Try multiple selectors for job container
    items = []
    for selector in SELECTORS["job_item"]:
        items = soup.select(selector)
        if items:
            break
    
    if not items:
        logger.warning("No job items found")
        return []
    
    jobs = []
    seen_ids = set()
    
    for item in items:
        job = _extract_job(item)
        if job and job['job_id'] not in seen_ids:
            seen_ids.add(job['job_id'])
            jobs.append(job)
    
    # Log stats
    if EXTRACTION_STATS.get("enabled"):
        stats = get_extraction_stats()
        if stats["warnings"]:
            logger.warning(f"Low extraction rates: {', '.join(stats['warnings'])}")
    
    logger.info(f"Parsed {len(jobs)} jobs from HTML")
    return jobs


def parse_html_file(filepath: Path) -> List[Dict[str, Any]]:
    """Parse HTML file and extract jobs"""
    return parse_html(filepath.read_text(encoding='utf-8'))


def jobs_to_dataframe(jobs: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert jobs list to DataFrame"""
    if not jobs:
        return pd.DataFrame()
    
    for job in jobs:
        if isinstance(job.get('skills'), list):
            job['skills'] = json.dumps(job['skills'])
    
    df = pd.DataFrame(jobs)
    df = df.drop_duplicates(subset=['job_id'])
    df = df.replace({'': None, 'None': None})
    
    columns = [
        'job_id', 'title', 'job_url', 'company_name', 'company_url',
        'salary', 'skills', 'location', 'deadline',
        'verified_employer', 'last_update', 'logo_url', 'posted_time', 'crawled_at'
    ]
    return df[[c for c in columns if c in df.columns]]
