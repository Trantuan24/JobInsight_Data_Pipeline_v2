"""TopCV Parser - Parse HTML to extract job data"""
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path

from bs4 import BeautifulSoup
import pandas as pd

logger = logging.getLogger(__name__)


def parse_last_update(text: str) -> int:
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


def extract_job(item) -> Optional[Dict[str, Any]]:
    """Extract job data from a job item element"""
    try:
        job = {
            'job_id': None,
            'title': None,
            'job_url': None,
            'company_name': None,
            'company_url': None,
            'salary': None,
            'skills': [],
            'location': None,
            'deadline': None,
            'verified_employer': False,
            'last_update': None,
            'logo_url': None,
            'posted_time': None,
            'crawled_at': datetime.now().isoformat()
        }
        
        # Job ID
        job['job_id'] = item.get('data-job-id')
        if not job['job_id']:
            link = item.find('a', href=True)
            if link:
                href = link['href'].split('?')[0]
                job['job_id'] = href.split('-')[-1].replace('.html', '')
        
        # Title
        title_span = item.select_one('h3.title a span[data-original-title]')
        if title_span:
            job['title'] = title_span['data-original-title'].strip()
        else:
            title_elem = item.find('h3', class_='title')
            if title_elem:
                job['title'] = title_elem.get_text(strip=True)
        
        # Job URL
        job_link = item.select_one('h3.title a')
        if job_link and job_link.get('href'):
            href = job_link['href']
            job['job_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
        
        # Company
        company = item.find('a', class_='company')
        if company:
            job['company_name'] = company.get_text(strip=True)
            href = company.get('href')
            if href:
                job['company_url'] = f"https://www.topcv.vn{href}" if href.startswith('/') else href
        
        # Location
        loc = item.select_one('label.address')
        if loc:
            job['location'] = loc.get_text(strip=True)
        
        # Salary
        salary = item.select_one('label.title-salary')
        if salary:
            job['salary'] = salary.get_text(strip=True)
        
        # Skills
        skills = []
        for skill in item.select('div.skills label.item'):
            text = skill.get_text(strip=True)
            if text.endswith('+') and skill.get('data-original-title'):
                extra = skill['data-original-title']
                if extra and not extra.startswith('<'):
                    skills.extend([s.strip() for s in extra.split(',')])
            else:
                skills.append(text)
        job['skills'] = skills
        
        # Deadline
        deadline = item.select_one('label.time strong')
        if deadline:
            job['deadline'] = deadline.get_text(strip=True)
        
        # Verified
        if item.select_one('span.vip-badge'):
            job['verified_employer'] = True
        
        # Last update
        update = item.select_one('label.deadline') or item.select_one('span.time')
        if update:
            job['last_update'] = update.get_text(strip=True)
        
        # Logo
        logo = item.select_one('a img')
        if logo and logo.get('src'):
            job['logo_url'] = logo['src']
        
        # Posted time
        if job['last_update']:
            seconds = parse_last_update(job['last_update'])
            job['posted_time'] = datetime.fromtimestamp(
                datetime.now().timestamp() - seconds
            ).isoformat()
        
        # Validate
        if not job['job_id'] or not job['title']:
            return None
        
        return job
        
    except Exception as e:
        logger.error(f"Error extracting job: {e}")
        return None



def parse_html(html: str) -> List[Dict[str, Any]]:
    """Parse HTML content and extract all jobs"""
    soup = BeautifulSoup(html, 'html.parser')
    items = soup.find_all('div', class_='job-item-2')
    
    jobs = []
    seen_ids = set()
    
    for item in items:
        job = extract_job(item)
        if job and job['job_id'] not in seen_ids:
            seen_ids.add(job['job_id'])
            jobs.append(job)
    
    logger.info(f"Parsed {len(jobs)} jobs from HTML")
    return jobs


def parse_html_file(filepath: Path) -> List[Dict[str, Any]]:
    """Parse HTML file and extract jobs"""
    html = filepath.read_text(encoding='utf-8')
    return parse_html(html)


def jobs_to_dataframe(jobs: List[Dict[str, Any]]) -> pd.DataFrame:
    """Convert jobs list to DataFrame"""
    if not jobs:
        return pd.DataFrame()
    
    for job in jobs:
        if isinstance(job.get('skills'), list):
            job['skills'] = json.dumps(job['skills'])
        for field in ['posted_time', 'crawled_at']:
            if not job.get(field):
                job[field] = None
    
    df = pd.DataFrame(jobs)
    df = df.drop_duplicates(subset=['job_id'])
    df = df.replace({'': None, 'None': None})
    
    columns = [
        'job_id', 'title', 'job_url', 'company_name', 'company_url',
        'salary', 'skills', 'location', 'deadline',
        'verified_employer', 'last_update', 'logo_url', 'posted_time', 'crawled_at'
    ]
    return df[[c for c in columns if c in df.columns]]
