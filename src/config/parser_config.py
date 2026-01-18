"""Parser configuration - Selectors with fallbacks for resilience"""

# Multi-selector strategy: Primary selector first, then fallbacks
# Format: List of CSS selectors, tried in order until one works

SELECTORS = {
    # Job container
    "job_item": [
        "div.job-item-2",
        "div.job-item",
        "div[class*='job-item']",
        "article.job-listing",
    ],
    
    # Job ID - from data attribute or URL
    "job_id_attr": "data-job-id",
    
    # Title selectors
    "title": [
        "h3.title a span[data-original-title]",  # Primary: tooltip
        "h3.title a",                             # Fallback: link text
        "h3.title",                               # Fallback: h3 text
        ".job-title a",
        "[class*='title'] a",
    ],
    "title_attr": "data-original-title",  # Attribute to get title from
    
    # Job URL
    "job_url": [
        "h3.title a",
        ".job-title a",
        "a[href*='/viec-lam/']",
        "a[href*='/job/']",
    ],
    
    # Company
    "company": [
        "a.company",
        ".company-name a",
        "a[class*='company']",
        ".employer-name",
    ],
    
    # Location
    "location": [
        "label.address",
        ".job-address",
        ".location",
        "[class*='address']",
        "[class*='location']",
    ],
    
    # Salary
    "salary": [
        "label.title-salary",
        ".salary",
        "[class*='salary']",
        ".job-salary",
    ],
    
    # Skills
    "skills_container": [
        "div.skills label.item",
        ".skills .item",
        ".skill-tag",
        "[class*='skill']",
    ],
    "skills_extra_attr": "data-original-title",  # For "+N more" skills
    
    # Deadline
    "deadline": [
        "label.time strong",
        ".deadline strong",
        ".time strong",
        "[class*='deadline']",
    ],
    
    # Verified badge
    "verified_badge": [
        "span.vip-badge",
        ".verified-badge",
        "[class*='vip']",
        "[class*='verified']",
    ],
    
    # Last update
    "last_update": [
        "label.deadline",
        "span.time",
        ".update-time",
        "[class*='update']",
    ],
    
    # Logo
    "logo": [
        "a img",
        ".company-logo img",
        "img[class*='logo']",
    ],
}

# Field validation rules
FIELD_VALIDATORS = {
    "job_id": {
        "required": True,
        "pattern": r"^\d+$",  # Numeric ID
        "min_length": 1,
        "max_length": 20,
    },
    "title": {
        "required": True,
        "min_length": 3,
        "max_length": 500,
    },
    "job_url": {
        "required": False,
        "pattern": r"^https?://",
    },
    "company_name": {
        "required": False,
        "min_length": 1,
        "max_length": 300,
    },
    "salary": {
        "required": False,
        "min_length": 1,
    },
    "location": {
        "required": False,
        "min_length": 1,
    },
}

# Extraction stats tracking
EXTRACTION_STATS = {
    "enabled": True,
    "warn_threshold": 0.7,  # Warn if field success rate < 70%
}
