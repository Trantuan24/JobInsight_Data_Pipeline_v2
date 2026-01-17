"""Crawler configuration - User agents, viewports, anti-detection"""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).parent.parent.parent
BACKUP_DIR = BASE_DIR / "data" / "raw_backup"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)

# Crawler settings
BASE_URL = "https://www.topcv.vn/viec-lam-it"
NUM_PAGES = int(os.getenv("CRAWLER_NUM_PAGES", "5"))
CONCURRENT_PAGES = int(os.getenv("CRAWLER_CONCURRENT_PAGES", "2"))
PAGE_TIMEOUT = 60000
SELECTOR_TIMEOUT = 20000
MIN_DELAY = 3
MAX_DELAY = 6

# Desktop User Agents
DESKTOP_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Edge/120.0.0.0",
]

# Mobile User Agents
MOBILE_AGENTS = [
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.144 Mobile Safari/537.36",
]

# Desktop Viewports
VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1440, "height": 900},
]

# Mobile Viewports
MOBILE_VIEWPORTS = [
    {"width": 390, "height": 844},
    {"width": 414, "height": 896},
    {"width": 375, "height": 812},
    {"width": 360, "height": 800},
]

# Anti-Detection Script
ANTI_DETECTION_SCRIPT = """
() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {},
    };
    
    if (!window.Notification) {
        window.Notification = { permission: 'denied' };
    }
    
    Object.defineProperty(navigator, 'plugins', {
        get: () => {
            const plugins = [];
            for (let i = 0; i < 5; i++) {
                plugins.push({
                    name: `Plugin ${i}`,
                    description: `Plugin description ${i}`,
                    filename: `plugin_${i}.dll`
                });
            }
            return plugins;
        }
    });
    
    Object.defineProperty(navigator, 'languages', {
        get: () => ['vi-VN', 'vi', 'en-US', 'en']
    });
    
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) return 'Intel Inc.';
        if (parameter === 37446) return 'Intel Iris OpenGL Engine';
        return getParameter.call(this, parameter);
    };
}
"""

# Captcha Detection Patterns
BLOCK_PATTERNS = [
    r'<div[^>]*class="[^"]*captcha[^"]*"',
    r'<form[^>]*captcha',
    r'access\s+denied',
    r'blocked\s+by',
    r'please\s+verify\s+you\s+are\s+human',
    r'unusual\s+traffic',
    r'suspicious\s+activity',
]
