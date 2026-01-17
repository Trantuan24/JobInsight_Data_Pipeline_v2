"""Data Quality configuration"""
import os

# DQ Thresholds (from env vars)
DQ_MIN_JOBS_COUNT = int(os.getenv("DQ_MIN_JOBS_COUNT", "50"))
DQ_MAX_DUPLICATE_RATE = float(os.getenv("DQ_MAX_DUPLICATE_RATE", "0.20"))
DQ_SUCCESS_THRESHOLD = float(os.getenv("DQ_SUCCESS_THRESHOLD", "0.90"))
DQ_WARNING_THRESHOLD = float(os.getenv("DQ_WARNING_THRESHOLD", "0.70"))

# Staging thresholds (stricter)
DQ_STAGING_SUCCESS_THRESHOLD = float(os.getenv("DQ_STAGING_SUCCESS_THRESHOLD", "0.95"))
DQ_STAGING_WARNING_THRESHOLD = float(os.getenv("DQ_STAGING_WARNING_THRESHOLD", "0.90"))
