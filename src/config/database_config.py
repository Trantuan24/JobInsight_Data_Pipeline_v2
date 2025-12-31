"""Database configuration"""
import os

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "postgres"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "user": os.getenv("DB_USER", "jobinsight"),
    "password": os.getenv("DB_PASSWORD", "jobinsight"),
    "database": os.getenv("DB_NAME", "jobinsight"),
}
