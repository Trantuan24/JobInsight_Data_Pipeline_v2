#!/bin/bash
set -e

echo "🚀 Initializing Superset..."

# Wait for database to be ready
sleep 5

# Initialize database
superset db upgrade

# Create admin user if not exists
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@jobinsight.local \
    --password admin || true

# Initialize Superset
superset init

echo "✅ Superset initialized successfully!"
echo "📊 Access Superset at http://localhost:8088"
echo "🔑 Login: admin / admin"
