"""
DimLocation dimension processor.
"""

import logging
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# Provinces/Cities that were MERGED (sáp nhập) - need "(mới)" suffix
MERGED_PROVINCES = {
    'Tuyên Quang', 'Lào Cai', 'Thái Nguyên', 'Phú Thọ', 'Bắc Ninh', 
    'Hưng Yên', 'Hải Phòng', 'Ninh Bình', 'Quảng Trị', 'Đà Nẵng',
    'Quảng Ngãi', 'Gia Lai', 'Khánh Hòa', 'Lâm Đồng', 'Đắk Lắk',
    'Hồ Chí Minh', 'Đồng Nai', 'Tây Ninh', 'Cần Thơ', 'Vĩnh Long',
    'Đồng Tháp', 'Cà Mau', 'An Giang'
}

# Provinces/Cities that were NOT merged - keep original name
NOT_MERGED_PROVINCES = {
    'Hà Nội', 'Huế', 'Lai Châu', 'Điện Biên', 'Sơn La', 'Lạng Sơn',
    'Quảng Ninh', 'Thanh Hóa', 'Nghệ An', 'Hà Tĩnh', 'Cao Bằng'
}

# Known foreign countries
FOREIGN_COUNTRIES = [
    'Nước Ngoài', 'Nhật Bản', 'Hàn Quốc', 'Đài Loan', 'Singapore', 
    'Malaysia', 'Thái Lan', 'Trung Quốc', 'Mỹ', 'Úc', 'Đức', 'Pháp',
    'Anh', 'Canada', 'Japan', 'Korea', 'Taiwan', 'USA', 'Australia'
]


def normalize_city_name(city: str) -> str:
    """
    Normalize city name based on Vietnam administrative reform.
    """
    if not city:
        return city
    
    city = city.strip()
    
    has_moi = city.endswith('(mới)')
    base_name = city.replace(' (mới)', '').strip() if has_moi else city
    
    if base_name in NOT_MERGED_PROVINCES:
        return base_name
    
    if base_name in MERGED_PROVINCES:
        return f"{base_name} (mới)" if not has_moi else city
    
    return city


def parse_location(location_str: str) -> List[Tuple[str, str]]:
    """
    Parse location string from TopCV into list of (city, country) tuples.
    """
    if not location_str or str(location_str).lower() in ['', 'nan', 'none', 'null']:
        return []
    
    location_str = str(location_str).strip()
    
    if location_str in FOREIGN_COUNTRIES:
        return [('Unknown', location_str)]
    
    results = []
    parts = location_str.split(' & ')
    
    for part in parts:
        part = part.strip()
        
        if 'nơi khác' in part.lower():
            continue
        
        if not part or part.lower() in ['', 'nan', 'none']:
            continue
        
        if part in FOREIGN_COUNTRIES:
            results.append(('Unknown', part))
        else:
            normalized_city = normalize_city_name(part)
            results.append((normalized_city, 'Vietnam'))
    
    return results


def process_dim_location(
    conn: duckdb.DuckDBPyConnection,
    staging_df: pd.DataFrame
) -> Dict[str, int]:
    """
    Process DimLocation (no SCD2, simple INSERT) - batch processing.
    
    Unknown record (SK=-1) already created in schema.
    """
    stats = {'inserted': 0, 'unchanged': 0}
    
    if staging_df.empty:
        return stats
    
    # Collect all city-country pairs
    city_country_pairs = set()
    for loc in staging_df['location'].dropna():
        parsed = parse_location(loc)
        for city, country in parsed:
            city_country_pairs.add((city, country))
    
    if not city_country_pairs:
        return stats
    
    # Batch fetch all existing locations
    pairs_list = list(city_country_pairs)
    existing_rows = conn.execute("""
        SELECT city, country FROM DimLocation
    """).fetchall()
    existing_set = {(row[0], row[1]) for row in existing_rows}
    
    # Insert only new locations
    for city, country in pairs_list:
        if (city, country) not in existing_set:
            conn.execute("""
                INSERT INTO DimLocation (location_sk, city, country)
                VALUES (NEXTVAL('seq_dim_location_sk'), ?, ?)
            """, [city, country])
            stats['inserted'] += 1
        else:
            stats['unchanged'] += 1
    
    logger.info(f"DimLocation: inserted={stats['inserted']}, unchanged={stats['unchanged']}")
    return stats
