import pandas as pd
import psycopg2
import re
from psycopg2.extras import execute_batch
import json

# =====================================================
# CONFIG
# =====================================================
EXCEL_FILE = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"
SHEET_NAME = "Hilton Properties"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": "dost"
}

# =====================================================
# HELPERS
# =====================================================
def normalize_name(name: str) -> str:
    if not name:
        return None
    name = str(name)
    name = name.lower()
    name = re.sub(r"[^a-z0-9 ]+", " ", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()

def safe_numeric(value, max_val=None):
    """Convert to float and clip to max_val if provided."""
    if pd.isnull(value):
        return None
    try:
        value = float(value)
        if max_val is not None and value > max_val:
            return max_val
        return value
    except:
        return None

def safe_int(value, max_val=None):
    """Convert to int and clip to max_val if provided."""
    if pd.isnull(value):
        return None
    try:
        value = int(value)
        if max_val is not None and value > max_val:
            return max_val
        return value
    except:
        return None

# =====================================================
# LOAD EXCEL
# =====================================================
df_excel = pd.read_excel(EXCEL_FILE)
df_excel = df_excel.where(pd.notnull(df_excel), None)
df_excel['normalized_name'] = df_excel['Global Property Name'].apply(normalize_name)

# =====================================================
# CONNECT TO DB
# =====================================================
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# =====================================================
# FETCH HOTEL_MASTERFILE
# =====================================================
cur.execute("SELECT * FROM hotel_masterfile;")
master_cols = [desc[0] for desc in cur.description]
master_data = cur.fetchall()
df_master = pd.DataFrame(master_data, columns=master_cols)
df_master['normalized_name'] = df_master['name'].apply(normalize_name)

# =====================================================
# MERGE EXCEL + MASTER ON NORMALIZED NAME
# =====================================================
df_merged = pd.merge(
    df_master,
    df_excel,
    how='left',
    on='normalized_name',
    suffixes=('', '_excel')  # Excel columns get _excel suffix
)

# =====================================================
# PREPARE RECORDS FOR INSERT
# =====================================================
records = []
for _, row in df_merged.iterrows():
    record = {
        "hotel_code": str(row['Global Property ID']) if pd.notnull(row.get('Global Property ID')) else row['hotel_code'],
        "chain_code": row['Global Chain Code'] if pd.notnull(row.get('Global Chain Code')) else row['chain_code'],
        "chain": row['chain'],
        "name": row['Global Property Name'] if pd.notnull(row.get('Global Property Name')) else row['name'],
        "state_code": row['Property State/Province'] if pd.notnull(row.get('Property State/Province')) else row['state_code'],
        "state": row['state'],
        "country_code": row['Property Country Code'] if pd.notnull(row.get('Property Country Code')) else row['country_code'],
        "country": row['country'],
        "city": row['Property City Name'] if pd.notnull(row.get('Property City Name')) else row['city'],
        "postal_code": row['Property Zip/Postal'] if pd.notnull(row.get('Property Zip/Postal')) else row['postal_code'],
        "address_line_1": row['Property Address 1'] if pd.notnull(row.get('Property Address 1')) else row['address_line_1'],
        "address_line_2": row['Property Address 2'] if pd.notnull(row.get('Property Address 2')) else row['address_line_2'],
        "full_address": row['full_address'],
        "latitude": safe_numeric(row['Property Latitude']) if pd.notnull(row.get('Property Latitude')) else safe_numeric(row['latitude']),
        "longitude": safe_numeric(row['Property Longitude']) if pd.notnull(row.get('Property Longitude')) else safe_numeric(row['longitude']),
        "primary_airport_code": row['Primary Airport Code'] if pd.notnull(row.get('Primary Airport Code')) else row['primary_airport_code'],
        "property_quality_type": row['property_quality_type'],
        # description_master → property_style_description
        "property_style_description": row['description'] if pd.notnull(row.get('description')) else row['property_style_description'],
        "sabre_rating": safe_numeric(row['Sabre Property Rating'], max_val=99.9) if pd.notnull(row.get('Sabre Property Rating')) else safe_numeric(row['sabre_rating'], max_val=99.9),
        "sabre_context": row['sabre_context'],
        "parking": row['parking'],
        "links": row['links'],
        "phone_number": row['Property Phone Number'] if pd.notnull(row.get('Property Phone Number')) else row['phone_number'],
        "fax_number": row['Property Fax Number'] if pd.notnull(row.get('Property Fax Number')) else row['fax_number'],
        "is_verified": row['is_verified'],
        "verification_type": row['verification_type'],
        "is_pet_friendly": row['is_pet_friendly'],
        "pet_policy": row['pet_policy'],
        "service_animal_policy": row['service_animal_policy'],
        "pet_fee_night": safe_numeric(row['pet_fee_night']),
        "pet_fee_total_max": safe_numeric(row['pet_fee_total_max']),
        "pet_fee_deposit": safe_numeric(row['pet_fee_deposit']),
        "pet_fee_currency": row['pet_fee_currency'],
        "pet_fee_interval": row['pet_fee_interval'],
        "pet_fee_variations": row['pet_fee_variations'],
        "has_pet_deposit": row['has_pet_deposit'],
        "is_deposit_refundable": row['is_deposit_refundable'],
        "has_extra_fee_info": row['has_extra_fee_info'],
        "allowed_pet_types": row['allowed_pet_types'],
        "weight_limit": row['weight_limit'],
        "has_extra_weight_info": row['has_extra_weight_info'],
        "has_pet_friendly_rooms": row['has_pet_friendly_rooms'],
        "max_pets": safe_int(row['max_pets'], max_val=150),
        "has_max_pets_extra_info": row['has_max_pets_extra_info'],
        "breed_restrictions": row['breed_restrictions'],
        "pet_amenities": row['pet_amenities'],
        "has_pet_amenities": row['has_pet_amenities'],
        "nearby_parks": row['nearby_parks'],
        "parks_distance_miles": safe_numeric(row['parks_distance_miles'], max_val=999.99),
        "contact_note": row['contact_note'],
        "followup": row['followup'],
        "source": 'CSL_EXCEL' if pd.notnull(row.get('Global Property Name')) else 'MASTERFILE',
        "created_at": row['created_at'],
        "updated_at": row['updated_at'],
        "last_updated": row['last_updated'],
        "description": row['description']
    }
    records.append(record)

# =====================================================
# CONVERT JSON/DICT FIELDS TO STRING
# =====================================================
for record in records:
    for field in ['links', 'pet_fee_variations', 'pet_amenities']:
        if record.get(field) is not None:
            record[field] = json.dumps(record[field])

# =====================================================
# INSERT INTO WEB_SCRAPED_HOTELS
# =====================================================
insert_cols = list(records[0].keys())
insert_sql = f"""
INSERT INTO web_scraped_hotels ({', '.join(insert_cols)})
VALUES ({', '.join([f'%({c})s' for c in insert_cols])})
ON CONFLICT (hotel_code) DO NOTHING;
"""

execute_batch(cur, insert_sql, records, page_size=50)
conn.commit()
cur.close()
conn.close()

print(f"✅ SUCCESS: {len(records)} hotels inserted into web_scraped_hotels")
