import pandas as pd
import psycopg2
import re
from psycopg2.extras import execute_batch
import json
from rapidfuzz import fuzz

# =====================================================
# CONFIG
# =====================================================
EXCEL_FILE = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"

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
    """Normalize names for comparison."""
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
print("📄 Loading Excel file...")
df_excel = pd.read_excel(EXCEL_FILE)
df_excel = df_excel.where(pd.notnull(df_excel), None)
df_excel['normalized_name'] = df_excel['Global Property Name'].apply(normalize_name)

# =====================================================
# CONNECT TO DATABASE
# =====================================================
print("🔌 Connecting to database...")
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# =====================================================
# FETCH MASTERFILE DATA
# =====================================================
print("📥 Fetching MASTERFILE data...")
cur.execute("SELECT * FROM web_scraped_hotels WHERE source = 'MASTERFILE';")
master_cols = [desc[0] for desc in cur.description]
master_data = cur.fetchall()
df_master = pd.DataFrame(master_data, columns=master_cols)
df_master['normalized_name'] = df_master['name'].apply(normalize_name)

print(f"🔎 Loaded {len(df_master)} MASTERFILE rows")
print(f"🔎 Loaded {len(df_excel)} Excel rows")

# =====================================================
# ENHANCED MERGE — FUZZY MATCH + COUNTRY + CHAIN BOOST
# =====================================================
print("🤖 Performing fuzzy match (≥80% threshold)...")

excel_records = df_excel.to_dict(orient="records")
matched_rows = []
unmatched_rows = []

for _, master_row in df_master.iterrows():
    master_name = master_row['normalized_name']
    master_country = master_row.get('country_code', None)
    if not master_name:
        continue

    candidates = (
        [r for r in excel_records if r.get('Property Country Code') == master_country]
        if master_country else excel_records
    )

    best_match = None
    best_score = 0

    # Fuzzy name matching
    for excel_row in candidates:
        excel_name = excel_row['normalized_name']
        if not excel_name:
            continue

        score = fuzz.token_sort_ratio(master_name, excel_name)

        # Add bonus for HY or HI chain codes
        chain_code = excel_row.get('Global Chain Code')
        if chain_code in ('HY', 'HI'):
            score += 5

        if score > best_score:
            best_score = score
            best_match = excel_row

    if best_score >= 80 and best_match:
        merged = {**master_row.to_dict(), **{f"{k}_excel": v for k, v in best_match.items()}}
        merged["match_score"] = best_score
        matched_rows.append(merged)
    else:
        unmatched_rows.append(master_row.to_dict())

df_merged = pd.DataFrame(matched_rows)
print(f"✅ Fuzzy matched {len(df_merged)} / {len(df_master)} MASTERFILE records")

# =====================================================
# PREPARE RECORDS FOR INSERT
# =====================================================
records = []
for _, row in df_merged.iterrows():
    record = {
        "hotel_code": str(row.get('Global Property ID_excel')) if pd.notnull(row.get('Global Property ID_excel')) else row['hotel_code'],
        "chain_code": row.get('Global Chain Code_excel') if pd.notnull(row.get('Global Chain Code_excel')) else row['chain_code'],
        "chain": row['chain'],
        "name": row.get('Global Property Name_excel') if pd.notnull(row.get('Global Property Name_excel')) else row['name'],
        "state_code": row.get('Property State/Province_excel') if pd.notnull(row.get('Property State/Province_excel')) else row['state_code'],
        "state": row['state'],
        "country_code": row.get('Property Country Code_excel') if pd.notnull(row.get('Property Country Code_excel')) else row['country_code'],
        "country": row['country'],
        "city": row.get('Property City Name_excel') if pd.notnull(row.get('Property City Name_excel')) else row['city'],
        "postal_code": row.get('Property Zip/Postal_excel') if pd.notnull(row.get('Property Zip/Postal_excel')) else row['postal_code'],
        "address_line_1": row.get('Property Address 1_excel') if pd.notnull(row.get('Property Address 1_excel')) else row['address_line_1'],
        "address_line_2": row.get('Property Address 2_excel') if pd.notnull(row.get('Property Address 2_excel')) else row['address_line_2'],
        "full_address": row['full_address'],
        "latitude": safe_numeric(row.get('Property Latitude_excel')) if pd.notnull(row.get('Property Latitude_excel')) else safe_numeric(row['latitude']),
        "longitude": safe_numeric(row.get('Property Longitude_excel')) if pd.notnull(row.get('Property Longitude_excel')) else safe_numeric(row['longitude']),
        "primary_airport_code": row.get('Primary Airport Code_excel') if pd.notnull(row.get('Primary Airport Code_excel')) else row['primary_airport_code'],
        "property_quality_type": row['property_quality_type'],
        "property_style_description": row['description'] if pd.notnull(row.get('description')) else row['property_style_description'],
        "sabre_rating": safe_numeric(row.get('Sabre Property Rating_excel'), max_val=99.9) if pd.notnull(row.get('Sabre Property Rating_excel')) else safe_numeric(row.get('sabre_rating'), max_val=99.9),
        "sabre_context": row['sabre_context'],
        "parking": row['parking'],
        "links": row['links'],
        "phone_number": row.get('Property Phone Number_excel') if pd.notnull(row.get('Property Phone Number_excel')) else row['phone_number'],
        "fax_number": row.get('Property Fax Number_excel') if pd.notnull(row.get('Property Fax Number_excel')) else row['fax_number'],
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
        "source": 'CSL_EXCEL_SCRAPING_MAPPED' if pd.notnull(row.get('Global Property Name_excel')) else 'MASTERFILE',
        "created_at": row['created_at'],
        "updated_at": row['updated_at'],
        "last_updated": row['last_updated'],
        "description": row['description']
    }
    records.append(record)

# =====================================================
# CONVERT JSON-LIKE FIELDS TO STRINGS
# =====================================================
for record in records:
    for field in ['links', 'pet_fee_variations', 'pet_amenities']:
        if record.get(field) is not None:
            record[field] = json.dumps(record[field])

# =====================================================
# INSERT INTO DATABASE
# =====================================================
if records:
    insert_cols = list(records[0].keys())
    insert_sql = f"""
    INSERT INTO web_scraped_hotels ({', '.join(insert_cols)})
    VALUES ({', '.join([f'%({c})s' for c in insert_cols])})
    ON CONFLICT (hotel_code) DO UPDATE SET
    chain_code = EXCLUDED.chain_code,
    name = EXCLUDED.name,
    state_code = EXCLUDED.state_code,
    country_code = EXCLUDED.country_code,
    city = EXCLUDED.city,
    postal_code = EXCLUDED.postal_code,
    address_line_1 = EXCLUDED.address_line_1,
    address_line_2 = EXCLUDED.address_line_2,
    latitude = EXCLUDED.latitude,
    longitude = EXCLUDED.longitude,
    phone_number = EXCLUDED.phone_number,
    source = 'CSL_EXCEL_SCRAPING_MAPPED',
    updated_at = NOW();
    """

    print("💾 Inserting merged records into database...")
    execute_batch(cur, insert_sql, records, page_size=50)
    conn.commit()
    print(f"✅ SUCCESS: {len(records)} hotels inserted into web_scraped_hotels")
else:
    print("⚠️ No matched records found — nothing inserted.")

cur.close()
conn.close()











# import pandas as pd
# import psycopg2
# import re
# from psycopg2.extras import execute_batch
# import json

# # =====================================================
# # CONFIG
# # =====================================================
# EXCEL_FILE = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"

# DB_CONFIG = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "kruiz-dev",
#     "user": "postgres",
#     "password": "dost"
# }

# ALLOWED_COUNTRIES = {
#     "US","GB","DO","IT","MX","CN","DE","IN","VN","CA","SE","PA","AT","CR",
#     "CZ","FR","PH","PL","LT","PT","HU","GT","CO","CV","AW","MA","SK","PE",
#     "SV","KR","PR","KY","CL","QA","NL","AR","LC","TC","AO","OM","ES","MC",
#     "TH","BA","JO","BG"
# }

# CHAIN_STRICT = {"HY", "HI"}

# # =====================================================
# # HELPERS
# # =====================================================
# def normalize_name(name):
#     if not name:
#         return None
#     name = str(name).lower()
#     name = re.sub(r"[^a-z0-9 ]+", " ", name)
#     name = re.sub(r"\s+", " ", name)
#     return name.strip()

# def safe_numeric(v):
#     try:
#         return float(v) if pd.notnull(v) else None
#     except:
#         return None

# # =====================================================
# # LOAD EXCEL (FILTER COUNTRY FIRST)
# # =====================================================
# df_excel = pd.read_excel(EXCEL_FILE)
# df_excel = df_excel.where(pd.notnull(df_excel), None)

# df_excel = df_excel[df_excel["Property Country Code"].isin(ALLOWED_COUNTRIES)]
# df_excel["normalized_name"] = df_excel["Global Property Name"].apply(normalize_name)

# # =====================================================
# # LOAD MASTERFILE
# # =====================================================
# conn = psycopg2.connect(**DB_CONFIG)
# cur = conn.cursor()

# cur.execute("""
#     SELECT *
#     FROM web_scraped_hotels
#     WHERE source = 'MASTERFILE'
#       AND country_code = ANY(%s)
# """, (list(ALLOWED_COUNTRIES),))

# cols = [c[0] for c in cur.description]
# df_master = pd.DataFrame(cur.fetchall(), columns=cols)
# df_master["normalized_name"] = df_master["name"].apply(normalize_name)

# # =====================================================
# # MERGE (NAME + COUNTRY ONLY)
# # =====================================================
# df = pd.merge(
#     df_master,
#     df_excel,
#     how="inner",
#     left_on=["normalized_name", "country_code"],
#     right_on=["normalized_name", "Property Country Code"]
# )

# # =====================================================
# # CHAIN RULE (ONLY HY / HI)
# # =====================================================
# def valid_chain(row):
#     if row["chain_code"] in CHAIN_STRICT:
#         return row["chain_code"] == row["Global Chain Code"]
#     return True

# df = df[df.apply(valid_chain, axis=1)]

# # =====================================================
# # PREPARE UPSERT RECORDS (KEEP hotel_code)
# # =====================================================
# records = []

# for _, r in df.iterrows():
#     records.append({
#         "hotel_code": r["hotel_code"],  # KEEP MASTERFILE CODE
#         "name": r["Global Property Name"] or r["name"],
#         "chain_code": r["Global Chain Code"] or r["chain_code"],
#         "state_code": r["Property State/Province"] or r["state_code"],
#         "city": r["Property City Name"] or r["city"],
#         "postal_code": r["Property Zip/Postal"] or r["postal_code"],
#         "address_line_1": r["Property Address 1"] or r["address_line_1"],
#         "address_line_2": r["Property Address 2"] or r["address_line_2"],
#         "latitude": safe_numeric(r["Property Latitude"]) or safe_numeric(r["latitude"]),
#         "longitude": safe_numeric(r["Property Longitude"]) or safe_numeric(r["longitude"]),
#         "phone_number": r["Property Phone Number"] or r["phone_number"],
#         "is_verified": True,
#         "verification_type": "NAME_COUNTRY_MATCH",
#         "source": "MASTERFILE"
#     })

# # =====================================================
# # UPSERT (UPDATE EXISTING)
# # =====================================================
# sql = """
# INSERT INTO web_scraped_hotels (
#     hotel_code, name, chain_code, state_code, city,
#     postal_code, address_line_1, address_line_2,
#     latitude, longitude, phone_number,
#     is_verified, verification_type, source
# )
# VALUES (
#     %(hotel_code)s, %(name)s, %(chain_code)s, %(state_code)s, %(city)s,
#     %(postal_code)s, %(address_line_1)s, %(address_line_2)s,
#     %(latitude)s, %(longitude)s, %(phone_number)s,
#     %(is_verified)s, %(verification_type)s, %(source)s
# )
# ON CONFLICT (hotel_code) DO UPDATE SET
#     name = COALESCE(EXCLUDED.name, web_scraped_hotels.name),
#     chain_code = COALESCE(EXCLUDED.chain_code, web_scraped_hotels.chain_code),
#     state_code = COALESCE(EXCLUDED.state_code, web_scraped_hotels.state_code),
#     city = COALESCE(EXCLUDED.city, web_scraped_hotels.city),
#     postal_code = COALESCE(EXCLUDED.postal_code, web_scraped_hotels.postal_code),
#     address_line_1 = COALESCE(EXCLUDED.address_line_1, web_scraped_hotels.address_line_1),
#     address_line_2 = COALESCE(EXCLUDED.address_line_2, web_scraped_hotels.address_line_2),
#     latitude = COALESCE(EXCLUDED.latitude, web_scraped_hotels.latitude),
#     longitude = COALESCE(EXCLUDED.longitude, web_scraped_hotels.longitude),
#     phone_number = COALESCE(EXCLUDED.phone_number, web_scraped_hotels.phone_number),
#     is_verified = TRUE,
#     verification_type = 'NAME_COUNTRY_MATCH',
#     updated_at = NOW(),
#     last_updated = NOW();
# """

# execute_batch(cur, sql, records, page_size=100)
# conn.commit()
# cur.close()
# conn.close()

# print(f"✅ VERIFIED & UPDATED: {len(records)} MASTERFILE rows")
