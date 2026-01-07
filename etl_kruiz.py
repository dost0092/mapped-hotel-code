# import os
# import json
# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine, text
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv

# load_dotenv()

# # =====================================================
# # DATABASE CONNECTIONS
# # =====================================================
# LOCAL_DB = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "kruiz-dev",
#     "user": "postgres",
#     "password": os.getenv("LOCAL_DB_PASSWORD", "dost"),
# }

# GCP_DB = {
#     "host": os.getenv("DB_HOST"),
#     "port": int(os.getenv("DB_PORT", "5433")),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # =====================================================
# # SAFE VALUE UTILITIES
# # =====================================================
# NULL_STRINGS = {"", "nan", "NaN", "None", "null", "NULL"}

# def is_nullish(v):
#     if v is None or (isinstance(v, float) and pd.isna(v)):
#         return True
#     if isinstance(v, str) and v.strip() in NULL_STRINGS:
#         return True
#     return False

# def safe_text(v):
#     if is_nullish(v):
#         return None
#     return str(v).strip()

# def safe_float(v):
#     try:
#         if is_nullish(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         return float(v)
#     except Exception:
#         return None

# def safe_int(v, min_v=-2147483648, max_v=2147483647):
#     try:
#         if is_nullish(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         iv = int(float(v))
#         if iv < min_v or iv > max_v:
#             return None
#         return iv
#     except Exception:
#         return None

# def safe_smallint(v):
#     return safe_int(v, -32768, 32767)

# def safe_bigint(v):
#     try:
#         if is_nullish(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         iv = int(float(v))
#         if iv < -9223372036854775808 or iv > 9223372036854775807:
#             return None
#         return iv
#     except Exception:
#         return None

# def safe_bool(v):
#     if is_nullish(v):
#         return None
#     if isinstance(v, bool):
#         return v
#     if isinstance(v, (int, float)):
#         return bool(v)
#     if isinstance(v, str):
#         s = v.strip().lower()
#         if s in {"true", "1", "t", "yes", "y"}:
#             return True
#         if s in {"false", "0", "f", "no", "n"}:
#             return False
#     return None

# def safe_json_text(v):
#     # Return a JSON string (for TEXT/VARCHAR destination)
#     if is_nullish(v):
#         return None
#     if isinstance(v, (dict, list)):
#         return json.dumps(v, ensure_ascii=False)
#     s = str(v).strip()
#     if s in NULL_STRINGS:
#         return None
#     try:
#         json.loads(s)
#         return s
#     except Exception:
#         return json.dumps(s, ensure_ascii=False)

# def safe_json_native(v):
#     # Return Python dict/list or None (for JSON/JSONB destination)
#     if is_nullish(v):
#         return None
#     if isinstance(v, (dict, list)):
#         return v
#     s = str(v).strip()
#     if s in NULL_STRINGS:
#         return None
#     try:
#         return json.loads(s)
#     except Exception:
#         return None

# def extract_weight(v):
#     try:
#         if is_nullish(v):
#             return None
#         return float(str(v).split()[0])
#     except Exception:
#         return None

# def normalize_phone(v):
#     s = safe_text(v)
#     if not s:
#         return None
#     # Keep digits, +, spaces, -, parentheses
#     allowed = set("0123456789+ -()")
#     s = "".join(ch for ch in s if ch in allowed)
#     return s or None

# # =====================================================
# # FETCH TARGET SCHEMA FROM GCP
# # =====================================================
# def get_target_columns_and_types():
#     conn = psycopg2.connect(**GCP_DB)
#     cur = conn.cursor()
#     cur.execute("""
#         SELECT column_name, data_type, udt_name
#         FROM information_schema.columns
#         WHERE table_schema = 'ingestion'
#           AND table_name = 'web_scraped_hotel'
#         ORDER BY ordinal_position;
#     """)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     return rows  # list of (name, data_type, udt_name)

# # =====================================================
# # COERCION TO MATCH DESTINATION TYPES
# # =====================================================
# def coerce_df_to_schema(df, schema_cols):
#     json_like = {"links", "pet_amenities", "pet_fee_variations"}

#     for name, data_type, udt_name in schema_cols:
#         if name not in df.columns:
#             continue

#         # Normalize common null-like strings to None
#         df[name] = df[name].apply(lambda v: None if is_nullish(v) else v)

#         # Special case
#         if name == "phone_number":
#             df[name] = df[name].apply(normalize_phone)
#             continue

#         # JSON handling
#         if name in json_like:
#             if data_type in ("json", "jsonb"):
#                 df[name] = df[name].apply(safe_json_native)
#             else:
#                 df[name] = df[name].apply(safe_json_text)
#             continue

#         # Type-based coercion
#         if data_type in ("text", "character varying", "character"):
#             df[name] = df[name].apply(safe_text)
#         elif data_type == "boolean":
#             df[name] = df[name].apply(safe_bool)
#         elif data_type == "smallint":
#             df[name] = df[name].apply(safe_smallint)
#         elif data_type == "integer":
#             df[name] = df[name].apply(safe_int)
#         elif data_type == "bigint":
#             df[name] = df[name].apply(safe_bigint)
#         elif data_type in ("real", "double precision", "numeric", "decimal"):
#             df[name] = df[name].apply(safe_float)
#         elif data_type in ("timestamp without time zone", "timestamp with time zone", "date"):
#             df[name] = pd.to_datetime(df[name], errors="coerce")
#         else:
#             # default to text
#             df[name] = df[name].apply(safe_text)

#     return df

# def drop_out_of_range(df, schema_cols):
#     bad_indices = set()
#     for name, data_type, _ in schema_cols:
#         if name not in df.columns:
#             continue
#         if data_type == "smallint":
#             bad = df.index[df[name].apply(lambda x: x is not None and isinstance(x, int) and (x < -32768 or x > 32767))]
#             bad_indices.update(bad)
#         elif data_type == "integer":
#             bad = df.index[df[name].apply(lambda x: x is not None and isinstance(x, int) and (x < -2147483648 or x > 2147483647))]
#             bad_indices.update(bad)
#         elif data_type == "bigint":
#             bad = df.index[df[name].apply(lambda x: x is not None and isinstance(x, int) and (x < -9223372036854775808 or x > 9223372036854775807))]
#             bad_indices.update(bad)
#     if bad_indices:
#         print(f"⚠️ Dropping {len(bad_indices)} rows due to out-of-range integer values.")
#         df.drop(index=list(bad_indices), inplace=True)
#     return df

# # =====================================================
# # MAIN ETL LOGIC
# # =====================================================
# def run_etl():
#     print("🔌 Connecting to LOCAL DB via SQLAlchemy...")
#     engine = create_engine(
#         f"postgresql+psycopg2://{LOCAL_DB['user']}:{LOCAL_DB['password']}@{LOCAL_DB['host']}:{LOCAL_DB['port']}/{LOCAL_DB['dbname']}"
#     )
#     df = pd.read_sql(text("SELECT * FROM public.hotel_masterfile"), con=engine)
#     print(f"📦 {len(df)} rows loaded from local PostgreSQL")

#     # =====================================================
#     # GENERIC NORMALIZATION
#     # =====================================================
#     # Pre-clean common "nan"/"null" strings
#     for c in df.columns:
#         if df[c].dtype == object:
#             df[c] = df[c].apply(lambda v: None if is_nullish(v) else v)

#     # These will be re-coerced strictly later, but light normalization helps
#     if "weight_limit" in df.columns:
#         df["weight_limit"] = df["weight_limit"].apply(extract_weight)

#     # =====================================================
#     # MATCH WITH TARGET COLUMNS AND COERCE TO TYPES
#     # =====================================================
#     print("🧭 Syncing with GCP schema...")
#     schema_cols = get_target_columns_and_types()
#     target_cols = [name for name, _, _ in schema_cols]

#     # Align to target columns order, keep only existing columns
#     df = df[[c for c in target_cols if c in df.columns]]

#     # Coerce each column to the exact destination type
#     df = coerce_df_to_schema(df, schema_cols)

#     # Drop rows violating integer bounds (SMALLINT/INT/BIGINT)
#     df = drop_out_of_range(df, schema_cols)

#     # Final NaN -> None
#     df = df.where(pd.notnull(df), None)

#     # =====================================================
#     # WRITE TO GCP
#     # =====================================================
#     print("☁️ Connecting to GCP via Cloud SQL Proxy...")
#     tgt = psycopg2.connect(**GCP_DB)
#     cur = tgt.cursor()

#     placeholders = ",".join(["%s"] * len(df.columns))
#     insert_sql = f"""
#         INSERT INTO ingestion.web_scraped_hotel ({",".join(df.columns)})
#         VALUES ({placeholders});
#     """

#     print(f"🚀 Inserting {len(df)} cleaned rows into ingestion.web_scraped_hotel...")

#     try:
#         execute_batch(cur, insert_sql, df.values.tolist(), page_size=200)
#         tgt.commit()
#         print("✅ ETL completed successfully 🎉")
#     except psycopg2.Error as e:
#         tgt.rollback()
#         print("❌ Batch insert failed:", e)
#         print("\n🔎 Locating problematic row (row-by-row)...")
#         # Row-by-row to find the exact failure
#         bad_row_data = None
#         bad_error = None

#         for i, row in enumerate(df.values.tolist()):
#             try:
#                 cur.execute(insert_sql, row)
#             except psycopg2.Error as e2:
#                 bad_row_data = dict(zip(df.columns, row))
#                 bad_error = e2
#                 print(f"❌ Row {i} failed with error: {e2}")
#                 # Print diagnostics per column with types
#                 print("---- Problem Row (values and inferred Python types) ----")
#                 for k, v in bad_row_data.items():
#                     print(f"{k}: {repr(v)}  ({type(v).__name__})")
#                 break

#         if bad_row_data is not None:
#             # Extra: show destination types to compare
#             print("\n---- Destination Schema Types ----")
#             for name, data_type, udt_name in schema_cols:
#                 if name in bad_row_data:
#                     print(f"{name}: {data_type} ({udt_name})")
#         else:
#             print("No single bad row identified; check triggers/defaults on the target table.")
#         raise
#     finally:
#         try:
#             cur.close()
#             tgt.close()
#         except Exception:
#             pass

# # =====================================================
# if __name__ == "__main__":
#     run_etl()





# import os
# import json
# import psycopg2
# import pandas as pd
# import numpy as np
# from sqlalchemy import create_engine, text
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv

# load_dotenv()

# # =====================================================
# # DATABASE CONNECTIONS
# # =====================================================
# LOCAL_DB = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "kruiz-dev",
#     "user": "postgres",
#     "password": os.getenv("LOCAL_DB_PASSWORD", "dost"),
# }

# GCP_DB = {
#     "host": os.getenv("DB_HOST"),
#     "port": int(os.getenv("DB_PORT", "5433")),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # =====================================================
# # SAFE VALUE UTILITIES
# # =====================================================
# NULL_STRINGS = {"", "nan", "NaN", "None", "null", "NULL", "none"}

# def is_nullish(v):
#     if v is None:
#         return True
#     if isinstance(v, float) and pd.isna(v):
#         return True
#     if isinstance(v, str) and v.strip() in NULL_STRINGS:
#         return True
#     return False

# def safe_text(v):
#     if is_nullish(v):
#         return None
#     return str(v).strip()

# def safe_float(v):
#     try:
#         if is_nullish(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         return float(v)
#     except Exception:
#         return None

# def safe_int(v, min_v=-2147483648, max_v=2147483647):
#     try:
#         if is_nullish(v):
#             return None
#         if isinstance(v, float) and pd.isna(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         # Handle float strings like "1.0"
#         fv = float(v)
#         if pd.isna(fv):
#             return None
#         iv = int(fv)
#         if iv < min_v or iv > max_v:
#             return None
#         return iv
#     except Exception:
#         return None

# def safe_smallint(v):
#     return safe_int(v, -32768, 32767)

# def safe_bigint(v):
#     try:
#         if is_nullish(v):
#             return None
#         if isinstance(v, float) and pd.isna(v):
#             return None
#         v = str(v).replace(",", "").strip()
#         if v in NULL_STRINGS:
#             return None
#         fv = float(v)
#         if pd.isna(fv):
#             return None
#         iv = int(fv)
#         if iv < -9223372036854775808 or iv > 9223372036854775807:
#             return None
#         return iv
#     except Exception:
#         return None

# def safe_bool(v):
#     if is_nullish(v):
#         return None
#     if isinstance(v, bool):
#         return v
#     if isinstance(v, (int, float)):
#         if pd.isna(v):
#             return None
#         return bool(v)
#     if isinstance(v, str):
#         s = v.strip().lower()
#         if s in {"true", "1", "t", "yes", "y"}:
#             return True
#         if s in {"false", "0", "f", "no", "n"}:
#             return False
#     return None

# def safe_json_text(v):
#     # Return a JSON string (for TEXT/VARCHAR destination)
#     if is_nullish(v):
#         return None
#     if isinstance(v, (dict, list)):
#         return json.dumps(v, ensure_ascii=False)
#     s = str(v).strip()
#     if s in NULL_STRINGS:
#         return None
#     try:
#         json.loads(s)
#         return s
#     except Exception:
#         return json.dumps(s, ensure_ascii=False)

# def safe_json_native(v):
#     # Return Python dict/list or None (for JSON/JSONB destination)
#     if is_nullish(v):
#         return None
#     if isinstance(v, (dict, list)):
#         return v
#     s = str(v).strip()
#     if s in NULL_STRINGS:
#         return None
#     try:
#         return json.loads(s)
#     except Exception:
#         return None

# def extract_weight(v):
#     try:
#         if is_nullish(v):
#             return None
#         if isinstance(v, float) and pd.isna(v):
#             return None
#         return float(str(v).split()[0])
#     except Exception:
#         return None

# def normalize_phone(v):
#     s = safe_text(v)
#     if not s:
#         return None
#     # Keep digits, +, spaces, -, parentheses
#     allowed = set("0123456789+ -()")
#     s = "".join(ch for ch in s if ch in allowed)
#     return s or None

# # =====================================================
# # FETCH TARGET SCHEMA FROM GCP
# # =====================================================
# def get_target_columns_and_types():
#     conn = psycopg2.connect(**GCP_DB)
#     cur = conn.cursor()
#     cur.execute("""
#         SELECT column_name, data_type, udt_name
#         FROM information_schema.columns
#         WHERE table_schema = 'ingestion'
#           AND table_name = 'web_scraped_hotel'
#         ORDER BY ordinal_position;
#     """)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()
#     return rows  # list of (name, data_type, udt_name)

# # =====================================================
# # COERCION TO MATCH DESTINATION TYPES
# # =====================================================
# def coerce_df_to_schema(df, schema_cols):
#     json_like = {"links", "pet_amenities", "pet_fee_variations"}
    
#     # Convert all numpy NaN to None first
#     df = df.replace({np.nan: None})
    
#     for name, data_type, udt_name in schema_cols:
#         if name not in df.columns:
#             continue

#         # Skip if all values are already None
#         if df[name].isna().all():
#             df[name] = None
#             continue

#         # Special case
#         if name == "phone_number":
#             df[name] = df[name].apply(normalize_phone)
#             continue

#         # JSON handling
#         if name in json_like:
#             if data_type in ("json", "jsonb"):
#                 df[name] = df[name].apply(safe_json_native)
#             else:
#                 df[name] = df[name].apply(safe_json_text)
#             continue

#         # Type-based coercion
#         if data_type in ("text", "character varying", "character"):
#             df[name] = df[name].apply(safe_text)
#         elif data_type == "boolean":
#             df[name] = df[name].apply(safe_bool)
#         elif data_type == "smallint":
#             df[name] = df[name].apply(safe_smallint)
#         elif data_type == "integer":
#             df[name] = df[name].apply(safe_int)
#         elif data_type == "bigint":
#             df[name] = df[name].apply(safe_bigint)
#         elif data_type in ("real", "double precision", "numeric", "decimal"):
#             df[name] = df[name].apply(safe_float)
#         elif data_type in ("timestamp without time zone", "timestamp with time zone", "date"):
#             # Handle datetime conversion
#             if df[name].dtype == 'object':
#                 df[name] = pd.to_datetime(df[name], errors="coerce")
#         else:
#             # default to text
#             df[name] = df[name].apply(safe_text)

#     return df

# def validate_integer_bounds(df, schema_cols):
#     """Validate integer bounds and return filtered DataFrame"""
#     bad_indices = set()
    
#     for name, data_type, _ in schema_cols:
#         if name not in df.columns:
#             continue
            
#         if data_type == "smallint":
#             mask = df[name].apply(lambda x: x is not None and isinstance(x, (int, float)) and not pd.isna(x) and (x < -32768 or x > 32767))
#             bad = df.index[mask]
#             if len(bad) > 0:
#                 print(f"⚠️ Column '{name}': {len(bad)} rows with out-of-range smallint values")
#                 bad_indices.update(bad)
                
#         elif data_type == "integer":
#             mask = df[name].apply(lambda x: x is not None and isinstance(x, (int, float)) and not pd.isna(x) and (x < -2147483648 or x > 2147483647))
#             bad = df.index[mask]
#             if len(bad) > 0:
#                 print(f"⚠️ Column '{name}': {len(bad)} rows with out-of-range integer values")
#                 bad_indices.update(bad)
                
#         elif data_type == "bigint":
#             mask = df[name].apply(lambda x: x is not None and isinstance(x, (int, float)) and not pd.isna(x) and (x < -9223372036854775808 or x > 9223372036854775807))
#             bad = df.index[mask]
#             if len(bad) > 0:
#                 print(f"⚠️ Column '{name}': {len(bad)} rows with out-of-range bigint values")
#                 bad_indices.update(bad)
    
#     if bad_indices:
#         print(f"⚠️ Dropping {len(bad_indices)} rows due to out-of-range integer values.")
#         df_filtered = df.drop(index=list(bad_indices)).copy()
#         return df_filtered
    
#     return df

# def debug_dataframe(df, schema_cols, sample_rows=5):
#     """Debug function to check DataFrame types and values"""
#     print("\n🔍 DEBUG: DataFrame Info")
#     print(f"Total rows: {len(df)}")
#     print(f"Columns: {len(df.columns)}")
    
#     print("\n📋 First few rows with types:")
#     for idx, row in df.head(sample_rows).iterrows():
#         print(f"\nRow {idx}:")
#         for name, data_type, _ in schema_cols:
#             if name in df.columns:
#                 val = row[name]
#                 print(f"  {name}: {repr(val)} (Python type: {type(val).__name__})")
    
#     print("\n📊 Column dtypes:")
#     for col in df.columns:
#         print(f"  {col}: {df[col].dtype}")
        
#     # Check for problematic columns
#     print("\n🔍 Checking for problematic values:")
#     for name, data_type, _ in schema_cols:
#         if name not in df.columns:
#             continue
            
#         if data_type in ("integer", "smallint", "bigint"):
#             non_null = df[name].dropna()
#             if len(non_null) > 0:
#                 sample_vals = non_null.head(3).tolist()
#                 print(f"  {name} ({data_type}): Sample values: {sample_vals}")
                
#                 # Check for float values in integer columns
#                 float_vals = df[name].apply(lambda x: isinstance(x, float) and not pd.isna(x))
#                 if float_vals.any():
#                     print(f"    WARNING: Contains float values: {df[name][float_vals].head(3).tolist()}")

# # =====================================================
# # MAIN ETL LOGIC
# # =====================================================
# def run_etl():
#     print("🔌 Connecting to LOCAL DB via SQLAlchemy...")
#     engine = create_engine(
#         f"postgresql+psycopg2://{LOCAL_DB['user']}:{LOCAL_DB['password']}@{LOCAL_DB['host']}:{LOCAL_DB['port']}/{LOCAL_DB['dbname']}"
#     )
#     df = pd.read_sql(text("SELECT * FROM public.hotel_masterfile"), con=engine)
#     print(f"📦 {len(df)} rows loaded from local PostgreSQL")

#     # =====================================================
#     # GENERIC NORMALIZATION
#     # =====================================================
#     # Pre-clean common "nan"/"null" strings
#     for c in df.columns:
#         if df[c].dtype == object:
#             df[c] = df[c].apply(lambda v: None if is_nullish(v) else v)
    
#     # Replace numpy NaN with None
#     df = df.replace({np.nan: None})

#     # These will be re-coerced strictly later, but light normalization helps
#     if "weight_limit" in df.columns:
#         df["weight_limit"] = df["weight_limit"].apply(extract_weight)

#     # =====================================================
#     # MATCH WITH TARGET COLUMNS AND COERCE TO TYPES
#     # =====================================================
#     print("🧭 Syncing with GCP schema...")
#     schema_cols = get_target_columns_and_types()
#     target_cols = [name for name, _, _ in schema_cols]

#     # Align to target columns order, keep only existing columns
#     existing_cols = [c for c in target_cols if c in df.columns]
#     missing_cols = [c for c in target_cols if c not in df.columns]
    
#     if missing_cols:
#         print(f"⚠️ Missing columns in source data: {missing_cols}")
    
#     df = df[existing_cols]

#     # Coerce each column to the exact destination type
#     df = coerce_df_to_schema(df, schema_cols)

#     # Validate integer bounds and filter
#     df = validate_integer_bounds(df, schema_cols)
    
#     # Final NaN -> None conversion
#     df = df.replace({np.nan: None})
    
#     # Debug output
#     debug_dataframe(df, schema_cols)

#     # =====================================================
#     # WRITE TO GCP
#     # =====================================================
#     print("☁️ Connecting to GCP via Cloud SQL Proxy...")
#     tgt = psycopg2.connect(**GCP_DB)
#     cur = tgt.cursor()

#     # Prepare insert statement
#     placeholders = ",".join(["%s"] * len(df.columns))
#     insert_sql = f"""
#         INSERT INTO ingestion.web_scraped_hotel ({",".join(df.columns)})
#         VALUES ({placeholders});
#     """

#     print(f"🚀 Inserting {len(df)} cleaned rows into ingestion.web_scraped_hotel...")

#     try:
#         # Convert DataFrame to list of tuples
#         data_tuples = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
#         execute_batch(cur, insert_sql, data_tuples, page_size=200)
#         tgt.commit()
#         print(f"✅ Successfully inserted {len(df)} rows 🎉")
#     except psycopg2.Error as e:
#         tgt.rollback()
#         print(f"❌ Batch insert failed: {e}")
        
#         # Row-by-row debugging
#         print("\n🔎 Locating problematic row (row-by-row)...")
        
#         for i, row_tuple in enumerate(data_tuples):
#             try:
#                 cur.execute(insert_sql, row_tuple)
#                 tgt.rollback()  # Rollback after each successful test
#             except psycopg2.Error as e2:
#                 print(f"\n❌ Row {i} failed with error: {e2}")
                
#                 # Create dict for better display
#                 row_dict = dict(zip(df.columns, row_tuple))
                
#                 print("---- Problem Row (values and inferred Python types) ----")
#                 for k, v in row_dict.items():
#                     print(f"{k}: {repr(v)}  ({type(v).__name__})")
                
#                 print("\n---- Destination Schema Types ----")
#                 for name, data_type, udt_name in schema_cols:
#                     if name in row_dict:
#                         print(f"{name}: {data_type} ({udt_name})")
                
#                 # Additional diagnostics
#                 print("\n---- Data Type Mismatches ----")
#                 for name, data_type, udt_name in schema_cols:
#                     if name in row_dict:
#                         val = row_dict[name]
#                         expected_type = data_type
#                         actual_type = type(val).__name__
                        
#                         if data_type in ("integer", "smallint", "bigint") and isinstance(val, float):
#                             print(f"❌ {name}: Expected {expected_type}, got float {val}")
#                         elif val is not None:
#                             print(f"  {name}: Expected {expected_type}, got {actual_type}")
                
#                 break
        
#         raise
#     finally:
#         try:
#             cur.close()
#             tgt.close()
#         except Exception:
#             pass

# # =====================================================
# if __name__ == "__main__":
#     run_etl()




# import os
# import json
# import psycopg2
# import pandas as pd
# from sqlalchemy import create_engine, text
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv

# load_dotenv()



# INT4_MIN = -2147483648
# INT4_MAX = 2147483647
# # =====================================================
# # DATABASE CONNECTIONS
# # =====================================================
# LOCAL_DB = {
#     "host": "localhost",
#     "port": 5432,
#     "dbname": "kruiz-dev",
#     "user": "postgres",
#     "password": os.getenv("LOCAL_DB_PASSWORD", "dost"),
# }

# GCP_DB = {
#     "host": os.getenv("DB_HOST"),
#     "port": int(os.getenv("DB_PORT", "5433")),
#     "dbname": os.getenv("DB_NAME"),
#     "user": os.getenv("DB_USER"),
#     "password": os.getenv("DB_PASSWORD"),
# }

# # =====================================================
# # SAFE UTILITIES
# # =====================================================
# NULL_STRINGS = {"", "nan", "NaN", "None", "null", "NULL"}



# INT4_MIN = -2147483648
# INT4_MAX = 2147483647

# def safe_int4(v):
#     try:
#         if v is None or (isinstance(v, float) and pd.isna(v)):
#             return None
#         iv = int(float(v))
#         if iv < INT4_MIN or iv > INT4_MAX:
#             return None
#         return iv
#     except Exception:
#         return None


# def is_nullish(v):
#     return (
#         v is None
#         or (isinstance(v, float) and pd.isna(v))
#         or (isinstance(v, str) and v.strip() in NULL_STRINGS)
#     )

# def safe_text(v):
#     return None if is_nullish(v) else str(v).strip()

# def safe_float(v):
#     try:
#         return None if is_nullish(v) else float(str(v).replace(",", ""))
#     except Exception:
#         return None

# def safe_int(v):
#     try:
#         if is_nullish(v):
#             return None
#         iv = int(float(v))
#         return iv if -2147483648 <= iv <= 2147483647 else None
#     except Exception:
#         return None

# def safe_bool(v):
#     if is_nullish(v):
#         return None
#     if isinstance(v, bool):
#         return v
#     if isinstance(v, (int, float)):
#         return bool(v)
#     if isinstance(v, str):
#         return v.strip().lower() in {"true", "1", "yes", "y"}
#     return None

# def safe_json_text(v):
#     if is_nullish(v):
#         return None
#     if isinstance(v, (dict, list)):
#         return json.dumps(v, ensure_ascii=False)
#     try:
#         json.loads(v)
#         return v
#     except Exception:
#         return json.dumps(str(v), ensure_ascii=False)

# def normalize_phone(v):
#     v = safe_text(v)
#     if not v:
#         return None
#     return "".join(c for c in v if c in "0123456789+ -()")

# # =====================================================
# # TARGET SCHEMA
# # =====================================================
# def get_target_schema():
#     conn = psycopg2.connect(**GCP_DB)
#     cur = conn.cursor()
#     cur.execute("""
#         SELECT column_name, data_type
#         FROM information_schema.columns
#         WHERE table_schema='ingestion'
#           AND table_name='web_scraped_hotel'
#         ORDER BY ordinal_position;
#     """)
#     rows = cur.fetchall()
#     cur.close()
#     conn.close()

#     # 🚨 REMOVE ID — let Postgres generate it
#     return [(n, t) for n, t in rows if n != "id"]

# # =====================================================
# # COERCION
# # =====================================================
# def coerce(df, schema):
#     json_cols = {"links", "pet_amenities", "pet_fee_variations"}

#     for col, dtype in schema:
#         if col not in df.columns:
#             continue

#         if col == "phone_number":
#             df[col] = df[col].apply(normalize_phone)
#             continue

#         if col in json_cols:
#             df[col] = df[col].apply(safe_json_text)
#             continue

#         if dtype in ("text", "character varying"):
#             df[col] = df[col].apply(safe_text)
#         elif dtype == "boolean":
#             df[col] = df[col].apply(safe_bool)
#         elif dtype == "integer":
#             if col == "max_pets":
#                 df[col] = df[col].apply(safe_int4).astype("Int64")
#             else:
#                 df[col] = df[col].apply(safe_int4)
#         elif dtype in ("double precision", "numeric"):
#             df[col] = df[col].apply(safe_float)
#         elif "timestamp" in dtype:
#             df[col] = pd.to_datetime(df[col], errors="coerce")

#     return df.where(pd.notnull(df), None)
# def detect_int_overflows(df):
#     offenders = df[
#         df["max_pets"].notna() &
#         ((df["max_pets"] < INT4_MIN) | (df["max_pets"] > INT4_MAX))
#     ]

#     if not offenders.empty:
#         print("🚨 INT4 OVERFLOW DETECTED IN max_pets")
#         print(offenders[["hotel_code", "max_pets"]].head(10))
#         df.loc[offenders.index, "max_pets"] = None

#     return df

# # =====================================================
# # MAIN ETL
# # =====================================================
# def run_etl():
#     print("🔌 Loading local data...")
#     engine = create_engine(
#         f"postgresql+psycopg2://{LOCAL_DB['user']}:{LOCAL_DB['password']}@"
#         f"{LOCAL_DB['host']}:{LOCAL_DB['port']}/{LOCAL_DB['dbname']}"
#     )

#     # for updation
#     query = """
#     SELECT *
#     FROM public.hotel_masterfile
#     WHERE last_updated > '2025-12-31 12:55:19.530863'
#     AND chain_code = 'HILTON'
#     """
#     df = pd.read_sql(query, engine)


#     # for all insertion 
#     # df = pd.read_sql("SELECT * FROM public.hotel_masterfile", engine) 
#     print(f"📦 Loaded {len(df)} rows")

#     print("🧭 Fetching target schema...")
#     schema = get_target_schema()
#     target_cols = [c for c, _ in schema]

#     df = df[[c for c in target_cols if c in df.columns]]
#     df = coerce(df, schema)
#     df = detect_int_overflows(df)
#     df = df.astype(object).where(pd.notna(df), None)
#     print("☁️ Inserting into GCP...")
#     conn = psycopg2.connect(**GCP_DB)
#     cur = conn.cursor()

#     cols = ",".join(df.columns)
#     placeholders = ",".join(["%s"] * len(df.columns))
#     sql = f"INSERT INTO ingestion.web_scraped_hotel ({cols}) VALUES ({placeholders})"

#     try:
#         execute_batch(cur, sql, df.values.tolist(), page_size=500)
#         conn.commit()
#         print("✅ ETL completed successfully 🎉")
#     except Exception as e:
#         conn.rollback()
#         print("❌ Insert failed:", e)
#         raise
#     finally:
#         cur.close()
#         conn.close()

# # =====================================================
# if __name__ == "__main__":
#     run_etl()







import os
import json
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

INT4_MIN = -2147483648
INT4_MAX = 2147483647
NULL_STRINGS = {"", "nan", "NaN", "None", "null", "NULL"}

# =====================================================
# DATABASE CONNECTIONS
# =====================================================
LOCAL_DB = {
    "host": "localhost",
    "port": 5432,
    "dbname": "kruiz-dev",
    "user": "postgres",
    "password": os.getenv("LOCAL_DB_PASSWORD", "dost"),
}

GCP_DB = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", "5433")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
}

# =====================================================
# SAFE UTILITIES
# =====================================================
def is_nullish(v):
    return v is None or (isinstance(v, float) and pd.isna(v)) or (isinstance(v, str) and v.strip() in NULL_STRINGS)

def safe_text(v):
    return None if is_nullish(v) else str(v).strip()

def safe_float(v):
    try:
        return None if is_nullish(v) else float(str(v).replace(",", ""))
    except Exception:
        return None

def safe_int4(v):
    try:
        if is_nullish(v):
            return None
        iv = int(float(v))
        return iv if INT4_MIN <= iv <= INT4_MAX else None
    except Exception:
        return None

def safe_bool(v):
    if is_nullish(v):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"true", "1", "yes", "y"}
    return None

def safe_json_text(v):
    if is_nullish(v):
        return None
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    try:
        json.loads(v)
        return v
    except Exception:
        return json.dumps(str(v), ensure_ascii=False)

def normalize_phone(v):
    v = safe_text(v)
    if not v:
        return None
    return "".join(c for c in v if c in "0123456789+ -()")

# =====================================================
# FETCH TARGET SCHEMA
# =====================================================
def get_target_schema():
    conn = psycopg2.connect(**GCP_DB)
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema='ingestion'
          AND table_name='web_scraped_hotel'
        ORDER BY ordinal_position;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    # exclude id
    return [(n, t) for n, t in rows if n != "id"]

# =====================================================
# COERCION
# =====================================================
def coerce(df, schema):
    json_cols = {"links", "pet_amenities", "pet_fee_variations"}
    for col, dtype in schema:
        if col not in df.columns:
            continue
        if col == "phone_number":
            df[col] = df[col].apply(normalize_phone)
        elif col in json_cols:
            df[col] = df[col].apply(safe_json_text)
        elif dtype in ("text", "character varying"):
            df[col] = df[col].apply(safe_text)
        elif dtype == "boolean":
            df[col] = df[col].apply(safe_bool)
        elif dtype == "integer":
            if col == "max_pets":
                df[col] = df[col].apply(safe_int4).astype("Int64")
            else:
                df[col] = df[col].apply(safe_int4)
        elif dtype in ("double precision", "numeric"):
            df[col] = df[col].apply(safe_float)
        elif "timestamp" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df.where(pd.notnull(df), None)

def detect_int_overflows(df):
    offenders = df[df["max_pets"].notna() & ((df["max_pets"] < INT4_MIN) | (df["max_pets"] > INT4_MAX))]
    if not offenders.empty:
        print("🚨 INT4 OVERFLOW DETECTED IN max_pets")
        print(offenders[["hotel_code", "max_pets"]].head(10))
        df.loc[offenders.index, "max_pets"] = None
    return df

# =====================================================
# MAIN ETL
# =====================================================
def run_etl():
    print("🔌 Loading local web_scraped_hotels...")
    engine = create_engine(
        f"postgresql+psycopg2://{LOCAL_DB['user']}:{LOCAL_DB['password']}@"
        f"{LOCAL_DB['host']}:{LOCAL_DB['port']}/{LOCAL_DB['dbname']}"
    )

    df = pd.read_sql(
        """SELECT * FROM public.web_scraped_hotels """,
        engine
    )
    print(f"📦 Loaded {len(df)} rows from local web_scraped_hotels")

    print("🧭 Fetching target schema...")
    schema = get_target_schema()
    target_cols = [c for c, _ in schema]

    df = df[[c for c in target_cols if c in df.columns]]
    df = coerce(df, schema)
    df = detect_int_overflows(df)
    df = df.astype(object).where(pd.notna(df), None)

    print("⚠️ Truncating existing GCP ingestion.web_scraped_hotel table...")
    conn = psycopg2.connect(**GCP_DB)
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE ingestion.web_scraped_hotel;")
    conn.commit()

    print("☁️ Inserting new data into GCP...")
    cols = ",".join(df.columns)
    placeholders = ",".join(["%s"] * len(df.columns))
    sql = f"INSERT INTO ingestion.web_scraped_hotel ({cols}) VALUES ({placeholders})"

    try:
        execute_batch(cur, sql, df.values.tolist(), page_size=500)
        conn.commit()
        print(f"✅ ETL completed successfully! Inserted {len(df)} hotels 🎉")
    except Exception as e:
        conn.rollback()
        print("❌ Insert failed:", e)
        raise
    finally:
        cur.close()
        conn.close()

# =====================================================
if __name__ == "__main__":
    run_etl()
