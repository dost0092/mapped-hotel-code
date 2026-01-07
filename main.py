import pandas as pd

# =====================================================
# CONFIG
# =====================================================
INPUT_FILE = "USE THIS - All CSL Properties with Global Ids and GDS Ids (Active)_Jul2025_2 2 - excel.xlsx"
OUTPUT_FILE = "Hilton_Properties.xlsx"
FILTER_COLUMN = "Global Property Name"

# =====================================================
# LOAD EXCEL
# =====================================================
df = pd.read_excel(INPUT_FILE)

# =====================================================
# FILTER HILTON PROPERTIES (case-insensitive)
# =====================================================
hilton_df = df[
    df[FILTER_COLUMN]
    .astype(str)
    .str.contains("Hilton", case=False, na=False)
]

# =====================================================
# WRITE TO NEW EXCEL WITH SEPARATE SHEET
# =====================================================
with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
    hilton_df.to_excel(writer, sheet_name="Hilton Properties", index=False)

print(f"✅ Done! {len(hilton_df)} Hilton properties written to '{OUTPUT_FILE}'")
