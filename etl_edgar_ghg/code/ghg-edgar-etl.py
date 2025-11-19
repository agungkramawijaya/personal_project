# ===============================================================
#  EDGAR 2025 GHG Data Loader
#  From wide Excel to PostgreSQL (Schema: edgar_data)
# ===============================================================

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# ---------------------------------------------------------------
# 1️⃣ PostgreSQL Connection Setup
# ---------------------------------------------------------------
# Create SQL connection details
USER = "postgres"
PASSWORD = "Sialan317117773780#"
HOST = "localhost"
PORT = "5432"
DBNAME = "emission_db"

# Create engine to connect to SQL database
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")

# ---------------------------------------------------------------
# 2️⃣ Load Excel File
# ---------------------------------------------------------------
# Create excel file path
excel_path = r"C:\Users\ASUS\Downloads\EDGAR_2025_GHG_booklet_2025.xlsx"
xls = pd.ExcelFile(excel_path)
print("✅ Sheets Found:", xls.sheet_names)

# ---------------------------------------------------------------
# 3️⃣ Helper Function for Upload
# ---------------------------------------------------------------
# Create helper function for upload data to database
def load_to_postgres(df, table_name, schema="edgar_data"):
    """Load DataFrame to PostgreSQL with error handling"""
    try:
        print(f"Uploading {len(df):,} rows into {schema}.{table_name}...")
        df.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)
        print(f"✅ {table_name} uploaded successfully.\n")
    except SQLAlchemyError as e:
        print(f"❌ Error uploading {table_name}: {e}\n")

# ---------------------------------------------------------------
# 4️⃣ Country Table
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading Country Table...")
df_total = pd.read_excel(xls, sheet_name="GHG_totals_by_country")
print("📦 Loading LULUCF Table...")
df_lulucf = pd.read_excel(xls, sheet_name="LULUCF_countries")

# Clean duplicate rows and rename some columns
df_country = df_total[["EDGAR Country Code", "Country"]].drop_duplicates(subset=["EDGAR Country Code"]).rename(
    columns={"EDGAR Country Code": "country_code", "Country": "country"}
)
df_region = df_lulucf[["EDGAR Country Code", "Macro-region"]].drop_duplicates(subset=["EDGAR Country Code"]).rename(
    columns={"EDGAR Country Code": "country_code", "Macro-region": "macro_region"}
)

# Merge two datasets
df_country = pd.merge(df_country, df_region, on="country_code", how="left")

# Complete standardization
df_country.loc[df_country['country_code'] == 'GLOBAL TOTAL','country_code'] = "GLOBAL"
df_country.loc[df_country['country_code'] == 'GLOBAL','macro_region'] = "GLOBAL TOTAL"
df_country.loc[df_country['country_code'] == 'AIR','macro_region'] = "International Aviation"
df_country.loc[df_country['country_code'] == 'COK','macro_region'] = "Oceania"
df_country.loc[df_country['country_code'] == 'ESH','macro_region'] = "Western Sahara"
df_country.loc[df_country['country_code'] == 'MTQ','macro_region'] = "North America"
df_country.loc[df_country['country_code'] == 'PYF','macro_region'] = "Oceania"
df_country.loc[df_country['country_code'] == 'SEA','macro_region'] = "International Shipping"
df_country.loc[df_country['country_code'] == 'TON','macro_region'] = "Oceania"
df_country.loc[df_country['country_code'] == 'EU27','macro_region'] = "EU27"
df_country = df_country.dropna(how='all')

# Check the final claned data
print(df_country.head())
print(df_country.tail())

# Upload data to database
load_to_postgres(df_country, "country")

# ---------------------------------------------------------------
# 5️⃣ Substance Table
# ---------------------------------------------------------------
# Create data for substance table
print("📦 Loading Substance Table...")
df_substance = pd.DataFrame({
    "substance_code": ["CO2", "CH4", "N2O", "F-gases"],
    "substance_info": ["Carbon dioxide", "Methane", "Nitrous oxide", "Fluorinated gases"]
})

# Upload data to database
load_to_postgres(df_substance, "substance")

# ---------------------------------------------------------------
# 6️⃣ Emission Total (wide → long)
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading Emission Total Table...")
df_total = pd.read_excel(xls, sheet_name="GHG_totals_by_country")
print("📦 Loading Emission Per Capita Table...")
df_per_capita = pd.read_excel(xls, sheet_name="GHG_per_capita_by_country")
print("📦 Loading Emission Per GDP Table...")
df_per_gdp = pd.read_excel(xls, sheet_name="GHG_per_GDP_by_country")

# -------------------------------------------------------------------------------
# Melt Total GHG Data
# -------------------------------------------------------------------------------
# Identify year columns (numeric only)
year_cols = [c for c in df_total.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_total_long = df_total.melt(
    id_vars=["EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_total"
)

# Clean all rows with NA value
df_total_long = df_total_long.dropna(subset=[col for col in df_total_long.columns if col != "year"], how="all")

# Check the final claned data
print(df_total_long.head())
print(df_total_long.tail())

# -------------------------------------------------------------------------------
# Melt Total GHG per Capita
# -------------------------------------------------------------------------------

# Identify year columns (numeric only)
year_cols = [c for c in df_per_capita.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_per_capita_long = df_per_capita.melt(
    id_vars=["EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_per_capita"
)

# Clean all rows with NA value
df_per_capita_long  = df_per_capita_long.dropna(subset=[col for col in df_per_capita_long.columns if col != "year"], how="all")

# Check the final claned data
print(df_per_capita_long.head())
print(df_per_capita_long.tail())

# -------------------------------------------------------------------------------
# Melt Total GHG per GDP
# -------------------------------------------------------------------------------
# Identify year columns (numeric only)
year_cols = [c for c in df_per_gdp.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_per_gdp_long = df_per_gdp.melt(
    id_vars=["EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_per_gdp"
)

# Clean all rows with NA value
df_per_gdp_long  = df_per_gdp_long.dropna(subset=[col for col in df_per_gdp_long.columns if col != "year"], how="all")

# Check final cleaned data
print(df_per_gdp_long.head())
print(df_per_gdp_long.tail())

# -------------------------------------------------------------------------------
# Combining all dataset
# -------------------------------------------------------------------------------
# Rename some columns
df_total_long, df_per_capita_long, df_per_gdp_long = [
    df.rename(columns={"EDGAR Country Code": "country_code", "Country": "country"})
    for df in [df_total_long, df_per_capita_long, df_per_gdp_long]
]

# Change data type to integer
df_total_long["year"] = df_total_long["year"].astype(int)
df_per_capita_long["year"] = df_per_capita_long["year"].astype(int)
df_per_gdp_long["year"] = df_per_gdp_long["year"].astype(int)

# Merging ghg per capita and ghg per gdp into ghg total
df_total_long = pd.merge(df_total_long, df_per_capita_long, on=["country_code","country","year"], how="left").dropna(subset='country_code') 
df_total_long = pd.merge(df_total_long, df_per_gdp_long, on=["country_code","country","year"], how="left").dropna(subset='country_code')

# Complete standardization
df_total_long["data_source"] = "EDGAR_2025"
df_total_long.loc[df_total_long['country_code'] == 'GLOBAL TOTAL','country_code'] = "GLOBAL"

# Check final cleaned data
print(df_total_long.head())
print(df_total_long.tail())
print(df_total_long.loc[(df_total_long["country"] == "Aruba") & (df_total_long["ghg_per_gdp"].isna()),["year"]].drop_duplicates())
print(df_total_long.loc[df_total_long["ghg_per_gdp"].isna(), ["year"]].drop_duplicates())
print(df_total_long[df_total_long["ghg_per_gdp"].isna()])
print(df_total_long.loc[df_total_long["ghg_per_gdp"].isna(), "country"].unique()) #Check data unique country that has NA value in ghg_per_gdp column
print(df_per_capita_long.head())
print(df_per_gdp_long.head())

# Upload data to database
load_to_postgres(df_total_long[["country_code", "year", "ghg_total", "ghg_per_capita", "ghg_per_gdp", "data_source"]],
                 "emission_total")

# ---------------------------------------------------------------
# 7️⃣ Emission Sectoral (wide → long)
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading Emission Sectoral Table...")
df_sector = pd.read_excel(xls, sheet_name="GHG_by_sector_and_country")

# Identify year columns (numeric only)
year_cols = [c for c in df_sector.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_sector_long = df_sector.melt(
    id_vars=["Substance", "Sector", "EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_value"
)

# Clean all rows with NA value
df_sector_long  = df_sector_long.dropna(subset=[col for col in df_sector_long.columns if col != "year"], how="all") 

# Rename some columns
df_sector_long = df_sector_long.rename(
    columns={
        "Substance": "substance_code",
        "Sector": "sector",
        "EDGAR Country Code": "country_code",
        "Country": "country"
    }
)

# Complete standardization
df_sector_long["year"] = df_sector_long["year"].astype(int)
df_sector_long["data_source"] = "EDGAR_2025"
df_sector_long.loc[df_sector_long['country_code'] == 'GLOBAL TOTAL','country_code'] = "GLOBAL"
df_sector_long["substance_code"] = df_sector_long["substance_code"].replace({
    "GWP_100_AR5_CH4": "CH4",
    "GWP_100_AR5_F-gases": "F-gases",
    "GWP_100_AR5_N2O": "N2O"
})

# Check final cleaned data
print(df_sector_long.head())
print(df_sector_long.tail())
print(df_sector_long[df_sector_long["country_code"].isna()])
print(df_sector_long[df_sector_long.drop(columns="year").isna().all(axis=1)]) # to check all row that has NA value in all column

# Upload data to database
load_to_postgres(df_sector_long[["substance_code", "sector", "country_code", "year", "ghg_value", "data_source"]],
                 "emission_sectoral")

# ---------------------------------------------------------------
# 8️⃣ LULUCF COUNTRY (wide → long)
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading LULUCF Table...")
df_lulucf = pd.read_excel(xls, sheet_name="LULUCF_countries")

# Identify year columns (numeric only)
year_cols = [c for c in df_lulucf.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_lulucf_country = df_lulucf.melt(
    id_vars=["Substance", "EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_value"
)

# Clean all rows with NA value
df_lulucf_country  = df_lulucf_country.dropna(subset=[col for col in df_lulucf_country.columns if col != "year"], how="all")

# Standardization
df_lulucf_country = df_lulucf_country.rename(columns={"EDGAR Country Code": "country_code", "Country": "country"})
df_lulucf_country["year"] = df_lulucf_country["year"].astype(int)
df_lulucf_country["data_source"] = "EDGAR_2025"

# Check final cleaned data
print(df_lulucf_country.head())
print(df_lulucf_country.tail())

# Upload data to database
load_to_postgres(df_lulucf_country[["country_code", "year", "ghg_value", "data_source"]],
                 "lulucf_country")

# ---------------------------------------------------------------
# 9️⃣ LULUCF SECTORAL (wide → long)
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading LULUCF Table...")
df_lulucf = pd.read_excel(xls, sheet_name="LULUCF_sectoral")

# Identify year columns (numeric only)
year_cols = [c for c in df_lulucf.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_lulucf_sectoral = df_lulucf.melt(
    id_vars=["Substance", "Sector", "EDGAR Country Code", "Country"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_value"
)

# Clean all rows with NA value
df_lulucf_sectoral  = df_lulucf_sectoral.dropna(subset=[col for col in df_lulucf_sectoral.columns if col != "year"], how="all") #Clean data with NA value for all column except year

# Complete standardization
df_lulucf_sectoral = df_lulucf_sectoral.rename(columns={"Substance": "substance_code","Sector": "sector","EDGAR Country Code": "country_code", "Country": "country"})
df_lulucf_sectoral["year"] = df_lulucf_sectoral["year"].astype(int)
df_lulucf_sectoral["data_source"] = "EDGAR_2025"
df_lulucf_sectoral["substance_code"] = df_lulucf_sectoral["substance_code"].replace({
    "GWP_100_AR5_CH4": "CH4",
    "GWP_100_AR5_F-gases": "F-gases",
    "GWP_100_AR5_N2O": "N2O"
})

# Check final cleaned data
print(df_lulucf_sectoral.head())
print(df_lulucf_sectoral.tail())

# Upload data to database
load_to_postgres(df_lulucf_sectoral[["substance_code","sector","country_code", "year", "ghg_value", "data_source"]],
                 "lulucf_sectoral")

# ---------------------------------------------------------------
# 🔟 LULUCF REGIONAL (wide → long)
# ---------------------------------------------------------------
# Read required sheets in excel file
print("📦 Loading LULUCF Table...")
df_lulucf = pd.read_excel(xls, sheet_name="LULUCF_macroregions")

# Identify year columns (numeric only)
year_cols = [c for c in df_lulucf.columns if str(c).isdigit()]

# Choose required columns and convert horizontal-style ghg data into vertical-style
df_lulucf_regions = df_lulucf.melt(
    id_vars=["Macro-region"],
    value_vars=year_cols,
    var_name="year",
    value_name="ghg_value"
)

# Clean all rows with NA value
df_lulucf_regions  = df_lulucf_regions.dropna(subset=[col for col in df_lulucf_regions.columns if col != "year"], how="all")

# Complete standardization
df_lulucf_regions = df_lulucf_regions.rename(columns={"Macro-region": "macro_region"})
df_lulucf_regions["year"] = df_lulucf_regions["year"].astype(int)
df_lulucf_regions["data_source"] = "Calculation_2026"

# Check final cleaned data
print(df_lulucf_regions.head())
print(df_lulucf_regions.tail())

# Upload data to database
load_to_postgres(df_lulucf_regions[["macro_region","year", "ghg_value", "data_source"]],
                 "lulucf_regions")