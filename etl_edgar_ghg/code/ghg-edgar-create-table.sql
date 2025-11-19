-- ================================================================
-- Database edgar
-- ================================================================

CREATE DATABASE edgar_db
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_Indonesia.1252'
    LC_CTYPE = 'English_Indonesia.1252'
    LOCALE_PROVIDER = 'libc'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1
    IS_TEMPLATE = False;

-- ================================================================
-- Country Reference Table (country)
-- ================================================================
CREATE TABLE edgar_data.country (
    country_code CHAR(10) PRIMARY KEY,          -- ISO Alpha-3 country code (e.g., IDN, USA)
    country VARCHAR(100) NOT NULL,          	-- Full country name
    macro_region VARCHAR(100) 					-- Continent or regional classification
);

-- ================================================================
-- Macro-region Reference Table (macro_region)
-- ================================================================
CREATE TABLE edgar_data.macro_region (
    macro_region VARCHAR(100) PRIMARY KEY					--  Continent or regional classification
);

-- ================================================================
-- GHG Gas Reference Table (substance)
-- ================================================================
CREATE TABLE edgar_data.substance(
	substance_code CHAR(10) PRIMARY KEY,					-- e.g., CO2, CH4, N2O
	substance_info VARCHAR(100)				-- e.g., carbon dioxide, methane, nitrous oxide
)

-- ================================================================
-- Total GHG Emissions by country (ghg_country)
-- ================================================================
CREATE TABLE edgar_data.ghg_country (
	country_code CHAR(10) REFERENCES edgar_data.country (country_code),
	year INT NOT NULL,
	ghg_total FLOAT,						-- Emission Total GHG emissions (MtCO2eq per year)
	ghg_per_capita FLOAT,					-- Emissions per person (ton CO2eq per capita per year)
	ghg_per_gdp FLOAT,						-- Emissions intensity (ton CO2eq per kUSD GDP per year)
	PRIMARY KEY(country_code, year),
	data_source VARCHAR(50) DEFAULT 'EDGAR_2025'
)

-- ================================================================
-- Sectoral GHG Emissions by country (ghg_sectoral_country)
-- ================================================================
CREATE TABLE edgar_data.ghg_sectoral_country (
	substance_code CHAR(10) REFERENCES edgar_data.substance (substance_code),		-- e.g., CO2, CH4, N2O
	sector VARCHAR(100) NOT NULL,													-- e.g., Energy, Transport, Agriculture, Waste, Total_GHG	
	country_code CHAR(10) REFERENCES edgar_data.country (country_code),
	year INT NOT NULL,
	ghg_value FLOAT,																-- Emissions per sector (Mt CO2eq per year)
	PRIMARY KEY (substance_code, sector, country_code, year),
	data_source VARCHAR(50) DEFAULT 'EDGAR_2025'
)

-- ==========================================================================================
-- Total Land Use, Land-Use Change, and Forestry (LULUCF) by country (lulucf_country)
-- ==========================================================================================
CREATE TABLE edgar_data.lulucf_country(
	country_code CHAR(10) REFERENCES edgar_data.country (country_code),
	year INT NOT NULL,
	ghg_value FLOAT,																-- Emissions per sector (Mt CO2eq per year)
	PRIMARY KEY(country_code, year),
	data_source VARCHAR(50) DEFAULT 'EDGAR_2025'
)

-- =======================================================================================================
-- Sectoral Land Use, Land-Use Change, and Forestry (LULUCF) by country (lulucf_sectoral_country)
-- =======================================================================================================
CREATE TABLE edgar_data.lulucf_sectoral_country (
	substance_code CHAR(10) REFERENCES edgar_data.substance (substance_code),		-- e.g., CO2, CH4, N2O
	sector VARCHAR(100) NOT NULL,													-- e,g., deforestation, fires, forest land, organic soil, other land	
	country_code CHAR(10) REFERENCES edgar_data.country (country_code),
	year INT NOT NULL,
	ghg_value FLOAT,																-- Emissions per sector (Mt CO2eq per year)
	PRIMARY KEY (substance_code, sector, country_code, year),
	data_source VARCHAR(50) DEFAULT 'EDGAR_2025'
)

-- ==========================================================================================
-- Total Land Use, Land-Use Change, and Forestry (LULUCF) by macro regions (lulucf_regions)
-- ==========================================================================================
CREATE TABLE edgar_data.lulucf_regions(
	macro_region VARCHAR(100) REFERENCES edgar_data.macro_region (macro_region),	
	year INT NOT NULL,
	ghg_value FLOAT,																-- Emissions per sector (Mt CO2eq per year)
	PRIMARY KEY (macro_region, year),
	data_source VARCHAR(50)
)

-- ===================================================================
-- Sectoral GHG Emissions by macro regions (ghg_sectoral_regions)
-- ===================================================================
CREATE TABLE edgar_data.ghg_sectoral_regions AS
SELECT
    gsc.substance_code,                    											-- Reference to the substance table
    gsc.sector,
    c.macro_region,                        											-- Reference to the macro_region table
    gsc.year,
    SUM(gsc.ghg_value) AS total_ghg_value,
    'Calculation_2026' AS data_source 
FROM 
    edgar_data.ghg_sectoral_country gsc
JOIN 
    edgar_data.country c
    ON gsc.country_code = c.country_code   											-- Join to get the macro_region from the country table
GROUP BY 
    gsc.substance_code, gsc.sector, c.macro_region, gsc.year;


-- Add the PRIMARY KEY constraint
ALTER TABLE edgar_data.ghg_sectoral_regions
ADD CONSTRAINT pk_substance_code PRIMARY KEY (substance_code, sector, macro_region, year);

-- Add the FOREIGN KEY constraint for substance_code
ALTER TABLE edgar_data.ghg_sectoral_regions
ADD CONSTRAINT sectoral_macro_region_substance_fkey 
    FOREIGN KEY (substance_code) 
    REFERENCES edgar_data.substance (substance_code);

-- Add the FOREIGN KEY constraint for macro_region
ALTER TABLE edgar_data.ghg_sectoral_regions
ADD CONSTRAINT sectoral_macro_region_fkey 
    FOREIGN KEY (macro_region) 
    REFERENCES edgar_data.macro_region (macro_region);

-- =======================================================================================================
-- Sectoral Land Use, Land-Use Change, and Forestry (LULUCF) by macro regions (lulucf_sectoral_regions)
-- =======================================================================================================
CREATE TABLE edgar_data.lulucf_sectoral_regions AS
SELECT
    lsc.substance_code,                    												-- Reference to the substance table
    lsc.sector,
    c.macro_region,                        												-- Reference to the macro_region table
    lsc.year,
    SUM(lsc.ghg_value) AS total_ghg_value,
    'Calculation_2026' AS data_source 
FROM 
    edgar_data.lulucf_sectoral_country lsc
JOIN 
    edgar_data.country c
    ON lsc.country_code = c.country_code   												-- Join to get the macro_region from the country table
GROUP BY 
    lsc.substance_code, lsc.sector, c.macro_region, lsc.year;

-- Add the PRIMARY KEY constraint
ALTER TABLE edgar_data.lulucf_sectoral_regions
ADD CONSTRAINT pk_sc_s_mc_y PRIMARY KEY (substance_code, sector, macro_region, year);

-- Add the FOREIGN KEY constraint for substance_code
ALTER TABLE edgar_data.lulucf_sectoral_regions
ADD CONSTRAINT sectoral_macro_region_substance_fkey
FOREIGN KEY (substance_code) REFERENCES edgar_data.substance (substance_code);

-- Add the FOREIGN KEY constraint for macro_region
ALTER TABLE edgar_data.lulucf_sectoral_regions
ADD CONSTRAINT sectoral_macro_region_regions_fkey
FOREIGN KEY (macro_region) REFERENCES edgar_data.macro_region (macro_region);

-- ================================================================================
-- Total GHG Emissions by macro regions (ghg_regions)
-- ================================================================================
CREATE TABLE edgar_data.ghg_regions AS
SELECT
    c.macro_region,                        													-- Reference to the macro_region table
    gc.year,
    SUM(gc.ghg_total) AS total_ghg_value,
    'Calculation_2026' AS data_source 
FROM 
    edgar_data.ghg_country gc
JOIN 
    edgar_data.country c
    ON gc.country_code = c.country_code  	 												-- Join to get the macro_region from the country table
GROUP BY 
    c.macro_region, gc.year;

-- Add the PRIMARY KEY constraint
ALTER TABLE edgar_data.ghg_regions 
ADD CONSTRAINT pk_mc_y PRIMARY KEY (macro_region, year);

-- Add the FOREIGN KEY constraint
ALTER TABLE edgar_data.ghg_regions
ADD CONSTRAINT regions_macro_region_regions_fkey
FOREIGN KEY (macro_region) REFERENCES edgar_data.macro_region (macro_region);
