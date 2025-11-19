<h1 align = "center">GLOBAL GHG EMISSION DATA WAREHOUSE</h1>

<br>

<h2>Project Background</h2>

<p>
Greenhouse gas (GHG) emissions have become a pressing global concern, driving governments, researchers, and organizations worldwide to better understand, quantify, and mitigate their impact on climate change. Reliable data is essential for informed policy-making, scientific research, and monitoring progress toward international climate goals.
</p>

<p>
The <strong>Emissions Database for Global Atmospheric Research (EDGAR)</strong> serves as one of the most credible and comprehensive sources of global anthropogenic emissions data. Managed by the European Commission, EDGAR provides consistent and comparable datasets of GHG and air pollutant emissions for every country and sector worldwide. These datasets are shared in the form of detailed Excel spreadsheets containing emission values derived from standardized methodologies and verified sources.
</p>

<p>
However, the structure of EDGAR’s raw Excel files is not optimized for advanced data analysis. To enable efficient querying, integration, and further processing, the data must first be extracted, cleaned, and transformed into a relational format suitable for SQL-based exploration.
</p>

<p>
This project addresses that need by developing a data pipeline that:
</p>

<ul>
  <li>Extracts raw GHG emission data from EDGAR’s published spreadsheets (<a href="https://github.com/agungkramawijaya/personal_project/blob/main/etl_edgar_ghg/data/raw-data/EDGAR_2025_GHG_booklet_2025.xlsx" download>available here</a>).</li>
  <li>Transforms the data into a normalized relational structure using <strong>Python</strong>.</li>
  <li>Loads the transformed datasets into a <strong>PostgreSQL</strong> database for scalable storage, querying, and analysis.</li>
</ul>

<p>
Through this process, the project ensures that complex emission data can be handled programmatically and analyzed efficiently — laying a solid foundation for environmental data science, policy evaluation, and global emission trend studies.
</p>

<h2>Project Goal</h2>

<p>
The primary objective of this project is to design and implement an <strong>ETL (Extract, Transform, Load) pipeline</strong> that efficiently converts EDGAR’s raw emission spreadsheets into a structured SQL database. By doing so, the project aims to bridge the gap between raw environmental datasets and analytical readiness.
</p>

<p>Specifically, the project seeks to:</p>

<ul>
  <li>Develop a ETL pipeline capable of extracting emission data directly from EDGAR’s publicly available spreadsheets, transforming it into a clean, consistent schema, and loading it into a PostgreSQL database.</li>
  <li>Deliver a well-structured relational database that supports efficient querying, integration, and statistical analysis — enabling researchers, policymakers, and developers to gain deeper insights into global GHG emission patterns.</li>
</ul>

<p>
Through these goals, the project provides a scalable foundation for environmental data analytics and supports informed decision-making in the pursuit of sustainable development and climate action.
</p>

<h2>Data Initial Check</h2>

<p>
Before designing the SQL database schema, an <strong>initial examination of the raw EDGAR spreadsheet</strong> was conducted to understand its structure, content, and data quality. This step is critical to ensure that the subsequent data modeling and transformation processes are built upon a clear understanding of the dataset’s composition and potential issues.
</p>

<p>Based on the initial inspection, the following key findings were identified:</p>

<ul>
  <li>
    <strong>Multiple sheets with distinct data scopes:</strong><br>
    The spreadsheet contains several sheets, each representing different dimensions of GHG emission data — such as total emissions per country, per sector and country, per GDP, and per capita. It also includes additional datasets related to total GHG emissions from <strong>LULUCF</strong> (Land Use, Land Use Change, and Forestry), presented per sector, per macro-region, and per country.
  </li>

  <li>
    <strong>Combined data structures requiring separation:</strong><br>
    Within the spreadsheet, total GHG emission data per macro-region and per sector–macro-region are still merged within a single sheet. This structure is not suitable for relational database design and must be <strong>restructured into distinct, logically separated tables</strong> to maintain data integrity and clarity.
  </li>

  <li>
    <strong>Horizontal year-based format:</strong><br>
    The emission data is currently stored in a <strong>horizontal layout</strong>, where each year corresponds to a separate column. For effective storage and analysis, the dataset must be <strong>transposed into a vertical structure</strong>, where emission values across years are consolidated within a single column — following best practices for relational and analytical data modeling.
  </li>

  <li>
    <strong>Presence of incomplete or missing records:</strong><br>
    Several rows contain <strong>NA (Not Available)</strong> values for entire column, indicating missing rows. These rows need to be <strong>identified and removed</strong> (or handled appropriately) during the data cleaning process to ensure the resulting database maintains high accuracy and consistency.
  </li>
  <li>
    <strong>Missing Essential Dataset:</strong><br>
    Some key datasets required for comprehensive analysis are <strong>absent</strong> from the provided spreadsheet. Specifically, data such as <em>total GHG emissions per macro-region</em>, <em>sectoral total GHG emissions by macro-region</em>, and <em>total sectoral GHG emissions from LULUCF by macro-region</em> are missing. To support advanced analysis, these datasets must be <strong>aggregated and derived</strong> from the available raw data through structured data transformation processes.
  </li>
</ul>

<p>
Through this initial data assessment, a clear roadmap was established for the subsequent data transformation and loading processes. This foundational step ensures that the resulting SQL database is well-structured, normalized, and ready for reliable analytical use.
</p>

<h2>Executive Summary</h2>

<h3>Data Restructuring</h3>

<p>
Based on the results of the initial data check, the data structure for the SQL database has been carefully designed. Each table has been assigned a <strong>primary key</strong> and, where necessary, <strong>foreign key relationships</strong> to ensure data integrity and consistency. The new schema aims to represent EDGAR’s greenhouse gas (GHG) emission data in a clean, normalized, and analysis-ready format.
</p>

<p>
During this process, it was also identified that several worksheets in the original EDGAR spreadsheet contained <strong>merged data</strong> — specifically, multiple datasets combined within a single sheet for LULUCF sector. To achieve a reliable and relational structure, these merged datasets were <strong>split into separate tables</strong> before loading into the database. This adjustment ensures that each dataset maintains a single, clear analytical purpose and aligns with best practices in database normalization.
</p>

<p>
The entity relationship diagram (ERD) below illustrates the final database structure, including the relationships among core tables such as <code>country</code>, <code>macro_region</code>, <code>substance</code>, and various GHG and LULUCF emission datasets.
</p>

<p align="center">
  <img src="https://github.com/agungkramawijaya/personal_project/blob/main/etl_edgar_ghg/assets/erd-edgar-ghg.png" alt="ERD Diagram for Global GHG Emission Database" width="700">
</p>

<p>
In summary, this data model captures:
</p>

<ul>
  <li><strong>Country-level data</strong> — including total emissions, per capita, and per GDP values.</li>
  <li><strong>Sectoral and regional breakdowns</strong> — providing emission data per sector, macro-region, and country for both GHG and LULUCF categories.</li>
  <li><strong>Substance linkage</strong> — connecting each emission record to a specific gas or pollutant through a shared <code>substance_code</code> key.</li>
  <li><strong>Normalization and referential consistency</strong> — using reference tables like <code>country</code> and <code>macro_region</code> to avoid duplication and improve database integrity.</li>
</ul>

<p>
Based on this new structure, all corresponding tables were created in <strong>PostgreSQL</strong> using <strong>DBeaver</strong>.  
The table creation script can be found here: <a href="https://github.com/agungkramawijaya/personal_project/blob/main/etl_edgar_ghg/code/ghg-edgar-create-table.sql">[SQL Table Creation Script ↗]</a>

</p>

<h3>Data Extraction, Transformation, and Load (ETL)</h3>

<p>
The ETL process was designed in alignment with the findings from the initial data assessment. Using <strong>Python</strong>, the project performs three main operations:
</p>

<ul>
  <li><strong>Extraction</strong> — retrieves raw GHG emission data directly from EDGAR’s published spreadsheets. The modified spreadsheet can be found here: <a href="https://github.com/agungkramawijaya/personal_project/blob/main/etl_edgar_ghg/data/processed-data/EDGAR_2025_GHG_booklet_2025_processed.xlsx" download>Modified EDGAR Spreadsheet</a> </li>
  <li><strong>Transformation</strong> — restructures horizontal year-based data into a vertical format, normalizes field names, removes missing values, separates merged datasets, and aligns data types to match the SQL schema.</li>
  <li><strong>Load</strong> — inserts the cleaned and transformed datasets into the corresponding PostgreSQL tables to enable structured queries and analysis.</li>
</ul>

<p>
This ETL pipeline ensures that the data flow from the original EDGAR source to the final database is repeatable and reliable.  
The complete Python script implementing the ETL process can be found here: <a href="https://github.com/agungkramawijaya/personal_project/blob/main/etl_edgar_ghg/code/ghg-edgar-etl.py" target="_blank">[ETL Python Script  ↗]</a>.
</p>


<h2>Conclusion</h2>

<p>
This project successfully restructures and transforms the EDGAR greenhouse gas (GHG) emission datasets into a relational SQL database that is both clean and analysis-ready. Through the integration of <strong>Python</strong> and <strong>PostgreSQL</strong>, the project establishes a robust and reproducible ETL workflow that ensures consistent data quality and accessibility.  
</p>

<p>
The resulting database enables efficient querying, supports data-driven environmental research, and forms a solid foundation for future analytical and visualization projects focused on global GHG emission patterns.
</p>

<h2>Future Work</h2>

<p>
With the database structure and ETL pipeline established, several opportunities exist to expand the analytical capabilities and practical applications of this project. The following initiatives represent potential next steps for leveraging the structured GHG emission database for advanced research, modeling, and visualization.
</p>

<ul>
  <li><strong>Global Emission Trend Analysis:</strong> Examine long-term changes in total, per-capita, and per-GDP GHG emissions across countries and macro-regions through interactive dashboards and time-series analysis.</li>

  <li><strong>Sectoral and Substance-Based Insights:</strong> Analyze emissions by sector and gas type (e.g., CO₂, CH₄, N₂O) to identify dominant contributors and evaluate mitigation priorities.</li>

  <li><strong>LULUCF Contribution Assessment:</strong> Investigate the role of land use and forestry activities in influencing regional and national emission balances.</li>

  <li><strong>Economic and Demographic Correlations:</strong> Study relationships between GHG emissions, GDP, and population growth to assess emission intensity and sustainability efficiency.</li>

  <li><strong>Regional Clustering and Benchmarking:</strong> Apply statistical or machine learning techniques to group countries or regions based on emission patterns and environmental performance indicators.</li>

  <li><strong>Emission Forecasting and Scenario Modeling:</strong> Use predictive models such as ARIMA, Prophet, or LSTM to forecast future emission levels and simulate policy scenarios.</li>

  <li><strong>Comprehensive Climate Intelligence Dashboard:</strong> Integrate analytical outputs into a unified, interactive web-based platform for visualization, reporting, and data-driven decision support.</li>
</ul>

<p>
These developments will extend the current project beyond data warehousing toward a complete analytical ecosystem—enabling dynamic exploration, forecasting, and actionable insights into global greenhouse gas emissions.
</p>



     
