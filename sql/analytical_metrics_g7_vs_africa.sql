/* ECONOMIC INDICATORS – ANALYTICAL SQL
   
   Source:
   - Data ingested via Jupyter ETL pipeline (World Bank API)
   - Table created using pandas.to_sql(if_exists='replace')
   - Outliers detected using IQR and stored as is_outlier = 1

   Table: economic_indicators
*/

/*METRIC 1: Economic Volatility (Shock Frequency)
   Counts abnormal economic events instead of deleting them
*/

SELECT 
    country,
    country_group,
    COUNT(*) AS shock_events
FROM economic_indicators
WHERE is_outlier = 1
GROUP BY country, country_group
ORDER BY shock_events DESC;

/* METRIC 2: Stable Economic Growth (Outlier-Adjusted GDP)
   Baseline GDP growth excluding shock years
*/

SELECT 
    country_group,
    ROUND(AVG(value), 2) AS stable_avg_gdp_growth
FROM economic_indicators
WHERE indicator_code = 'NY.GDP.MKTP.KD.ZG'
  AND is_outlier = 0
GROUP BY country_group;

/* METRIC 3: Debt vs Infrastructure (Development Trap)
   Evaluates whether public debt translates into electricity access
*/

SELECT 
    country,
    country_group,
    ROUND(AVG(CASE 
        WHEN indicator_code = 'GC.DOD.TOTL.GD.ZS' 
        THEN value END), 2) AS avg_debt_gdp,
    ROUND(AVG(CASE 
        WHEN indicator_code = 'EG.ELC.ACCS.ZS' 
        THEN value END), 2) AS avg_electricity_access
FROM economic_indicators
GROUP BY country, country_group
HAVING avg_debt_gdp IS NOT NULL
ORDER BY avg_debt_gdp DESC;

/* METRIC 4: Inflation Momentum (Post-Pandemic)
   Year-over-year inflation acceleration using window functions
*/

WITH inflation_data AS (
    SELECT 
        country,
        year,
        value AS inflation_rate
    FROM economic_indicators
    WHERE indicator_code = 'FP.CPI.TOTL.ZG'
)
SELECT 
    country,
    year,
    inflation_rate,
    LAG(inflation_rate) OVER (PARTITION BY country ORDER BY year) 
        AS prev_year_inflation,
    ROUND(
        inflation_rate -
        LAG(inflation_rate) OVER (PARTITION BY country ORDER BY year),
        2
    ) AS yoy_change
FROM inflation_data
WHERE year >= 2021
ORDER BY country, year;

/* 5: Trade Power Ranking in Exports (Post-Pandemic)
   Ranks countries by export intensity within economic groups
*/

SELECT 
    year,
    country,
    country_group,
    value AS exports_percent_gdp,
    DENSE_RANK() OVER (
        PARTITION BY year, country_group
        ORDER BY value DESC
    ) AS trade_rank
FROM economic_indicators
WHERE indicator_code = 'NE.EXP.GNFS.ZS'
  AND year >= 2021;
