SELECT * FROM sql_task.data_set_2;

USE sql_task;

CREATE TABLE coal_prices (
    Date DATE,
    Coal_RB_4800_FOB_London_Close_USD FLOAT,
    Coal_RB_5500_FOB_London_Close_USD FLOAT,
    Coal_RB_5700_FOB_London_Close_USD FLOAT,
    Coal_RB_6000_FOB_CurrentWeek_Avg_USD FLOAT,
    Coal_India_5500_CFR_London_Close_USD FLOAT,
    Price_WTI FLOAT,
    Price_Brent_Oil FLOAT,
    Price_Dubai_Brent_Oil FLOAT,
    Price_ExxonMobil FLOAT,
    Price_Shenhua FLOAT,
    Price_All_Share FLOAT,
    Price_Mining FLOAT,
    Price_LNG_Japan_Korea_Marker_PLATTS FLOAT,
    Price_ZAR_USD FLOAT,
    Price_Natural_Gas FLOAT,
    Price_ICE FLOAT,
    Price_Dutch_TTF FLOAT,
    Price_Indian_en_exg_rate FLOAT
);

SELECT
    AVG(Coal_RB_4800_FOB_London_Close_USD) AS avg_Coal_RB_4800_FOB_London,
    AVG(Coal_RB_5500_FOB_London_Close_USD) AS avg_Coal_RB_5500_FOB_London,
    AVG(Coal_RB_5700_FOB_London_Close_USD) AS avg_Coal_RB_5700_FOB_London,
    AVG(Coal_RB_6000_FOB_CurrentWeek_Avg_USD) AS avg_Coal_RB_6000_Week_Avg,
    AVG(Coal_India_5500_CFR_London_Close_USD) AS avg_Coal_India_5500_CFR_London,
    AVG(Price_WTI) AS avg_Price_WTI,
    AVG(Price_Brent_Oil) AS avg_Price_Brent_Oil,
    AVG(Price_Dubai_Brent_Oil) AS avg_Price_Dubai_Brent_Oil,
    AVG(Price_ExxonMobil) AS avg_Price_ExxonMobil,
    AVG(Price_Shenhua) AS avg_Price_Shenhua,
    AVG(Price_All_Share) AS avg_Price_All_Share,
    AVG(Price_Mining) AS avg_Price_Mining,
    AVG(Price_LNG_Japan_Korea_Marker_PLATTS) AS avg_LNG_Japan_Korea,
    AVG(Price_ZAR_USD) AS avg_ZAR_USD,
    AVG(Price_Natural_Gas) AS avg_Natural_Gas,
    AVG(Price_ICE) AS avg_ICE,
    AVG(Price_Dutch_TTF) AS avg_Dutch_TTF,
    AVG(Price_Indian_en_exg_rate) AS avg_Indian_Exg_Rate
FROM data_set_2;



WITH
wti AS (
    SELECT Price_WTI, 
           ROW_NUMBER() OVER (ORDER BY Price_WTI) rn, 
           COUNT(*) OVER () cnt 
    FROM data_set_2 
    WHERE Price_WTI IS NOT NULL
),
brent AS (
    SELECT Price_Brent_Oil, 
           ROW_NUMBER() OVER (ORDER BY Price_Brent_Oil) rn, 
           COUNT(*) OVER () cnt 
    FROM data_set_2 
    WHERE Price_Brent_Oil IS NOT NULL
),
-- Repeat for others...
dubai AS (
    SELECT Price_Dubai_Brent_Oil, ROW_NUMBER() OVER (ORDER BY Price_Dubai_Brent_Oil) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Dubai_Brent_Oil IS NOT NULL
),
exxon AS (
    SELECT Price_ExxonMobil, ROW_NUMBER() OVER (ORDER BY Price_ExxonMobil) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_ExxonMobil IS NOT NULL
),
shenhua AS (
    SELECT Price_Shenhua, ROW_NUMBER() OVER (ORDER BY Price_Shenhua) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Shenhua IS NOT NULL
),
allshare AS (
    SELECT Price_All_Share, ROW_NUMBER() OVER (ORDER BY Price_All_Share) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_All_Share IS NOT NULL
),
mining AS (
    SELECT Price_Mining, ROW_NUMBER() OVER (ORDER BY Price_Mining) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Mining IS NOT NULL
),
lng AS (
    SELECT Price_LNG_Japan_Korea_Marker_PLATTS, ROW_NUMBER() OVER (ORDER BY Price_LNG_Japan_Korea_Marker_PLATTS) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_LNG_Japan_Korea_Marker_PLATTS IS NOT NULL
),
zar AS (
    SELECT Price_ZAR_USD, ROW_NUMBER() OVER (ORDER BY Price_ZAR_USD) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_ZAR_USD IS NOT NULL
),
gas AS (
    SELECT Price_Natural_Gas, ROW_NUMBER() OVER (ORDER BY Price_Natural_Gas) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Natural_Gas IS NOT NULL
),
ice AS (
    SELECT Price_ICE, ROW_NUMBER() OVER (ORDER BY Price_ICE) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_ICE IS NOT NULL
),
ttf AS (
    SELECT Price_Dutch_TTF, ROW_NUMBER() OVER (ORDER BY Price_Dutch_TTF) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Dutch_TTF IS NOT NULL
),
inrex AS (
    SELECT Price_Indian_en_exg_rate, ROW_NUMBER() OVER (ORDER BY Price_Indian_en_exg_rate) rn, COUNT(*) OVER () cnt 
    FROM data_set_2 WHERE Price_Indian_en_exg_rate IS NOT NULL
)

SELECT
    (SELECT AVG(Price_WTI) FROM wti WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_WTI,
    (SELECT AVG(Price_Brent_Oil) FROM brent WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_Brent_Oil,
    (SELECT AVG(Price_Dubai_Brent_Oil) FROM dubai WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_Dubai_Brent_Oil,
    (SELECT AVG(Price_ExxonMobil) FROM exxon WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_ExxonMobil,
    (SELECT AVG(Price_Shenhua) FROM shenhua WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_Shenhua,
    (SELECT AVG(Price_All_Share) FROM allshare WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_All_Share,
    (SELECT AVG(Price_Mining) FROM mining WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Price_Mining,
    (SELECT AVG(Price_LNG_Japan_Korea_Marker_PLATTS) FROM lng WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_LNG_Japan_Korea,
    (SELECT AVG(Price_ZAR_USD) FROM zar WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_ZAR_USD,
    (SELECT AVG(Price_Natural_Gas) FROM gas WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Natural_Gas,
    (SELECT AVG(Price_ICE) FROM ice WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_ICE,
    (SELECT AVG(Price_Dutch_TTF) FROM ttf WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Dutch_TTF,
    (SELECT AVG(Price_Indian_en_exg_rate) FROM inrex WHERE rn IN ((cnt + 1) DIV 2, (cnt + 2) DIV 2)) AS median_Indian_Exg_Rate;


USE sql_task;
SELECT
  (SELECT `Price_WTI` FROM data_set_2 WHERE `Price_WTI` IS NOT NULL GROUP BY `Price_WTI` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_WTI,
  (SELECT `Price_Brent_Oil` FROM data_set_2 WHERE `Price_Brent_Oil` IS NOT NULL GROUP BY `Price_Brent_Oil` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_Brent_Oil,
  (SELECT `Price_Dubai_Brent_Oil` FROM data_set_2 WHERE `Price_Dubai_Brent_Oil` IS NOT NULL GROUP BY `Price_Dubai_Brent_Oil` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_Dubai_Brent_Oil,
  (SELECT `Price_ExxonMobil` FROM data_set_2 WHERE `Price_ExxonMobil` IS NOT NULL GROUP BY `Price_ExxonMobil` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_ExxonMobil,
  (SELECT `Price_Shenhua` FROM data_set_2 WHERE `Price_Shenhua` IS NOT NULL GROUP BY `Price_Shenhua` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_Shenhua,
  (SELECT `Price_All_Share` FROM data_set_2 WHERE `Price_All_Share` IS NOT NULL GROUP BY `Price_All_Share` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_All_Share,
  (SELECT `Price_Mining` FROM data_set_2 WHERE `Price_Mining` IS NOT NULL GROUP BY `Price_Mining` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Price_Mining,
  (SELECT `Price_LNG_Japan_Korea_Marker_PLATTS` FROM data_set_2 WHERE `Price_LNG_Japan_Korea_Marker_PLATTS` IS NOT NULL GROUP BY `Price_LNG_Japan_Korea_Marker_PLATTS` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_LNG_Japan_Korea,
  (SELECT `Price_ZAR_USD` FROM data_set_2 WHERE `Price_ZAR_USD` IS NOT NULL GROUP BY `Price_ZAR_USD` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_ZAR_USD,
  (SELECT `Price_Natural_Gas` FROM data_set_2 WHERE `Price_Natural_Gas` IS NOT NULL GROUP BY `Price_Natural_Gas` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Natural_Gas,
  (SELECT `Price_ICE` FROM data_set_2 WHERE `Price_ICE` IS NOT NULL GROUP BY `Price_ICE` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_ICE,
  (SELECT `Price_Dutch_TTF` FROM data_set_2 WHERE `Price_Dutch_TTF` IS NOT NULL GROUP BY `Price_Dutch_TTF` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Dutch_TTF,
  (SELECT `Price_Indian_en_exg_rate` FROM data_set_2 WHERE `Price_Indian_en_exg_rate` IS NOT NULL GROUP BY `Price_Indian_en_exg_rate` ORDER BY COUNT(*) DESC LIMIT 1) AS mode_Indian_Exg_Rate;


USE sql_task;
SELECT 
  VAR_POP(`Price_WTI`) AS var_Price_WTI,
  VAR_POP(`Price_Brent_Oil`) AS var_Price_Brent_Oil,
  VAR_POP(`Price_Dubai_Brent_Oil`) AS var_Price_Dubai_Brent_Oil,
  VAR_POP(`Price_ExxonMobil`) AS var_Price_ExxonMobil,
  VAR_POP(`Price_Shenhua`) AS var_Price_Shenhua,
  VAR_POP(`Price_All_Share`) AS var_Price_All_Share,
  VAR_POP(`Price_Mining`) AS var_Price_Mining,
  VAR_POP(`Price_LNG_Japan_Korea_Marker_PLATTS`) AS var_LNG_Japan_Korea,
  VAR_POP(`Price_ZAR_USD`) AS var_ZAR_USD,
  VAR_POP(`Price_Natural_Gas`) AS var_Natural_Gas,
  VAR_POP(`Price_ICE`) AS var_ICE,
  VAR_POP(`Price_Dutch_TTF`) AS var_Dutch_TTF,
  VAR_POP(`Price_Indian_en_exg_rate`) AS var_Indian_Exg_Rate
FROM data_set_2;


USE sql_task;
SELECT 
    STDDEV(`Price_WTI`) AS std_dev_Price_WTI,
    STDDEV(`Price_Brent_Oil`) AS std_dev_Price_Brent_Oil,
    STDDEV(`Price_Natural_Gas`) AS std_dev_Price_Natural_Gas
FROM data_set_2;


USE sql_task;
SELECT
    COUNT(*) AS n,
    AVG(Price_WTI) AS mean,
    STDDEV_POP(Price_WTI) AS stddev,
    SUM(POWER(Price_WTI - AVG(Price_WTI), 3)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_WTI), 3)) AS skewness
FROM data_set_2;



USE sql_task;
SELECT
    SUM(POWER(Coal_RB_4800_FOB_London_Close_USD - AVG(Coal_RB_4800_FOB_London_Close_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Coal_RB_4800_FOB_London_Close_USD), 4)) AS kurtosis_Coal_RB_4800,

    SUM(POWER(Coal_RB_5500_FOB_London_Close_USD - AVG(Coal_RB_5500_FOB_London_Close_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Coal_RB_5500_FOB_London_Close_USD), 4)) AS kurtosis_Coal_RB_5500,

    SUM(POWER(Coal_RB_5700_FOB_London_Close_USD - AVG(Coal_RB_5700_FOB_London_Close_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Coal_RB_5700_FOB_London_Close_USD), 4)) AS kurtosis_Coal_RB_5700,

    SUM(POWER(Coal_RB_6000_FOB_CurrentWeek_Avg_USD - AVG(Coal_RB_6000_FOB_CurrentWeek_Avg_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Coal_RB_6000_FOB_CurrentWeek_Avg_USD), 4)) AS kurtosis_Coal_RB_6000,

    SUM(POWER(Coal_India_5500_CFR_London_Close_USD - AVG(Coal_India_5500_CFR_London_Close_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Coal_India_5500_CFR_London_Close_USD), 4)) AS kurtosis_Coal_India_5500,

    SUM(POWER(Price_WTI - AVG(Price_WTI), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_WTI), 4)) AS kurtosis_Price_WTI,

    SUM(POWER(Price_Brent_Oil - AVG(Price_Brent_Oil), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Brent_Oil), 4)) AS kurtosis_Price_Brent,

    SUM(POWER(Price_Dubai_Brent_Oil - AVG(Price_Dubai_Brent_Oil), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Dubai_Brent_Oil), 4)) AS kurtosis_Dubai_Brent,

    SUM(POWER(Price_ExxonMobil - AVG(Price_ExxonMobil), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_ExxonMobil), 4)) AS kurtosis_ExxonMobil,

    SUM(POWER(Price_Shenhua - AVG(Price_Shenhua), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Shenhua), 4)) AS kurtosis_Shenhua,

    SUM(POWER(Price_All_Share - AVG(Price_All_Share), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_All_Share), 4)) AS kurtosis_All_Share,

    SUM(POWER(Price_Mining - AVG(Price_Mining), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Mining), 4)) AS kurtosis_Mining,

    SUM(POWER(Price_LNG_Japan_Korea_Marker_PLATTS - AVG(Price_LNG_Japan_Korea_Marker_PLATTS), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_LNG_Japan_Korea_Marker_PLATTS), 4)) AS kurtosis_LNG_JK,

    SUM(POWER(Price_ZAR_USD - AVG(Price_ZAR_USD), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_ZAR_USD), 4)) AS kurtosis_ZAR_USD,

    SUM(POWER(Price_Natural_Gas - AVG(Price_Natural_Gas), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Natural_Gas), 4)) AS kurtosis_Natural_Gas,

    SUM(POWER(Price_ICE - AVG(Price_ICE), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_ICE), 4)) AS kurtosis_ICE,

    SUM(POWER(Price_Dutch_TTF - AVG(Price_Dutch_TTF), 4)) / 
        (COUNT(*) * POWER(STDDEV_POP(Price_Dutch_TTF), 4)) AS kurtosis_Dutch_TTF

FROM data_set_2;





