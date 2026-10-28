-- gold.ba_weekly_energy: drafted by the gridlake self-service data product agent.
-- Request: Weekly demand and peak per balancing authority for our BI dashboard
-- Reads only the inputs its contract declares; the pipeline builds it per period.
SELECT CAST(date_trunc('week', s.date) AS DATE) AS week_start,
       s.ba_code AS ba_code,
       s.ba_name AS ba_name,
       sum(s.total_demand_mwh) AS total_demand_mwh,
       max(s.peak_demand_mwh) AS peak_demand_mwh,
       CAST(count(*) AS INTEGER) AS days_reported,
       CAST(sum(s.hours_reported) AS INTEGER) AS hours_reported
FROM gold.ba_daily_energy s
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
