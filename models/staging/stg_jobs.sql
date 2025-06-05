{{
  config(
    materialized='table',
    alias='stg_jobs',
    location='US'
  )
}}

WITH cleaned AS (
  SELECT
    job_id,
    LOWER(job_title) AS job_title_normalized,
    CASE experience_level
      WHEN 'SE' THEN 'Senior'
      WHEN 'MI' THEN 'Mid-Level'
      WHEN 'EN' THEN 'Entry'
      ELSE 'Other'
    END AS experience_level_clean,
    
   
    CASE                                                
      WHEN company_location = 'Australia' THEN 'AU'
      WHEN company_location = 'Austria' THEN 'AT'
      WHEN company_location = 'Canada' THEN 'CA'
      WHEN company_location = 'China' THEN 'CN'
      WHEN company_location = 'Denmark' THEN 'DK'
      WHEN company_location = 'Finland' THEN 'FI'
      WHEN company_location = 'France' THEN 'FR'
      WHEN company_location = 'Germany' THEN 'DE'
      WHEN company_location = 'India' THEN 'IN'
      WHEN company_location = 'Ireland' THEN 'IE'
      WHEN company_location = 'Israel' THEN 'IL'
      WHEN company_location = 'Japan' THEN 'JP'
      WHEN company_location = 'Netherlands' THEN 'NL'
      WHEN company_location = 'Norway' THEN 'NO'
      WHEN company_location = 'Singapore' THEN 'SG'
      WHEN company_location = 'South Korea' THEN 'KR'
      WHEN company_location = 'Sweden' THEN 'SE'
      WHEN company_location = 'Switzerland' THEN 'CH'
      WHEN company_location = 'United Kingdom' THEN 'GB'
      WHEN company_location = 'United States' THEN 'US'
      ELSE 'OT'  
    END AS country_code,
    
    CAST(salary_usd AS FLOAT64) AS salary_usd
  FROM {{ source('raw_source', 'raw_jobs') }}
)

SELECT 
  job_id,
  job_title_normalized,
  experience_level_clean,
  country_code,
  salary_usd,
  salary_usd * 0.92 AS salary_eur
FROM cleaned