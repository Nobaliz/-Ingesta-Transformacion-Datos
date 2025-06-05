{{
  config(
    materialized='table',
    alias='fact_jobs'
  )
}}

SELECT
  j.job_id,
  j.job_title_normalized,
  j.experience_level_clean,
  j.country_code,
  j.salary_usd,
  j.salary_eur,
  COALESCE(c.continent, 'Unknown') AS continent
FROM {{ ref('stg_jobs') }} j
LEFT JOIN {{ ref('country_codes') }} c
  ON j.country_code = c.alpha_2