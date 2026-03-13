CREATE OR REPLACE VIEW vw_verified_bler_kpi_region_time AS
SELECT
    dataset_family,
    region_code AS region,
    collect_time,
    AVG(kpi_value) FILTER (WHERE kpi_code = 'dl_bler') AS dl_bler,
    AVG(kpi_value) FILTER (WHERE kpi_code = 'ul_bler') AS ul_bler
FROM vw_verified_bler_kpi_entity_time
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
  AND region_code IS NOT NULL
GROUP BY
    dataset_family,
    region_code,
    collect_time;
