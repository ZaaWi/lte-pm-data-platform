CREATE OR REPLACE VIEW vw_verified_prb_kpi_region_time AS
SELECT
    dataset_family,
    region_code AS region,
    collect_time,
    AVG(kpi_value) FILTER (WHERE kpi_code = 'dl_prb_utilization') AS dl_prb_utilization,
    AVG(kpi_value) FILTER (WHERE kpi_code = 'ul_prb_utilization') AS ul_prb_utilization
FROM vw_verified_prb_kpi_entity_time
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
  AND region_code IS NOT NULL
GROUP BY
    dataset_family,
    region_code,
    collect_time;
