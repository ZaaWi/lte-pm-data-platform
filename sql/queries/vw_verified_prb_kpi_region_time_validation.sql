CREATE OR REPLACE VIEW vw_verified_prb_kpi_region_time_validation AS
SELECT
    dataset_family,
    COUNT(*) AS region_time_rows,
    COUNT(*) FILTER (WHERE dl_prb_utilization IS NOT NULL) AS rows_with_dl_prb_utilization,
    COUNT(*) FILTER (WHERE ul_prb_utilization IS NOT NULL) AS rows_with_ul_prb_utilization
FROM vw_verified_prb_kpi_region_time
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
GROUP BY dataset_family;
