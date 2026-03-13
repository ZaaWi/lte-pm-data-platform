CREATE OR REPLACE VIEW vw_verified_rrc_kpi_region_time_validation AS
SELECT
    dataset_family,
    COUNT(*) AS region_time_rows,
    COUNT(*) FILTER (WHERE rrc_connected_users_max IS NOT NULL) AS rows_with_rrc_connected_users_max,
    COUNT(*) FILTER (WHERE rrc_connected_users_mean IS NOT NULL) AS rows_with_rrc_connected_users_mean,
    COUNT(*) FILTER (WHERE rrc_connected_users_online IS NOT NULL) AS rows_with_rrc_connected_users_online
FROM vw_verified_rrc_kpi_region_time
WHERE dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
GROUP BY dataset_family;
