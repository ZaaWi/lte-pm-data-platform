CREATE OR REPLACE VIEW vw_verified_rrc_kpi_site_time AS
SELECT
    r.dataset_family,
    t.site_code AS site,
    r.collect_time,
    MAX(r.rrc_connected_users_max) AS rrc_connected_users_max,
    AVG(r.rrc_connected_users_mean) AS rrc_connected_users_mean,
    SUM(r.rrc_connected_users_online) AS rrc_connected_users_online
FROM vw_verified_rrc_kpi_entity_time AS r
JOIN vw_pm_entity_topology_projection AS t
    ON t.logical_entity_key = r.logical_entity_key
   AND t.dataset_family = r.dataset_family
WHERE r.dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
  AND t.site_code IS NOT NULL
GROUP BY
    r.dataset_family,
    t.site_code,
    r.collect_time;
