CREATE OR REPLACE VIEW vw_verified_rrc_kpi_entity_time AS
WITH rrc_inputs AS (
    SELECT
        b.dataset_family,
        b.logical_entity_key,
        b.collect_time,
        b.counter_alias,
        b.max_counter_value,
        b.avg_counter_value,
        b.sum_counter_value
    FROM vw_semantic_kpi_base_inputs AS b
    JOIN ref_semantic_counter_dictionary AS c
        ON c.dataset_family = b.dataset_family
       AND c.counter_alias = b.counter_alias
       AND c.verification_status = 'VERIFIED'
    WHERE b.dataset_family IN ('PM/itbbu/ltefdd', 'PM/sdr/ltefdd')
      AND b.counter_alias IN (
          'max_rrc_connected_users',
          'mean_rrc_connected_users',
          'online_rrc_connected_users'
      )
)
SELECT
    dataset_family,
    logical_entity_key,
    collect_time,
    MAX(max_counter_value) FILTER (WHERE counter_alias = 'max_rrc_connected_users') AS rrc_connected_users_max,
    MAX(avg_counter_value) FILTER (WHERE counter_alias = 'mean_rrc_connected_users') AS rrc_connected_users_mean,
    MAX(sum_counter_value) FILTER (WHERE counter_alias = 'online_rrc_connected_users') AS rrc_connected_users_online
FROM rrc_inputs
GROUP BY
    dataset_family,
    logical_entity_key,
    collect_time;
