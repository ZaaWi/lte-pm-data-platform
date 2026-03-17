from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

from psycopg import Connection
from psycopg.rows import dict_row

from lte_pm_platform.pipeline.ingest.semantic_kpi_seed import (
    SemanticCounterDictionarySeedRow,
    SemanticKpiDefinitionSeedRow,
    VendorIndicatorSeedRow,
)


class SemanticKpiRepository:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def _fetch_limited_rows(self, query: str, limit: int) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, (limit,))
            return list(cursor.fetchall())

    def _fetch_rows(self, query: str) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query)
            return list(cursor.fetchall())

    def list_verified_kpi_results(
        self,
        *,
        family: str,
        grain: str,
        limit: int = 100,
        offset: int = 0,
        dataset_family: str | None = None,
        site_code: str | None = None,
        region_code: str | None = None,
        collect_time_from: datetime | None = None,
        collect_time_to: datetime | None = None,
    ) -> list[dict]:
        if grain == "entity-time":
            if dataset_family is None:
                raise ValueError("dataset_family is required for entity-time KPI results")
            collect_time_from, collect_time_to = self._normalize_collect_time_window(
                dataset_family=dataset_family,
                collect_time_from=collect_time_from,
                collect_time_to=collect_time_to,
            )
            return self._list_verified_entity_time_results(
                family=family,
                dataset_family=dataset_family,
                limit=limit,
                offset=offset,
                collect_time_from=collect_time_from,
                collect_time_to=collect_time_to,
            )
        if grain in {"site-time", "region-time"} and family in {"prb", "bler"}:
            if dataset_family is None:
                raise ValueError("dataset_family is required for site-time and region-time KPI results")
            collect_time_from, collect_time_to = self._normalize_collect_time_window(
                dataset_family=dataset_family,
                collect_time_from=collect_time_from,
                collect_time_to=collect_time_to,
            )
            return self._list_verified_topology_rollup_results(
                family=family,
                grain=grain,
                dataset_family=dataset_family,
                site_code=site_code,
                region_code=region_code,
                limit=limit,
                offset=offset,
                collect_time_from=collect_time_from,
                collect_time_to=collect_time_to,
            )

        query_specs: dict[tuple[str, str], tuple[str, str]] = {
            (
                "prb",
                "site-time",
            ): (
                """
                SELECT
                    dataset_family,
                    site AS site_code,
                    collect_time,
                    dl_prb_utilization,
                    ul_prb_utilization
                FROM vw_verified_prb_kpi_site_time
                """,
                "ORDER BY collect_time DESC, dataset_family, site_code",
            ),
            (
                "bler",
                "site-time",
            ): (
                """
                SELECT
                    dataset_family,
                    site AS site_code,
                    collect_time,
                    dl_bler,
                    ul_bler
                FROM vw_verified_bler_kpi_site_time
                """,
                "ORDER BY collect_time DESC, dataset_family, site_code",
            ),
            (
                "rrc",
                "site-time",
            ): (
                """
                SELECT
                    dataset_family,
                    site AS site_code,
                    collect_time,
                    rrc_connected_users_max,
                    rrc_connected_users_mean,
                    rrc_connected_users_online
                FROM vw_verified_rrc_kpi_site_time
                """,
                "ORDER BY collect_time DESC, dataset_family, site_code",
            ),
            (
                "prb",
                "region-time",
            ): (
                """
                SELECT
                    dataset_family,
                    region AS region_code,
                    collect_time,
                    dl_prb_utilization,
                    ul_prb_utilization
                FROM vw_verified_prb_kpi_region_time
                """,
                "ORDER BY collect_time DESC, dataset_family, region_code",
            ),
            (
                "bler",
                "region-time",
            ): (
                """
                SELECT
                    dataset_family,
                    region AS region_code,
                    collect_time,
                    dl_bler,
                    ul_bler
                FROM vw_verified_bler_kpi_region_time
                """,
                "ORDER BY collect_time DESC, dataset_family, region_code",
            ),
            (
                "rrc",
                "region-time",
            ): (
                """
                SELECT
                    dataset_family,
                    region AS region_code,
                    collect_time,
                    rrc_connected_users_max,
                    rrc_connected_users_mean,
                    rrc_connected_users_online
                FROM vw_verified_rrc_kpi_region_time
                """,
                "ORDER BY collect_time DESC, dataset_family, region_code",
            ),
        }
        base_query, order_by = query_specs[(family, grain)]
        query = f"{base_query}\nWHERE TRUE"
        params: list[object] = []
        if dataset_family is not None:
            query += "\n  AND dataset_family = %s"
            params.append(dataset_family)
        if site_code is not None and grain == "site-time":
            query += "\n  AND site_code = %s"
            params.append(site_code)
        if region_code is not None and grain == "region-time":
            query += "\n  AND region_code = %s"
            params.append(region_code)
        if collect_time_from is not None:
            query += "\n  AND collect_time >= %s"
            params.append(collect_time_from)
        if collect_time_to is not None:
            query += "\n  AND collect_time <= %s"
            params.append(collect_time_to)
        query += f"\n{order_by}\nLIMIT %s\nOFFSET %s"
        params.append(limit)
        params.append(offset)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def _latest_collect_time(self, dataset_family: str) -> datetime | None:
        with self.connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(collect_time)
                FROM pm_ltefdd_sample
                WHERE dataset_family = %s
                """,
                (dataset_family,),
            )
            row = cursor.fetchone()
        if row is None:
            return None
        return row[0]

    def _normalize_collect_time_window(
        self,
        *,
        dataset_family: str,
        collect_time_from: datetime | None,
        collect_time_to: datetime | None,
    ) -> tuple[datetime | None, datetime | None]:
        if collect_time_from is not None or collect_time_to is not None:
            return collect_time_from, collect_time_to

        latest_collect_time = self._latest_collect_time(dataset_family)
        if latest_collect_time is None:
            return None, None
        return latest_collect_time, latest_collect_time

    def _get_verified_counter_specs(
        self,
        *,
        dataset_family: str,
        aliases: Sequence[str],
    ) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    counter_id,
                    counter_alias
                FROM ref_semantic_counter_dictionary
                WHERE dataset_family = %s
                  AND counter_alias = ANY(%s)
                  AND verification_status = 'VERIFIED'
                ORDER BY counter_alias
                """,
                (dataset_family, list(aliases)),
            )
            return list(cursor.fetchall())

    def _list_verified_entity_time_results(
        self,
        *,
        family: str,
        dataset_family: str,
        limit: int,
        offset: int,
        collect_time_from: datetime | None,
        collect_time_to: datetime | None,
    ) -> list[dict]:
        family_specs: dict[str, dict[str, object]] = {
            "prb": {
                "aliases": {
                    "dl_prb_used": ("sum_counter_value", "dl_prb_used"),
                    "dl_prb_available": ("sum_counter_value", "dl_prb_available"),
                    "ul_prb_used": ("sum_counter_value", "ul_prb_used"),
                    "ul_prb_available": ("sum_counter_value", "ul_prb_available"),
                },
                "select": """
                    SELECT
                        dataset_family,
                        logical_entity_key,
                        collect_time,
                        entity_level,
                        site_code,
                        region_code,
                        CASE
                            WHEN dl_prb_available IS NULL OR dl_prb_available = 0 THEN NULL
                            ELSE 100.0 * dl_prb_used / dl_prb_available
                        END AS dl_prb_utilization,
                        CASE
                            WHEN ul_prb_available IS NULL OR ul_prb_available = 0 THEN NULL
                            ELSE 100.0 * ul_prb_used / ul_prb_available
                        END AS ul_prb_utilization
                    FROM pivoted
                    WHERE dl_prb_available IS NOT NULL
                       OR ul_prb_available IS NOT NULL
                    ORDER BY collect_time DESC, dataset_family, logical_entity_key
                    LIMIT %s
                    OFFSET %s
                """,
            },
            "bler": {
                "aliases": {
                    "dl_tb_error_blocks": ("sum_counter_value", "dl_tb_error_blocks"),
                    "dl_tb_total_blocks": ("sum_counter_value", "dl_tb_total_blocks"),
                    "ul_tb_error_blocks": ("sum_counter_value", "ul_tb_error_blocks"),
                    "ul_tb_total_blocks": ("sum_counter_value", "ul_tb_total_blocks"),
                },
                "select": """
                    SELECT
                        dataset_family,
                        logical_entity_key,
                        collect_time,
                        entity_level,
                        site_code,
                        region_code,
                        CASE
                            WHEN dl_tb_total_blocks IS NULL OR dl_tb_total_blocks = 0 THEN NULL
                            ELSE 100.0 * dl_tb_error_blocks / dl_tb_total_blocks
                        END AS dl_bler,
                        CASE
                            WHEN ul_tb_total_blocks IS NULL OR ul_tb_total_blocks = 0 THEN NULL
                            ELSE 100.0 * ul_tb_error_blocks / ul_tb_total_blocks
                        END AS ul_bler
                    FROM pivoted
                    WHERE dl_tb_total_blocks IS NOT NULL
                       OR ul_tb_total_blocks IS NOT NULL
                    ORDER BY collect_time DESC, dataset_family, logical_entity_key
                    LIMIT %s
                    OFFSET %s
                """,
            },
            "rrc": {
                "aliases": {
                    "max_rrc_connected_users": ("max_counter_value", "rrc_connected_users_max"),
                    "mean_rrc_connected_users": ("avg_counter_value", "rrc_connected_users_mean"),
                    "online_rrc_connected_users": ("sum_counter_value", "rrc_connected_users_online"),
                },
                "select": """
                    SELECT
                        dataset_family,
                        logical_entity_key,
                        collect_time,
                        entity_level,
                        site_code,
                        region_code,
                        rrc_connected_users_max,
                        rrc_connected_users_mean,
                        rrc_connected_users_online
                    FROM pivoted
                    WHERE rrc_connected_users_max IS NOT NULL
                       OR rrc_connected_users_mean IS NOT NULL
                       OR rrc_connected_users_online IS NOT NULL
                    ORDER BY collect_time DESC, dataset_family, logical_entity_key
                    LIMIT %s
                    OFFSET %s
                """,
            },
        }
        alias_specs = family_specs[family]["aliases"]  # type: ignore[index]
        aliases = tuple(alias_specs.keys())  # type: ignore[union-attr]
        counter_specs = self._get_verified_counter_specs(
            dataset_family=dataset_family,
            aliases=aliases,
        )
        if not counter_specs:
            return []
        counter_ids = [row["counter_id"] for row in counter_specs]
        counter_aliases = [row["counter_alias"] for row in counter_specs]

        pivot_columns = []
        for counter_alias, (value_column, result_alias) in alias_specs.items():  # type: ignore[union-attr]
            pivot_columns.append(
                f"MAX({value_column}) FILTER (WHERE counter_alias = '{counter_alias}') AS {result_alias}"
            )

        query = f"""
            WITH selected_counters AS (
                SELECT *
                FROM unnest(%s::text[], %s::text[]) AS c(counter_id, counter_alias)
            ),
            filtered_inputs AS (
                SELECT
                    r.dataset_family,
                    r.logical_entity_key,
                    r.collect_time,
                    r.entity_level,
                    r.site_code,
                    r.region_code,
                    c.counter_alias,
                    SUM(r.counter_value) AS sum_counter_value,
                    AVG(r.counter_value) AS avg_counter_value,
                    MAX(r.counter_value) AS max_counter_value
                FROM vw_pm_raw_with_entity_topology AS r
                JOIN selected_counters AS c
                    ON c.counter_id = r.counter_id
                WHERE r.dataset_family = %s
                  AND r.counter_id = ANY(%s)
        """
        params: list[object] = [counter_ids, counter_aliases, dataset_family, counter_ids]
        if collect_time_from is not None:
            query += "\n                  AND r.collect_time >= %s"
            params.append(collect_time_from)
        if collect_time_to is not None:
            query += "\n                  AND r.collect_time <= %s"
            params.append(collect_time_to)

        query += f"""
                GROUP BY
                    r.dataset_family,
                    r.logical_entity_key,
                    r.collect_time,
                    r.entity_level,
                    r.site_code,
                    r.region_code,
                    c.counter_alias
            ),
            pivoted AS (
                SELECT
                    dataset_family,
                    logical_entity_key,
                    collect_time,
                    entity_level,
                    site_code,
                    region_code,
                    {", ".join(pivot_columns)}
                FROM filtered_inputs
                GROUP BY
                    dataset_family,
                    logical_entity_key,
                    collect_time,
                    entity_level,
                    site_code,
                    region_code
            )
            {family_specs[family]["select"]}
        """
        params.extend([limit, offset])
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def _list_verified_topology_rollup_results(
        self,
        *,
        family: str,
        grain: str,
        dataset_family: str,
        site_code: str | None,
        region_code: str | None,
        limit: int,
        offset: int,
        collect_time_from: datetime | None,
        collect_time_to: datetime | None,
    ) -> list[dict]:
        family_specs: dict[str, dict[str, object]] = {
            "prb": {
                "aliases": {
                    "dl_prb_used": ("sum_counter_value", "dl_prb_used"),
                    "dl_prb_available": ("sum_counter_value", "dl_prb_available"),
                    "ul_prb_used": ("sum_counter_value", "ul_prb_used"),
                    "ul_prb_available": ("sum_counter_value", "ul_prb_available"),
                },
                "entity_select": """
                    SELECT
                        dataset_family,
                        collect_time,
                        logical_entity_key,
                        site_code,
                        site_name,
                        region_code,
                        region_name,
                        CASE
                            WHEN dl_prb_available IS NULL OR dl_prb_available = 0 THEN NULL
                            ELSE 100.0 * dl_prb_used / dl_prb_available
                        END AS dl_metric,
                        CASE
                            WHEN ul_prb_available IS NULL OR ul_prb_available = 0 THEN NULL
                            ELSE 100.0 * ul_prb_used / ul_prb_available
                        END AS ul_metric
                    FROM entity_pivot
                    WHERE dl_prb_available IS NOT NULL
                       OR ul_prb_available IS NOT NULL
                """,
                "site_select": """
                    SELECT
                        dataset_family,
                        site_code,
                        site_name,
                        collect_time,
                        AVG(dl_metric) AS dl_prb_utilization,
                        AVG(ul_metric) AS ul_prb_utilization
                    FROM entity_metrics
                    WHERE TRUE
                    GROUP BY dataset_family, site_code, site_name, collect_time
                    ORDER BY collect_time DESC, dataset_family, site_code
                    LIMIT %s
                    OFFSET %s
                """,
                "region_select": """
                    SELECT
                        dataset_family,
                        region_code,
                        region_name,
                        collect_time,
                        AVG(dl_metric) AS dl_prb_utilization,
                        AVG(ul_metric) AS ul_prb_utilization
                    FROM entity_metrics
                    WHERE TRUE
                    GROUP BY dataset_family, region_code, region_name, collect_time
                    ORDER BY collect_time DESC, dataset_family, region_code
                    LIMIT %s
                    OFFSET %s
                """,
            },
            "bler": {
                "aliases": {
                    "dl_tb_error_blocks": ("sum_counter_value", "dl_tb_error_blocks"),
                    "dl_tb_total_blocks": ("sum_counter_value", "dl_tb_total_blocks"),
                    "ul_tb_error_blocks": ("sum_counter_value", "ul_tb_error_blocks"),
                    "ul_tb_total_blocks": ("sum_counter_value", "ul_tb_total_blocks"),
                },
                "entity_select": """
                    SELECT
                        dataset_family,
                        collect_time,
                        logical_entity_key,
                        site_code,
                        site_name,
                        region_code,
                        region_name,
                        CASE
                            WHEN dl_tb_total_blocks IS NULL OR dl_tb_total_blocks = 0 THEN NULL
                            ELSE 100.0 * dl_tb_error_blocks / dl_tb_total_blocks
                        END AS dl_metric,
                        CASE
                            WHEN ul_tb_total_blocks IS NULL OR ul_tb_total_blocks = 0 THEN NULL
                            ELSE 100.0 * ul_tb_error_blocks / ul_tb_total_blocks
                        END AS ul_metric
                    FROM entity_pivot
                    WHERE dl_tb_total_blocks IS NOT NULL
                       OR ul_tb_total_blocks IS NOT NULL
                """,
                "site_select": """
                    SELECT
                        dataset_family,
                        site_code,
                        site_name,
                        collect_time,
                        AVG(dl_metric) AS dl_bler,
                        AVG(ul_metric) AS ul_bler
                    FROM entity_metrics
                    WHERE TRUE
                    GROUP BY dataset_family, site_code, site_name, collect_time
                    ORDER BY collect_time DESC, dataset_family, site_code
                    LIMIT %s
                    OFFSET %s
                """,
                "region_select": """
                    SELECT
                        dataset_family,
                        region_code,
                        region_name,
                        collect_time,
                        AVG(dl_metric) AS dl_bler,
                        AVG(ul_metric) AS ul_bler
                    FROM entity_metrics
                    WHERE TRUE
                    GROUP BY dataset_family, region_code, region_name, collect_time
                    ORDER BY collect_time DESC, dataset_family, region_code
                    LIMIT %s
                    OFFSET %s
                """,
            },
        }
        alias_specs = family_specs[family]["aliases"]  # type: ignore[index]
        aliases = tuple(alias_specs.keys())  # type: ignore[union-attr]
        counter_specs = self._get_verified_counter_specs(
            dataset_family=dataset_family,
            aliases=aliases,
        )
        if not counter_specs:
            return []
        counter_ids = [row["counter_id"] for row in counter_specs]
        counter_aliases = [row["counter_alias"] for row in counter_specs]

        entity_pivot_columns = []
        for counter_alias, (value_column, result_alias) in alias_specs.items():  # type: ignore[union-attr]
            entity_pivot_columns.append(
                f"MAX({value_column}) FILTER (WHERE counter_alias = '{counter_alias}') AS {result_alias}"
            )

        topology_filter = "site_code IS NOT NULL" if grain == "site-time" else "region_code IS NOT NULL"
        topology_code_filter = ""
        filter_value: str | None = site_code if grain == "site-time" else region_code
        params: list[object] = [counter_ids, counter_aliases, dataset_family, counter_ids]
        if collect_time_from is not None:
            topology_code_filter += "\n                  AND s.collect_time >= %s"
            params.append(collect_time_from)
        if collect_time_to is not None:
            topology_code_filter += "\n                  AND s.collect_time <= %s"
            params.append(collect_time_to)

        logical_entity_key_expression = """
            CASE
                WHEN n.dataset_family = 'PM/sdr/ltefdd' THEN
                    concat(
                        'family=', COALESCE(n.dataset_family, 'UNKNOWN'),
                        '|sbnid=', COALESCE(n.sbnid, ''),
                        '|enodebid=', COALESCE(n.enodebid, ''),
                        '|cellid=', COALESCE(n.cellid, '')
                    )
                WHEN n.dataset_family = 'PM/itbbu/ltefdd' THEN
                    concat(
                        'family=', COALESCE(n.dataset_family, 'UNKNOWN'),
                        '|sbnid=', COALESCE(n.sbnid, ''),
                        '|enbid=', COALESCE(n.enbid, ''),
                        '|cellid=', COALESCE(n.cellid, '')
                    )
                WHEN n.dataset_family = 'PM/itbbu/itbbuplat' THEN
                    concat(
                        'family=', COALESCE(n.dataset_family, 'UNKNOWN'),
                        '|sbnid=', COALESCE(n.sbnid, ''),
                        '|meid=', COALESCE(n.meid, '')
                    )
                ELSE
                    concat(
                        'family=', COALESCE(n.dataset_family, 'UNKNOWN'),
                        '|sbnid=', COALESCE(n.sbnid, ''),
                        '|enbid=', COALESCE(n.enbid, ''),
                        '|enodebid=', COALESCE(n.enodebid, ''),
                        '|cellid=', COALESCE(n.cellid, ''),
                        '|meid=', COALESCE(n.meid, ''),
                        '|ani=', COALESCE(n.ani, '')
                    )
            END
        """

        query = f"""
            WITH selected_counters AS (
                SELECT *
                FROM unnest(%s::text[], %s::text[]) AS c(counter_id, counter_alias)
            ),
            narrowed_raw AS (
                SELECT
                    s.dataset_family,
                    s.collect_time,
                    s.sbnid,
                    s.enbid,
                    s.enodebid,
                    s.cellid,
                    s.meid,
                    s.ani,
                    c.counter_alias,
                    s.counter_value
                FROM pm_ltefdd_sample AS s
                JOIN selected_counters AS c
                    ON c.counter_id = s.counter_id
                WHERE s.dataset_family = %s
                  AND s.counter_id = ANY(%s)
                  {topology_code_filter}
            ),
            entity_inputs AS (
                SELECT
                    n.dataset_family,
                    n.collect_time,
                    {logical_entity_key_expression} AS logical_entity_key,
                    n.counter_alias,
                    SUM(n.counter_value) AS sum_counter_value
                FROM narrowed_raw AS n
                GROUP BY
                    n.dataset_family,
                    n.collect_time,
                    logical_entity_key,
                    n.counter_alias
            ),
            topology_inputs AS (
                SELECT
                    e.dataset_family,
                    e.collect_time,
                    e.logical_entity_key,
                    t.site_code,
                    t.site_name,
                    t.region_code,
                    t.region_name,
                    e.counter_alias,
                    e.sum_counter_value
                FROM entity_inputs AS e
                JOIN ref_lte_entity_topology_enrichment AS t
                    ON t.logical_entity_key = e.logical_entity_key
                WHERE t.{topology_filter}
            ),
            entity_pivot AS (
                SELECT
                    dataset_family,
                    collect_time,
                    logical_entity_key,
                    site_code,
                    site_name,
                    region_code,
                    region_name,
                    {", ".join(entity_pivot_columns)}
                FROM topology_inputs
                GROUP BY
                    dataset_family,
                    collect_time,
                    logical_entity_key,
                    site_code,
                    site_name,
                    region_code,
                    region_name
            ),
            entity_metrics AS (
                {family_specs[family]["entity_select"]}
            )
        """
        rollup_select = family_specs[family]["site_select"] if grain == "site-time" else family_specs[family]["region_select"]  # type: ignore[operator]
        if filter_value is not None:
            rollup_select = rollup_select.replace(
                "WHERE TRUE",
                "WHERE TRUE\n  AND site_code = %s" if grain == "site-time" else "WHERE TRUE\n  AND region_code = %s",
            )
            params.append(filter_value)
        query += rollup_select
        params.extend([limit, offset])

        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def list_verified_kpi_validation(
        self,
        *,
        family: str,
        grain: str,
        dataset_family: str | None = None,
    ) -> list[dict]:
        if grain == "entity-time" and family in {"prb", "bler"}:
            return self._list_fast_entity_time_validation(family=family, dataset_family=dataset_family)

        query_specs: dict[tuple[str, str], str] = {
            (
                "prb",
                "entity-time",
            ): """
                SELECT
                    kpi_code,
                    dataset_family,
                    covered_input_aliases,
                    min_counter_verification_status,
                    min_input_collect_times,
                    min_input_entities,
                    executed_rows,
                    executed_collect_times,
                    executed_entities
                FROM vw_verified_prb_kpi_execution_validation
                ORDER BY kpi_code, dataset_family
            """,
            (
                "bler",
                "entity-time",
            ): """
                SELECT
                    kpi_code,
                    dataset_family,
                    covered_input_aliases,
                    min_counter_verification_status,
                    min_input_collect_times,
                    min_input_entities,
                    executed_rows,
                    executed_collect_times,
                    executed_entities
                FROM vw_verified_bler_kpi_execution_validation
                ORDER BY kpi_code, dataset_family
            """,
            (
                "rrc",
                "entity-time",
            ): """
                SELECT
                    dataset_family,
                    entity_time_rows,
                    rows_with_rrc_connected_users_max,
                    rows_with_rrc_connected_users_mean,
                    rows_with_rrc_connected_users_online
                FROM vw_verified_rrc_kpi_output_validation
                ORDER BY dataset_family
            """,
            (
                "prb",
                "site-time",
            ): """
                SELECT
                    dataset_family,
                    site_time_rows,
                    rows_with_dl_prb_utilization,
                    rows_with_ul_prb_utilization
                FROM vw_verified_prb_kpi_site_time_validation
                ORDER BY dataset_family
            """,
            (
                "bler",
                "site-time",
            ): """
                SELECT
                    dataset_family,
                    site_time_rows,
                    rows_with_dl_bler,
                    rows_with_ul_bler
                FROM vw_verified_bler_kpi_site_time_validation
                ORDER BY dataset_family
            """,
            (
                "rrc",
                "site-time",
            ): """
                SELECT
                    dataset_family,
                    site_time_rows,
                    rows_with_rrc_connected_users_max,
                    rows_with_rrc_connected_users_mean,
                    rows_with_rrc_connected_users_online
                FROM vw_verified_rrc_kpi_site_time_validation
                ORDER BY dataset_family
            """,
            (
                "prb",
                "region-time",
            ): """
                SELECT
                    dataset_family,
                    region_time_rows,
                    rows_with_dl_prb_utilization,
                    rows_with_ul_prb_utilization
                FROM vw_verified_prb_kpi_region_time_validation
                ORDER BY dataset_family
            """,
            (
                "bler",
                "region-time",
            ): """
                SELECT
                    dataset_family,
                    region_time_rows,
                    rows_with_dl_bler,
                    rows_with_ul_bler
                FROM vw_verified_bler_kpi_region_time_validation
                ORDER BY dataset_family
            """,
            (
                "rrc",
                "region-time",
            ): """
                SELECT
                    dataset_family,
                    region_time_rows,
                    rows_with_rrc_connected_users_max,
                    rows_with_rrc_connected_users_mean,
                    rows_with_rrc_connected_users_online
                FROM vw_verified_rrc_kpi_region_time_validation
                ORDER BY dataset_family
            """,
        }
        query = query_specs[(family, grain)]
        params: list[object] = []
        if dataset_family is not None:
            query = query.replace(
                "\n                ORDER BY",
                "\n                WHERE dataset_family = %s\n                ORDER BY",
                1,
            )
            params.append(dataset_family)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def _get_entity_time_validation_input_specs(self, *, kpi_codes: Sequence[str]) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    i.kpi_code,
                    i.dataset_family,
                    i.input_alias,
                    c.counter_id,
                    c.verification_status AS counter_verification_status
                FROM ref_semantic_kpi_definition AS d
                JOIN ref_semantic_kpi_formula_input AS i
                    ON i.kpi_code = d.kpi_code
                JOIN ref_semantic_counter_dictionary AS c
                    ON c.dataset_family = i.dataset_family
                   AND c.counter_alias = i.counter_alias
                WHERE d.kpi_code = ANY(%s)
                  AND d.verification_status = 'VERIFIED'
                  AND i.required = TRUE
                ORDER BY
                    i.kpi_code,
                    i.dataset_family,
                    i.input_alias,
                    c.counter_id
                """,
                (list(kpi_codes),),
            )
            return list(cursor.fetchall())

    def _list_fast_entity_time_validation(
        self,
        *,
        family: str,
        dataset_family: str | None = None,
    ) -> list[dict]:
        family_kpi_codes: dict[str, tuple[str, ...]] = {
            "prb": ("dl_prb_utilization", "ul_prb_utilization"),
            "bler": ("dl_bler", "ul_bler"),
            # RRC can plug into the same path once its reference rows are loaded.
            "rrc": (
                "rrc_connected_users_max",
                "rrc_connected_users_mean",
                "rrc_connected_users_online",
            ),
        }
        input_specs = self._get_entity_time_validation_input_specs(
            kpi_codes=family_kpi_codes[family],
        )
        if not input_specs:
            return []

        grouped_specs: dict[str, list[dict]] = {}
        for row in input_specs:
            if dataset_family is not None and row["dataset_family"] != dataset_family:
                continue
            grouped_specs.setdefault(row["dataset_family"], []).append(row)

        rows: list[dict] = []
        for dataset_family, dataset_specs in grouped_specs.items():
            entity_identity_expr = self._entity_identity_expr(dataset_family)
            coverage_rows = self._fetch_entity_time_validation_coverage(
                dataset_family=dataset_family,
                dataset_specs=dataset_specs,
                entity_identity_expr=entity_identity_expr,
            )
            executed_rows = self._fetch_entity_time_validation_executed(
                dataset_family=dataset_family,
                dataset_specs=dataset_specs,
                entity_identity_expr=entity_identity_expr,
            )

            coverage_by_key = {
                (row["kpi_code"], row["input_alias"]): row for row in coverage_rows
            }
            executed_by_kpi = {
                row["kpi_code"]: row for row in executed_rows
            }
            grouped_kpis: dict[str, list[dict]] = {}
            for spec in dataset_specs:
                grouped_kpis.setdefault(spec["kpi_code"], []).append(spec)

            for kpi_code, kpi_specs in grouped_kpis.items():
                per_input_rows = [
                    coverage_by_key.get((kpi_code, spec["input_alias"]))
                    for spec in kpi_specs
                    if coverage_by_key.get((kpi_code, spec["input_alias"])) is not None
                ]
                executed = executed_by_kpi.get(kpi_code)
                rows.append(
                    {
                        "kpi_code": kpi_code,
                        "dataset_family": dataset_family,
                        "covered_input_aliases": len(per_input_rows),
                        "min_counter_verification_status": min(
                            spec["counter_verification_status"] for spec in kpi_specs
                        ),
                        "min_input_collect_times": min(
                            row["distinct_collect_times"] for row in per_input_rows
                        )
                        if per_input_rows
                        else None,
                        "min_input_entities": min(
                            row["distinct_entities"] for row in per_input_rows
                        )
                        if per_input_rows
                        else None,
                        "executed_rows": executed["executed_rows"] if executed is not None else 0,
                        "executed_collect_times": executed["executed_collect_times"]
                        if executed is not None
                        else 0,
                        "executed_entities": executed["executed_entities"] if executed is not None else 0,
                    }
                )

        return sorted(rows, key=lambda row: (row["kpi_code"], row["dataset_family"]))

    @staticmethod
    def _entity_identity_expr(dataset_family: str) -> str:
        if dataset_family == "PM/sdr/ltefdd":
            return "ROW(s.sbnid, s.enodebid, s.cellid)"
        if dataset_family == "PM/itbbu/ltefdd":
            return "ROW(s.sbnid, s.enbid, s.cellid)"
        return "ROW(s.sbnid, s.enbid, s.enodebid, s.cellid, s.meid, s.ani)"

    def _fetch_entity_time_validation_coverage(
        self,
        *,
        dataset_family: str,
        dataset_specs: Sequence[dict],
        entity_identity_expr: str,
    ) -> list[dict]:
        counter_ids = [row["counter_id"] for row in dataset_specs]
        kpi_codes = [row["kpi_code"] for row in dataset_specs]
        input_aliases = [row["input_alias"] for row in dataset_specs]
        query = f"""
            WITH selected_inputs AS (
                SELECT *
                FROM unnest(
                    %s::text[],
                    %s::text[],
                    %s::text[]
                ) AS si(counter_id, kpi_code, input_alias)
            )
            SELECT
                si.kpi_code,
                si.input_alias,
                COUNT(DISTINCT s.collect_time) AS distinct_collect_times,
                COUNT(DISTINCT {entity_identity_expr}) AS distinct_entities
            FROM pm_ltefdd_sample AS s
            JOIN selected_inputs AS si
                ON si.counter_id = s.counter_id
            WHERE s.dataset_family = %s
              AND s.counter_id = ANY(%s)
            GROUP BY
                si.kpi_code,
                si.input_alias
            ORDER BY
                si.kpi_code,
                si.input_alias
        """
        params = (counter_ids, kpi_codes, input_aliases, dataset_family, sorted(set(counter_ids)))
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())

    def _fetch_entity_time_validation_executed(
        self,
        *,
        dataset_family: str,
        dataset_specs: Sequence[dict],
        entity_identity_expr: str,
    ) -> list[dict]:
        counter_ids = [row["counter_id"] for row in dataset_specs]
        kpi_codes = [row["kpi_code"] for row in dataset_specs]
        input_aliases = [row["input_alias"] for row in dataset_specs]
        required_input_aliases = []
        required_counts_by_kpi: dict[str, set[str]] = {}
        for row in dataset_specs:
            required_counts_by_kpi.setdefault(row["kpi_code"], set()).add(row["input_alias"])
        normalized_required_counts = {
            kpi_code: len(input_alias_set)
            for kpi_code, input_alias_set in required_counts_by_kpi.items()
        }
        required_input_aliases = [normalized_required_counts[row["kpi_code"]] for row in dataset_specs]

        query = f"""
            WITH selected_inputs AS (
                SELECT *
                FROM unnest(
                    %s::text[],
                    %s::text[],
                    %s::text[],
                    %s::int[]
                ) AS si(counter_id, kpi_code, input_alias, required_input_aliases)
            )
            SELECT
                entity_inputs.kpi_code,
                COUNT(*) AS executed_rows,
                COUNT(DISTINCT entity_inputs.collect_time) AS executed_collect_times,
                COUNT(DISTINCT entity_inputs.entity_key) AS executed_entities
            FROM (
                SELECT
                    si.kpi_code,
                    s.collect_time,
                    {entity_identity_expr} AS entity_key,
                    COUNT(DISTINCT si.input_alias) AS present_input_aliases,
                    MAX(si.required_input_aliases) AS required_input_aliases
                FROM pm_ltefdd_sample AS s
                JOIN selected_inputs AS si
                    ON si.counter_id = s.counter_id
                WHERE s.dataset_family = %s
                  AND s.counter_id = ANY(%s)
                GROUP BY
                    si.kpi_code,
                    s.collect_time,
                    {entity_identity_expr}
                HAVING COUNT(DISTINCT si.input_alias) = MAX(si.required_input_aliases)
            ) AS entity_inputs
            GROUP BY
                entity_inputs.kpi_code
            ORDER BY
                entity_inputs.kpi_code
        """
        params = (
            counter_ids,
            kpi_codes,
            input_aliases,
            required_input_aliases,
            dataset_family,
            sorted(set(counter_ids)),
        )
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())

    def upsert_counter_dictionary(self, rows: Sequence[SemanticCounterDictionarySeedRow]) -> dict[str, int]:
        if not rows:
            return {
                "dictionary_rows_loaded": 0,
                "group_rows_loaded": 0,
                "group_member_rows_loaded": 0,
            }

        group_rows = list(
            dict.fromkeys(
                (row.group_code, row.group_name or row.group_code, row.group_notes)
                for row in rows
                if row.group_code is not None
            )
        )
        dictionary_payload = [
            (
                row.dataset_family,
                row.counter_id,
                row.counter_alias,
                row.counter_name,
                row.unit,
                row.aggregation_behavior,
                row.verification_status,
                row.source_note,
            )
            for row in rows
        ]
        group_member_rows = list(
            dict.fromkeys(
                (row.group_code, row.dataset_family, row.counter_alias, row.group_notes)
                for row in rows
                if row.group_code is not None
            )
        )

        with self.connection.cursor() as cursor:
            if group_rows:
                cursor.executemany(
                    """
                    INSERT INTO ref_semantic_counter_group (
                        group_code,
                        group_name,
                        notes
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (group_code) DO UPDATE
                    SET
                        group_name = EXCLUDED.group_name,
                        notes = EXCLUDED.notes,
                        updated_at = NOW()
                    """,
                    group_rows,
                )
            cursor.executemany(
                """
                INSERT INTO ref_semantic_counter_dictionary (
                    dataset_family,
                    counter_id,
                    counter_alias,
                    counter_name,
                    unit,
                    aggregation_behavior,
                    verification_status,
                    source_note
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (dataset_family, counter_id) DO UPDATE
                SET
                    counter_alias = EXCLUDED.counter_alias,
                    counter_name = EXCLUDED.counter_name,
                    unit = EXCLUDED.unit,
                    aggregation_behavior = EXCLUDED.aggregation_behavior,
                    verification_status = EXCLUDED.verification_status,
                    source_note = EXCLUDED.source_note,
                    updated_at = NOW()
                """,
                dictionary_payload,
            )
            if group_rows:
                group_codes = sorted({row[0] for row in group_rows})
                cursor.execute(
                    "DELETE FROM ref_semantic_counter_group_member WHERE group_code = ANY(%s)",
                    (group_codes,),
                )
                if group_member_rows:
                    cursor.executemany(
                        """
                        INSERT INTO ref_semantic_counter_group_member (
                            group_code,
                            dataset_family,
                            counter_alias,
                            notes
                        )
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (group_code, dataset_family, counter_alias) DO UPDATE
                        SET
                            notes = EXCLUDED.notes
                        """,
                        group_member_rows,
                    )
        self.connection.commit()
        return {
            "dictionary_rows_loaded": len(dictionary_payload),
            "group_rows_loaded": len(group_rows),
            "group_member_rows_loaded": len(group_member_rows),
        }

    def upsert_kpi_definitions(self, rows: Sequence[SemanticKpiDefinitionSeedRow]) -> dict[str, int]:
        if not rows:
            return {"kpi_rows_loaded": 0, "input_rows_loaded": 0}

        definition_rows = list(
            dict.fromkeys(
                (
                    row.kpi_code,
                    row.kpi_name,
                    row.formula_expression,
                    row.grain,
                    row.unit,
                    row.verification_status,
                    row.topology_rollup_allowed,
                    row.notes,
                )
                for row in rows
            )
        )
        input_rows = [
            (
                row.kpi_code,
                row.input_alias,
                row.dataset_family,
                row.counter_alias,
                row.required,
                row.input_notes,
            )
            for row in rows
            if row.input_alias is not None and row.dataset_family is not None and row.counter_alias is not None
        ]
        kpi_codes = sorted({row.kpi_code for row in rows})

        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_semantic_kpi_definition (
                    kpi_code,
                    kpi_name,
                    formula_expression,
                    grain,
                    unit,
                    verification_status,
                    topology_rollup_allowed,
                    notes
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (kpi_code) DO UPDATE
                SET
                    kpi_name = EXCLUDED.kpi_name,
                    formula_expression = EXCLUDED.formula_expression,
                    grain = EXCLUDED.grain,
                    unit = EXCLUDED.unit,
                    verification_status = EXCLUDED.verification_status,
                    topology_rollup_allowed = EXCLUDED.topology_rollup_allowed,
                    notes = EXCLUDED.notes,
                    updated_at = NOW()
                """,
                definition_rows,
            )
            cursor.execute(
                "DELETE FROM ref_semantic_kpi_formula_input WHERE kpi_code = ANY(%s)",
                (kpi_codes,),
            )
            if input_rows:
                cursor.executemany(
                    """
                    INSERT INTO ref_semantic_kpi_formula_input (
                        kpi_code,
                        input_alias,
                        dataset_family,
                        counter_alias,
                        required,
                        notes
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (kpi_code, input_alias, dataset_family, counter_alias) DO UPDATE
                    SET
                        required = EXCLUDED.required,
                        notes = EXCLUDED.notes
                    """,
                    input_rows,
                )
        self.connection.commit()
        return {"kpi_rows_loaded": len(definition_rows), "input_rows_loaded": len(input_rows)}

    def list_unmapped_counters(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                counter_id,
                row_count,
                distinct_logical_entities,
                min_collect_time,
                max_collect_time
            FROM vw_semantic_counter_mapping_gaps
            ORDER BY row_count DESC, dataset_family, counter_id
            LIMIT %s
            """,
            limit,
        )

    def list_provisional_kpis(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                kpi_name,
                formula_expression,
                grain,
                unit,
                verification_status,
                topology_rollup_allowed,
                notes,
                input_count,
                input_aliases,
                counter_aliases,
                unverified_counter_aliases
            FROM vw_semantic_provisional_kpis
            ORDER BY kpi_code
            LIMIT %s
            """,
            limit,
        )

    def summarize_kpi_input_coverage(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                kpi_name,
                input_alias,
                dataset_family,
                counter_alias,
                required,
                counter_verification_status,
                distinct_logical_entities,
                distinct_collect_times,
                row_count
            FROM vw_semantic_kpi_input_coverage
            ORDER BY row_count DESC, kpi_code, input_alias, dataset_family
            LIMIT %s
            """,
            limit,
        )

    def list_verified_prb_kpi_outputs(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                kpi_name,
                dataset_family,
                collect_time,
                logical_entity_key,
                entity_level,
                site_code,
                region_code,
                numerator_counter_alias,
                denominator_counter_alias,
                numerator_value,
                denominator_value,
                kpi_value,
                unit
            FROM vw_verified_prb_kpi_entity_time
            ORDER BY collect_time DESC, kpi_code, dataset_family, logical_entity_key
            LIMIT %s
            """,
            limit,
        )

    def summarize_verified_prb_kpi_execution(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                dataset_family,
                covered_input_aliases,
                min_counter_verification_status,
                min_input_collect_times,
                min_input_entities,
                executed_rows,
                executed_collect_times,
                executed_entities
            FROM vw_verified_prb_kpi_execution_validation
            ORDER BY kpi_code, dataset_family
            LIMIT %s
            """,
            limit,
        )

    def list_verified_bler_kpi_outputs(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                kpi_name,
                dataset_family,
                collect_time,
                logical_entity_key,
                entity_level,
                site_code,
                region_code,
                numerator_counter_alias,
                denominator_counter_alias,
                numerator_value,
                denominator_value,
                kpi_value,
                unit
            FROM vw_verified_bler_kpi_entity_time
            ORDER BY collect_time DESC, kpi_code, dataset_family, logical_entity_key
            LIMIT %s
            """,
            limit,
        )

    def summarize_verified_bler_kpi_execution(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                kpi_code,
                dataset_family,
                covered_input_aliases,
                min_counter_verification_status,
                min_input_collect_times,
                min_input_entities,
                executed_rows,
                executed_collect_times,
                executed_entities
            FROM vw_verified_bler_kpi_execution_validation
            ORDER BY kpi_code, dataset_family
            LIMIT %s
            """,
            limit,
        )

    def list_verified_rrc_kpi_entity_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                logical_entity_key,
                collect_time,
                rrc_connected_users_max,
                rrc_connected_users_mean,
                rrc_connected_users_online
            FROM vw_verified_rrc_kpi_entity_time
            ORDER BY collect_time DESC, dataset_family, logical_entity_key
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_rrc_kpi_entity_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                entity_time_rows,
                rows_with_rrc_connected_users_max,
                rows_with_rrc_connected_users_mean,
                rows_with_rrc_connected_users_online
            FROM vw_verified_rrc_kpi_output_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_prb_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                site,
                collect_time,
                dl_prb_utilization,
                ul_prb_utilization
            FROM vw_verified_prb_kpi_site_time
            ORDER BY collect_time DESC, dataset_family, site
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_prb_kpi_site_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                site_time_rows,
                rows_with_dl_prb_utilization,
                rows_with_ul_prb_utilization
            FROM vw_verified_prb_kpi_site_time_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_bler_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                site,
                collect_time,
                dl_bler,
                ul_bler
            FROM vw_verified_bler_kpi_site_time
            ORDER BY collect_time DESC, dataset_family, site
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_bler_kpi_site_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                site_time_rows,
                rows_with_dl_bler,
                rows_with_ul_bler
            FROM vw_verified_bler_kpi_site_time_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_rrc_kpi_site_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                site,
                collect_time,
                rrc_connected_users_max,
                rrc_connected_users_mean,
                rrc_connected_users_online
            FROM vw_verified_rrc_kpi_site_time
            ORDER BY collect_time DESC, dataset_family, site
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_rrc_kpi_site_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                site_time_rows,
                rows_with_rrc_connected_users_max,
                rows_with_rrc_connected_users_mean,
                rows_with_rrc_connected_users_online
            FROM vw_verified_rrc_kpi_site_time_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_prb_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                region,
                collect_time,
                dl_prb_utilization,
                ul_prb_utilization
            FROM vw_verified_prb_kpi_region_time
            ORDER BY collect_time DESC, dataset_family, region
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_prb_kpi_region_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                region_time_rows,
                rows_with_dl_prb_utilization,
                rows_with_ul_prb_utilization
            FROM vw_verified_prb_kpi_region_time_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_bler_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                region,
                collect_time,
                dl_bler,
                ul_bler
            FROM vw_verified_bler_kpi_region_time
            ORDER BY collect_time DESC, dataset_family, region
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_bler_kpi_region_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                region_time_rows,
                rows_with_dl_bler,
                rows_with_ul_bler
            FROM vw_verified_bler_kpi_region_time_validation
            ORDER BY dataset_family
            """
        )

    def list_verified_rrc_kpi_region_time(self, limit: int = 100) -> list[dict]:
        return self._fetch_limited_rows(
            """
            SELECT
                dataset_family,
                region,
                collect_time,
                rrc_connected_users_max,
                rrc_connected_users_mean,
                rrc_connected_users_online
            FROM vw_verified_rrc_kpi_region_time
            ORDER BY collect_time DESC, dataset_family, region
            LIMIT %s
            """,
            limit,
        )

    def validate_verified_rrc_kpi_region_time(self) -> list[dict]:
        return self._fetch_rows(
            """
            SELECT
                dataset_family,
                region_time_rows,
                rows_with_rrc_connected_users_max,
                rows_with_rrc_connected_users_mean,
                rows_with_rrc_connected_users_online
            FROM vw_verified_rrc_kpi_region_time_validation
            ORDER BY dataset_family
            """
        )

    def upsert_vendor_indicators(self, rows: Sequence[VendorIndicatorSeedRow]) -> dict[str, int]:
        if not rows:
            return {"vendor_indicator_rows_loaded": 0, "lineage_rows_loaded": 0}

        dictionary_payload = [
            (
                row.indicator_code,
                row.indicator_name,
                row.semantic_alias,
                row.aggregation_method,
                row.unit,
                row.verification_status,
                row.source,
            )
            for row in rows
        ]
        lineage_payload = [
            (
                row.indicator_code,
                row.lineage_expression,
                row.lineage_type,
                row.raw_counter_dependencies,
            )
            for row in rows
        ]

        with self.connection.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ref_vendor_indicator_dictionary (
                    indicator_code,
                    indicator_name,
                    semantic_alias,
                    aggregation_method,
                    unit,
                    verification_status,
                    source
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (indicator_code) DO UPDATE
                SET
                    indicator_name = EXCLUDED.indicator_name,
                    semantic_alias = EXCLUDED.semantic_alias,
                    aggregation_method = EXCLUDED.aggregation_method,
                    unit = EXCLUDED.unit,
                    verification_status = EXCLUDED.verification_status,
                    source = EXCLUDED.source,
                    updated_at = NOW()
                """,
                dictionary_payload,
            )
            cursor.executemany(
                """
                INSERT INTO ref_vendor_indicator_lineage (
                    indicator_code,
                    lineage_expression,
                    lineage_type,
                    raw_counter_dependencies
                )
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (indicator_code) DO UPDATE
                SET
                    lineage_expression = EXCLUDED.lineage_expression,
                    lineage_type = EXCLUDED.lineage_type,
                    raw_counter_dependencies = EXCLUDED.raw_counter_dependencies,
                    updated_at = NOW()
                """,
                lineage_payload,
            )
        self.connection.commit()
        return {
            "vendor_indicator_rows_loaded": len(dictionary_payload),
            "lineage_rows_loaded": len(lineage_payload),
        }

    def list_vendor_indicators(self, limit: int = 100) -> list[dict]:
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    indicator_code,
                    indicator_name,
                    semantic_alias,
                    aggregation_method,
                    unit,
                    verification_status,
                    source,
                    lineage_expression,
                    lineage_type,
                    raw_counter_dependencies
                FROM vw_vendor_indicator_dictionary_details
                ORDER BY indicator_code
                LIMIT %s
                """,
                (limit,),
            )
            return list(cursor.fetchall())

    def fetch_vendor_indicators_by_aliases(self, semantic_aliases: Sequence[str]) -> list[dict]:
        if not semantic_aliases:
            return []
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                """
                SELECT
                    indicator_code,
                    indicator_name,
                    semantic_alias,
                    aggregation_method,
                    unit,
                    verification_status,
                    source,
                    lineage_expression,
                    lineage_type,
                    raw_counter_dependencies
                FROM vw_vendor_indicator_dictionary_details
                WHERE semantic_alias = ANY(%s)
                ORDER BY semantic_alias, indicator_code
                """,
                (list(semantic_aliases),),
            )
            return list(cursor.fetchall())
