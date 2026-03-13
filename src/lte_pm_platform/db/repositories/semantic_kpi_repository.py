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
        dataset_family: str | None = None,
        site_code: str | None = None,
        region_code: str | None = None,
        collect_time_from: datetime | None = None,
        collect_time_to: datetime | None = None,
    ) -> list[dict]:
        query_specs: dict[tuple[str, str], tuple[str, str]] = {
            (
                "prb",
                "entity-time",
            ): (
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
                """,
                "ORDER BY collect_time DESC, kpi_code, dataset_family, logical_entity_key",
            ),
            (
                "bler",
                "entity-time",
            ): (
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
                """,
                "ORDER BY collect_time DESC, kpi_code, dataset_family, logical_entity_key",
            ),
            (
                "rrc",
                "entity-time",
            ): (
                """
                SELECT
                    dataset_family,
                    logical_entity_key,
                    collect_time,
                    rrc_connected_users_max,
                    rrc_connected_users_mean,
                    rrc_connected_users_online
                FROM vw_verified_rrc_kpi_entity_time
                """,
                "ORDER BY collect_time DESC, dataset_family, logical_entity_key",
            ),
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
        query += f"\n{order_by}\nLIMIT %s"
        params.append(limit)
        with self.connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, tuple(params))
            return list(cursor.fetchall())

    def list_verified_kpi_validation(
        self,
        *,
        family: str,
        grain: str,
    ) -> list[dict]:
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
        return self._fetch_rows(query_specs[(family, grain)])

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
