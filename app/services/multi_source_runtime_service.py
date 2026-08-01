from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from app.core.config import Settings
from app.core.senior_analyst_policy import (
    MNQ_EARNINGS_SELECTION_POLICY,
    MNQ_PRIMARY_SYMBOLS,
)
from app.services import provider_capability_registry
from app.services.market_fact_repository import MarketFactRepository, now_iso
from app.services.data_freshness_service import (
    CanonicalFreshnessPolicy,
    CanonicalFreshnessResult,
    DataFreshnessService,
    evaluate_canonical_freshness,
    parse_datetime,
)
from app.services.positioning_runtime_service import PositioningRuntimeService
from app.services.provider_adapter_factory import (
    adapter_path_for,
    create_registered_adapter,
)
from app.services.provider_capability_registry import (
    dataset_policy_by_id,
    dataset_runtime_provider_order,
    provider_by_id,
)
from app.services.provider_observation_repository import ProviderObservationRepository
from app.infrastructure.persistence.provider_cache_repository import (
    ProviderCacheRepository,
)


FetchCallable = Callable[[], Awaitable[dict[str, Any]]]


FACT_TYPES = {
    "investing_economic_calendar": "investing_economic_calendar",
    "xtb_economic_calendar": "xtb_economic_calendar",
    "investing_holidays": "investing_holidays",
    "marketbeat_holidays": "marketbeat_holidays",
    "cme_market_schedule": "cme_market_schedule",
    "investing_fed_rate_monitor": "investing_fed_rate_monitor",
    "cboe_risk_indices": "cboe_risk_indices",
    "nasdaq_earnings": "earnings_event",
    "fmp_earnings": "fmp_earnings_calendar",
    "nasdaq_100": "nasdaq_100_constituents",
    "nasdaq_market_info": "nasdaq_market_info",
    "nasdaq_qqq_options": "nasdaq_qqq_options",
    "aaii_sentiment": "aaii_sentiment",
    "macromicro_aaii_crosscheck": "macromicro_aaii_crosscheck",
    "polymarket_prediction_markets": "polymarket_prediction_markets",
    "quikstrike_review": "quikstrike_review",
}
_POLICY_RUNTIME_DATASET_IDS = {
    "investing_economic_calendar": "macro_calendar",
    "xtb_economic_calendar": "macro_calendar",
    "investing_holidays": "market_schedule",
    "marketbeat_holidays": "market_schedule",
    "cme_market_schedule": "market_schedule",
    "nasdaq_market_info": "market_schedule",
    "investing_fed_rate_monitor": "fomc_expectations",
    "cboe_risk_indices": "risk",
    "nasdaq_earnings": "earnings",
    "fmp_earnings": "earnings",
    "nasdaq_100": "nasdaq_100",
    "nasdaq_qqq_options": "options_positioning",
}
_LOCAL_PROVIDER_CACHE_MAX_AGE = {
    "aaii_sentiment": timedelta(days=10),
    "macromicro_aaii_crosscheck": timedelta(days=10),
    "polymarket_prediction_markets": timedelta(hours=6),
    "quikstrike_review": timedelta(days=30),
}
_SUPPLEMENTAL_CONTEXT_DISABLED_REASON = "SUPPLEMENTAL_CONTEXT_DISABLED_FOR_REQUEST_ACCOUNTING"
_EARNINGS_RUNTIME_BLOCKS = {
    "NASDAQ": "nasdaq_earnings",
    "FMP_EARNINGS_CALENDAR": "fmp_earnings",
}
_MARKET_SCHEDULE_RUNTIME_SPECS: dict[str, dict[str, Any]] = {
    "NASDAQ_MARKET_INFO": {
        "block_name": "nasdaq_market_info",
        "item_count": lambda payload: 1 if payload.get("status") == "found" else 0,
        "source": "Nasdaq Market Info",
        "persist_unmaterialized": True,
    },
    "CME": {
        "block_name": "cme_market_schedule",
        "item_count": lambda payload: 1 if payload.get("calendar_verified") else 0,
        "source": "CME Group Trading Hours",
        "persist_unmaterialized": False,
    },
    "INVESTING_HOLIDAYS": {
        "block_name": "investing_holidays",
        "item_count": lambda payload: len(payload.get("holidays") or []),
        "source": "Investing Holiday Calendar",
        "persist_unmaterialized": True,
    },
    "MARKETBEAT": {
        "block_name": "marketbeat_holidays",
        "item_count": lambda payload: len(payload.get("holidays") or []),
        "source": "MarketBeat Stock Market Holidays",
        "persist_unmaterialized": False,
    },
}


def _provider_cache_max_age(name: str) -> timedelta:
    """Resolve a runtime cache SLA from its canonical dataset policy.

    The four non-Senior-Analyst review/enrichment runtimes keep their local
    operational TTLs.  Every runtime that materializes one of the 25 governed
    datasets is resolved dynamically so a policy update is effective for the
    very next lookup and persistence decision.
    """

    policy_names = set(_POLICY_RUNTIME_DATASET_IDS)
    local_names = set(_LOCAL_PROVIDER_CACHE_MAX_AGE)
    fact_names = set(FACT_TYPES)
    if policy_names & local_names or policy_names | local_names != fact_names:
        raise RuntimeError("RUNTIME_CACHE_POLICY_PARTITION_INVALID")

    if name in _LOCAL_PROVIDER_CACHE_MAX_AGE:
        return _LOCAL_PROVIDER_CACHE_MAX_AGE[name]

    dataset_id = _POLICY_RUNTIME_DATASET_IDS.get(name)
    if not dataset_id:
        raise RuntimeError(f"RUNTIME_CACHE_POLICY_MAPPING_MISSING:{name}")
    try:
        policy = dataset_policy_by_id(dataset_id)
    except KeyError as exc:
        raise RuntimeError(f"RUNTIME_CACHE_POLICY_REGISTRY_MISSING:{name}:{dataset_id}") from exc
    if (
        not isinstance(policy.sla_seconds, int)
        or isinstance(policy.sla_seconds, bool)
        or policy.sla_seconds <= 0
    ):
        raise RuntimeError(f"RUNTIME_CACHE_POLICY_SLA_INVALID:{name}:{dataset_id}")
    return timedelta(seconds=policy.sla_seconds)


def _earnings_provider_order() -> tuple[str, ...]:
    return dataset_runtime_provider_order(
        "earnings",
        _EARNINGS_RUNTIME_BLOCKS,
    )


def _market_schedule_provider_order() -> tuple[str, ...]:
    return dataset_runtime_provider_order(
        "market_schedule",
        _MARKET_SCHEDULE_RUNTIME_SPECS,
    )


def _market_schedule_capability_groups() -> tuple[tuple[str, tuple[str, ...]], ...]:
    """Group policy-ordered providers by their registered atomic capability.

    Market schedule is a fan-in dataset: cash-session, futures-session, and
    holiday observations are complementary.  Only providers for the same
    atomic capability form a fallback chain.  The registry is deliberately
    the sole source of those capability identities and their semantics.
    """

    provider_groups: dict[str, list[str]] = {}
    capability_signatures: dict[
        str,
        tuple[str, str, tuple[str, ...]],
    ] = {}
    for provider_id in _market_schedule_provider_order():
        capabilities = tuple(
            capability
            for capability in provider_by_id(provider_id).capabilities
            if capability.dataset_id == "market_schedule"
        )
        if len(capabilities) != 1:
            raise RuntimeError(
                "MARKET_SCHEDULE_RUNTIME_CAPABILITY_MAPPING_INVALID:"
                f"{provider_id}:{len(capabilities)}"
            )
        capability = capabilities[0]
        metric_id = str(capability.metric_id or "").strip()
        if not metric_id:
            raise RuntimeError(f"MARKET_SCHEDULE_RUNTIME_CAPABILITY_ID_MISSING:{provider_id}")
        signature = (
            str(capability.frequency or "").strip(),
            str(capability.transformation or "").strip(),
            tuple(sorted(capability.supported_fields)),
        )
        if not all(signature[:2]) or not signature[2]:
            raise RuntimeError(
                f"MARKET_SCHEDULE_RUNTIME_CAPABILITY_SEMANTICS_MISSING:{provider_id}:{metric_id}"
            )
        registered_signature = capability_signatures.setdefault(
            metric_id,
            signature,
        )
        if registered_signature != signature:
            raise RuntimeError(
                f"MARKET_SCHEDULE_RUNTIME_CAPABILITY_GROUP_INCONGRUENT:{metric_id}:{provider_id}"
            )
        provider_groups.setdefault(metric_id, []).append(provider_id)
    return tuple(
        (metric_id, tuple(provider_ids)) for metric_id, provider_ids in provider_groups.items()
    )


def _runtime_capability_spec(
    provider_id: str,
    *,
    dataset_id: str,
    metric_id: str | None = None,
) -> dict[str, Any]:
    """Resolve one provider's exact dataset capability and leaf adapter.

    Provider-level adapter membership is insufficient for providers such as
    Nasdaq, whose default adapter serves constituents while a distinct leaf
    adapter serves earnings.  The effective adapter path therefore comes from
    the matching capability, not from the provider's aggregate adapter list.
    """

    registration = provider_by_id(provider_id)
    capabilities = tuple(
        capability
        for capability in registration.capabilities
        if capability.dataset_id == dataset_id
        and (metric_id is None or capability.metric_id == metric_id)
    )
    if len(capabilities) != 1:
        raise RuntimeError(
            "RUNTIME_CAPABILITY_MAPPING_INVALID:"
            f"{dataset_id}:{provider_id}:{metric_id or '*'}:{len(capabilities)}"
        )
    capability = capabilities[0]
    effective_adapter_path = capability.probe_adapter_path or registration.adapter_path
    if not str(effective_adapter_path or "").strip():
        raise RuntimeError(
            f"RUNTIME_CAPABILITY_ADAPTER_MISSING:{dataset_id}:{provider_id}:{capability.metric_id}"
        )
    return {
        "capability": capability,
        "adapter_path": effective_adapter_path,
    }


def _runtime_adapter_name(path: str) -> str:
    _, separator, qualified_name = str(path or "").partition(":")
    if not separator or not qualified_name:
        raise RuntimeError(f"RUNTIME_CAPABILITY_ADAPTER_PATH_INVALID:{path}")
    return qualified_name.rsplit(".", 1)[-1]


def _earnings_runtime_spec(provider_id: str) -> dict[str, str]:
    block_name = _EARNINGS_RUNTIME_BLOCKS[provider_id]
    registration = provider_by_id(provider_id)
    capability_spec = _runtime_capability_spec(
        provider_id,
        dataset_id="earnings",
    )
    capability = capability_spec["capability"]
    if not registration.distributor:
        raise RuntimeError(f"EARNINGS_RUNTIME_DISTRIBUTOR_MISSING:{provider_id}")
    return {
        "attribute": block_name,
        "adapter_path": capability_spec["adapter_path"],
        "block_name": block_name,
        "capability_metric_id": capability.metric_id,
        "enabled_setting": f"enable_{block_name}",
        "fetcher": f"_fetch_{block_name}",
        "source": f"{registration.distributor} Earnings Calendar",
    }


class MultiSourceRuntimeService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.facts = MarketFactRepository(settings)
        self.freshness = DataFreshnessService(settings)
        self.observations = ProviderObservationRepository(settings)
        self.investing_calendar = create_registered_adapter(
            "INVESTING_ECONOMIC_CALENDAR",
            settings,
        )
        self.xtb_calendar = create_registered_adapter("XTB", settings)
        self.investing_holidays = create_registered_adapter(
            "INVESTING_HOLIDAYS",
            settings,
        )
        self.marketbeat_holidays = create_registered_adapter("MARKETBEAT", settings)
        self.cme_market_schedule = create_registered_adapter("CME", settings)
        self.investing_fed_rate_monitor = create_registered_adapter(
            "INVESTING_FED_RATE_MONITOR",
            settings,
        )
        self.cboe = create_registered_adapter("CBOE", settings)
        nasdaq_earnings_capability = _runtime_capability_spec(
            "NASDAQ",
            dataset_id="earnings",
        )
        self.nasdaq_earnings = create_registered_adapter(
            "NASDAQ",
            settings,
            adapter_name=_runtime_adapter_name(nasdaq_earnings_capability["adapter_path"]),
        )
        fmp_earnings_capability = _runtime_capability_spec(
            "FMP_EARNINGS_CALENDAR",
            dataset_id="earnings",
        )
        self.fmp_earnings = create_registered_adapter(
            "FMP_EARNINGS_CALENDAR",
            ProviderCacheRepository(settings.database_path),
            settings,
            adapter_name=_runtime_adapter_name(fmp_earnings_capability["adapter_path"]),
        )
        self.nasdaq_100 = create_registered_adapter("NASDAQ", settings)
        self.nasdaq_market_info = create_registered_adapter(
            "NASDAQ_MARKET_INFO",
            settings,
        )
        self.nasdaq_options = create_registered_adapter(
            "NASDAQ_QQQ_OPTIONS",
            settings,
        )
        self.positioning_runtime = PositioningRuntimeService(settings)
        self.macromicro = create_registered_adapter("MACROMICRO", settings)
        self.polymarket = create_registered_adapter("POLYMARKET", settings)

    async def snapshot(
        self,
        *,
        refresh: str = "auto",
        preloaded_blocks: dict[str, dict[str, Any]] | None = None,
        include_supplemental_context: bool = True,
    ) -> dict[str, Any]:
        preloaded_blocks = preloaded_blocks or {}
        earnings_blocks = await self._earnings_chain(
            refresh=refresh,
            preloaded_primary=preloaded_blocks.get("nasdaq_earnings"),
            preloaded_fallback=preloaded_blocks.get("fmp_earnings"),
        )
        schedule_blocks = await self._market_schedule_chain(
            refresh=refresh,
        )
        if include_supplemental_context:
            supplemental_blocks = {
                "nasdaq_100": await self._run_provider(
                    "nasdaq_100",
                    FACT_TYPES["nasdaq_100"],
                    self.nasdaq_100.fetch,
                    item_count=lambda payload: len(payload.get("constituents") or []),
                    enabled=self.settings.enable_nasdaq_100,
                    source="Nasdaq-100 Constituents",
                    refresh=refresh,
                ),
                "nasdaq_qqq_options": preloaded_blocks.get("nasdaq_qqq_options")
                or await self._run_provider(
                    "nasdaq_qqq_options",
                    FACT_TYPES["nasdaq_qqq_options"],
                    self.nasdaq_options.fetch,
                    item_count=lambda payload: len(payload.get("contracts") or []),
                    enabled=self.settings.enable_nasdaq_qqq_options,
                    source="Nasdaq QQQ Option Chain",
                    refresh=refresh,
                ),
                "aaii_sentiment": await self._run_aaii(refresh=refresh),
                "macromicro_aaii_crosscheck": await self._run_provider(
                    "macromicro_aaii_crosscheck",
                    FACT_TYPES["macromicro_aaii_crosscheck"],
                    self.macromicro.fetch,
                    item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                    enabled=self.settings.enable_macromicro_aaii_crosscheck,
                    source="MacroMicro AAII Cross-check",
                    refresh=refresh,
                ),
                "polymarket_prediction_markets": await self._run_provider(
                    "polymarket_prediction_markets",
                    FACT_TYPES["polymarket_prediction_markets"],
                    self.polymarket.fetch,
                    item_count=lambda payload: len(payload.get("markets") or []),
                    enabled=self.settings.enable_polymarket,
                    source="Polymarket",
                    refresh=refresh,
                ),
                "quikstrike_review": self._quikstrike_review(refresh=refresh),
            }
        else:
            supplemental_blocks = {
                name: _supplemental_runtime_block(source=source)
                for name, source in {
                    "nasdaq_100": "Nasdaq-100 Constituents",
                    "nasdaq_qqq_options": "Nasdaq QQQ Option Chain",
                    "aaii_sentiment": "AAII Sentiment Survey",
                    "macromicro_aaii_crosscheck": ("MacroMicro AAII Cross-check"),
                    "polymarket_prediction_markets": "Polymarket",
                    "quikstrike_review": ("CME QuikStrike Open Interest Heatmap"),
                }.items()
            }
        blocks = {
            "investing_economic_calendar": preloaded_blocks.get("investing_economic_calendar")
            or await self._run_provider(
                "investing_economic_calendar",
                FACT_TYPES["investing_economic_calendar"],
                self.investing_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_investing_calendar,
                source="Investing Economic Calendar",
                refresh=refresh,
            ),
            "investing_holidays": schedule_blocks["investing_holidays"],
            "marketbeat_holidays": schedule_blocks["marketbeat_holidays"],
            "xtb_economic_calendar": preloaded_blocks.get("xtb_economic_calendar")
            or await self._run_provider(
                "xtb_economic_calendar",
                FACT_TYPES["xtb_economic_calendar"],
                self.xtb_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_xtb_calendar,
                source="XTB Economic Calendar",
                refresh=refresh,
            ),
            "cme_market_schedule": schedule_blocks["cme_market_schedule"],
            "investing_fed_rate_monitor": preloaded_blocks.get("investing_fed_rate_monitor")
            or await self._run_provider(
                "investing_fed_rate_monitor",
                FACT_TYPES["investing_fed_rate_monitor"],
                self.investing_fed_rate_monitor.fetch,
                item_count=lambda payload: len(payload.get("meetings") or []),
                enabled=self.settings.enable_investing_fed_rate_monitor,
                source="Investing.com Fed Rate Monitor",
                refresh=refresh,
                persist_unmaterialized=False,
            ),
            "cboe_risk_indices": preloaded_blocks.get("cboe_risk_indices")
            or await self._run_provider(
                "cboe_risk_indices",
                FACT_TYPES["cboe_risk_indices"],
                self.cboe.fetch,
                item_count=lambda payload: len(payload.get("indices") or {}),
                enabled=self.settings.enable_cboe_risk_indices,
                source="CBOE",
                refresh=refresh,
            ),
            **earnings_blocks,
            "nasdaq_100": supplemental_blocks["nasdaq_100"],
            "nasdaq_market_info": schedule_blocks["nasdaq_market_info"],
            "nasdaq_qqq_options": supplemental_blocks["nasdaq_qqq_options"],
            "aaii_sentiment": supplemental_blocks["aaii_sentiment"],
            "macromicro_aaii_crosscheck": supplemental_blocks["macromicro_aaii_crosscheck"],
            "polymarket_prediction_markets": supplemental_blocks["polymarket_prediction_markets"],
            "quikstrike_review": supplemental_blocks["quikstrike_review"],
        }
        quality = _quality_summary(blocks)
        return {
            "status": "available",
            "refresh_mode": refresh,
            "supplemental_context_enabled": include_supplemental_context,
            "blocks": blocks,
            "context_blocks": build_multi_source_context_blocks(blocks),
            "data_quality": quality,
            "service_role": "data provider only",
        }

    async def _market_schedule_chain(
        self,
        *,
        refresh: str,
    ) -> dict[str, dict[str, Any]]:
        capability_groups = _market_schedule_capability_groups()
        # Prove the complete dispatch plan before the first provider path can
        # execute.  A registry/runtime mismatch therefore fails closed without
        # a partial acquisition.
        for _, provider_ids in capability_groups:
            for provider_id in provider_ids:
                spec = _MARKET_SCHEDULE_RUNTIME_SPECS[provider_id]
                name = str(spec["block_name"])
                registration = provider_by_id(provider_id)
                enabled_setting = registration.enable_setting
                if (
                    name not in FACT_TYPES
                    or not callable(spec.get("item_count"))
                    or not enabled_setting
                    or not hasattr(self.settings, enabled_setting)
                ):
                    raise RuntimeError(f"MARKET_SCHEDULE_RUNTIME_MAPPING_INVALID:{provider_id}")
                self._validate_runtime_adapter(
                    provider_id,
                    attribute=name,
                    dataset_id="market_schedule",
                )

        output: dict[str, dict[str, Any]] = {}
        for capability_metric_id, provider_ids in capability_groups:
            selected = False
            for provider_id in provider_ids:
                spec = _MARKET_SCHEDULE_RUNTIME_SPECS[provider_id]
                name = str(spec["block_name"])
                enabled_setting = provider_by_id(provider_id).enable_setting
                if selected:
                    block = _skipped_runtime_block(
                        source=str(spec["source"]),
                        reason=("PRIOR_SCHEDULE_CAPABILITY_PROVIDER_SUCCEEDED"),
                    )
                else:
                    block = await self._run_provider(
                        name,
                        FACT_TYPES[name],
                        getattr(self, name).fetch,
                        item_count=spec["item_count"],
                        enabled=bool(getattr(self.settings, enabled_setting)),
                        source=str(spec["source"]),
                        refresh=refresh,
                        persist_unmaterialized=bool(spec["persist_unmaterialized"]),
                    )
                output[name] = {
                    **block,
                    "dataset_id": "market_schedule",
                    "provider_id": provider_id,
                    "capability_metric_id": capability_metric_id,
                }
                selected = selected or _runtime_block_succeeded(block)
        return output

    async def _earnings_chain(
        self,
        *,
        refresh: str,
        preloaded_primary: dict[str, Any] | None = None,
        preloaded_fallback: dict[str, Any] | None = None,
    ) -> dict[str, dict[str, Any]]:
        plan = self._earnings_runtime_plan()
        preloaded_by_block = {
            "nasdaq_earnings": preloaded_primary,
            "fmp_earnings": preloaded_fallback,
        }
        cache_candidates: dict[str, dict[str, Any]] = {}
        lookup_by_provider: dict[str, dict[str, Any]] = {}
        for provider_id, spec in plan:
            block_name = spec["block_name"]
            preloaded = preloaded_by_block[block_name]
            revalidated_preload = self._revalidated_preloaded_runtime_cache(
                block_name,
                preloaded,
            )
            if revalidated_preload is not None:
                cache_candidates[provider_id] = revalidated_preload
                lookup_by_provider[provider_id] = dict(
                    revalidated_preload.get("database_lookup") or {}
                )
                continue
            cached_block, lookup = self._runtime_cache_candidate(
                block_name,
                FACT_TYPES[block_name],
                enabled=bool(getattr(self.settings, spec["enabled_setting"])),
                source=spec["source"],
            )
            lookup_by_provider[provider_id] = lookup
            if cached_block is not None:
                cache_candidates[provider_id] = cached_block

        selected_cache_provider = next(
            (provider_id for provider_id, _ in plan if provider_id in cache_candidates),
            None,
        )
        if selected_cache_provider is not None:
            selected_cache = cache_candidates[selected_cache_provider]
            selected_lookup = lookup_by_provider[selected_cache_provider]
            output: dict[str, dict[str, Any]] = {}
            for index, (provider_id, spec) in enumerate(plan):
                block_name = spec["block_name"]
                if index == 0:
                    # The canonical repository belongs to the dataset, not to
                    # the configured primary provider.  Promote the selected
                    # canonical value into the primary chain slot so request
                    # accounting records every provider as not called while
                    # preserving the record's real source/provider identity.
                    block = {
                        **selected_cache,
                        "database_lookup": selected_lookup,
                        "canonical_record_provider_id": selected_cache_provider,
                        "database_lookup_provider_id": selected_cache_provider,
                    }
                else:
                    block = {
                        **_skipped_runtime_block(
                            source=spec["source"],
                            reason="VALID_CANONICAL_EARNINGS_RECORD_SELECTED",
                        ),
                        "database_lookup": lookup_by_provider[provider_id],
                        "canonical_record_provider_id": selected_cache_provider,
                    }
                output[block_name] = {
                    **block,
                    "dataset_id": "earnings",
                    "provider_id": provider_id,
                    "capability_metric_id": spec["capability_metric_id"],
                }
            return output

        output: dict[str, dict[str, Any]] = {}
        prior_succeeded = False
        for provider_id, spec in plan:
            block_name = spec["block_name"]
            if prior_succeeded:
                block = _skipped_runtime_block(
                    source=spec["source"],
                    reason="PRIOR_EARNINGS_PROVIDER_SUCCEEDED",
                )
            else:
                block = await self._run_provider(
                    block_name,
                    FACT_TYPES[block_name],
                    getattr(self, spec["fetcher"]),
                    item_count=lambda payload: len(payload.get("events") or []),
                    enabled=bool(
                        getattr(
                            self.settings,
                            spec["enabled_setting"],
                        )
                    ),
                    source=spec["source"],
                    refresh=refresh,
                )
            output[block_name] = {
                **block,
                "dataset_id": "earnings",
                "provider_id": provider_id,
                "capability_metric_id": spec["capability_metric_id"],
            }
            prior_succeeded = prior_succeeded or _runtime_block_succeeded(block)
        return output

    def _earnings_runtime_plan(
        self,
    ) -> tuple[tuple[str, dict[str, str]], ...]:
        """Validate the complete earnings dispatch plan before acquisition."""

        query = provider_capability_registry.MARKET_FACT_REPOSITORY_DATASET_QUERIES.get("earnings")
        if query is None or not query.fact_types:
            raise RuntimeError("EARNINGS_RUNTIME_REPOSITORY_MAPPING_MISSING")
        plan: list[tuple[str, dict[str, str]]] = []
        capability_signature: (
            tuple[
                str,
                str,
                str,
                tuple[str, ...],
            ]
            | None
        ) = None
        observed_blocks: set[str] = set()
        for provider_id in _earnings_provider_order():
            spec = _earnings_runtime_spec(provider_id)
            block_name = spec["block_name"]
            fact_type = FACT_TYPES.get(block_name)
            if (
                not block_name
                or block_name in observed_blocks
                or fact_type not in query.fact_types
                or _POLICY_RUNTIME_DATASET_IDS.get(block_name) != "earnings"
                or spec["enabled_setting"] not in type(self.settings).model_fields
                or not hasattr(self, spec["attribute"])
                or not callable(getattr(self, spec["fetcher"], None))
            ):
                raise RuntimeError(f"EARNINGS_RUNTIME_MAPPING_INVALID:{provider_id}:{block_name}")
            capability = _runtime_capability_spec(
                provider_id,
                dataset_id="earnings",
                metric_id=spec["capability_metric_id"],
            )["capability"]
            signature = (
                capability.metric_id,
                capability.frequency,
                capability.transformation,
                tuple(capability.supported_fields),
            )
            if capability_signature is None:
                capability_signature = signature
            elif capability_signature != signature:
                raise RuntimeError(
                    "EARNINGS_RUNTIME_CAPABILITY_SEMANTICS_MISMATCH:"
                    f"{provider_id}:{capability.metric_id}"
                )
            self._validate_runtime_adapter(
                provider_id,
                attribute=spec["attribute"],
                dataset_id="earnings",
                capability_metric_id=spec["capability_metric_id"],
            )
            observed_blocks.add(block_name)
            plan.append((provider_id, spec))
        mapped_fact_types = {FACT_TYPES[spec["block_name"]] for _, spec in plan}
        if len(plan) != len(query.fact_types) or mapped_fact_types != set(query.fact_types):
            raise RuntimeError("EARNINGS_RUNTIME_REPOSITORY_MAPPING_INCOMPLETE")
        return tuple(plan)

    def _revalidated_preloaded_runtime_cache(
        self,
        name: str,
        block: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        if not _preloaded_runtime_cache_shape_complete(block):
            return None
        lookup = dict((block or {}).get("database_lookup") or {})
        decision = self.freshness.evaluate_canonical(
            {
                "database_data_as_of": lookup.get("data_as_of"),
                "database_content_valid_until": lookup.get("content_valid_until"),
                "database_refresh_due_at": lookup.get("refresh_due_at"),
                "database_lifecycle_status": lookup.get("lifecycle_status"),
            },
            max_age=_provider_cache_max_age(name),
            data_reference_mode="point_in_time",
        )
        if not decision.usable:
            return None
        return {
            **dict(block or {}),
            "database_lookup": _database_lookup_evidence(decision),
        }

    def _runtime_cache_candidate(
        self,
        name: str,
        fact_type: str,
        *,
        enabled: bool,
        source: str,
    ) -> tuple[dict[str, Any] | None, dict[str, Any]]:
        cached = self.facts.get_valid_facts_by_type(
            fact_type,
            allow_stale=True,
        )
        cached_row = cached[0] if cached else None
        freshness_row = _provider_cache_freshness_row(
            name,
            cached_row,
        )
        database_lookup = self.freshness.evaluate_canonical(
            freshness_row,
            max_age=_provider_cache_max_age(name),
            data_reference_mode="point_in_time",
        )
        lookup_evidence = _database_lookup_evidence(database_lookup)
        if not database_lookup.usable or not cached_row:
            return None, lookup_evidence
        raw = (
            dict(cached_row.get("raw_payload"))
            if isinstance(cached_row.get("raw_payload"), dict)
            else {}
        )
        raw.setdefault("source", cached_row.get("source") or source)
        return (
            _with_runtime_fields(
                raw,
                enabled=enabled,
                cache_used=True,
                provider_calls=0,
                attempted=False,
                persisted_count=1,
                read_back_count=1,
                materialized_count=1,
                database_lookup=lookup_evidence,
            ),
            lookup_evidence,
        )

    async def _fetch_nasdaq_earnings(self) -> dict[str, Any]:
        return await self.nasdaq_earnings.fetch()

    async def _fetch_fmp_earnings(self) -> dict[str, Any]:
        result = await self.fmp_earnings.fetch()
        payload = dict(result.data or {})
        payload.setdefault("source", result.metadata.source)
        payload.setdefault(
            "retrieved_at",
            result.metadata.retrieved_at.isoformat(),
        )
        payload.setdefault(
            "errors",
            list(result.metadata.errors or []),
        )
        return payload

    def _validate_runtime_adapter(
        self,
        provider_id: str,
        *,
        attribute: str,
        dataset_id: str,
        capability_metric_id: str | None = None,
    ) -> None:
        instance = getattr(self, attribute)
        path = adapter_path_for(instance)
        capability_spec = _runtime_capability_spec(
            provider_id,
            dataset_id=dataset_id,
            metric_id=capability_metric_id,
        )
        expected_path = capability_spec["adapter_path"]
        if path != expected_path:
            raise RuntimeError(
                "RUNTIME_ADAPTER_CAPABILITY_MISMATCH:"
                f"{dataset_id}:{provider_id}:"
                f"{capability_spec['capability'].metric_id}:{path}:"
                f"expected={expected_path}"
            )

    def persist_provider_result(self, name: str, result: dict[str, Any], *, source: str) -> int:
        return self._save_fact(name, FACT_TYPES[name], result, source=source)

    async def provider(self, name: str, *, refresh: str = "auto") -> dict[str, Any]:
        if name == "investing_economic_calendar":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_investing_calendar,
                source="Investing Economic Calendar",
                refresh=refresh,
            )
        if name == "investing_holidays":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_investing_holidays,
                source="Investing Holiday Calendar",
                refresh=refresh,
            )
        if name == "marketbeat_holidays":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.marketbeat_holidays.fetch,
                item_count=lambda payload: len(payload.get("holidays") or []),
                enabled=self.settings.enable_marketbeat_holidays,
                source="MarketBeat Stock Market Holidays",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "xtb_economic_calendar":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.xtb_calendar.fetch,
                item_count=_count_investing_calendar,
                enabled=self.settings.enable_xtb_calendar,
                source="XTB Economic Calendar",
                refresh=refresh,
            )
        if name == "cme_market_schedule":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.cme_market_schedule.fetch,
                item_count=lambda payload: 1 if payload.get("calendar_verified") else 0,
                enabled=self.settings.enable_cme_market_schedule,
                source="CME Group Trading Hours",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "investing_fed_rate_monitor":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.investing_fed_rate_monitor.fetch,
                item_count=lambda payload: len(payload.get("meetings") or []),
                enabled=self.settings.enable_investing_fed_rate_monitor,
                source="Investing.com Fed Rate Monitor",
                refresh=refresh,
                persist_unmaterialized=False,
            )
        if name == "cboe_risk_indices":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.cboe.fetch,
                item_count=lambda payload: len(payload.get("indices") or {}),
                enabled=self.settings.enable_cboe_risk_indices,
                source="CBOE",
                refresh=refresh,
            )
        if name == "nasdaq_earnings":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_earnings.fetch,
                item_count=lambda payload: len(payload.get("events") or []),
                enabled=self.settings.enable_nasdaq_earnings,
                source="Nasdaq Earnings Calendar",
                refresh=refresh,
            )
        if name == "nasdaq_100":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_100.fetch,
                item_count=lambda payload: len(payload.get("constituents") or []),
                enabled=self.settings.enable_nasdaq_100,
                source="Nasdaq-100 Constituents",
                refresh=refresh,
            )
        if name == "nasdaq_market_info":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_market_info.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_nasdaq_market_info,
                source="Nasdaq Market Info",
                refresh=refresh,
            )
        if name == "nasdaq_qqq_options":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.nasdaq_options.fetch,
                item_count=lambda payload: len(payload.get("contracts") or []),
                enabled=self.settings.enable_nasdaq_qqq_options,
                source="Nasdaq QQQ Option Chain",
                refresh=refresh,
            )
        if name == "aaii_sentiment":
            return await self._run_aaii(refresh=refresh)
        if name == "macromicro_aaii_crosscheck":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.macromicro.fetch,
                item_count=lambda payload: 1 if payload.get("status") == "found" else 0,
                enabled=self.settings.enable_macromicro_aaii_crosscheck,
                source="MacroMicro AAII Cross-check",
                refresh=refresh,
            )
        if name == "polymarket_prediction_markets":
            return await self._run_provider(
                name,
                FACT_TYPES[name],
                self.polymarket.fetch,
                item_count=lambda payload: len(payload.get("markets") or []),
                enabled=self.settings.enable_polymarket,
                source="Polymarket",
                refresh=refresh,
            )
        if name == "quikstrike_review":
            return self._quikstrike_review(refresh=refresh)
        raise KeyError(name)

    async def _run_provider(
        self,
        name: str,
        fact_type: str,
        fetcher: FetchCallable,
        *,
        item_count: Callable[[dict[str, Any]], int],
        enabled: bool,
        source: str,
        refresh: str | None = None,
        persist_unmaterialized: bool = True,
    ) -> dict[str, Any]:
        refresh_mode = refresh or "auto"
        max_age = _provider_cache_max_age(name)
        cached = self.facts.get_valid_facts_by_type(
            fact_type,
            allow_stale=True,
        )
        cached_row = cached[0] if cached else None
        freshness_row = _provider_cache_freshness_row(
            name,
            cached_row,
        )
        database_lookup = self.freshness.evaluate_canonical(
            freshness_row,
            max_age=max_age,
            data_reference_mode="point_in_time",
        )
        lookup_evidence = _database_lookup_evidence(database_lookup)
        if database_lookup.usable and cached_row:
            raw = (
                cached_row.get("raw_payload")
                if isinstance(cached_row.get("raw_payload"), dict)
                else {}
            )
            return _with_runtime_fields(
                raw,
                enabled=enabled,
                cache_used=True,
                provider_calls=0,
                attempted=False,
                persisted_count=1,
                read_back_count=1,
                materialized_count=1,
                database_lookup=lookup_evidence,
            )
        if not enabled:
            return {
                **_missing_payload(
                    name,
                    source,
                    enabled=False,
                    reason=f"{name}_disabled",
                ),
                "status": "disabled",
                "database_lookup": lookup_evidence,
            }
        if refresh_mode == "false":
            return {
                **_missing_payload(
                    name,
                    source,
                    enabled,
                    reason=f"{fact_type}_not_in_db_refresh_false",
                ),
                "database_lookup": lookup_evidence,
            }
        try:
            result = await fetcher()
        except Exception as exc:
            result = _provider_exception(name, source, exc)
        result = _canonical_runtime_result(name, result)
        count = item_count(result)
        self._record(name, fact_type, result, count)
        materialized = _materialized(result, count)
        persisted_count = (
            self._save_fact(name, fact_type, result, source=source)
            if _should_persist(result, count, persist_unmaterialized)
            else 0
        )
        read_back_count = (
            1 if persisted_count and self.facts.get_fact(_fact_key(name, fact_type)) else 0
        )
        materialized_count = 1 if read_back_count and materialized else 0
        return _with_runtime_fields(
            result,
            enabled=enabled,
            cache_used=False,
            provider_calls=1,
            attempted=True,
            persisted_count=persisted_count,
            read_back_count=read_back_count,
            materialized_count=materialized_count,
            item_count=count,
            database_lookup=lookup_evidence,
        )

    async def _run_aaii(self, *, refresh: str) -> dict[str, Any]:
        result = await self.positioning_runtime.aaii(refresh=refresh)
        cache_used = result.get("cache_status") == "hit"
        provider_calls = 0 if cache_used or refresh == "false" else 1
        found = 1 if result.get("status") == "found" and result.get("survey_date") else 0
        return _with_runtime_fields(
            result,
            enabled=self.settings.enable_aaii_sentiment,
            cache_used=cache_used,
            provider_calls=provider_calls,
            attempted=provider_calls > 0,
            persisted_count=found,
            read_back_count=found,
            materialized_count=found,
            item_count=found,
            database_lookup=result.get("database_lookup"),
        )

    def _quikstrike_review(self, *, refresh: str) -> dict[str, Any]:
        cached = self.facts.get_valid_facts_by_type(
            FACT_TYPES["quikstrike_review"],
            allow_stale=True,
        )
        cached_row = cached[0] if cached else None
        database_lookup = self.freshness.evaluate_canonical(
            cached_row,
            max_age=_provider_cache_max_age("quikstrike_review"),
            data_reference_mode="point_in_time",
        )
        lookup_evidence = _database_lookup_evidence(database_lookup)
        if database_lookup.usable and cached_row:
            raw = (
                cached_row.get("raw_payload")
                if isinstance(cached_row.get("raw_payload"), dict)
                else {}
            )
            return _with_runtime_fields(
                raw,
                enabled=True,
                cache_used=True,
                provider_calls=0,
                attempted=False,
                persisted_count=0,
                read_back_count=1,
                materialized_count=1,
                item_count=1,
                database_lookup=lookup_evidence,
            )
        payload = {
            "status": "reviewed_excluded",
            "provider": "CME QuikStrike",
            "source": "CME QuikStrike Open Interest Heatmap",
            "source_url": "https://www.cmegroup.com/tools-information/quikstrike/open-interest-heatmap.html",
            "retrieved_at": now_iso(),
            "valid_until": (datetime.now(UTC) + _provider_cache_max_age("quikstrike_review"))
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "session_bound": True,
            "operational_integration": False,
            "reason": "authentication/session/compliance",
            "warnings": ["quikstrike_excluded_session_bound_source"],
            "errors": [],
            "diagnostics": {"reviewed": True, "credentials_used": False},
            "service_role": "data provider only",
        }
        persisted = 0
        read_back = 0
        if refresh != "false":
            persisted = self._save_fact(
                "quikstrike_review",
                FACT_TYPES["quikstrike_review"],
                payload,
                source="CME QuikStrike",
            )
            read_back = 1 if persisted else 0
        return _with_runtime_fields(
            payload,
            enabled=True,
            cache_used=refresh == "false",
            provider_calls=0,
            attempted=False,
            persisted_count=persisted,
            read_back_count=read_back,
            materialized_count=1,
            item_count=1,
            database_lookup=lookup_evidence,
        )

    def _save_fact(self, name: str, fact_type: str, result: dict[str, Any], *, source: str) -> int:
        max_age = _provider_cache_max_age(name)
        retrieved_at = result.get("retrieved_at") or now_iso()
        data_as_of = (
            _provider_payload_data_as_of(name, result) or result.get("data_as_of") or retrieved_at
        )
        valid_until = result.get("valid_until") or (datetime.now(UTC) + max_age).replace(
            microsecond=0
        ).isoformat().replace("+00:00", "Z")
        refresh_due_at = (
            result.get("refresh_due_at") or result.get("next_refresh_at") or valid_until
        )
        persisted_payload = dict(result)
        persisted_payload.update(
            {
                "data_as_of": data_as_of,
                "content_valid_until": valid_until,
                "refresh_due_at": refresh_due_at,
            }
        )
        self.facts.upsert_fact(
            {
                "fact_key": _fact_key(name, fact_type),
                "fact_type": fact_type,
                "country": "US",
                "symbol": _symbol_for(name),
                "category": name,
                "event_name": source,
                "source": result.get("source") or source,
                "source_url": result.get("source_url"),
                "provider_type": "PUBLIC_HTTP",
                "reliability": result.get("reliability") or _reliability(result),
                "confidence": result.get("reliability") or _reliability(result),
                "retrieved_at": retrieved_at,
                "release_at": data_as_of,
                "valid_until": valid_until,
                "next_refresh_at": refresh_due_at,
                "status": "active",
                "raw_payload_json": persisted_payload,
                "warnings_json": result.get("warnings") or [],
                "errors_json": result.get("errors") or [],
            }
        )
        return 1

    def _record(self, name: str, fact_type: str, result: dict[str, Any], item_count: int) -> None:
        self.observations.record(
            provider_name=name,
            provider_type="PUBLIC_HTTP",
            status=result.get("status"),
            country="US",
            symbol=_symbol_for(name),
            category=fact_type,
            url=result.get("source_url"),
            item_count=item_count,
            error="; ".join(result.get("errors") or []) or None,
            warning="; ".join(result.get("warnings") or []) or None,
            duration_ms=result.get("duration_ms"),
            raw_payload_json={
                "status": result.get("status"),
                "diagnostics": result.get("diagnostics") or {},
                "warnings": result.get("warnings") or [],
                "errors": result.get("errors") or [],
            },
        )


def build_multi_source_context_blocks(blocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    investing = blocks.get("investing_economic_calendar") or {}
    xtb = blocks.get("xtb_economic_calendar") or {}
    holidays = blocks.get("investing_holidays") or {}
    marketbeat_holidays = blocks.get("marketbeat_holidays") or {}
    cme_market_schedule = blocks.get("cme_market_schedule") or {}
    fed_rate_monitor = blocks.get("investing_fed_rate_monitor") or {}
    cboe = blocks.get("cboe_risk_indices") or {}
    earnings_primary = blocks.get("nasdaq_earnings") or {}
    earnings_fallback = blocks.get("fmp_earnings") or {}
    earnings = (
        earnings_primary
        if _runtime_block_succeeded(earnings_primary)
        else earnings_fallback
        if _runtime_block_succeeded(earnings_fallback)
        else earnings_primary
    )
    earnings_candidates = (
        earnings.get("relevant_upcoming") or earnings.get("events") or []
    )
    relevant_earnings, relevant_earnings_count = _select_mnq_primary_earnings(
        earnings_candidates
    )
    observed_earnings_counts = _earnings_selection_counts(earnings)
    if observed_earnings_counts is not None:
        relevant_earnings_count = max(
            relevant_earnings_count,
            observed_earnings_counts["relevant_count"],
        )
        total_earnings_available = max(
            len(earnings_candidates),
            relevant_earnings_count,
            observed_earnings_counts["total_available"],
        )
    else:
        total_earnings_available = len(earnings_candidates)
    delivered_earnings_count = len(relevant_earnings)
    earnings_exclusion_counts = {
        "provider_prefiltered_or_invalid": max(
            total_earnings_available - relevant_earnings_count,
            0,
        ),
        "bounded_limit": max(
            relevant_earnings_count - delivered_earnings_count,
            0,
        ),
    }
    nasdaq_100 = blocks.get("nasdaq_100") or {}
    market_info = blocks.get("nasdaq_market_info") or {}
    options = blocks.get("nasdaq_qqq_options") or {}
    aaii = blocks.get("aaii_sentiment") or {}
    macromicro = blocks.get("macromicro_aaii_crosscheck") or {}
    polymarket = blocks.get("polymarket_prediction_markets") or {}
    quikstrike = blocks.get("quikstrike_review") or {}
    primary_holidays = holidays.get("relevant_holidays") or holidays.get("holidays") or []
    secondary_holidays = (
        marketbeat_holidays.get("relevant_holidays") or marketbeat_holidays.get("holidays") or []
    )
    merged_holidays = _merge_calendar_events(primary_holidays, secondary_holidays)
    return {
        "economic_calendar_enrichment": {
            "investing": {**investing, "events": investing.get("items") or []},
            "xtb": {**xtb, "events": xtb.get("items") or []},
            "source_ranking": {
                "official_release_calendars": "primary_for_schedule_and_official_actuals",
                "investing_economic_calendar": "primary_aggregated_consensus_when_verified",
                "xtb_economic_calendar": "secondary_consensus_crosscheck_and_calendar_fallback",
                "conflict_policy": "preserve_higher_reliability_verified_field_and_retain_lineage",
                "stale_policy": "never_promote_expired_secondary_values",
            },
            "consensus_coverage": {
                "events_with_consensus": sum(
                    1
                    for source in (investing, xtb)
                    for item in source.get("items") or []
                    if item.get("consensus") is not None
                ),
                "events_total": sum(len(source.get("items") or []) for source in (investing, xtb)),
            },
            "previous_coverage": {
                "events_with_previous": sum(
                    1
                    for source in (investing, xtb)
                    for item in source.get("items") or []
                    if item.get("previous") is not None
                ),
                "events_total": sum(len(source.get("items") or []) for source in (investing, xtb)),
            },
            "secondary_actuals": {
                "count": sum(
                    1
                    for source in (investing, xtb)
                    for item in source.get("items") or []
                    if item.get("actual") is not None
                ),
                "actual_is_official": False,
            },
        },
        "market_schedule": {
            "nasdaq_cash_session": market_info,
            "cme_calendar": cme_market_schedule,
            "holidays": primary_holidays or secondary_holidays,
            "holiday_source": holidays if primary_holidays else marketbeat_holidays,
            "holiday_fallback_source": marketbeat_holidays if secondary_holidays else {},
        },
        "market_calendar": {
            "cme_equity_futures": cme_market_schedule,
            "market_holidays": {
                "official_sources": {},
                "primary_sources": {
                    "investing": holidays,
                },
                "secondary_sources": {
                    "marketbeat": marketbeat_holidays,
                },
                "merged_relevant_holidays": merged_holidays,
            },
        },
        "rates_expectations": {
            "fed_funds_futures": {
                "investing_fed_rate_monitor": fed_rate_monitor,
                "primary_source": None,
                "secondary_source": "Investing.com Fed Rate Monitor" if fed_rate_monitor else None,
                "official_fed_source": False,
            }
        },
        "risk_context": {
            "vvix": _risk_index_block((cboe.get("indices") or {}).get("vvix")),
            "skew": _risk_index_block((cboe.get("indices") or {}).get("skew")),
            "status": cboe.get("status"),
            "source": cboe.get("source"),
            "warnings": cboe.get("warnings") or [],
        },
        "corporate_events": {
            "earnings": {
                "status": earnings.get("status"),
                "selection_policy": MNQ_EARNINGS_SELECTION_POLICY.policy_id,
                "total_available": total_earnings_available,
                "relevant_count": relevant_earnings_count,
                "delivered_count": delivered_earnings_count,
                "excluded_count": (
                    total_earnings_available
                    - delivered_earnings_count
                ),
                "exclusion_counts": earnings_exclusion_counts,
                "relevant_upcoming": relevant_earnings,
                "mega_cap": relevant_earnings,
                "semiconductors": _filter_symbols(
                    relevant_earnings,
                    {"NVDA", "AMD"},
                ),
                "coverage": earnings.get("diagnostics") or {},
            }
        },
        "nasdaq_context_additions": {
            "nasdaq_100_official_snapshot": nasdaq_100,
            "qqq_options": {
                "status": options.get("status"),
                "snapshot": options.get("snapshot") or {},
                "open_interest_matrix": options.get("open_interest_matrix") or {},
                "observed_aggregates": options.get("observed_aggregates")
                or options.get("aggregates")
                or {},
                "global_aggregates": options.get("global_aggregates"),
                "diagnostics": options.get("diagnostics") or {},
                "warnings": options.get("warnings") or [],
            },
        },
        "sentiment": {
            "aaii": aaii,
            "aaii_crosscheck": macromicro,
            "prediction_markets": polymarket,
        },
        "source_reviews": {
            "quikstrike": quikstrike,
        },
    }


def apply_multi_source_context(
    contract: dict[str, Any], snapshot: dict[str, Any]
) -> dict[str, Any]:
    context_blocks = snapshot.get("context_blocks") or {}
    contract["economic_calendar_enrichment"] = (
        context_blocks.get("economic_calendar_enrichment") or {}
    )
    contract["market_schedule"] = context_blocks.get("market_schedule") or {}
    contract["market_calendar"] = context_blocks.get("market_calendar") or {}
    contract["rates_expectations"] = context_blocks.get("rates_expectations") or {}
    contract["risk_context"] = context_blocks.get("risk_context") or {}
    contract["corporate_events"] = context_blocks.get("corporate_events") or {}
    additions = context_blocks.get("nasdaq_context_additions") or {}
    nasdaq = dict(contract.get("nasdaq_context") or {})
    nasdaq.update(additions)
    contract["nasdaq_context"] = nasdaq
    sentiment = context_blocks.get("sentiment") or {}
    contract["sentiment"] = sentiment
    sentiment_context = dict(contract.get("sentiment_context") or {})
    if sentiment.get("aaii_crosscheck"):
        sentiment_context["aaii_crosscheck"] = sentiment["aaii_crosscheck"]
    if sentiment.get("prediction_markets"):
        sentiment_context["prediction_markets"] = sentiment["prediction_markets"]
    contract["sentiment_context"] = sentiment_context
    contract["source_reviews"] = context_blocks.get("source_reviews") or {}
    quality = dict(contract.get("data_quality") or {})
    quality["multi_source_pipeline"] = snapshot.get("data_quality") or {}
    contract["data_quality"] = quality
    metadata = dict(contract.get("metadata") or {})
    metadata["multi_source_runtime"] = {
        "refresh_mode": snapshot.get("refresh_mode"),
        "provider_calls": (snapshot.get("data_quality") or {}).get("provider_calls"),
        "actual_network_calls": (snapshot.get("data_quality") or {}).get("actual_network_calls"),
        "cache_used": (snapshot.get("data_quality") or {}).get("cache_used"),
    }
    contract["metadata"] = metadata
    return contract


def _quality_summary(blocks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    return {
        "provider_calls": sum(int(block.get("provider_calls") or 0) for block in blocks.values()),
        "actual_network_calls": sum(
            int(block.get("actual_network_calls") or 0) for block in blocks.values()
        ),
        "cache_used": all(
            bool(block.get("cache_used")) or int(block.get("provider_calls") or 0) == 0
            for block in blocks.values()
        ),
        "fetched_count": sum(int(block.get("fetched_count") or 0) for block in blocks.values()),
        "validated_count": sum(int(block.get("validated_count") or 0) for block in blocks.values()),
        "rejected_count": sum(int(block.get("rejected_count") or 0) for block in blocks.values()),
        "persisted_count": sum(int(block.get("persisted_count") or 0) for block in blocks.values()),
        "read_back_count": sum(int(block.get("read_back_count") or 0) for block in blocks.values()),
        "materialized_count": sum(
            int(block.get("materialized_count") or 0) for block in blocks.values()
        ),
        "warnings": [
            warning for block in blocks.values() for warning in (block.get("warnings") or [])
        ],
        "errors": [error for block in blocks.values() for error in (block.get("errors") or [])],
        "blocks": {
            name: {
                "status": block.get("status"),
                "provider_calls": block.get("provider_calls"),
                "actual_network_calls": block.get("actual_network_calls"),
                "cache_used": block.get("cache_used"),
                "fetched_count": block.get("fetched_count"),
                "persisted_count": block.get("persisted_count"),
                "read_back_count": block.get("read_back_count"),
                "materialized_count": block.get("materialized_count"),
            }
            for name, block in blocks.items()
        },
    }


def _with_runtime_fields(
    payload: dict[str, Any],
    *,
    enabled: bool,
    cache_used: bool,
    provider_calls: int,
    attempted: bool,
    persisted_count: int,
    read_back_count: int,
    materialized_count: int,
    item_count: int | None = None,
    database_lookup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output = dict(payload)
    selection_counts = _earnings_selection_counts(output)
    fetched = (
        selection_counts["total_available"]
        if selection_counts is not None
        else item_count
        if item_count is not None
        else _generic_item_count(output)
    )
    rejected = (
        selection_counts["excluded_count"]
        if selection_counts is not None
        else _rejected_count(output)
    )
    validated = (
        selection_counts["relevant_count"]
        if selection_counts is not None
        else max(fetched - rejected, 0)
    )
    excluded = (
        selection_counts["excluded_count"]
        if selection_counts is not None
        else _excluded_count(output)
    )
    output.update(
        {
            "enabled": enabled,
            "attempted": attempted,
            "provider_calls": provider_calls,
            "actual_network_calls": (
                0
                if cache_used
                else int(
                    output.get("actual_network_calls")
                    or (output.get("diagnostics") or {}).get("actual_network_calls")
                    or provider_calls
                )
            ),
            "cache_used": cache_used,
            "AI_called": False,
            "fetched_count": fetched,
            "validated_count": validated,
            "rejected_count": rejected,
            "persisted_count": persisted_count,
            "committed": persisted_count > 0 or provider_calls == 0,
            "read_back_count": read_back_count,
            "materialized_count": materialized_count,
            "excluded_count": excluded,
            "exclusion_reasons": _exclusion_reasons(output),
            "database_lookup": database_lookup,
        }
    )
    return output


def _database_lookup_evidence(
    result: CanonicalFreshnessResult,
) -> dict[str, Any]:
    return {
        "performed": True,
        "found": result.found,
        "data_as_of": result.data_as_of,
        "content_valid_until": result.content_valid_until,
        "refresh_due_at": result.refresh_due_at,
        "lifecycle_status": result.lifecycle,
        "expired": result.expired,
        "freshness": result.evaluation,
        "reason_code": result.reason_code,
    }


def _runtime_block_succeeded(block: dict[str, Any]) -> bool:
    status = str(block.get("status") or "").lower()
    return bool(
        status in {"found", "available", "valid", "partial"}
        and (
            int(block.get("fetched_count") or 0) > 0
            or int(block.get("materialized_count") or 0) > 0
        )
    )


def _preloaded_runtime_cache_shape_complete(
    block: dict[str, Any] | None,
) -> bool:
    """Require evidence fields before re-evaluating a preloaded DB record."""

    if not isinstance(block, dict):
        return False
    lookup = block.get("database_lookup") if isinstance(block.get("database_lookup"), dict) else {}
    return bool(
        _runtime_block_succeeded(block)
        and block.get("cache_used") is True
        and block.get("attempted") is False
        and int(block.get("provider_calls") or 0) == 0
        and lookup.get("performed") is True
        and lookup.get("found") is True
        and type(lookup.get("expired")) is bool
        and str(lookup.get("freshness") or "").strip()
        and lookup.get("data_as_of")
        and lookup.get("content_valid_until")
        and lookup.get("refresh_due_at")
    )


def _canonical_runtime_result(
    name: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    max_age = _provider_cache_max_age(name)
    output = dict(result)
    retrieved_at = output.get("retrieved_at") or now_iso()
    observed_data_as_of = _provider_payload_data_as_of(
        name,
        output,
    )
    data_as_of = (
        observed_data_as_of
        or (
            None
            if name == "investing_fed_rate_monitor"
            else output.get("data_as_of") or retrieved_at
        )
    )
    valid_until = (
        output.get("valid_until")
        or (datetime.now(UTC) + max_age).replace(microsecond=0).isoformat()
    )
    refresh_due_at = output.get("refresh_due_at") or output.get("next_refresh_at") or valid_until
    output.update(
        {
            "retrieved_at": retrieved_at,
            "data_as_of": data_as_of,
            "content_valid_until": valid_until,
            "valid_until": valid_until,
            "refresh_due_at": refresh_due_at,
            "next_refresh_at": refresh_due_at,
        }
    )
    if name == "investing_fed_rate_monitor":
        freshness = evaluate_canonical_freshness(
            output,
            policy=CanonicalFreshnessPolicy(
                max_age=max_age,
                data_reference_mode="point_in_time",
            ),
            observed_at=datetime.now(UTC),
        )
        output["freshness_state"] = freshness.evaluation
        if not freshness.usable:
            observed_meetings = len(output.get("meetings") or [])
            output.update(
                {
                    "status": "not_found",
                    "reason_code": freshness.reason_code,
                    "current_meeting": None,
                    "meetings": [],
                }
            )
            diagnostics = dict(output.get("diagnostics") or {})
            diagnostics[
                "rejected_stale_observation_count"
            ] = observed_meetings
            output["diagnostics"] = diagnostics
            output["warnings"] = list(
                dict.fromkeys(
                    [
                        *(output.get("warnings") or []),
                        freshness.reason_code,
                    ]
                )
            )
    return output


def _provider_cache_freshness_row(
    name: str,
    row: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not row:
        return row
    raw = (
        row.get("raw_payload")
        if isinstance(row.get("raw_payload"), dict)
        else row.get("raw_payload_json")
        if isinstance(row.get("raw_payload_json"), dict)
        else {}
    )
    data_as_of = _provider_payload_data_as_of(name, raw)
    if data_as_of is None:
        if name == "investing_fed_rate_monitor":
            return {
                **row,
                "database_data_as_of": (
                    "UNPROVEN_PROVIDER_OBSERVATION_TIME"
                ),
            }
        return row
    return {
        **row,
        # This request-scoped cache lookup must be evaluated against the
        # provider observation, not the time at which stale content happened
        # to be retrieved or persisted.
        "database_data_as_of": data_as_of,
    }


def _provider_payload_data_as_of(
    name: str,
    payload: dict[str, Any],
) -> str | None:
    if name == "investing_fed_rate_monitor":
        observations = [
            observed
            for item in payload.get("meetings") or []
            if isinstance(item, dict)
            and (
                observed := parse_datetime(
                    item.get("updated_at")
                    or item.get("data_as_of")
                )
            )
            is not None
        ]
        if (
            not observations
            or len(observations)
            != len(payload.get("meetings") or [])
        ):
            return None
        return min(observations).astimezone(UTC).isoformat()
    if name != "cboe_risk_indices":
        return None
    observations: list[datetime] = []
    for item in (payload.get("indices") or {}).values():
        if not isinstance(item, dict):
            continue
        observed = parse_datetime(
            item.get("last_trade_time")
            or item.get("provider_timestamp")
            or item.get("valid_from")
            or item.get("data_as_of")
        )
        if observed is not None:
            observations.append(observed)
    if not observations:
        return None
    return min(observations).astimezone(UTC).isoformat()


def _skipped_runtime_block(
    *,
    source: str,
    reason: str,
) -> dict[str, Any]:
    return _with_runtime_fields(
        {
            "status": "not_called",
            "provider": source,
            "source": source,
            "retrieved_at": now_iso(),
            "warnings": [reason],
            "errors": [],
            "reason": reason,
        },
        enabled=True,
        cache_used=False,
        provider_calls=0,
        attempted=False,
        persisted_count=0,
        read_back_count=0,
        materialized_count=0,
        database_lookup={
            "performed": False,
            "found": None,
            "data_as_of": None,
            "content_valid_until": None,
            "refresh_due_at": None,
            "expired": None,
            "freshness": "NOT_LOOKED_UP",
            "reason_code": reason,
        },
    )


def _supplemental_runtime_block(*, source: str) -> dict[str, Any]:
    block = _skipped_runtime_block(
        source=source,
        reason=_SUPPLEMENTAL_CONTEXT_DISABLED_REASON,
    )
    block.update(
        {
            "reason_code": _SUPPLEMENTAL_CONTEXT_DISABLED_REASON,
            "supplemental_context_enabled": False,
        }
    )
    return block


def _missing_payload(name: str, source: str, enabled: bool, *, reason: str) -> dict[str, Any]:
    now = now_iso()
    return _with_runtime_fields(
        {
            "status": "not_found",
            "provider": source,
            "source": source,
            "source_url": None,
            "retrieved_at": now,
            "valid_until": None,
            "warnings": [reason],
            "errors": [],
            "diagnostics": {"reason": reason},
        },
        enabled=enabled,
        cache_used=False,
        provider_calls=0,
        attempted=False,
        persisted_count=0,
        read_back_count=0,
        materialized_count=0,
        item_count=0,
    )


def _provider_exception(name: str, source: str, exc: Exception) -> dict[str, Any]:
    now = now_iso()
    return {
        "status": "provider_failed",
        "provider": source,
        "source": source,
        "source_url": None,
        "retrieved_at": now,
        "valid_until": (datetime.now(UTC) + timedelta(minutes=30))
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "warnings": [],
        "errors": [str(exc) or type(exc).__name__],
        "diagnostics": {"provider": name},
    }


def _count_investing_calendar(payload: dict[str, Any]) -> int:
    return len(payload.get("items") or [])


def _generic_item_count(payload: dict[str, Any]) -> int:
    for key in ("items", "holidays", "indices", "events", "constituents", "contracts", "markets"):
        value = payload.get(key)
        if isinstance(value, dict):
            return len(value)
        if isinstance(value, list):
            return len(value)
    return 1 if payload.get("status") in {"found", "valid", "reviewed_excluded"} else 0


def _earnings_selection_counts(
    payload: dict[str, Any],
) -> dict[str, int] | None:
    """Return only observed, internally consistent earnings selection counts.

    Current providers emit ``selection_counts`` directly.  The diagnostics and
    data-quality fallbacks preserve request-observed counts from canonical rows
    written before that compact envelope existed; analytic payload fields are
    deliberately not used to invent the upstream calendar size.
    """

    candidates: list[dict[str, Any]] = []
    nested = payload.get("selection_counts")
    if isinstance(nested, dict):
        candidates.append(nested)
    if all(
        key in payload
        for key in (
            "total_available",
            "relevant_count",
            "delivered_count",
            "excluded_count",
        )
    ):
        candidates.append(payload)

    diagnostics = (
        payload.get("diagnostics")
        if isinstance(payload.get("diagnostics"), dict)
        else {}
    )
    data_quality = (
        payload.get("data_quality")
        if isinstance(payload.get("data_quality"), dict)
        else {}
    )
    events = payload.get("relevant_upcoming") or payload.get("events")
    delivered_count = len(events) if isinstance(events, list) else 0
    observed_total = diagnostics.get("events_fetched")
    if observed_total is None:
        observed_total = data_quality.get("fetched_count")
    observed_relevant = diagnostics.get("relevant_upcoming")
    if observed_relevant is None:
        observed_relevant = diagnostics.get("events")
    if observed_relevant is None:
        observed_relevant = data_quality.get("validated_count")
    if observed_total is not None and observed_relevant is not None:
        candidates.append(
            {
                "total_available": observed_total,
                "relevant_count": observed_relevant,
                "delivered_count": delivered_count,
                "excluded_count": (
                    observed_total - delivered_count
                    if type(observed_total) is int
                    else None
                ),
            }
        )

    for candidate in candidates:
        counts = {
            key: candidate.get(key)
            for key in (
                "total_available",
                "relevant_count",
                "delivered_count",
                "excluded_count",
            )
        }
        if not all(
            type(value) is int and value >= 0
            for value in counts.values()
        ):
            continue
        if not (
            counts["total_available"]
            >= counts["relevant_count"]
            >= counts["delivered_count"]
            and counts["excluded_count"]
            == counts["total_available"]
            - counts["delivered_count"]
        ):
            continue
        return counts
    return None


def _rejected_count(payload: dict[str, Any]) -> int:
    diagnostics = payload.get("diagnostics") or {}
    return int(
        diagnostics.get("rejected_future_actual")
        or diagnostics.get("rejected_count")
        or diagnostics.get("rejected_invalid_probability")
        or 0
    )


def _excluded_count(payload: dict[str, Any]) -> int:
    diagnostics = payload.get("diagnostics") or {}
    return sum(
        int(diagnostics.get(key) or 0)
        for key in (
            "rejected_irrelevant",
            "rejected_weak_indirect",
            "rejected_low_relevance",
            "rejected_rules_only",
            "rejected_low_liquidity",
            "rejected_low_volume",
            "rejected_wide_spread",
            "rejected_expired",
        )
    )


def _exclusion_reasons(payload: dict[str, Any]) -> dict[str, int]:
    diagnostics = payload.get("diagnostics") or {}
    reasons: dict[str, int] = {}
    for key, value in diagnostics.items():
        if not key.startswith("rejected_"):
            continue
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            continue
        if count:
            reasons[key.removeprefix("rejected_")] = count
    return reasons


def _materialized(payload: dict[str, Any], item_count: int) -> bool:
    return bool(
        item_count > 0
        or payload.get("status") in {"found", "valid", "partial", "reviewed_excluded"}
    )


def _fact_key(name: str, fact_type: str) -> str:
    return f"multi_source:{name}:{fact_type}:latest"


def _symbol_for(name: str) -> str | None:
    if name in {"nasdaq_qqq_options", "nasdaq_100", "nasdaq_earnings"}:
        return "QQQ"
    if name == "cboe_risk_indices":
        return "VVIX,SKEW"
    return None


def _reliability(result: dict[str, Any]) -> float:
    status = str(result.get("status") or "")
    if status in {"found", "valid"}:
        return 0.82
    if status in {"partial", "anomalous", "not_found", "restricted", "reviewed_excluded"}:
        return 0.55
    return 0.0


def _filter_symbols(events: list[dict[str, Any]], symbols: set[str]) -> list[dict[str, Any]]:
    return [item for item in events if str(item.get("symbol") or "").upper() in symbols]


def _select_mnq_primary_earnings(
    events: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    relevant = _filter_symbols(events, set(MNQ_PRIMARY_SYMBOLS))
    relevant.sort(
        key=lambda item: tuple(
            str(
                item.get(field)
                or (
                    item.get("date")
                    if field == "event_date"
                    else ""
                )
                or ""
            )
            for field in MNQ_EARNINGS_SELECTION_POLICY.sort_fields
        )
    )
    return (
        relevant[: MNQ_EARNINGS_SELECTION_POLICY.max_events],
        len(relevant),
    )


def _risk_index_block(index: dict[str, Any] | None) -> dict[str, Any]:
    if not index:
        return {"status": "not_found", "value": None, "current_price": None}
    return {
        **index,
        "status": "found" if index.get("current_price") is not None else "not_found",
        "value": index.get("current_price"),
    }


def _merge_calendar_events(
    primary: list[dict[str, Any]], secondary: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    merged: dict[tuple[str, str], dict[str, Any]] = {}
    for item in secondary:
        date = str(item.get("date") or "")
        name = str(item.get("holiday_name") or item.get("name") or "")
        if date and name:
            merged[(date, name.lower())] = item
    for item in primary:
        date = str(item.get("date") or "")
        name = str(item.get("holiday_name") or item.get("name") or "")
        if date and name:
            merged[(date, name.lower())] = item
    return sorted(
        merged.values(),
        key=lambda item: (
            item.get("date") or "",
            item.get("holiday_name") or item.get("name") or "",
        ),
    )


def _should_persist(payload: dict[str, Any], item_count: int, persist_unmaterialized: bool) -> bool:
    accepted_status = payload.get("status") in {
        "found",
        "valid",
        "anomalous",
        "partial",
        "reviewed_excluded",
    }
    return bool(item_count > 0 or accepted_status and persist_unmaterialized)


def assert_fetcher_shape(fetcher: FetchCallable) -> None:
    if not inspect.iscoroutinefunction(fetcher):
        raise TypeError("provider fetcher must be async")
