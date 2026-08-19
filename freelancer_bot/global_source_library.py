"""Global Source Library domain primitives.

This module is deliberately transport- and provider-neutral.  It contains the
deterministic campaign/taxonomy/query/scheduler decisions used by both the
durable operator workflow and offline verification.  Telegram calls remain in
the existing governed collector services.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import re
from time import perf_counter
import tracemalloc
from typing import Iterable, Mapping, Sequence


BOOTSTRAP_TAXONOMY_VERSION = "bootstrap-buyer-ecosystems.v1"
QUERY_STRATEGY_VERSION = "global-source-query.v1"


class CampaignType(str, Enum):
    BOOTSTRAP = "bootstrap"
    PROFILE_GAP = "profile_gap"
    SOURCE_GRAPH_EXPANSION = "source_graph_expansion"
    MANUAL_OPERATOR = "manual_operator"


class CampaignStatus(str, Enum):
    PLANNED = "planned"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class QueryFamily(str, Enum):
    DIRECT_TELEGRAM_SOURCE = "DIRECT_TELEGRAM_SOURCE"
    SITE_TELEGRAM = "SITE_TELEGRAM"
    COMMUNITY_DIRECTORY = "COMMUNITY_DIRECTORY"
    BUYER_HABITAT = "BUYER_HABITAT"
    HUB_LISTICLE = "HUB_LISTICLE"
    PROFILE_GAP = "PROFILE_GAP"


@dataclass(frozen=True)
class BootstrapTaxonomyEntry:
    key: str
    label: str
    buyer_habitats: tuple[str, ...]
    industries: tuple[str, ...]
    languages: tuple[str, ...] = ("en", "ru")


BOOTSTRAP_TAXONOMY: tuple[BootstrapTaxonomyEntry, ...] = (
    BootstrapTaxonomyEntry("startups_saas", "Startups / founders / SaaS", ("startup founders", "SaaS teams", "product companies"), ("startups", "saas")),
    BootstrapTaxonomyEntry("software_automation", "Software / development / automation", ("software buyers", "automation teams", "engineering teams"), ("software", "automation")),
    BootstrapTaxonomyEntry("product_design", "Product / design", ("product teams", "design studios", "UX buyers"), ("product", "design")),
    BootstrapTaxonomyEntry("marketing_agencies", "Marketing / advertising / agencies", ("marketing agencies", "brand teams", "advertisers"), ("marketing", "advertising")),
    BootstrapTaxonomyEntry("ecommerce_marketplaces", "Ecommerce / marketplaces / sellers", ("ecommerce sellers", "marketplace teams", "online stores"), ("ecommerce", "retail")),
    BootstrapTaxonomyEntry("creators_media", "Creators / media / influencers", ("content creators", "media teams", "influencers"), ("media", "creator")),
    BootstrapTaxonomyEntry("hr_recruiting", "HR / recruiting / hiring", ("recruiters", "hiring managers", "people teams"), ("hr", "recruiting")),
    BootstrapTaxonomyEntry("sales_business", "Sales / business development", ("sales teams", "business development", "partnerships"), ("sales", "business")),
    BootstrapTaxonomyEntry("local_services", "Local business / services", ("local business owners", "service businesses", "small businesses"), ("local_services",)),
    BootstrapTaxonomyEntry("education_infoproducts", "Education / online education / infoproducts", ("online schools", "course creators", "education businesses"), ("education", "infoproducts")),
    BootstrapTaxonomyEntry("retail_brands", "Retail / consumer brands", ("consumer brands", "retail teams", "brand owners"), ("retail", "consumer")),
    BootstrapTaxonomyEntry("hospitality_travel", "Hospitality / restaurants / travel", ("restaurants", "hotels", "travel businesses"), ("hospitality", "travel")),
    BootstrapTaxonomyEntry("beauty_wellness", "Beauty / wellness", ("beauty businesses", "wellness studios", "health services"), ("beauty", "wellness")),
    BootstrapTaxonomyEntry("real_estate_building", "Real estate / architecture / construction", ("real estate developers", "architecture studios", "construction companies"), ("real_estate", "construction")),
    BootstrapTaxonomyEntry("professional_consulting", "Professional services / consulting", ("consultancies", "professional services", "advisory firms"), ("consulting", "professional_services")),
)


@dataclass(frozen=True)
class DiscoveryCampaignSpec:
    campaign_type: CampaignType
    languages: tuple[str, ...]
    geo_constraints: tuple[str, ...]
    specialist_concepts: tuple[str, ...]
    buyer_concepts: tuple[str, ...]
    buyer_habitats: tuple[str, ...]
    industry_contexts: tuple[str, ...]
    priority: int = 50
    created_from: str = "BASE_TAXONOMY"
    query_strategy_version: str = QUERY_STRATEGY_VERSION
    profile_id: str | None = None
    gap_key: str | None = None

    @property
    def campaign_key(self) -> str:
        return campaign_key(self)


def validate_bootstrap_targets(
    *,
    target_unique_candidates: int,
    target_validated_sources: int,
    target_approved_sources: int,
) -> None:
    """Validate monotonic bootstrap targets before creating durable work."""

    values = {
        "target_unique_candidates": target_unique_candidates,
        "target_validated_sources": target_validated_sources,
        "target_approved_sources": target_approved_sources,
    }
    if any(value <= 0 for value in values.values()):
        raise ValueError("bootstrap targets must be positive")
    if not (
        target_approved_sources
        <= target_validated_sources
        <= target_unique_candidates
    ):
        raise ValueError(
            "bootstrap targets must satisfy "
            "target_approved_sources <= target_validated_sources <= "
            "target_unique_candidates"
        )


@dataclass(frozen=True)
class GlobalDiscoveryQuery:
    text: str
    family: QueryFamily
    language: str
    normalized_query_key: str
    strategy_version: str
    campaign_key: str
    topic: str


def campaign_key(spec: DiscoveryCampaignSpec) -> str:
    payload = {
        "type": spec.campaign_type.value,
        "languages": sorted(set(spec.languages)),
        "geo": sorted(set(spec.geo_constraints)),
        "specialist": sorted(_norm(x) for x in spec.specialist_concepts),
        "buyer": sorted(_norm(x) for x in spec.buyer_concepts),
        "habitats": sorted(_norm(x) for x in spec.buyer_habitats),
        "industries": sorted(_norm(x) for x in spec.industry_contexts),
        "created_from": spec.created_from.casefold(),
        "profile_id": "" if spec.campaign_type is CampaignType.PROFILE_GAP else (spec.profile_id or ""),
        "gap_key": spec.gap_key or "",
        "version": spec.query_strategy_version,
    }
    digest = hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    prefix = spec.campaign_type.value.replace("_", "-")
    return f"{prefix}:{digest[:40]}"


def bootstrap_campaign_specs(*, priority: int = 50) -> tuple[DiscoveryCampaignSpec, ...]:
    return tuple(
        DiscoveryCampaignSpec(
            campaign_type=CampaignType.BOOTSTRAP,
            languages=entry.languages,
            geo_constraints=(),
            specialist_concepts=(),
            buyer_concepts=(entry.label,),
            buyer_habitats=entry.buyer_habitats,
            industry_contexts=entry.industries,
            priority=priority,
        )
        for entry in BOOTSTRAP_TAXONOMY
    )


def profile_gap_campaign_spec(
    *,
    profile_id: str,
    buyer_habitats: Sequence[str],
    industries: Sequence[str],
    specialist_concepts: Sequence[str] = (),
    languages: Sequence[str] = ("en", "ru"),
    geographies: Sequence[str] = (),
) -> DiscoveryCampaignSpec:
    habitats = tuple(dict.fromkeys(_norm(x) for x in buyer_habitats if _norm(x)))
    industries_clean = tuple(dict.fromkeys(_norm(x) for x in industries if _norm(x)))
    gap_key = hashlib.sha256(
        "|".join(sorted((*habitats, *industries_clean))).encode()
    ).hexdigest()[:24]
    return DiscoveryCampaignSpec(
        campaign_type=CampaignType.PROFILE_GAP,
        languages=tuple(dict.fromkeys(x.casefold() for x in languages)),
        geo_constraints=tuple(dict.fromkeys(_norm(x) for x in geographies if _norm(x))),
        specialist_concepts=tuple(dict.fromkeys(_norm(x) for x in specialist_concepts if _norm(x))),
        buyer_concepts=industries_clean,
        buyer_habitats=habitats,
        industry_contexts=industries_clean,
        priority=75,
        created_from="SEARCH_PROFILE",
        profile_id=profile_id,
        gap_key=gap_key,
    )


def source_graph_campaign_spec(
    seed_source_ids: Sequence[int],
    *,
    priority: int = 60,
) -> DiscoveryCampaignSpec:
    """Build one reusable campaign identity for a bounded graph seed set."""

    normalized_ids = tuple(sorted({int(source_id) for source_id in seed_source_ids}))
    if not normalized_ids or any(source_id <= 0 for source_id in normalized_ids):
        raise ValueError("source graph campaigns require positive seed source IDs")
    digest = hashlib.sha256(
        ",".join(str(source_id) for source_id in normalized_ids).encode("ascii")
    ).hexdigest()[:24]
    return DiscoveryCampaignSpec(
        campaign_type=CampaignType.SOURCE_GRAPH_EXPANSION,
        languages=("en", "ru"),
        geo_constraints=(),
        specialist_concepts=(),
        buyer_concepts=("approved Telegram source graph",),
        buyer_habitats=("approved Telegram communities",),
        industry_contexts=("telegram",),
        priority=priority,
        created_from="APPROVED_SOURCE",
        gap_key=f"seed-set:{digest}",
    )


def generate_campaign_queries(spec: DiscoveryCampaignSpec) -> tuple[GlobalDiscoveryQuery, ...]:
    queries: list[GlobalDiscoveryQuery] = []
    topics = tuple(dict.fromkeys((*spec.buyer_habitats, *spec.buyer_concepts, *spec.industry_contexts)))
    for language in spec.languages:
        lang = language.casefold()
        for topic in topics:
            if not topic:
                continue
            phrases = _localized_phrases(topic, lang)
            for family, text in (
                (QueryFamily.DIRECT_TELEGRAM_SOURCE, f"Telegram chat {phrases[0]}"),
                (QueryFamily.DIRECT_TELEGRAM_SOURCE, f"Telegram community {phrases[0]}"),
                (QueryFamily.DIRECT_TELEGRAM_SOURCE, f"Телеграм чат {phrases[0]}" if lang == "ru" else f"Telegram group {phrases[0]}"),
                (QueryFamily.SITE_TELEGRAM, f"site:t.me {phrases[0]}"),
                (QueryFamily.COMMUNITY_DIRECTORY, f"best Telegram communities for {phrases[0]}" if lang == "en" else f"список Telegram чатов {phrases[0]}"),
                (QueryFamily.BUYER_HABITAT, f"{phrases[0]} Telegram community" if lang == "en" else f"{phrases[0]} Telegram сообщество"),
                (QueryFamily.HUB_LISTICLE, f"Telegram communities list {phrases[0]}" if lang == "en" else f"лучшие Telegram чаты {phrases[0]}"),
            ):
                queries.append(_query(spec, text, family, lang, topic))
    if spec.campaign_type is CampaignType.PROFILE_GAP:
        for language in spec.languages:
            for topic in spec.buyer_habitats:
                text = f"{topic} Telegram" if language.casefold() == "en" else f"{topic} Telegram"
                queries.append(_query(spec, text, QueryFamily.PROFILE_GAP, language.casefold(), topic))
    return collapse_campaign_queries(queries)


def collapse_campaign_queries(queries: Sequence[GlobalDiscoveryQuery]) -> tuple[GlobalDiscoveryQuery, ...]:
    output: list[GlobalDiscoveryQuery] = []
    exact: set[tuple[str, str, str]] = set()
    for query in queries:
        key = (query.campaign_key, query.family.value, query.normalized_query_key)
        if key in exact:
            continue
        exact.add(key)
        if any(
            query.campaign_key == existing.campaign_key
            and query.family is existing.family
            and query.language == existing.language
            and _near_duplicate(query.topic, existing.topic, query.text, existing.text)
            for existing in output
        ):
            continue
        output.append(query)
    return tuple(output)


def _query(spec: DiscoveryCampaignSpec, text: str, family: QueryFamily, language: str, topic: str) -> GlobalDiscoveryQuery:
    return GlobalDiscoveryQuery(
        text=text.strip(),
        family=family,
        language=language,
        normalized_query_key=_norm(text),
        strategy_version=spec.query_strategy_version,
        campaign_key=spec.campaign_key,
        topic=topic,
    )


def _localized_phrases(topic: str, language: str) -> tuple[str, ...]:
    return (topic,)


def _near_duplicate(left_topic: str, right_topic: str, left: str, right: str) -> bool:
    left_tokens = set(_norm(left_topic).split())
    right_tokens = set(_norm(right_topic).split())
    if left_tokens != right_tokens or not left_tokens:
        return False
    return _norm(left) == _norm(right)


def _norm(value: str) -> str:
    return " ".join(re.findall(r"[\w-]+", str(value).casefold(), flags=re.UNICODE))


class CandidatePriority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"
    INSUFFICIENT = "INSUFFICIENT"


def prioritize_candidate(context: Mapping[str, object], *, previously_rejected: bool = False) -> CandidatePriority:
    if previously_rejected:
        return CandidatePriority.INSUFFICIENT
    score = 0
    if context.get("normalized_reference") or context.get("telegram_reference"):
        score += 2
    if context.get("direct_telegram_result"):
        score += 3
    if context.get("source_graph_provenance"):
        score += 2
    score += min(3, int(context.get("independent_domains", 0) or 0))
    score += min(2, int(context.get("profile_gap_count", 0) or 0))
    if context.get("bot_like") or context.get("contact_like") or context.get("spam_directory"):
        score -= 5
    if score >= 6:
        return CandidatePriority.HIGH
    if score >= 3:
        return CandidatePriority.MEDIUM
    if score >= 1:
        return CandidatePriority.LOW
    return CandidatePriority.INSUFFICIENT


class MonitoringTier(str, Enum):
    A = "A"
    B = "B"
    C = "C"
    D = "D"


@dataclass(frozen=True)
class MonitoringCandidate:
    source_id: int
    tier: MonitoringTier
    due_at: float
    campaign_key: str = ""


class WeightedMonitoringScheduler:
    """Bounded fair scheduler; it selects due work, never Telegram timing."""

    def __init__(self, *, exploration_share: float = 0.20) -> None:
        if not 0 <= exploration_share <= 1:
            raise ValueError("exploration_share must be between 0 and 1")
        self.exploration_share = exploration_share

    def choose(self, candidates: Sequence[MonitoringCandidate], *, now: float, limit: int) -> tuple[MonitoringCandidate, ...]:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        due = [item for item in candidates if item.due_at <= now]
        due.sort(key=lambda item: (item.tier.value, item.due_at, item.source_id))
        exploration = [item for item in due if item.tier in {MonitoringTier.C, MonitoringTier.D}]
        exploitation = [item for item in due if item not in exploration]
        explore_limit = min(len(exploration), int(limit * self.exploration_share))
        selected = exploration[:explore_limit]
        selected.extend(exploitation[: max(0, limit - len(selected))])
        if len(selected) < limit:
            selected.extend(exploration[explore_limit : limit - len(selected) + explore_limit])
        return tuple(selected[:limit])


@dataclass(frozen=True)
class BackpressureDecision:
    state: str
    queue_lag: int
    slow_low_tier: bool
    pause_cold_catch_up: bool


def decide_backpressure(*, queued_analysis_jobs: int, threshold: int, low_tier: bool = True) -> BackpressureDecision:
    lag = max(0, queued_analysis_jobs)
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if lag >= threshold * 2:
        return BackpressureDecision("PAUSE_COLD_CATCH_UP", lag, True, True)
    if lag >= threshold:
        return BackpressureDecision("SLOW_LOW_TIER", lag, low_tier, False)
    return BackpressureDecision("NORMAL", lag, False, False)


def run_offline_scale_test() -> dict[str, object]:
    """Synthetic infrastructure check; never a product-quality measurement."""

    started = perf_counter()
    tracemalloc.start()
    refs = [f"https://t.me/source_{index % 5000}" for index in range(10_000)]
    normalized = {ref.casefold() for ref in refs}
    campaigns = {spec.campaign_key for spec in bootstrap_campaign_specs()}
    assignments = {(source_id % 3, source_id) for source_id in range(1000)}
    profiles = 10_000
    scheduler = WeightedMonitoringScheduler()
    scheduler.choose(tuple(MonitoringCandidate(i, MonitoringTier.B, 0) for i in range(1000)), now=1, limit=100)
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return {
        "synthetic_only": True,
        "references": len(refs),
        "unique_normalized_candidates": len(normalized),
        "bootstrap_campaigns": len(campaigns),
        "approved_sources": 1000,
        "collector_accounts": 3,
        "search_profiles": profiles,
        "monitoring_assignments": len(assignments),
        "elapsed_ms": int((perf_counter() - started) * 1000),
        "peak_memory_bytes": peak,
        "current_memory_bytes": current,
    }


if __name__ == "__main__":
    print(json.dumps(run_offline_scale_test(), sort_keys=True))
