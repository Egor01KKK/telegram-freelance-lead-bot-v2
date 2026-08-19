from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import sqlalchemy as sa

from ..config import RuntimeConfig, RuntimeMode
from ..sources import Source, load_sources
from .database import Database
from .source_repository import SeedSource, SourceRepository, SourceStatus


SEED_PROVIDER = "repository_seed"
SEED_LOCK_NAME = "freelancer_bot:repository_source_seed"


@dataclass(frozen=True)
class SourceSeedResult:
    total: int
    created: int
    updated: int
    unchanged: int
    lineage_created: int
    approved_entries: int
    candidate_entries: int
    snapshot_sha256: str


class SourceSeedImporter:
    def __init__(
        self,
        database: Database,
        repository: SourceRepository | None = None,
    ) -> None:
        self._database = database
        self._repository = repository or SourceRepository()

    async def import_file(self, path: Path) -> SourceSeedResult:
        snapshot_sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
        configured_sources = load_sources(path)
        entries = [
            _seed_source(source, snapshot_sha256=snapshot_sha256)
            for source in configured_sources
        ]

        created = 0
        updated = 0
        unchanged = 0
        lineage_created = 0
        async with self._database.transaction() as connection:
            await connection.execute(
                sa.text("SELECT pg_advisory_xact_lock(hashtextextended(:name, 0))"),
                {"name": SEED_LOCK_NAME},
            )
            for entry in entries:
                outcome = await self._repository.upsert_seed(connection, entry)
                created += int(outcome.created)
                updated += int(outcome.updated)
                unchanged += int(not outcome.created and not outcome.updated)
                lineage_created += int(outcome.lineage_created)

        return SourceSeedResult(
            total=len(entries),
            created=created,
            updated=updated,
            unchanged=unchanged,
            lineage_created=lineage_created,
            approved_entries=sum(
                entry.initial_status is SourceStatus.APPROVED for entry in entries
            ),
            candidate_entries=sum(
                entry.initial_status is SourceStatus.CANDIDATE for entry in entries
            ),
            snapshot_sha256=snapshot_sha256,
        )


def _seed_source(source: Source, *, snapshot_sha256: str) -> SeedSource:
    handle = source.handle.lower()
    username = handle.removeprefix("@")
    enabled = source.enabled
    return SeedSource(
        platform="telegram",
        external_id=f"username:{username}",
        access_type="public",
        initial_status=(SourceStatus.APPROVED if enabled else SourceStatus.CANDIDATE),
        display_name=source.title,
        handle=handle,
        canonical_url=f"https://t.me/{username}",
        provider=SEED_PROVIDER,
        lineage_key=f"repository-json:{username}",
        provider_run_id=snapshot_sha256,
        seed_reference=handle,
        context={
            "enabled": enabled,
            "reason": source.reason,
            "seed_handle": handle,
            "tags": list(source.tags),
        },
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Import the repository source seed into PostgreSQL"
    )
    parser.add_argument("--sources-json", type=Path)
    arguments = parser.parse_args(argv)

    config = RuntimeConfig.from_env(mode=RuntimeMode.DATABASE)
    sources_path = arguments.sources_json or config.sources_path

    async def run() -> SourceSeedResult:
        database = Database(config.postgresql_url())
        try:
            return await SourceSeedImporter(database).import_file(sources_path)
        finally:
            await database.close()

    try:
        result = asyncio.run(run())
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
