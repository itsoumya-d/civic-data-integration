from __future__ import annotations

from .models import Asset


class AssetStore:
    """In-memory canonical asset store keyed by source system and source record."""

    def __init__(self) -> None:
        self._assets: dict[tuple[str, str], Asset] = {}

    def upsert(self, asset: Asset) -> str:
        key = (asset.source_system, asset.source_id)
        existing = self._assets.get(key)
        if existing is None:
            self._assets[key] = asset
            return "created"
        if existing.fingerprint() != asset.fingerprint():
            self._assets[key] = asset
            return "updated"
        return "unchanged"

    def get(self, source_system: str, source_id: str) -> Asset | None:
        return self._assets.get((source_system, source_id))

    def all(self) -> list[Asset]:
        return list(self._assets.values())

    def by_asset_id(self) -> dict[str, Asset]:
        return {asset.asset_id: asset for asset in self._assets.values()}
