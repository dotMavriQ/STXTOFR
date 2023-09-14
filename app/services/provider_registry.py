from __future__ import annotations

from app.providers.base import ProviderAdapter
from app.providers.circlek.adapter import CircleKAdapter
from app.providers.espresso_house.adapter import EspressoHouseAdapter
from app.providers.ids.adapter import IDSAdapter
from app.providers.preem.adapter import PreemAdapter
from app.providers.rasta.adapter import RastaAdapter
from app.providers.trafikverket.adapter import TrafikverketParkingAdapter
from app.providers.trb.adapter import TRBAdapter


class ProviderRegistry:
    def __init__(self, adapters: dict[str, ProviderAdapter]):
        self.adapters = adapters

    def get(self, provider_name: str) -> ProviderAdapter:
        try:
            return self.adapters[provider_name]
        except KeyError as exc:
            raise KeyError(f"unknown provider: {provider_name}") from exc

    def list(self) -> list[ProviderAdapter]:
        return list(self.adapters.values())

    def names(self) -> list[str]:
        return list(self.adapters.keys())


def build_provider_registry() -> ProviderRegistry:
    adapters: dict[str, ProviderAdapter] = {
        "circlek": CircleKAdapter(),
        "rasta": RastaAdapter(),
        "ids": IDSAdapter(),
        "preem": PreemAdapter(),
        "espresso_house": EspressoHouseAdapter(),
        "trafikverket": TrafikverketParkingAdapter(),
        "trb": TRBAdapter(),
    }
    return ProviderRegistry(adapters=adapters)

