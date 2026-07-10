from app.models.macro import MacroLatestResponse, MacroSeries
from app.providers.bea import BeaProvider
from app.providers.bls import BlsProvider
from app.providers.fred import FredProvider


class MacroService:
    def __init__(self, providers: list[FredProvider | BlsProvider | BeaProvider]) -> None:
        self.providers = providers

    async def latest(self) -> MacroLatestResponse:
        output: list[MacroSeries] = []
        provider_results = []
        for provider in self.providers:
            result = await provider.fetch_safe()
            provider_results.append(result.metadata)
            if not isinstance(result.data, dict):
                continue
            for item in result.data.values():
                if not isinstance(item, dict):
                    continue
                output.append(
                    MacroSeries(
                        series_id=str(item["series_id"]),
                        name=str(item["name"]),
                        value=item.get("value"),
                        units=item.get("units"),
                        data_as_of=item.get("data_as_of"),
                        source=str(item["source"]),
                        metadata=result.metadata,
                    )
                )
        return MacroLatestResponse(series=output, provider_results=provider_results)

