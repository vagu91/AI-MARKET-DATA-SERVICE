from app.core.config import Settings


def test_settings_reads_unprefixed_fred_and_bea_env_names(tmp_path, monkeypatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("FRED_API_KEY=fred-secret\nBEA_API_KEY=bea-secret\n", encoding="utf-8")

    settings = Settings(_env_file=env_file)

    assert settings.fred_api_key == "fred-secret"
    assert settings.bea_api_key == "bea-secret"


def test_settings_prefixed_env_names_still_work(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "AI_MARKET_FRED_API_KEY=fred-prefixed\nAI_MARKET_BEA_API_KEY=bea-prefixed\n",
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.fred_api_key == "fred-prefixed"
    assert settings.bea_api_key == "bea-prefixed"


def test_settings_reads_alpha_vantage_env_aliases(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ALPHA_VANTAGE_API_KEY=alpha-alias\n", encoding="utf-8")

    settings = Settings(_env_file=env_file)

    assert settings.alpha_vantage_api_key == "alpha-alias"


def test_settings_reads_prefixed_alpha_vantage_env_name(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("AI_MARKET_ALPHA_VANTAGE_API_KEY=alpha-prefixed\n", encoding="utf-8")

    settings = Settings(_env_file=env_file)

    assert settings.alpha_vantage_api_key == "alpha-prefixed"


def test_settings_reads_fmp_key_aliases_and_xtb_controls(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join([
            "FMP_API_KEY=fmp-secret",
            "AI_MARKET_ENABLE_XTB_CALENDAR=true",
            "AI_MARKET_XTB_CALENDAR_MIN_IMPACT=2",
            "AI_MARKET_XTB_CALENDAR_LOOKAHEAD_DAYS=7",
        ]),
        encoding="utf-8",
    )
    settings = Settings(_env_file=env_file)
    assert settings.fmp_api_key == "fmp-secret"
    assert settings.enable_xtb_calendar is True
    assert settings.xtb_calendar_min_impact == 2
    assert settings.xtb_calendar_lookahead_days == 7


def test_settings_reads_openai_event_enrichment_config(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("AI_MARKET_OPENAI_API_KEY", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "OPENAI_API_KEY=openai-alias",
                "AI_MARKET_ENABLE_OPENAI_EVENT_ENRICHMENT=true",
                "AI_MARKET_OPENAI_EVENT_ENRICHMENT_MODEL=test-model",
                "AI_MARKET_OPENAI_EVENT_ENRICHMENT_MAX_EVENTS=3",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.openai_api_key == "openai-alias"
    assert settings.enable_openai_event_enrichment is True
    assert settings.openai_event_enrichment_model == "test-model"
    assert settings.openai_event_enrichment_max_events == 3


def test_settings_reads_browser_scraping_config(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ENABLE_BROWSER_SCRAPING=true",
                "BROWSER_SCRAPING_HEADLESS=false",
                "BROWSER_SCRAPING_TIMEOUT_SECONDS=7",
                "BROWSER_SCRAPING_MAX_PAGES=2",
                "ENABLE_AGGRESSIVE_SCRAPING=true",
                "ENRICH_ONLY_HIGH_IMPACT=false",
                "ENRICHMENT_MAX_EVENTS=4",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.enable_browser_scraping is True
    assert settings.browser_scraping_headless is False
    assert settings.browser_scraping_timeout_seconds == 7
    assert settings.browser_scraping_max_pages == 2
    assert settings.enable_aggressive_scraping is True
    assert settings.enrich_only_high_impact is False
    assert settings.enrichment_max_events == 4


def test_settings_reads_targeted_search_config(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(
            [
                "ENABLE_TARGETED_SEARCH_ENRICHMENT=false",
                "TARGETED_SEARCH_MAX_EVENTS=4",
                "TARGETED_SEARCH_TIMEOUT_SECONDS=6",
                "TARGETED_SEARCH_RECENCY_DAYS=12",
                "TARGETED_SEARCH_REQUIRE_SOURCE_URL=false",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(_env_file=env_file)

    assert settings.enable_targeted_search_enrichment is False
    assert settings.targeted_search_max_events == 4
    assert settings.targeted_search_timeout_seconds == 6
    assert settings.targeted_search_recency_days == 12
    assert settings.targeted_search_require_source_url is False
