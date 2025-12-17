"""Tests pour les fetchers."""
import pytest
from pipeline.fetchers.openfoodfacts import OpenFoodFactsFetcher
from pipeline.fetchers.adresse import AdresseFetcher


class TestOpenFoodFactsFetcher:
    def test_fetch_batch_returns_list(self):
        fetcher = OpenFoodFactsFetcher()
        result = fetcher.fetch_batch("chocolats", page=1, page_size=5)
        assert isinstance(result, list)
        assert len(result) <= 5

    def test_fetch_batch_has_required_fields(self):
        fetcher = OpenFoodFactsFetcher()
        products = fetcher.fetch_batch("chocolats", page=1, page_size=3)
        if products:
            product = products[0]
            assert "code" in product


class TestAdresseFetcher:
    def test_geocode_single_valid_address(self):
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("20 avenue de ségur paris")
        assert result.original_address == "20 avenue de ségur paris"
        assert result.score > 0.5
        assert result.latitude is not None
        assert result.longitude is not None

    def test_geocode_single_invalid_address(self):
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("xyzabc123456")
        assert result.score < 0.5 or result.latitude is None

    def test_geocode_empty_address(self):
        fetcher = AdresseFetcher()
        result = fetcher.geocode_single("")
        assert result.score == 0
