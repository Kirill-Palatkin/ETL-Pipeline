import pandas as pd
import pytest
import sys
from pathlib import Path

# Добавляем родительскую директорию в путь для импорта etl.py
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl import validate_sales, validate_ads


class TestSalesValidation:
    """Тесты валидации данных продаж"""

    def test_validate_sales_duplicates(self):
        """Тест удаления дубликатов в sales"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': 20, 'revenue': 200},
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': 20, 'revenue': 200},
            # дубликат
            {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 0, 'units': 10, 'revenue': 100},
        ])
        result = validate_sales(df)
        assert len(result) == 2  # должен остаться только 1 уникальная строка + вторая уникальная

    def test_validate_sales_missing_values(self):
        """Тест обработки пропущенных значений"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': None, 'units': None, 'revenue': 200},
            {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 0, 'units': 10, 'revenue': None},
        ])
        result = validate_sales(df)
        assert result['units'].isna().sum() == 0
        assert result['revenue'].isna().sum() == 0
        assert result['promo_flag'].isna().sum() == 0

    def test_validate_sales_negative_values(self):
        """Тест фильтрации отрицательных значений"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': -5, 'revenue': 200},
            {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 0, 'units': 10, 'revenue': -100},
        ])
        result = validate_sales(df)
        assert len(result) == 0  # все строки с отрицательными значениями должны быть удалены

    def test_validate_sales_required_columns(self):
        """Тест проверки обязательных колонок"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1},  # отсутствуют обязательные колонки
        ])
        with pytest.raises(ValueError, match="Отсутствует колонка"):
            validate_sales(df)


class TestAdsValidation:
    """Тесты валидации рекламных данных"""

    def test_validate_ads_duplicates(self):
        """Тест удаления дубликатов в ads"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 50, 'cost': 25},
            {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 50, 'cost': 25},
            # дубликат
            {'date': '2025-01-01', 'channel': 'Y', 'campaign_id': 2, 'impressions': 500, 'clicks': 25, 'cost': 10},
        ])
        result = validate_ads(df)
        assert len(result) == 2

    def test_validate_ads_missing_values(self):
        """Тест обработки пропущенных значений в ads"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': None, 'clicks': 50, 'cost': 25},
            {'date': '2025-01-01', 'channel': 'Y', 'campaign_id': 2, 'impressions': 500, 'clicks': None, 'cost': 10},
        ])
        result = validate_ads(df)
        assert result['impressions'].isna().sum() == 0
        assert result['clicks'].isna().sum() == 0
        assert result['cost'].isna().sum() == 0

    def test_validate_ads_negative_values(self):
        """Тест фильтрации отрицательных значений в ads"""
        df = pd.DataFrame([
            {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': -100, 'clicks': 50, 'cost': 25},
            {'date': '2025-01-01', 'channel': 'Y', 'campaign_id': 2, 'impressions': 500, 'clicks': -25, 'cost': 10},
        ])
        result = validate_ads(df)
        assert len(result) == 0