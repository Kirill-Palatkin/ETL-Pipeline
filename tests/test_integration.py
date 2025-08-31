import pandas as pd
import pytest
import sys
from pathlib import Path
import tempfile
import shutil

# Добавляем родительскую директорию в путь для импорта etl.py
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl import transform, cmd_extract, _make_sales_sample, _make_ads_sample


class TestIntegration:
    """Интеграционные тесты полного ETL процесса"""

    def test_full_transform_integration(self):
        """Интеграционный тест полного transform процесса"""
        sales = _make_sales_sample()
        ads = _make_ads_sample()

        result = transform(sales, ads)

        # Проверяем что все ожидаемые ключи присутствуют
        expected_keys = ['uplift_by_store', 'ads_metrics', 'anomalies', 'ctr_anom', 'cpc_anom']
        for key in expected_keys:
            assert key in result
            assert result[key] is not None

        # Проверяем структуру данных
        assert 'store_id' in result['uplift_by_store'].columns
        assert 'channel' in result['ads_metrics'].columns
        assert 'anomaly' in result['anomalies'].columns

        # Проверяем что данные не пустые
        assert len(result['uplift_by_store']) > 0
        assert len(result['ads_metrics']) > 0
        assert 'ctr' in result['ads_metrics'].columns
        assert 'cpc' in result['ads_metrics'].columns

    def test_cmd_extract_integration(self, tmp_path):
        """Тест команды extract с временными файлами"""
        # Создаем временные CSV файлы
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        sales_data = """date,store_id,sku_id,promo_flag,units,revenue
2025-01-01,A,1,1,20,200
2025-01-01,A,1,0,10,100"""

        ads_data = """date,channel,campaign_id,impressions,clicks,cost
2025-01-01,X,1,1000,50,25"""

        (data_dir / "sales.csv").write_text(sales_data)
        (data_dir / "ads.csv").write_text(ads_data)

        # Запускаем extract
        result = cmd_extract(sales_path=data_dir / "sales.csv", ads_path=data_dir / "ads.csv")

        assert 'sales' in result
        assert 'ads' in result
        assert len(result['sales']) == 2
        assert len(result['ads']) == 1

        # Проверяем что данные прошли валидацию
        assert 'date' in result['sales'].columns
        assert 'store_id' in result['sales'].columns
        assert 'units' in result['sales'].columns
        assert 'revenue' in result['sales'].columns

        assert 'date' in result['ads'].columns
        assert 'channel' in result['ads'].columns
        assert 'impressions' in result['ads'].columns
        assert 'clicks' in result['ads'].columns
        assert 'cost' in result['ads'].columns

    def test_transform_with_realistic_data(self):
        """Тест transform с реалистичными данными"""
        # Создаем более реалистичные данные
        sales_data = pd.DataFrame([
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': 100, 'revenue': 1000},
            {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 0, 'units': 50, 'revenue': 500},
            {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 1, 'units': 80, 'revenue': 800},
            {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 0, 'units': 60, 'revenue': 600},
            {'date': '2025-01-02', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': 120, 'revenue': 1200},
        ])

        ads_data = pd.DataFrame([
            {'date': '2025-01-01', 'channel': 'google', 'campaign_id': 1, 'impressions': 10000, 'clicks': 500,
             'cost': 250},
            {'date': '2025-01-01', 'channel': 'facebook', 'campaign_id': 2, 'impressions': 8000, 'clicks': 400,
             'cost': 200},
            {'date': '2025-01-02', 'channel': 'google', 'campaign_id': 1, 'impressions': 12000, 'clicks': 600,
             'cost': 300},
            {'date': '2025-01-02', 'channel': 'facebook', 'campaign_id': 2, 'impressions': 9000, 'clicks': 300,
             'cost': 150},
        ])

        result = transform(sales_data, ads_data)

        # Проверяем основные результаты
        assert len(result['uplift_by_store']) == 2  # два магазина
        assert len(result['ads_metrics']) == 4  # 2 канала × 2 дня

        # Проверяем расчет метрик
        google_metrics = result['ads_metrics'][result['ads_metrics']['channel'] == 'google']
        assert 'ctr' in google_metrics.columns
        assert 'cpc' in google_metrics.columns

        # Проверяем что uplift рассчитан корректно
        uplift_a = result['uplift_by_store'][result['uplift_by_store']['store_id'] == 'A']
        assert uplift_a['units_uplift_pct'].iloc[0] > 0  # положительный uplift

    def test_extract_with_missing_files(self, tmp_path):
        """Тест обработки отсутствующих файлов в extract"""
        data_dir = tmp_path / "data"
        data_dir.mkdir()

        # Создаем только один файл
        sales_data = """date,store_id,sku_id,promo_flag,units,revenue
2025-01-01,A,1,1,20,200"""

        (data_dir / "sales.csv").write_text(sales_data)

        # Должна возникнуть ошибка из-за отсутствующего ads.csv
        with pytest.raises(FileNotFoundError, match="Ожидаемые файлы"):
            cmd_extract(sales_path=data_dir / "sales.csv", ads_path=data_dir / "ads.csv")

    def test_transform_edge_cases(self):
        """Тест transform с edge cases"""
        # Пустые данные
        empty_sales = pd.DataFrame(columns=['date', 'store_id', 'sku_id', 'promo_flag', 'units', 'revenue'])
        empty_ads = pd.DataFrame(columns=['date', 'channel', 'campaign_id', 'impressions', 'clicks', 'cost'])

        result = transform(empty_sales, empty_ads)

        # Должны вернуться все ключи, даже с пустыми данными
        expected_keys = ['uplift_by_store', 'ads_metrics', 'anomalies', 'ctr_anom', 'cpc_anom']
        for key in expected_keys:
            assert key in result
            assert result[key] is not None

    def test_anomaly_detection_integration(self):
        """Тест интеграции обнаружения аномалий"""
        sales = _make_sales_sample()

        # Создаем ads данные с явной аномалией
        ads_with_anomaly = pd.DataFrame([
            {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 50, 'cost': 25},
            {'date': '2025-01-02', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 5, 'cost': 25},
            # низкий CTR
            {'date': '2025-01-03', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 1000, 'cost': 25},
            # аномально высокий CTR
        ])

        result = transform(sales, ads_with_anomaly)

        # Должны быть обнаружены аномалии
        assert len(result['anomalies']) > 0
        assert result['anomalies']['anomaly'].sum() > 0