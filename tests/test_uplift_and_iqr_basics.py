import math
import pandas as pd
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from etl import compute_uplift, iqr_anomalies, _make_sales_sample, _make_ads_sample


class TestOriginalFunctions:
    """Тесты из оригинального etl.py"""

    def test_uplift_basic(self):
        """Оригинальный тест uplift"""
        s = _make_sales_sample()
        res = compute_uplift(s, by=["store_id"])
        # For store A: units_promo_mean = 20, units_nonpromo_mean=10 => uplift 100%
        a = res[res['store_id'] == 'A'].iloc[0]
        assert math.isclose(a['units_promo_mean'], 20)
        assert math.isclose(a['units_nonpromo_mean'], 10)
        assert math.isclose(a['units_uplift_pct'], 100.0)

    def test_iqr_basic(self):
        """Оригинальный тест IQR"""
        ads = _make_ads_sample()
        ads_metrics = ads.groupby(['date', 'channel']).agg({
            'impressions': 'sum',
            'clicks': 'sum',
            'cost': 'sum'
        }).reset_index()
        ads_metrics['ctr'] = ads_metrics['clicks'] / ads_metrics['impressions']
        res = iqr_anomalies(ads_metrics, ['channel'], 'ctr')
        # channel X has ctrs 0.05 and 0.0333 => median ~0.0416, iqr >0 so no NaN
        assert 'median' in res.columns and 'iqr' in res.columns