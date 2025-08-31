"""
ETL pipeline: extract -> transform -> load
Usage examples:
  python etl.py extract
  python etl.py transform
  python etl.py load
  python etl.py run_all --explain

Outputs:
 - SQLite DB ./etl.db (configurable via DB_URL env)
 - Parquet files in ./output/
 - report.md in ./reports/
 - small matplotlib charts in ./reports/
"""

from __future__ import annotations
import argparse
import sys
import sqlite3
from pathlib import Path
import pandas as pd
import numpy as np
import logging
from datetime import datetime
import json

from dotenv import dotenv_values

config = dotenv_values()

try:
    import pyarrow as pa
    import pyarrow.parquet as pq

    HAS_PYARROW = True
except Exception:
    HAS_PYARROW = False

try:
    import matplotlib.pyplot as plt

    HAS_MPL = True
except Exception:
    HAS_MPL = False

LOG = logging.getLogger("etl")
LOG.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
LOG.addHandler(handler)

BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "output"
REPORTS_DIR = BASE_DIR / "reports"
OUTPUT_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

DB_URL = config.get("DB_URL")
USE_LLM = config.get("USE_LLM")
MODEL_NAME = config.get("MODEL_NAME")


def read_csv_safe(path: Path, **kwargs) -> pd.DataFrame:
    LOG.info(f"Чтение {path}")
    df = pd.read_csv(path, **kwargs)
    LOG.info(f"Прочитано {len(df)} строк, колонки: {list(df.columns)}")
    return df


def validate_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Проверки: дубликаты, пропуски, диапазоны. Возвращает очищенный df"""
    before = len(df)
    # drop exact duplicate rows
    df = df.drop_duplicates()
    if len(df) != before:
        LOG.warning(f"Удалено {before - len(df)} полных дубликатов")

    required = ["date", "store_id", "sku_id", "promo_flag", "units", "revenue"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Отсутствует колонка {c} в sales")

    miss = df[required].isna().sum().sum()
    if miss:
        LOG.warning(f"Найдено {miss} пропусков в обязательных полях – они будут заполнены нулями там, где уместно")
    df["units"] = df["units"].fillna(0).astype(float)
    df["revenue"] = df["revenue"].fillna(0).astype(float)
    df["promo_flag"] = df["promo_flag"].fillna(0).astype(int)

    bad_units = df[df["units"] < 0]
    if len(bad_units):
        LOG.warning(f"Найдено {len(bad_units)} строк с отрицательными units. Удаляю их.")
        df = df[df["units"] >= 0]
    bad_rev = df[df["revenue"] < 0]
    if len(bad_rev):
        LOG.warning(f"Найдено {len(bad_rev)} строк с отрицательными revenue. Удаляю их.")
        df = df[df["revenue"] >= 0]

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    LOG.info(f"Sales: после валидации {len(df)} строк")
    return df


def validate_ads(df: pd.DataFrame) -> pd.DataFrame:
    before = len(df)
    df = df.drop_duplicates()
    if len(df) != before:
        LOG.warning(f"Удалено {before - len(df)} полных дубликатов в ads")

    required = ["date", "channel", "campaign_id", "impressions", "clicks", "cost"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Отсутствует колонка {c} в ads")

    miss = df[required].isna().sum().sum()
    if miss:
        LOG.warning(f"Найдено {miss} пропусков в ads. Заполнение нулями для числовых полей")
    df["impressions"] = df["impressions"].fillna(0).astype(float)
    df["clicks"] = df["clicks"].fillna(0).astype(float)
    df["cost"] = df["cost"].fillna(0).astype(float)

    df = df[df["impressions"] >= 0]
    df = df[df["clicks"] >= 0]
    df = df[df["cost"] >= 0]

    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    LOG.info(f"Ads: после валидации {len(df)} строк")
    return df


# Extract
def cmd_extract(sales_path: Path = DATA_DIR / "sales.csv", ads_path: Path = DATA_DIR / "ads.csv") -> dict:
    if not sales_path.exists() or not ads_path.exists():
        raise FileNotFoundError("Ожидаемые файлы data/sales.csv и data/ads.csv не найдены")
    sales = read_csv_safe(sales_path)
    ads = read_csv_safe(ads_path)
    sales = validate_sales(sales)
    ads = validate_ads(ads)
    # save intermediate cleansed versions
    cleandir = OUTPUT_DIR / "cleansed"
    cleandir.mkdir(parents=True, exist_ok=True)
    sales.to_parquet(cleandir / "sales.cleansed.parquet", index=False) if HAS_PYARROW else sales.to_csv(
        cleandir / "sales.cleansed.csv", index=False)
    ads.to_parquet(cleandir / "ads.cleansed.parquet", index=False) if HAS_PYARROW else ads.to_csv(
        cleandir / "ads.cleansed.csv", index=False)
    LOG.info("Extract finished and cleansed files saved")
    return {"sales": sales, "ads": ads}


# Transform
def compute_uplift(sales: pd.DataFrame, by: list = ["store_id"]) -> pd.DataFrame:
    """Compute uplift % for promo vs non-promo per grouping.
    Uplift computed for metrics: units, revenue. Formula: (mean_promo - mean_nonpromo) / mean_nonpromo *100
    If denom is zero, use NaN.
    Returns df with columns: grouping..., units_promo_mean, units_nonpromo_mean, units_uplift_pct, revenue_promo_mean, revenue_nonpromo_mean, revenue_uplift_pct
    """
    g = sales.groupby(by + ["promo_flag"]).agg({"units": "mean", "revenue": "mean"}).reset_index()
    # pivot promo_flag
    pivot = g.pivot_table(index=by, columns="promo_flag", values=["units", "revenue"]).fillna(0)

    # columns like ('units', 0) ('units',1)
    def safe_get(df, metric, flag):
        try:
            return df[(metric, flag)]
        except Exception:
            return pd.Series(0, index=df.index)

    units_p = safe_get(pivot, "units", 1)
    units_n = safe_get(pivot, "units", 0)
    rev_p = safe_get(pivot, "revenue", 1)
    rev_n = safe_get(pivot, "revenue", 0)

    uplift_units = ((units_p - units_n) / np.where(units_n == 0, np.nan, units_n)) * 100
    uplift_rev = ((rev_p - rev_n) / np.where(rev_n == 0, np.nan, rev_n)) * 100

    if len(by) > 1:
        idx_dict = {col: pivot.index.get_level_values(i) for i, col in enumerate(by)}
    else:
        idx_dict = {by[0]: pivot.index}

    out = pd.DataFrame({
        **idx_dict,
        "units_promo_mean": units_p.values,
        "units_nonpromo_mean": units_n.values,
        "units_uplift_pct": uplift_units,
        "revenue_promo_mean": rev_p.values,
        "revenue_nonpromo_mean": rev_n.values,
        "revenue_uplift_pct": uplift_rev,
    })

    return out


def iqr_anomalies(df: pd.DataFrame, group_by: list, metric: str) -> pd.DataFrame:
    """Flag anomalies where value is outside median ± IQR per group_by (group_by is list of columns to group)"""
    # compute per group the median and IQR
    stats = df.groupby(group_by)[metric].agg(['median', lambda x: np.percentile(x, 75) - np.percentile(x, 25)])
    stats = stats.rename(columns={'<lambda_0>': 'iqr'})
    stats = stats.reset_index()
    merged = df.merge(stats, on=group_by, how='left')
    merged['lower'] = merged['median'] - merged['iqr']
    merged['upper'] = merged['median'] + merged['iqr']
    merged['anomaly'] = (merged[metric] < merged['lower']) | (merged[metric] > merged['upper'])
    return merged


def transform(sales: pd.DataFrame, ads: pd.DataFrame) -> dict:
    LOG.info("Transform: расчёт uplift по магазинам")
    uplift_by_store = compute_uplift(sales, by=["store_id"])

    # ads metrics
    LOG.info("Transform: расчёт CTR и CPC по channel")
    ads_metrics = ads.groupby(["date", "channel"]).agg({
        "impressions": "sum",
        "clicks": "sum",
        "cost": "sum",
    }).reset_index()
    ads_metrics['ctr'] = (ads_metrics['clicks'] / ads_metrics['impressions']).replace([np.inf, -np.inf], np.nan)
    ads_metrics['cpc'] = (ads_metrics['cost'] / ads_metrics['clicks']).replace([np.inf, -np.inf], np.nan)

    # anomalies per channel across dates
    LOG.info("Transform: поиск аномалий по CTR и CPC (median ± IQR) по каналам")
    ctr_anom = iqr_anomalies(ads_metrics, ['channel'], 'ctr')
    cpc_anom = iqr_anomalies(ads_metrics, ['channel'], 'cpc')

    # We'll also prepare a top anomalies table merging both
    ctr_flags = ctr_anom[ctr_anom['anomaly']].copy()
    ctr_flags['metric'] = 'ctr'
    cpc_flags = cpc_anom[cpc_anom['anomaly']].copy()
    cpc_flags['metric'] = 'cpc'
    anomalies = pd.concat([ctr_flags, cpc_flags], ignore_index=True, sort=False)

    LOG.info(f"Найдено {len(anomalies)} аномалий")
    return {
        'uplift_by_store': uplift_by_store,
        'ads_metrics': ads_metrics,
        'anomalies': anomalies,
        'ctr_anom': ctr_anom,
        'cpc_anom': cpc_anom,
    }


# Load
def save_parquet(df, path: Path):
    if df is None or getattr(df, "empty", True):
        logging.warning(f"Пустой DataFrame, пропуск сохранения: {path}")
        return
    df.to_csv(path.with_suffix('.csv'), index=False)
    df.to_parquet(path, index=False)
    logging.info(f"Сохранено {path}")


def save_sqlite(table_name: str, df: pd.DataFrame, db_path: Path = Path('./etl.db')):
    con = sqlite3.connect(str(db_path))
    df.to_sql(table_name, con, if_exists='replace', index=False)
    con.close()
    LOG.info(f"Сохранено в sqlite: {db_path} таблица {table_name}")


def render_report(transform_res: dict, out_path: Path = REPORTS_DIR / 'report.md') -> None:
    anomalies = transform_res['anomalies']
    uplift = transform_res['uplift_by_store']

    lines = []
    lines.append(f"# Отчёт по аномалиям и uplift — {datetime.utcnow().isoformat()} UTC")
    lines.append("")
    lines.append("## Краткая сводка")
    lines.append("")
    lines.append(f"Найдено {len(anomalies)} строк с аномалиями по CTR/CPC (median ± IQR).")
    lines.append("")
    # Top-10 anomalies
    lines.append("## Top-10 аномалий (по abs отклонению от медианы)")
    if len(anomalies):
        anomalies['dev'] = (anomalies['metric'].map(lambda x: 0) if 'metric' not in anomalies.columns else 0)
        # compute deviation
        anomalies['deviation'] = (
                    anomalies['ctr'] - anomalies['median']).abs() if 'ctr' in anomalies.columns else np.nan

        # fallback compute whichever metric column present
        def compute_dev(row):
            if row['metric'] == 'ctr':
                return abs(row.get('ctr', np.nan) - row.get('median', np.nan))
            else:
                return abs(row.get('cpc', np.nan) - row.get('median', np.nan))

        anomalies['deviation'] = anomalies.apply(compute_dev, axis=1)
        top = anomalies.sort_values('deviation', ascending=False).head(10)
        lines.append(
            top[['date', 'channel', 'metric', 'median', 'iqr', 'lower', 'upper', 'deviation']].to_markdown(index=False))
    else:
        lines.append('Аномалии не обнаружены')
    lines.append("")

    lines.append("## Uplift по магазинам (promo vs non-promo)")
    lines.append(uplift.sort_values('revenue_uplift_pct', ascending=False).head(10).to_markdown(index=False))
    lines.append("")

    if USE_LLM == '1' or USE_LLM == 'True':
        ln = []
        ln.append('## LLM: краткий вывод (на основе найденных результатов)')
        if len(anomalies):
            chans = anomalies['channel'].unique().tolist()
            ln.append('Наблюдаются аномалии в следующих рекламных каналах: ' + ', '.join(map(str, chans)) + '.')
            ln.append(
                'Рекомендация: проверить таргетинг/кампании и распределение показов в указанных каналах; сверить данные кликов и затрат в периоды с аномалиями.')
            ln.append(
                'Также проверить соответствие promo_flag в витринах, где uplift по выручке сильно отличается от среднего.')
        else:
            ln.append(
                'Аномалий не обнаружено; фокус — обычная проверка распределения трафика и корректности импутаций.')
        lines.extend(ln)
    else:
        lines.append('## LLM: шаблон-объяснение')
        lines.append(
            'Проверьте указанные витрины/каналы и периоды, сопоставьте кампании и промо-активности. (USE_LLM=1 для авто-выводов)')

    out_path.write_text('\n\n'.join(lines), encoding='utf-8')
    LOG.info(f"Report written to {out_path}")

    if HAS_MPL:
        try:
            # Убедимся, что директория существует
            REPORTS_DIR.mkdir(exist_ok=True)

            # chart: top 10 channels by average ctr
            avg_ctr = transform_res['ads_metrics'].groupby('channel')['ctr'].mean().reset_index()
            avg_ctr = avg_ctr.sort_values('ctr', ascending=False).head(10)

            plt.figure(figsize=(8, 6))
            bars = plt.bar(avg_ctr['channel'], avg_ctr['ctr'])
            plt.xlabel('Канал')
            plt.ylabel('CTR')
            plt.title('Топ-10 каналов по среднему CTR')
            plt.xticks(rotation=45, ha='right')

            # Добавим значения на столбцы
            for bar in bars:
                height = bar.get_height()
                plt.text(bar.get_x() + bar.get_width() / 2., height,
                         f'{height:.3f}', ha='center', va='bottom')

            plt.tight_layout()
            plt.savefig(REPORTS_DIR / 'top_channels_ctr.png', dpi=100, bbox_inches='tight')
            plt.close()
            LOG.info('Saved chart top_channels_ctr.png')

        except Exception as e:
            LOG.warning('Не удалось сохранить графики: %s', e)


def cmd_load(transform_res: dict, db_path: Path = Path('./etl.db')) -> None:
    save_parquet(transform_res['uplift_by_store'], OUTPUT_DIR / 'uplift_by_store.parquet')
    save_parquet(transform_res['ads_metrics'], OUTPUT_DIR / 'ads_metrics.parquet')
    save_parquet(transform_res['anomalies'], OUTPUT_DIR / 'anomalies.parquet')

    save_sqlite('uplift_by_store', transform_res['uplift_by_store'], db_path)
    save_sqlite('ads_metrics', transform_res['ads_metrics'], db_path)
    save_sqlite('anomalies', transform_res['anomalies'], db_path)

    render_report(transform_res)


def parse_args():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest='cmd')
    sub.add_parser('extract')
    sub.add_parser('transform')
    sub.add_parser('load')
    run_all = sub.add_parser('run_all')
    run_all.add_argument('--explain', action='store_true', help='append LLM explain block (honour USE_LLM env)')
    return p.parse_args()


def main():
    args = parse_args()
    state = {}
    global USE_LLM
    if getattr(args, 'explain', False):
        USE_LLM = '1'
    try:
        if args.cmd == 'extract':
            state = cmd_extract()
            (OUTPUT_DIR / 'state.json').write_text(json.dumps({'extracted': True}), encoding='utf-8')
        elif args.cmd == 'transform':
            cleandir = OUTPUT_DIR / 'cleansed'
            sales = None
            ads = None
            if (cleandir / 'sales.cleansed.parquet').exists():
                sales = pd.read_parquet(cleandir / 'sales.cleansed.parquet')
            elif (cleandir / 'sales.cleansed.csv').exists():
                sales = pd.read_csv(cleandir / 'sales.cleansed.csv')
            if (cleandir / 'ads.cleansed.parquet').exists():
                ads = pd.read_parquet(cleandir / 'ads.cleansed.parquet')
            elif (cleandir / 'ads.cleansed.csv').exists():
                ads = pd.read_csv(cleandir / 'ads.cleansed.csv')
            if sales is None or ads is None:
                raise RuntimeError('Отсутствуют очищенные файлы. Запустите extract сначала.')
            res = transform(sales, ads)
            # save intermediate
            (OUTPUT_DIR / 'transform').mkdir(exist_ok=True)
            res['uplift_by_store'].to_parquet(OUTPUT_DIR / 'transform' / 'uplift_by_store.parquet') if HAS_PYARROW else \
            res['uplift_by_store'].to_csv(OUTPUT_DIR / 'transform' / 'uplift_by_store.csv', index=False)
            res['ads_metrics'].to_parquet(OUTPUT_DIR / 'transform' / 'ads_metrics.parquet') if HAS_PYARROW else res[
                'ads_metrics'].to_csv(OUTPUT_DIR / 'transform' / 'ads_metrics.csv', index=False)
            res['anomalies'].to_parquet(OUTPUT_DIR / 'transform' / 'anomalies.parquet') if HAS_PYARROW else res[
                'anomalies'].to_csv(OUTPUT_DIR / 'transform' / 'anomalies.csv', index=False)
            (OUTPUT_DIR / 'transform' / 'meta.json').write_text(json.dumps({'transformed': True}), encoding='utf-8')
        elif args.cmd == 'load':
            # read transform outputs
            tdir = OUTPUT_DIR / 'transform'
            if (tdir / 'uplift_by_store.parquet').exists():
                uplift = pd.read_parquet(tdir / 'uplift_by_store.parquet')
            elif (tdir / 'uplift_by_store.csv').exists():
                uplift = pd.read_csv(tdir / 'uplift_by_store.csv')
            else:
                raise RuntimeError('Нет transform output. Запустите transform сначала.')
            if (tdir / 'ads_metrics.parquet').exists():
                ads_metrics = pd.read_parquet(tdir / 'ads_metrics.parquet')
            elif (tdir / 'ads_metrics.csv').exists():
                ads_metrics = pd.read_csv(tdir / 'ads_metrics.csv')
            if (tdir / 'anomalies.parquet').exists():
                anomalies = pd.read_parquet(tdir / 'anomalies.parquet')
            elif (tdir / 'anomalies.csv').exists():
                anomalies = pd.read_csv(tdir / 'anomalies.csv')
            res = {'uplift_by_store': uplift, 'ads_metrics': ads_metrics, 'anomalies': anomalies}
            cmd_load(res)
        elif args.cmd == 'run_all':
            extracted = cmd_extract()
            transformed = transform(extracted['sales'], extracted['ads'])
            if getattr(args, 'explain', False):
                pass
            cmd_load(transformed)
        else:
            print('No command. Use extract|transform|load|run_all')
    except Exception as e:
        LOG.exception('Ошибка в ETL: %s', e)
        sys.exit(1)


if __name__ == '__main__':
    main()


def _make_sales_sample():
    df = pd.DataFrame([
        {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 1, 'units': 20, 'revenue': 200},
        {'date': '2025-01-01', 'store_id': 'A', 'sku_id': 1, 'promo_flag': 0, 'units': 10, 'revenue': 80},
        {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 1, 'units': 5, 'revenue': 50},
        {'date': '2025-01-01', 'store_id': 'B', 'sku_id': 2, 'promo_flag': 0, 'units': 5, 'revenue': 60},
    ])
    return df


def _make_ads_sample():
    df = pd.DataFrame([
        {'date': '2025-01-01', 'channel': 'X', 'campaign_id': 1, 'impressions': 1000, 'clicks': 50, 'cost': 25},
        {'date': '2025-01-02', 'channel': 'X', 'campaign_id': 1, 'impressions': 900, 'clicks': 30, 'cost': 20},
        {'date': '2025-01-01', 'channel': 'Y', 'campaign_id': 2, 'impressions': 100, 'clicks': 10, 'cost': 5},
        {'date': '2025-01-02', 'channel': 'Y', 'campaign_id': 2, 'impressions': 150, 'clicks': 3, 'cost': 2},
    ])
    return df

