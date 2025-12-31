from pathlib import Path
import pandas as pd
import os
import yaml
import logging
import argparse

from importer import MySQLImporter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

DEFAULT_DB_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'db.yaml'))

# Only the fields the user requested
SCHEMA = [
    {'field': 'annual_return_pct', 'type': 'DECIMAL(18,8)'},
    {'field': 'sharpe_ratio', 'type': 'DECIMAL(18,8)'},
    {'field': 'info_ratio', 'type': 'DECIMAL(18,8)'},
    {'field': 'max_drawdown_pct', 'type': 'DECIMAL(18,8)'},
    {'field': 'annual_vol_pct', 'type': 'DECIMAL(18,8)'},
    {'field': 'portfolio_name', 'type': 'VARCHAR(150)'},
    {'field': 'session_id', 'type': 'VARCHAR(50)'},
    {'field': 'id', 'type': 'VARCHAR(50)'},
    {'field': 'update_time', 'type': 'DATETIME'}
]


def discover_summary_csvs(base_folder: str):
    p = Path(base_folder)
    if not p.exists():
        raise FileNotFoundError(f"base folder not found: {base_folder}")
    return sorted([str(x) for x in p.rglob('*_performance_summary.csv') if x.is_file()])


def read_summary_csv(csv_path: str, session_id: str = None, id_val: str = None):
    # Read the single-line performance summary CSV and return a DataFrame with exact columns
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    mapping = {}
    for c in df.columns:
        lc = c.lower()
        if 'annual' in lc and 'return' in lc:
            mapping[c] = 'annual_return_pct'
        elif 'sharpe' in lc:
            mapping[c] = 'sharpe_ratio'
        elif 'info' in lc and 'ratio' in lc:
            mapping[c] = 'info_ratio'
        elif ('max' in lc and ('dd' in lc or 'draw' in lc)) or 'max_drawdown' in lc:
            mapping[c] = 'max_drawdown_pct'
        elif 'vol' in lc or ('std' in lc and 'annual' in lc):
            mapping[c] = 'annual_vol_pct'
        elif 'portfolio' in lc:
            mapping[c] = 'portfolio_name'
        elif 'session' in lc:
            mapping[c] = 'session_id'
        elif (lc == 'id' or lc.endswith('id')):
            mapping[c] = 'id'
        elif 'update' in lc and 'time' in lc:
            mapping[c] = 'update_time'

    if not mapping:
        # nothing recognized
        return pd.DataFrame()

    df2 = df[[k for k in mapping.keys()]].copy()
    df2 = df2.rename(columns=mapping)

    # ensure portfolio_name present
    if 'portfolio_name' not in df2.columns:
        pname = Path(csv_path).stem.replace('_performance_summary', '')
        df2['portfolio_name'] = pname


    if 'portfolio_name' in df2.columns:
        # ensure string type then remove trailing _<digits>
        df2['portfolio_name'] = df2['portfolio_name'].astype(str).str.replace(r'_\d+$', '', regex=True)

    # override session/id if provided
    if session_id is not None:
        df2['session_id'] = str(session_id)
    else:
        df2['session_id'] = df2.get('session_id', None)

    if id_val is not None:
        df2['id'] = str(id_val)
    else:
        df2['id'] = df2.get('id', None)

    # numeric conversions
    for col in ('annual_return_pct', 'sharpe_ratio', 'info_ratio', 'max_drawdown_pct', 'annual_vol_pct'):
        if col in df2.columns:
            df2[col] = pd.to_numeric(df2[col], errors='coerce')

    # update_time
    if 'update_time' in df2.columns:
        df2['update_time'] = pd.to_datetime(df2['update_time'], errors='coerce')
    else:
        df2['update_time'] = pd.Timestamp.now()

    # Only keep fields in SCHEMA order
    wanted = [c['field'] for c in SCHEMA]
    df2 = df2[[c for c in wanted if c in df2.columns]]
    return df2


def import_performance_to_mysql(database: str, table: str, cfg_path: str = DEFAULT_DB_CONFIG, sub1: str = None, base_folder_arg: str = None):
    cfg_path = os.path.abspath(cfg_path)
    if not os.path.exists(cfg_path):
        logging.error('config file not found: %s', cfg_path)
        raise FileNotFoundError(cfg_path)

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    if not table:
        raise ValueError('table name must be provided')
    if not database:
        raise ValueError('database name must be provided')

    base_folder = base_folder_arg or os.getcwd()
    parts = [base_folder]
    if sub1:
        parts.append(sub1)
    a_folder = os.path.join(*parts)

    csvs = discover_summary_csvs(a_folder)
    if not csvs:
        logging.info('在 %s 中未找到 performance_summary csv 文件。', a_folder)
        return

    all_rows = []
    for csv in csvs:
        try:
            p = Path(csv)
            # infer id from folder structure as before
            try:
                use_id = p.parent.parent.name
            except Exception:
                use_id = None

            use_session = sub1
            df = read_summary_csv(csv, session_id=use_session, id_val=use_id)
            if not df.empty:
                all_rows.append(df)
            else:
                logging.warning('文件 %s 未包含可识别的性能列，已跳过', csv)
        except Exception:
            logging.exception('读取或解析 CSV 失败: %s', csv)

    if not all_rows:
        logging.info('没有读取到任何性能摘要数据。')
        return

    combined = pd.concat(all_rows, ignore_index=True)
    logging.info('准备上传 %d 行到表 %s (database: %s)', len(combined), table, database)

    importer = MySQLImporter(cfg_path)
    pk = cfg.get('pk', 'session_id,id,portfolio_name,')
    pk_fields = [p.strip() for p in pk.split(',') if p.strip()]

    try:
        importer.create_table(table, SCHEMA, pk_fields, db=database)
    except Exception:
        logging.exception('尝试创建表 %s.%s 时出错', database, table)

    importer.df_to_mysql(combined, table, SCHEMA, pk_fields, database=database)
    importer.close()


def main(database: str, table: str, sub1: str = None, base_folder_arg: str = None):
    try:
        import_performance_to_mysql(database=database, table=table, sub1=sub1, base_folder_arg=base_folder_arg)
    except Exception as e:
        logging.error('导入失败: %s', e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import performance summary CSVs into MySQL.")
    parser.add_argument('table', help='Table name to write into (required).')
    parser.add_argument('sub1', nargs='?', help='Session id')
    parser.add_argument('base_folder', nargs='?', help='Base folder for backtest results (optional).')
    args = parser.parse_args()

    cfg = {}
    try:
        if os.path.exists(DEFAULT_DB_CONFIG):
            with open(DEFAULT_DB_CONFIG, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
    except Exception:
        logging.exception('读取默认配置文件失败: %s', DEFAULT_DB_CONFIG)

    db_name = cfg.get('database6') or cfg.get('database')
    main(db_name, args.table, args.sub1, args.base_folder)



