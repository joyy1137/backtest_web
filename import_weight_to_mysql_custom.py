from pathlib import Path
import pandas as pd
import os
import yaml
import logging

from importer import MySQLImporter
import argparse
import sys

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

# default config locations (relative to this file)
DEFAULT_DB_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'db.yaml'))
PATHS_CONFIG = os.path.abspath(os.path.join(os.path.dirname(__file__), 'config', 'paths.yaml'))

# try to read default temp dir from paths.yaml
try:
    with open(PATHS_CONFIG, 'r', encoding='utf-8') as f:
        path_cfg = yaml.safe_load(f) or {}
        A_FOLDER = path_cfg.get('tempp_dir')
except Exception:
    A_FOLDER = None


SCHEMA = [
    {'field': 'valuation_date', 'type': 'VARCHAR(50)'},
    {'field': 'code', 'type': 'VARCHAR(50)'},
    {'field': 'portfolio_name', 'type': 'VARCHAR(50)'},
    {'field': 'weight', 'type': 'FLOAT'},
    {'field': 'id', 'type': 'VARCHAR(50)'},
    {'field': 'session_id', 'type': 'VARCHAR(50)'},
    {'field': 'update_time', 'type': 'DATETIME'}
]


def discover_csv_files(a_folder: str):
    p = Path(a_folder)
    if not p.exists():
        raise FileNotFoundError(f"a folder not found: {a_folder}")
    return sorted([str(x) for x in p.rglob('*.csv') if x.is_file()])


def read_and_normalize(csv_path: str, session_id: str = None, id_val: str = None):
    df = pd.read_csv(csv_path, dtype=str)
    # normalize column names
    df.columns = [c.strip() for c in df.columns]

    # ensure required columns exist
    required = ['valuation_date', 'code', 'portfolio_name', 'weight', 'id', 'session_id']
    lower_map = {c.lower(): c for c in df.columns}

    # Try to map case-insensitive
    mapped = {}
    for req in required:
        if req in df.columns:
            mapped[req] = req
        elif req in lower_map:
            mapped[req] = lower_map[req]
        else:
          
            if req == 'session_id' and session_id is not None:
                df['session_id'] = str(session_id)
                mapped[req] = 'session_id'
            elif req == 'id' and id_val is not None:
                df['id'] = str(id_val)
                mapped[req] = 'id'
            else:
                raise KeyError(f"CSV {csv_path} 缺少必要列: {req}")

    # select and rename
    df2 = df[[mapped[r] for r in required]].copy()
    df2.columns = required

    # convert types
    df2['valuation_date'] = df2['valuation_date'].astype(str)
    df2['code'] = df2['code'].astype(str)
    df2['portfolio_name'] = df2['portfolio_name'].astype(str)
 
   
    df2['portfolio_name'] = df2['portfolio_name'].str.replace(r'_[0-9]+$', '', regex=True)


    # weight to numeric
    df2['weight'] = pd.to_numeric(df2['weight'], errors='coerce')

    # add upload timestamp column for all rows
    try:
        df2['update_time'] = pd.Timestamp.now()
    except Exception:
        # fallback to python datetime
        from datetime import datetime
        df2['update_time'] = datetime.now()

    return df2


def import_weights_to_mysql(database: str, table: str, cfg_path: str = DEFAULT_DB_CONFIG, sub1: str = None, sub2: str = None):
    
    cfg_path = os.path.abspath(cfg_path)
    if not os.path.exists(cfg_path):
        logging.error('config file not found: %s', cfg_path)
        raise FileNotFoundError(cfg_path)

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f) or {}

    # require database and table to be provided explicitly
    if not table:
        raise ValueError('table name must be provided as function argument')
    if not database:
        raise ValueError('database name must be provided as function argument')

    # primary key fields: use config pk or default triple
    pk = cfg.get('pk', 'valuation_date,code,portfolio_name,session_id,id')
    pk_fields = [p.strip() for p in pk.split(',') if p.strip()]

    # determine base folder from config or global A_FOLDER
    base_folder = cfg.get('a_folder') or A_FOLDER

    # construct final folder: base\sub1\sub2 if sub1/sub2 provided
    if sub1 or sub2:
        # fall back to cwd if base not configured
        if not base_folder:
            base_folder = os.getcwd()
        parts = [base_folder]
        if sub1:
            parts.append(sub1)
        if sub2:
            parts.append(sub2)
        a_folder = os.path.join(*parts)
    else:
        a_folder = base_folder


    csvs = discover_csv_files(a_folder)
    if not csvs:
        logging.info('在 %s 中未找到 csv 文件。', a_folder)
        return

    importer = MySQLImporter(cfg_path)

    all_rows = []
    for csv in csvs:
        try:
            df = read_and_normalize(csv, session_id=sub1, id_val=sub2)
            all_rows.append(df)
        except Exception:
            logging.exception('读取或解析 CSV 失败: %s', csv)

    if not all_rows:
        logging.info('没有读取到任何数据。')
        importer.close()
        return

    combined = pd.concat(all_rows, ignore_index=True)

    # call df_to_mysql with schema and pk_fields
    logging.info('上传 %d 行数据到表 %s (database: %s)', len(combined), table, database)
 
    try:
        # Pass the provided database name so the table is created in the right schema.
        importer.create_table(table, SCHEMA, pk_fields, db=database)
    except Exception:
        # If table creation fails for any reason, log but still attempt df_to_mysql
        logging.exception('尝试创建表 %s.%s 时出错', database, table)

    importer.df_to_mysql(combined, table, SCHEMA, pk_fields, database=database)

    importer.close()


def main(database: str, table: str, sub1: str = None, sub2: str = None):
    try:
        import_weights_to_mysql(database=database, table=table, sub1=sub1, sub2=sub2)
    except Exception as e:
        logging.error('导入失败: %s', e)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(
        description="Import portfolio weight CSVs into MySQL. Only a table positional argument is required; database is read from the default config."
    )
    parser.add_argument('table', help='Table name to write into (required).')
    parser.add_argument('sub1', nargs='?', help='First subfolder under the configured base folder (optional).')
    parser.add_argument('sub2', nargs='?', help='Second subfolder under the configured base folder (optional).')

    args = parser.parse_args()
    table_name = args.table

    # Load default config to find database name
    cfg = {}
    try:
        if os.path.exists(DEFAULT_DB_CONFIG):
            with open(DEFAULT_DB_CONFIG, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
    except Exception:
        logging.exception('读取默认配置文件失败: %s', DEFAULT_DB_CONFIG)


    db_name = cfg['database6']
    
    if not db_name:
        logging.error('数据库名未在默认配置文件中找到: %s', DEFAULT_DB_CONFIG)
        sys.exit(2)

    main(db_name, table_name, args.sub1, args.sub2)


    



   
