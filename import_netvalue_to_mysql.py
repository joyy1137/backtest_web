from pathlib import Path
import pandas as pd
import os
import yaml
import logging
import argparse
import sys

from importer import MySQLImporter

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")

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
    {'field': 'valuation_date', 'type': 'DATE'},
    {'field': 'portfolio_name', 'type': 'VARCHAR(150)'},
    {'field': 'benchmark_net_value', 'type': 'DECIMAL(18,8)'},
    {'field': 'portfolio_net_value', 'type': 'DECIMAL(18,8)'},
    {'field': 'excess_net_value', 'type': 'DECIMAL(18,8)'},
    {'field': 'session_id', 'type': 'VARCHAR(50)'},
    {'field': 'id', 'type': 'VARCHAR(50)'},
    {'field': 'update_time', 'type': 'DATETIME'}
]


def discover_csv_files(a_folder: str):
    p = Path(a_folder)
    if not p.exists():
        raise FileNotFoundError(f"a folder not found: {a_folder}")
    return sorted([str(x) for x in p.rglob('*.csv') if x.is_file()])


def read_netvalue_csv(csv_path: str, session_id: str = None, id_val: str = None):
    df = pd.read_csv(csv_path, dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # try to map common Chinese headers to normalized column names
    col_map = {}
    for c in df.columns:
        lc = c.lower()
        if 'valuation' in lc or '日期' in lc or 'valuation_date' in lc or 'valuation' in c:
            col_map[c] = 'valuation_date'
        elif '基准' in c or '基準' in c or 'benchmark' in lc:
            col_map[c] = 'benchmark_net_value'
        elif '组合' in c or '組合' in c or 'portfolio' in lc:
            col_map[c] = 'portfolio_net_value'
        elif '超额' in c or '超額' in c or 'excess' in lc:
            col_map[c] = 'excess_net_value'

    # Ensure required columns
    required = ['valuation_date', 'benchmark_net_value', 'portfolio_net_value', 'excess_net_value']
    for r in required:
        if r not in col_map.values():
            # try to find by direct name
            if r in df.columns:
                col_map[r] = r
            else:
                raise KeyError(f"CSV {csv_path} 缺少必要列: {r}")

    df2 = df[[k for k in col_map.keys()]].copy()
    df2 = df2.rename(columns=col_map)

    # derive portfolio_name from filename if possible
    fname = Path(csv_path).stem
    # strip common suffixes like _回测 or -回测
    pname = fname
    for suffix in ['_回测', '-回测', '回测']:
        if pname.endswith(suffix):
            pname = pname[: -len(suffix)]

    df2['portfolio_name'] = pname

    # add session_id and id if provided
    if session_id is not None:
        df2['session_id'] = str(session_id)
    else:
        df2['session_id'] = df2.get('session_id', None)

    if id_val is not None:
        df2['id'] = str(id_val)
    else:
        df2['id'] = df2.get('id', None)

    # convert numeric columns
    df2['benchmark_net_value'] = pd.to_numeric(df2['benchmark_net_value'], errors='coerce')
    df2['portfolio_net_value'] = pd.to_numeric(df2['portfolio_net_value'], errors='coerce')
    df2['excess_net_value'] = pd.to_numeric(df2['excess_net_value'], errors='coerce')

    # parse dates
    df2['valuation_date'] = pd.to_datetime(df2['valuation_date'], errors='coerce').dt.date

    # add update time
    try:
        df2['update_time'] = pd.Timestamp.now()
    except Exception:
        from datetime import datetime
        df2['update_time'] = datetime.now()

    # reorder
    cols = ['valuation_date', 'portfolio_name', 'benchmark_net_value', 'portfolio_net_value', 'excess_net_value', 'session_id', 'id', 'update_time']
    df2 = df2[[c for c in cols if c in df2.columns]]
    return df2


def import_netvalues_to_mysql(database: str, table: str, cfg_path: str = DEFAULT_DB_CONFIG, sub1: str = None, base_folder_arg: str = None):
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

   

    base_folder = base_folder_arg
   


    if not base_folder:
        base_folder = os.getcwd()

    parts = [base_folder]
    if sub1:
        parts.append(sub1)

    a_folder = os.path.join(*parts)
 

    csvs = discover_csv_files(a_folder)
    if not csvs:
        logging.info('在 %s 中未找到 csv 文件。', a_folder)
        return

    all_rows = []
    processed_files = []
    skipped_files = []

    for csv in csvs:
        try:
            
            fname = Path(csv).name
            

        
            if '回测' not in fname:
                skipped_files.append(csv)
                continue

           
            p = Path(csv)
            
            use_id = p.parent.parent.name
            

            use_session = sub1
            df = read_netvalue_csv(csv, session_id=use_session, id_val=use_id)
            all_rows.append(df)
            processed_files.append(csv)
        except Exception:
            logging.exception('读取或解析 CSV 失败: %s', csv)
    

    logging.info('处理完成，找到 %d 个回测 csv。', len(all_rows))

    
    if not all_rows:
        logging.info('没有读取到任何数据。')
        return

    combined = pd.concat(all_rows, ignore_index=True)

    logging.info('准备上传 %d 行到表 %s (database: %s)', len(combined), table, database)



    importer = MySQLImporter(cfg_path)
    pk = cfg.get('pk', 'valuation_date,session_id,id,')
    pk_fields = [p.strip() for p in pk.split(',') if p.strip()]

    try:
        importer.create_table(table, SCHEMA, pk_fields, db=database)
    except Exception:
        logging.exception('尝试创建表 %s.%s 时出错', database, table)

    importer.df_to_mysql(combined, table, SCHEMA, pk_fields, database=database)
    importer.close()


def main(database: str, table: str, sub1: str = None, base_folder_arg: str = None):
    try:
        import_netvalues_to_mysql(database=database, table=table, sub1=sub1, base_folder_arg=base_folder_arg)
    except Exception as e:
        logging.error('导入失败: %s', e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import backtest netvalue CSVs into MySQL.")
    parser.add_argument('table', help='Table name to write into (required).')
    parser.add_argument('sub1', nargs='?', help='Session id')
 
    parser.add_argument('base_folder', nargs='?', help='Base folder for backtest results (optional).')
   
    args = parser.parse_args()

    # load default config
    cfg = {}
    try:
        if os.path.exists(DEFAULT_DB_CONFIG):
            with open(DEFAULT_DB_CONFIG, 'r', encoding='utf-8') as f:
                cfg = yaml.safe_load(f) or {}
    except Exception:
        logging.exception('读取默认配置文件失败: %s', DEFAULT_DB_CONFIG)

    db_name = cfg['database6'] 
   
    main(db_name, args.table, args.sub1, args.base_folder)
