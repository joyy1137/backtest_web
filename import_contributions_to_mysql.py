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


def discover_files(base_folder: str, patterns):
    p = Path(base_folder)
    if not p.exists():
        raise FileNotFoundError(f"base folder not found: {base_folder}")
    files = set()
    for pat in patterns:
        for x in p.rglob(pat):
            if x.is_file():
                files.add(str(x))
    return sorted(files)


def try_read_csv(fp: str):
    # pandas can often infer compression; try robustly
    try:
        return pd.read_csv(fp, compression='infer')
    except Exception:
        try:
            return pd.read_csv(fp)
        except Exception:
            logging.exception('Failed to read CSV: %s', fp)
            return None


def infer_schema_from_df(df: pd.DataFrame):
    schema = []
    for col in df.columns:
        ser = df[col]
        dtype = ser.dtype
        if pd.api.types.is_integer_dtype(dtype):
            typ = 'BIGINT'
        elif pd.api.types.is_float_dtype(dtype):
            typ = 'DECIMAL(18,8)'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            typ = 'DATETIME'
        else:
            max_len = int(ser.dropna().astype(str).map(len).max() or 0)
            if max_len <= 150:
                typ = 'VARCHAR(150)'
            elif max_len <= 500:
                typ = 'VARCHAR(500)'
            else:
                typ = 'TEXT'
        schema.append({'field': col, 'type': typ})
    return schema


def import_files_to_table(files, table_name, cfg_path, database, sub1=None):
    if not files:
        logging.info('No files to import for table %s', table_name)
        return

    importer = MySQLImporter(cfg_path)
    # load pk from config if present
    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
    except Exception:
        cfg = {}

    pk = cfg.get('pk', '')
    pk_fields = [p.strip() for p in pk.split(',') if p.strip()]

    dfs = []
    for fp in files:
        df = try_read_csv(fp)
        if df is None or df.empty:
            logging.warning('skip unreadable/empty file: %s', fp)
            continue

        # infer id/session/portfolio_name if missing
        p = Path(fp)
        try:
            id_val = p.parent.parent.name
        except Exception:
            id_val = None

        if sub1 is not None and 'session_id' not in df.columns:
            df['session_id'] = sub1
        if 'id' not in df.columns and id_val is not None:
            df['id'] = id_val
        if 'portfolio_name' not in df.columns:
            df['portfolio_name'] = p.stem.replace('_contribution_weight', '').replace('_contribution', '')

        dfs.append(df)

    if not dfs:
        logging.info('No readable CSVs for table %s', table_name)
        return

    combined = pd.concat(dfs, ignore_index=True)


    for col in combined.columns:
        if combined[col].dtype == object:
            lname = col.lower()
            try:
                if lname == 'valuation_date':
                  
                    combined[col] = pd.to_datetime(combined[col], errors='coerce').dt.date
                    continue
            except Exception:
                # guard against unexpected types
                pass

          

    schema = infer_schema_from_df(combined)
    try:
        importer.create_table(table_name, schema, pk_fields, db=database)
    except Exception:
        logging.exception('create_table failed for %s', table_name)

    importer.df_to_mysql(combined, table_name, schema, pk_fields, database=database)
    logging.info('导入 %d 行到 %s.%s', len(combined), database, table_name)
    importer.close()


def main(contrib_table: str, weight_table: str = None, sub1: str = None, base_folder: str = None, cfg_path: str = DEFAULT_DB_CONFIG):
    base_folder = base_folder or os.getcwd()

    # discover files
    contrib_patterns = ('*_contribution.csv', '*_contribution.csvz', '*_contribution.*')
    weight_patterns = ('*_contribution_weight.csv', '*_contribution_weight.csvz', '*_contribution_weight.*')

    try:
        with open(cfg_path, 'r', encoding='utf-8') as f:
            cfg = yaml.safe_load(f) or {}
            db = cfg.get('database6') or cfg.get('database')
    except Exception:
        logging.exception('Failed to read db config: %s', cfg_path)
        db = None

    contrib_files = discover_files(base_folder, contrib_patterns)
    
    if contrib_table:
        import_files_to_table(contrib_files, contrib_table, cfg_path, db, sub1=sub1)

    if weight_table:
        weight_files = discover_files(base_folder, weight_patterns)
   
        import_files_to_table(weight_files, weight_table, cfg_path, db, sub1=sub1)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import contribution and weight CSVs into MySQL')
    parser.add_argument('contrib_table', help='Table name for contribution CSVs')
    parser.add_argument('weight_table', nargs='?', help='Table name for contribution weight CSVs (optional)')
    parser.add_argument('sub1', nargs='?', help='session id (optional)')
    parser.add_argument('base_folder', nargs='?', help='base folder to search (optional)')
    args = parser.parse_args()

    main(args.contrib_table, args.weight_table, args.sub1, args.base_folder)
