import pandas as pd
import pymysql
from sqlalchemy import create_engine, text, types
import yaml
from typing import Dict, List, Optional
import os
import logging
from logging import Logger
logger: Logger = logging.getLogger(__name__)


class MySQLImporter:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.engine = create_engine(
            f"mysql+pymysql://{self.config['user2']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
        
    
    def create_table(self, table_name, schema, pk_fields, db: Optional[str] = None):

        columns = []
        for col_def in schema:
            field = col_def['field']
            dtype = col_def['type']
            columns.append(f"`{field}` {dtype}")

        # 如果指定了主键字段，拼接主键定义
        pk_clause = ''
        if pk_fields:
            pk_cols = ', '.join([f"`{c}`" for c in pk_fields])
            pk_clause = f",\n            PRIMARY KEY ({pk_cols})"

        # 生成CREATE TABLE语句
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{table_name}` (
            {', '.join(columns)}{pk_clause}
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """

        # If db provided, qualify table with database name
        if db:
            create_sql = create_sql.replace(f"CREATE TABLE IF NOT EXISTS `{table_name}`", f"CREATE TABLE IF NOT EXISTS `{db}`.`{table_name}`")

        # use a transaction when creating the table
        with self.engine.begin() as conn:
            conn.execute(text(create_sql))

    def _table_exists(self, table_name, db: Optional[str] = None):
        if db is None:
            db = self.config['database4']
        sql = text("SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = :db AND table_name = :tbl")
        with self.engine.connect() as conn:
            # use scalar() to get the first column of the first row (count)
            scalar = conn.execute(sql, {
                'db': db,
                'tbl': table_name
            }).scalar()
            try:
                return bool(scalar and int(scalar) > 0)
            except Exception:
                return False

    def _get_table_pk_columns(self, table_name, db: Optional[str] = None):
        if db is None:
            db = self.config['database']
        sql = text("SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
                   "WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl AND CONSTRAINT_NAME = 'PRIMARY' "
                   "ORDER BY ORDINAL_POSITION")
        with self.engine.connect() as conn:
            res = conn.execute(sql, {'db': db, 'tbl': table_name}).fetchall()
            cols = []
            for row in res:
                # SQLAlchemy row may be a tuple or have a _mapping
                if hasattr(row, '_mapping'):
                    cols.append(row._mapping.get('COLUMN_NAME'))
                else:
                    # assume first element is the column name
                    cols.append(row[0])
            return [c for c in cols if c]

    def _count_duplicate_pk_groups(self, table_name, key_fields, db: Optional[str] = None):
        # Count number of duplicate key groups (groups having count > 1)
        if db is None:
            db = self.config['database']
        keys = ', '.join([f'`{k}`' for k in key_fields])
        dup_sql = f"SELECT COUNT(*) AS dup_groups FROM (SELECT {keys}, COUNT(*) AS cnt FROM `{db}`.`{table_name}` GROUP BY {keys} HAVING cnt>1) t"
        with self.engine.connect() as conn:
            try:
                scalar = conn.execute(text(dup_sql)).scalar()
                return int(scalar) if scalar is not None else 0
            except Exception:
                # If the query fails (e.g., permission issues), return a conservative non-zero value to prevent inserts
                return 1

    def _get_table_columns(self, table_name, db: Optional[str] = None):
        """Return a set of column names that exist in the given table."""
        if db is None:
            db = self.config['database']
        sql = text("SELECT COLUMN_NAME FROM information_schema.columns WHERE TABLE_SCHEMA = :db AND TABLE_NAME = :tbl")
        with self.engine.connect() as conn:
            res = conn.execute(sql, {'db': db, 'tbl': table_name}).fetchall()
            cols = set()
            for row in res:
                if hasattr(row, '_mapping'):
                    name = row._mapping.get('COLUMN_NAME')
                else:
                    name = row[0]
                if name:
                    cols.add(name)
            return cols
        
    def df_to_mysql(self, df, table_name, schema, pk_fields: Optional[List[str]] = None, database: Optional[str] = None):
        """Insert or upsert a DataFrame into a MySQL table.

        database: optional. If equals a key in the loaded config (e.g. 'database2'), the mapped value
                  from config is used; otherwise the provided string is treated as a literal database name.
        """
        # resolve target_db
        if database is not None and database in self.config:
            target_db = self.config[database]
        elif database is not None:
            target_db = database
        else:
            target_db = self.config['database']

        df_clean = self._preprocess_data(df, schema)
        key_fields = pk_fields if pk_fields else []

        # create table if not exists in target_db
        if not self._table_exists(table_name, db=target_db):
            self.create_table(table_name, schema, pk_fields, db=target_db)
            table_created = True
        else:
            table_created = False
            pk_cols = self._get_table_pk_columns(table_name, db=target_db)
            if key_fields and (not pk_cols or set(pk_cols) != set(key_fields)):
                dup_count = self._count_duplicate_pk_groups(table_name, key_fields, db=target_db)
                if dup_count > 0:
                    raise RuntimeError(f"目标表 {target_db}.{table_name} 中存在重复主键分组 ({dup_count})，请先清理数据库再导入。")

        # Deduplicate incoming DataFrame by key_fields, keep latest by update_time
        if key_fields and all(k in df_clean.columns for k in key_fields):
            if 'update_time' in df_clean.columns:
                try:
                    df_clean['update_time'] = pd.to_datetime(df_clean['update_time'])
                    df_clean = df_clean.sort_values('update_time').drop_duplicates(subset=key_fields, keep='last')
                except Exception:
                    df_clean = df_clean.drop_duplicates(subset=key_fields, keep='last')
            else:
                df_clean = df_clean.drop_duplicates(subset=key_fields, keep='last')

        records = df_clean.to_dict(orient='records')
        if not records:
            logger.info("没有要插入的数据")
            return

        # Ensure columns exist; if we just created the table assume schema present
        schema_cols = {col['field']: col['type'] for col in schema}
        if table_created:
            existing_cols = set(schema_cols.keys())
        else:
            existing_cols = self._get_table_columns(table_name, db=target_db) if self._table_exists(table_name, db=target_db) else set()

        missing = [f for f in schema_cols.keys() if f not in existing_cols]
        if missing:
            with self.engine.connect() as conn:
                for m in missing:
                    dtype = schema_cols[m]
                    alter_sql = f"ALTER TABLE `{target_db}`.`{table_name}` ADD COLUMN `{m}` {dtype} NULL"
                    try:
                        conn.execute(text(alter_sql))
                    except Exception as e:
                        raise RuntimeError(f"无法为表 {table_name} 添加列 {m}: {e}")
            existing_cols = self._get_table_columns(table_name, db=target_db)

        cols = list(df_clean.columns)
        col_list_sql = ', '.join([f"`{c}`" for c in cols])
        driver_placeholders = ', '.join(['%s'] * len(cols))

        non_key_cols = [c for c in cols if c not in key_fields]
        if non_key_cols:
            update_clause = ', '.join([f"`{c}`=VALUES(`{c}`)" for c in non_key_cols])
        else:
            update_clause = ', '.join([f"`{k}`=`{k}`" for k in key_fields])

        insert_sql = f"INSERT INTO `{target_db}`.`{table_name}` ({col_list_sql}) VALUES ({driver_placeholders}) ON DUPLICATE KEY UPDATE {update_clause}"

        def _sanitize_value(v):
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            if hasattr(v, 'to_pydatetime'):
                try:
                    return v.to_pydatetime()
                except Exception:
                    pass
            return v

        param_tuples = [tuple(_sanitize_value(rec[c]) for c in cols) for rec in records]

        try:
            raw_conn = self.engine.raw_connection()
            try:
                cur = raw_conn.cursor()
                cur.executemany(insert_sql, param_tuples)
                raw_conn.commit()
                logger.info(f"导入 %d 行到 %s.%s", len(param_tuples), target_db, table_name)
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    raw_conn.close()
                except Exception:
                    pass
        except Exception as e:
            # Log full exception with stacktrace so it's visible in logs
            logger.exception("导入 失败: %s", e)
    
    def _preprocess_data(self, df: pd.DataFrame, schema: List[Dict]) -> pd.DataFrame:
        """
        数据预处理：确保数据类型匹配
        """
        df_clean = df.copy()
        
        # 获取字段类型映射
        type_mapping = {col['field']: col['type'] for col in schema}
        
        for column in df_clean.columns:
            if column in type_mapping:
                mysql_type = type_mapping[column].upper()
                
         
                if 'DATETIME' in mysql_type or 'TIMESTAMP' in mysql_type:
                 
                    df_clean[column] = pd.to_datetime(df_clean[column])
                    # convert to python datetime objects which to_sql will insert as proper DATETIME
                    try:
                        df_clean[column] = [
                            (x.to_pydatetime() if hasattr(x, 'to_pydatetime') and not pd.isna(x) else None)
                            for x in df_clean[column]
                        ]
                    except Exception:
              
                        pass
                elif mysql_type == 'DATE' or (mysql_type.startswith('DATE') and 'DATETIME' not in mysql_type):
                    # convert to date only (no time part)
                    df_clean[column] = pd.to_datetime(df_clean[column]).dt.date
                elif 'DECIMAL' in mysql_type or 'FLOAT' in mysql_type or 'DOUBLE' in mysql_type:
                    df_clean[column] = pd.to_numeric(df_clean[column], errors='coerce')
                elif 'INT' in mysql_type:
                    df_clean[column] = pd.to_numeric(df_clean[column], errors='coerce').astype('Int64')
        
        return df_clean
    
    
    def close(self):
        """关闭连接"""
        self.engine.dispose()



if __name__ == "__main__":

    # 表结构定义
    schema = [
        {'field': 'valuation_date', 'type': 'DATE'},
        {'field': 'code', 'type': 'VARCHAR(50)'},
        {'field': 'final_score', 'type': 'DECIMAL(15,6)'},
        {'field': 'score_name', 'type': 'VARCHAR(50)'},
        {'field': 'update_time', 'type': 'DATETIME'}
    ]
    
    # 示例数据
    df = pd.DataFrame({
        'valuation_date': ['2025-01-01', '2025-01-01', '2025-01-02'],
        'code': ['000001', '000002', '000003'],
        'final_score': [9, 88.3, 92.7],
        'score_name': ['score_v1', 'score_v1', 'score_v1'],
        'update_time': ['2025-01-01 10:00:00', '2025-01-02 10:00:00', '2025-01-02 11:00:00']
    })
    db_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config', 'db.yaml'))
  
    

    importer = MySQLImporter(db_path)
    importer.df_to_mysql(df, 'qlib_scores', schema, database='database2')
  
    
    importer.close()