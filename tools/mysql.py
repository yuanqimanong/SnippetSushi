import copy
import hashlib
import inspect
import json
import logging
import os
import re
from collections import defaultdict
from functools import wraps
from json import JSONEncoder
from typing import List, Dict, Any, Union, Type, Optional
from urllib.parse import urlparse, quote, unquote

import pymysql
from dbutils.pooled_db import PooledDB
from pymysql import Connection
from pymysql.cursors import DictCursor, Cursor, SSDictCursor, SSCursor


def handle_errors(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except pymysql.Error as pe:
            self.logger.error(f"数据库操作失败: {pe}")
            self.conn.rollback()
            raise pe
        except TypeError as te:
            self.logger.error(f"数据类型错误: {te}")
            self.conn.rollback()
            raise te

    return wrapper


class DBJSONEncoder(JSONEncoder):
    def default(self, obj):
        # 处理特定类型的对象
        if isinstance(obj, Cursor):
            return obj.__dict__
        return str(obj)


class DBOperator:

    def __init__(
        self, conn: Connection, table_name: str = None, autocommit: bool = True
    ):
        self.logger = logging.getLogger(__name__)
        self.conn = conn
        self.table_name = table_name
        self.autocommit = autocommit

    def __getattr__(self, name):
        return getattr(self.conn, name)

    @handle_errors
    def query(self, sql: str):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()

    @handle_errors
    def query_one(self, sql: str):
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchone()

    query_all = query

    @handle_errors
    def query_fields(
        self, table_name: str = None, full_info=False
    ) -> list[Union[str, dict]]:
        """查询表字段信息

        Args:
            table_name: 需查询的表名（自动添加反引号）
            full_info: 是否返回完整列信息 (默认False)

        Returns:
            list: 字段名列表或完整列信息字典列表
        """
        table_name = table_name or self.table_name
        base_sql = "SHOW FULL COLUMNS FROM `{}`" if full_info else "DESC `{}`"
        sql = base_sql.format(table_name.replace("`", ""))

        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            return (
                cursor.fetchall()
                if full_info
                else [row["Field"] for row in cursor.fetchall()]
            )

    @handle_errors
    def insert(
        self,
        *,
        table_name: str = None,
        data: Union[Dict, list[Dict]],
        batch_size: int = 100,
        ignore_duplicate: bool = True,
        insert_ignore: bool = False,
    ) -> int:
        """插入数据"""
        # TODO 批量插入字段未对齐，性能优化改写 yield方式
        _table_name = table_name or self.table_name
        _data = copy.deepcopy(data)

        if isinstance(_data, Dict):
            _data = [_data]

        if isinstance(_data, list) and len(_data) > 0:
            columns = ", ".join(_data[0].keys())
            placeholders = ", ".join(["%s"] * len(_data[0]))

            sql = (
                f"INSERT IGNORE INTO {_table_name} ({columns}) VALUES ({placeholders})"
                if insert_ignore
                else f"INSERT INTO {_table_name} ({columns}) VALUES ({placeholders})"
            )

            with self.conn.cursor() as cursor:
                while _data:
                    _d = _data[:batch_size]
                    _data = _data[batch_size:]
                    try:
                        params = [tuple(_.values()) for _ in _d]
                        self.logger.debug(f"执行SQL:  {sql} \n params:  {params}")
                        cursor.executemany(sql, params)
                    except Exception as e:
                        if ignore_duplicate and "Duplicate entry" in str(e):
                            continue
                        raise e

                if not self.autocommit:
                    self.conn.commit()
                return cursor.rowcount
        else:
            return 0

    @handle_errors
    def update(
        self,
        *,
        table_name: str = None,
        data: Dict,
        conditions: Dict = None,
        where_sql: str = None,
    ) -> int:
        """更新数据"""
        _table_name = table_name or self.table_name

        with self.conn.cursor() as cursor:
            params = list(data.values())
            if not where_sql:
                if not conditions:
                    raise ValueError("必须提供查询条件（conditions参数）")
                where_clause = " AND ".join([f"`{k}`=%s" for k in conditions.keys()])
                params += list(conditions.values())
            else:
                where_clause = where_sql.replace("%", "%%")

            set_clause = ", ".join([f"`{k}`=%s" for k in data.keys()])
            sql = f"UPDATE {_table_name} SET {set_clause} WHERE {where_clause}"

            self.logger.debug(f"执行SQL:  {sql} \t，参数:  {params}")

            affected = cursor.execute(sql, params)
            if not self.autocommit:
                self.conn.commit()
            return affected

    @handle_errors
    def upsert(
        self,
        *,
        table_name: str = None,
        data: Union[Dict, list[Dict]],
        batch_size: int = 100,
        update_fields: List[str] = None,
    ) -> int:
        """更新插入操作"""
        _table_name = table_name or self.table_name
        _data = copy.deepcopy(data)

        if isinstance(_data, Dict):
            _data = [_data]

        rowcount = 0
        if len(_data) > 0:
            with self.conn.cursor() as cursor:
                for sql, params in self.generate_upsert_sql(
                    _table_name, _data, batch_size, update_fields
                ):
                    self.logger.debug(f"执行SQL:  {sql} \n params:  {params}")
                    cursor.executemany(sql, params)
                    rowcount += cursor.rowcount
                    if not self.autocommit:
                        self.conn.commit()
        return rowcount

    def generate_upsert_sql(
        self,
        table_name: str,
        data: list[Dict],
        batch_size: int,
        update_fields: List[str],
    ):

        data_columns_dict = self.get_data_columns_list(data)

        for data_columns, data in data_columns_dict.items():
            columns = ", ".join(data_columns)
            updates = update_fields or data_columns

            placeholders = ", ".join(["%s"] * len(data_columns))
            while data:
                _d = data[:batch_size]
                params = [tuple(_.get(c) for c in data_columns) for _ in _d]
                updates = ", ".join([f"{col}=VALUES({col})" for col in updates])

                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
                yield sql, params
                data = data[batch_size:]

    @staticmethod
    def get_data_columns_list(data: list[Dict]):
        data_columns_dict = defaultdict(list)
        for item in data:
            key = tuple(sorted(item.keys()))
            data_columns_dict[key].append(item)
        return data_columns_dict

    # @handle_errors
    # def delete(self, table_name: str, conditions: Dict) -> int:
    #     """删除数据"""
    #     with self.conn.cursor() as cursor:
    #         where_clause = " AND ".join([f"{k}=%s" for k in conditions.keys()])
    #         sql = f"DELETE FROM {table_name} WHERE {where_clause}"
    #         affected = cursor.execute(sql, tuple(conditions.values()))
    #         if self.autocommit:
    #             self.conn.commit()
    #         return affected


db_url_pattern = re.compile(
    r"^([^/]+://)(.+?)(@\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+/.*)$"
)


class MySQLHelper:

    CURSOR_CLASS_MAP = {
        "cursor": Cursor,
        "Cursor": Cursor,
        "dict_cursor": DictCursor,
        "DictCursor": DictCursor,
        "ss_cursor": SSCursor,
        "SSCursor": SSCursor,
        "ss_dict_cursor": SSDictCursor,
        "SSDictCursor": SSDictCursor,
    }
    pymysql_connect_args = list(inspect.signature(pymysql.connect).parameters.keys())
    _connection_list = {}

    def __init__(
        self,
        *,
        db_url: str = None,
        db_config: Dict[str, Any] = None,
        cursor_class: str = None,
        table_name: str = None,
        autocommit: bool = True,
        **kwargs,
    ):
        """
        初始化 MySQLHelper 实例
        :param db_url: 数据库连接 URL
        :param db_config: 数据库连接配置字典
        :param cursor_class: 数据库游标类型(默认 DictCursor，可选 cursor, dict_cursor, ss_cursor, ss_dict_cursor)
        :param table_name: 默认表名
        :param autocommit: 是否自动提交事务
        :param kwargs: 其他参数
        """
        self.logger = logging.getLogger(__name__)
        self.db_url = db_url
        self.db_config = db_config
        self.cursor_class = self._set_cursor_class(cursor_class)
        self.table_name = table_name
        self.autocommit = autocommit
        self.kwargs = kwargs

        self.helper_conn = None
        self.query_builder = defaultdict(str)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.helper_conn:
            self.helper_conn.close()
        if hasattr(self, "connection") and self.connection:
            self.connection.close()

    @staticmethod
    def _set_cursor_class(
        cursor_class: Union[str, Cursor, DictCursor, SSCursor, SSDictCursor],
    ) -> Type[Union[DictCursor, Cursor, SSCursor, SSDictCursor]]:
        if not cursor_class:
            return DictCursor

        if isinstance(cursor_class, str):
            if cursor_class.casefold() in MySQLHelper.CURSOR_CLASS_MAP:
                cursor_class = MySQLHelper.CURSOR_CLASS_MAP[cursor_class]
            else:
                cursor_class = DictCursor
        return cursor_class

    @property
    def raw_connection(self) -> Connection:
        return self._create_or_connect()

    @property
    def connection(self) -> DBOperator:
        return DBOperator(self.raw_connection, self.table_name, self.autocommit)

    @property
    def helper(self) -> "MySQLHelper":
        self.helper_conn = self.raw_connection
        return self

    def _create_or_connect(self) -> Connection:
        conn_params = self._get_connection_params()
        pool_key = self._gen_config_key(conn_params)

        if pool_key not in self._connection_list:
            self._connection_list[pool_key] = PooledDB(
                creator=pymysql,
                mincached=3,
                maxcached=int(os.cpu_count() * 0.83),
                maxconnections=int(os.cpu_count() * 2.5),
                blocking=True,
                maxusage=1000,
                setsession=[
                    "SET TIME_ZONE='+08:00'",
                    "SET SESSION range_optimizer_max_mem_size=83886080",
                    "SET SESSION sql_mode = 'STRICT_TRANS_TABLES,STRICT_ALL_TABLES'",
                ],
                ping=7,
                **conn_params,
            )

        return self._connection_list[pool_key].connection()

    def _get_connection_params(self) -> Dict:
        if self.db_url:
            params = self.parse_db_url(self.db_url)
        else:
            params = self.db_config or {
                "host": "127.0.0.1",
                "port": 3306,
                "user": "root",
            }

        params.update(
            {
                "autocommit": self.autocommit,
                "cursorclass": self.cursor_class,
            }
        )
        return {k: v for k, v in params.items() if k in self.pymysql_connect_args}

    @staticmethod
    def _gen_config_key(config: dict) -> str:
        return hashlib.md5(
            json.dumps(config, sort_keys=True, cls=DBJSONEncoder).encode()
        ).hexdigest()

    @staticmethod
    def parse_db_url(url):
        if user_password := re.search(db_url_pattern, url):
            url = f"{user_password[1]}{quote(user_password[2], safe=':%')}{user_password[3]}"

        parsed = urlparse(url)
        if "mysql" not in parsed.scheme:
            raise ValueError("MySQL DB URL 错误！")

        return {
            "host": parsed.hostname,
            "port": parsed.port,
            "user": parsed.username,
            "password": unquote(parsed.password),
            "database": parsed.path.lstrip("/"),
            **dict(param.split("=") for param in parsed.query.split("&") if param),
        }

    def table(self, table: str) -> "MySQLHelper":
        self.query_builder["table"] = table
        return self

    def select(self, columns: Optional[List[str]] = None) -> "MySQLHelper":
        if columns is None:
            columns = []
        safe_columns = [f"`{col}`" for col in columns]
        self.query_builder["select"] = ", ".join(safe_columns) or "*"
        return self

    def where(self, **conditions) -> "MySQLHelper":
        self.query_builder["where"] = " AND ".join(
            [f"`{k}`=%s" for k in conditions.keys()]
        )
        self.query_builder["where_params"] = json.dumps(list(conditions.values()))
        return self

    def order_by(self, column: str, asc: bool = True) -> "MySQLHelper":
        self.query_builder["order"] = f"{column} {'ASC' if asc else 'DESC'}"
        return self

    def limit(self, count: int) -> "MySQLHelper":
        self.query_builder["limit"] = str(count)
        return self

    def offset(self, count: int) -> "MySQLHelper":
        self.query_builder["offset"] = str(count)
        return self

    @handle_errors
    def commit(self) -> List[Dict]:
        sql = f"SELECT {self.query_builder['select']} FROM {self.query_builder['table'] or self.table_name}"

        params = []

        if where := self.query_builder.get("where"):
            sql += f" WHERE {where}"
            params.extend(json.loads(self.query_builder["where_params"]))

        if order := self.query_builder.get("order"):
            sql += f" ORDER BY {order}"

        if limit := self.query_builder.get("limit"):
            sql += f" LIMIT {limit}"
            if offset := self.query_builder.get("offset"):
                sql += f" OFFSET {offset}"

        self.logger.debug(f"执行SQL:  {sql}")

        with self.helper_conn.cursor() as cursor:
            cursor.execute(sql, params)
            results = cursor.fetchall()
            self.query_builder.clear()
            return results
