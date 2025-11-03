"""
简单易用的CSMAR API, 主要是为了避免200000条和30分钟内不得进行相同条件查询的查询限制。
"""

from csmarapi.CsmarService import CsmarService
from csmarapi.ReportUtil import ReportUtil
import pandas as pd
import datetime
from typing import List
import pickle

class EasyCsmarApi:
    """
    简单易用的CSMAR API, 主要是为了避免200000条和30分钟内不得进行相同条件查询的查询限制。
    """
    def __init__(self, username=None, password=None):
        self.csmar = CsmarService()
        # 查询历史{('condition', 'limit', 'startTime', 'endTime'): ('上次查询时间', '最近查询次数')}
        try:
            with open('EasyCsmarApi_cache.pkl', 'rb') as f:
                self.query_history: dict[tuple[str, str, str], tuple[datetime.datetime, int]] = pickle.load(f)
        except FileNotFoundError:
            self.query_history: dict[tuple[str, str, str], tuple[datetime.datetime, int]] = {}
        self.gap_limit = datetime.timedelta(minutes=30)  # 30分钟查询限制
        self.login(username, password)

    def login(self, username=None, password=None):
        """登录账户"""
        self.csmar.login(account=username, pwd=password)

    def get_available_database(self):
        """查询已购买的数据库名称"""
        database = self.csmar.getListDbs()
        ReportUtil(database)

    def get_available_table(self, database_name: str):
        """查询已购买的数据表名称"""
        tables = self.csmar.getListTables(database_name)
        ReportUtil(tables)

    def get_available_field(self, table_name: str):
        """查询已购买的数据表中所有的字段"""
        fields = self.csmar.getListFields(table_name)
        ReportUtil(fields)

    def _delete_limit_not_exist(self):
        """删除不再生效的限制"""
        now = datetime.datetime.now()
        self.query_history = {k: v for k, v in self.query_history if (now - v[1]) > self.gap_limit}

    def _check_query_30min_limit(
            self,
            condition: str, start_time: str, end_time: str
    ) -> bool:
        """检查是否为30分钟内已存在的查询"""
        if (condition, start_time, end_time) not in self.query_history:
            return False
        else:
            last_query_time = self.query_history[(condition, start_time, end_time)][0]  # 上一次查询时间
            if datetime.datetime.now() - last_query_time > self.gap_limit:
                self._delete_limit_not_exist()
                return False
            else:
                return True

    def query(
            self,
            columns: List[str], condition: str, table_name: str,
            start_time: str=None, end_time: str=None,
            exist_query: bool=False
    ) -> pd.DataFrame:
        """
        查询已购买的数据表数据\n
        此时完全不用担心单次查询不得超过200000条和30分钟内不得进行相同条件查询的查询限制
        :param columns: 字段的列表，如: ['Stkcd', 'ShortName', 'Accper', 'Typrep', 'A001000000']
        :param condition: 条件，类似SQL条件语句, 如: "Stkcd='000001'", 但不支持order by (该函数已有默认的排序方式)
        :param table_name: 表名称, 通过 get_available_table(database_name) 查看
        :param start_time: 开始时间, 填写格式为：YYYY-MM-DD
        :param end_time: 结束时间, 填写格式为：YYYY-MM-DD
        :param exist_query: 是否为30分钟内已存在的查询
        :return: pd.DataFrame
        """
        query_count = self.csmar.queryCount(
            columns, condition, table_name, start_time, end_time
        )
        if not self._check_query_30min_limit(condition, start_time, end_time):
            self.query_history[(condition, start_time, end_time)] = (datetime.datetime.now(), 11)
            if query_count < 200000:  # 不涉及查询数量限制
                data: pd.DataFrame = self.csmar.query_df(
                    columns, condition, table_name, start_time, end_time
                )
                if type(data) == pd.DataFrame:
                    return data
                else:
                    if ~exist_query:
                        return self.query(columns, condition, table_name, start_time, end_time, exist_query=True)
                    raise TypeError("查询出现未知问题")
            else:
                query_count_list = [i for i in range(0, query_count, 200000)]
                query_df_list = []
                for i in query_count_list:
                    condition_with_limit = f"{condition} limit {i},{i+200000}"
                    query_df_list.append(
                        self.csmar.query_df(
                            columns, condition_with_limit, table_name,
                            start_time, end_time
                        )
                    )
                return pd.concat(query_df_list, ignore_index=True)
        else:  # 存在30分钟内的相同query
            unique_count = self.query_history[(condition, start_time, end_time)][1]  # 使用不同的limit间隔来绕过限制
            self.query_history[(condition, start_time, end_time)] = (
                datetime.datetime.now(),
                unique_count + 1
            )  # 增加计数
            query_count_list = [i for i in range(0, query_count, 200000 - unique_count)]
            query_df_list = []
            for i in query_count_list:
                condition_with_limit = f"{condition} limit {i},{i + 200000 - unique_count}"
                query_df_list.append(
                    self.csmar.query_df(
                        columns, condition_with_limit, table_name,
                        start_time, end_time
                    )
                )
            with open('EasyCsmarApi_cache.pkl', 'wb') as f:
                pickle.dump(self.query_history, f)  # type: ignore
            return pd.concat(query_df_list, ignore_index=True)
