# CsvTableSource读取csv文件并定义Schema
# register_table_source将CsvTableSource注册成数据库中的Table
# scan读取数据库中的Table，生成table
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)


BT_ENV.register_table_source("nba_player",
                             CsvTableSource(source_file,
                                            ["season", "name", "apperances", "starts", \
                                             "minutes", "assist", "steal", "block", "score"],
                                            [DataTypes.STRING(),
                                             DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.FLOAT(),
                                             DataTypes.FLOAT(),
                                             DataTypes.FLOAT(),
                                             DataTypes.FLOAT(),
                                             DataTypes.FLOAT()]))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
print(type(t1)) # <class 'pyflink.table.table.Table'>
# 打印内容
t1.execute().print()

#BT_ENV.execute("Read data from csv file。")
