# Table API order_by示例
# 按照列的升序/降序排序
# key.asc key.desc
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink
from pyflink.table.descriptors import FileSystem 
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)
result_file = os.getcwd() + "/output/order_by.csv"

if os.path.exists(result_file):
    os.remove(result_file)

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

BT_ENV.register_table_sink("players",
                           CsvTableSink(["season", "name", "apperances", "starts", \
                                         "minutes", "assist", "steal", "block", "score"],
                                        [DataTypes.STRING(),
                                         DataTypes.STRING(),
                                         DataTypes.INT(),
                                         DataTypes.INT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT()],
                                         result_file))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
print("*************************")
# 按score升序，minutes降序排列
t2 = t1.order_by("score.asc, minutes.desc") # order_by
print(type(t2))
t2.insert_into("players")
# 打印结果
t2.select("score, minutes").execute().print()
BT_ENV.execute("order_by")

