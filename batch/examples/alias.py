# Table API alias 示例
# 重命名Table的列名。
# alias针对所有列操作，如果只操作某几列，使用rename_column
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink, WriteMode
from pyflink.table.descriptors import FileSystem 
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)
result_file = os.getcwd() + "/output/alias.csv"


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
                           CsvTableSink(["season", "name", "apperances", \
                                         "minutes", "assist", "steal", "block", "score"],
                                        [DataTypes.STRING(),
                                         DataTypes.STRING(),
                                         DataTypes.INT(),
                                         #DataTypes.INT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT()],
                                         result_file, ",", 1, WriteMode.OVERWRITE))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
t1.print_schema()
print("*************************")
# alias针对所有列操作，如果只操作某几列，使用rename_column
t2 = t1.alias("nba_season, player_name, apperances, starts, minutes, assists, steals, blocks, scores") # alias
t2.print_schema()
# select没有选择starts列
# select时使用新的列名
t3 = t2.select("nba_season, player_name, apperances, minutes, assists, steals, blocks, scores")  
t3.insert_into("players")
BT_ENV.execute("alias")

