# Table API add_column示例
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink, WriteMode
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)

result_file = os.getcwd() + "/output/add_column.csv"


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
# 添加列-total_score, apperances*score
# 添加列-total_assist
# 添加列-total_steal
# 添加列-total_block
# 9列变13列
BT_ENV.register_table_sink("players",
                           CsvTableSink(["season", "name", "apperances", "starts", \
                                         "minutes", "assist", "steal", "block", "score", \
                                         "total_score", "total_steal", "total_block", "total_assist"],
                                        [DataTypes.STRING(),
                                         DataTypes.STRING(),
                                         DataTypes.INT(),
                                         DataTypes.INT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT()],
                                         result_file,
                                         ",",
                                         1,
                                         WriteMode.OVERWRITE))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
print(type(t1)) # <class 'pyflink.table.table.Table'>
t1.execute().print()
# 允许一次添加一列或者多列
t2 = t1.add_columns("score*apperances as total_score, \
                     steal*apperances as total_steal, \
                     assist*apperances as total_assist, \
                     block*apperances as total_block")
t2.execute().print()
print("*************************")
BT_ENV.insert_into("players", t2) # insert into sink table
BT_ENV.execute("add column")
