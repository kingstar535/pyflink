# Table API rename_column 示例
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink, WriteMode
from pyflink.table.descriptors import FileSystem 
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)
result_file = os.getcwd() + "/output/rename_columns.csv"


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
                           CsvTableSink(["season", "name", \
                                         "minutes", "assist", "steal", "block", "score"],
                                        [DataTypes.STRING(),
                                         DataTypes.STRING(),
                                         #DataTypes.INT(),
                                         #DataTypes.INT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT()],
                                         result_file, "\t", 1, WriteMode.OVERWRITE))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
t1.print_schema()
print("*************************")
t2 = t1.rename_columns("season as nba_season, name as player_name, \
                        assist as assists, steal as steals, \
                        block as blocks, score as scores") # rename
t2.print_schema()
# select操作时使用新的列名，筛选出与TableSink对应的列，强匹配
t3 = t2.select("nba_season as season, player_name as name, minutes, assists, steals, blocks, scores")  
t3.print_schema()
t3.insert_into("players")
BT_ENV.execute("rename columns")

