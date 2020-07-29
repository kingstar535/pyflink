# Table API drop_column 示例
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink, WriteMode
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)
result_file = os.getcwd() + "/output/drop_column.csv"


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
# 删除列starts,block
# 9列变7列
BT_ENV.register_table_sink("players",
                           CsvTableSink(["season", "name", "apperances", \
                                         "minutes", "assist", "steal", "score"],
                                        [DataTypes.STRING(),
                                         DataTypes.STRING(),
                                         DataTypes.INT(),
                                         #DataTypes.INT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         DataTypes.FLOAT(),
                                         #DataTypes.FLOAT(),
                                         DataTypes.FLOAT()],
                                         result_file,
                                         ",",
                                         1,
                                         WriteMode.OVERWRITE))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan/from_path source table
t1.execute().print()
print("*************************")
t2 = t1.drop_columns("starts, block")
t2.execute().print()
BT_ENV.insert_into("players", t2) # insert into sink table
BT_ENV.execute("drop column")

