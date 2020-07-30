# CsvTableSource && CsvTableSink
import time
import os
from pyflink.table import CsvTableSource, CsvTableSink
from pyflink.table import WriteMode
from pyflink.table.descriptors import FileSystem 
from pyflink.table.types import DataTypes
from env import BT_ENV

print(BT_ENV.list_databases())


source_file = os.getcwd() + "/input/nba_players.csv"
print(source_file)
result_file = os.getcwd() + "/output/csvtablesource_to_csvtablesink.csv"

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
                                         result_file, 
                                         ",",
                                         1,
                                         WriteMode.OVERWRITE))

print(BT_ENV.list_tables())
t1 = BT_ENV.scan("nba_player") # scan source table
print("*************************")
#BT_ENV.insert_into("players", t1) # insert into sink table
t1.insert_into("players")
t1.execute().print()
BT_ENV.execute("table source & table sink")

