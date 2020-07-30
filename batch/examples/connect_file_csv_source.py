# 调用connect接口连接外部文件系统（csv）,读取csv文件

import os
from pyflink.table.descriptors import OldCsv
from pyflink.table.descriptors import Schema
from pyflink.table.descriptors import FileSystem
from pyflink.table import DataTypes, WriteMode

from env import BT_ENV

INPUT_FILE = os.getcwd() + "/input/nba_players.csv"

# table source
BT_ENV.connect(FileSystem().path(INPUT_FILE)) \
    .with_format(OldCsv()
                 .field_delimiter(',')
                 .field('season', DataTypes.STRING())
                 .field('name', DataTypes.STRING())
                 .field('apperances', DataTypes.INT())
                 .field('starts', DataTypes.INT())
                 .field('minutes', DataTypes.FLOAT())
                 .field('assist', DataTypes.FLOAT())
                 .field('steal', DataTypes.FLOAT())
                 .field('block', DataTypes.FLOAT())
                 .field('score', DataTypes.FLOAT()))\
    .with_schema(Schema()
                 .field('season', DataTypes.STRING())
                 .field('name', DataTypes.STRING())
                 .field('apperances', DataTypes.INT())
                 .field('starts', DataTypes.INT())
                 .field('minutes', DataTypes.FLOAT())
                 .field('assist', DataTypes.FLOAT())
                 .field('steal', DataTypes.FLOAT())
                 .field('block', DataTypes.FLOAT())
                 .field('score', DataTypes.FLOAT()))\
    .create_temporary_table('mySource')

print(BT_ENV.list_tables())

t1 = BT_ENV.from_path("mySource")
t1.execute().print()

