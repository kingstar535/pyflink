# 调用connect接口连接外部文件系统（csv）,将结果写入csv文件

import os
from pyflink.table.descriptors import OldCsv
from pyflink.table.descriptors import Schema
from pyflink.table.descriptors import FileSystem
from pyflink.table import DataTypes, WriteMode

from env import BT_ENV
from element import Elements

OUTPUT_FILE = os.getcwd() + "/output/connect_file_csv_sink.csv"

if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

# table sink
# 默认是in_overwrite_mode()
BT_ENV.connect(FileSystem().path(OUTPUT_FILE)) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

print(BT_ENV.list_tables())

elements = [[word, 1] for word in Elements]
print(elements)

# 创建Table
t1 = BT_ENV.from_elements(elements, ["word", "count"])
t1.execute().print()
# 
t1.insert_into('mySink')

BT_ENV.execute("python_job")

