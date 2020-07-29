# 
import os
from pyflink.table.descriptors import OldCsv
from pyflink.table.descriptors import Schema
from pyflink.table.descriptors import FileSystem
from pyflink.table import DataTypes

from env import BT_ENV
from element import Elements

OUTPUT_FILE = os.getcwd() + "/output/word_count.csv"

if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

# table sink
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

t1 = BT_ENV.from_elements(elements, ["word", "count"])
t1.execute().print()
# Table------>GroupedTable 
gt2 = t1.group_by("word")
print(type(gt2))
# GroupedTable------>Table
t2 = gt2.select("word, count(1) as count")
t2.execute().print()
t2.insert_into('mySink')

#BT_ENV.insert_into("mySink", t2)

BT_ENV.execute("python_job")

