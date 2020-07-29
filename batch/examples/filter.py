# 使用from_element方法，生成Table
# 调用Table api filter筛选出特定的行。
import os
from pyflink.table.descriptors import OldCsv
from pyflink.table.descriptors import Schema
from pyflink.table.descriptors import FileSystem
from pyflink.table import DataTypes

from env import BT_ENV
from element import Elements

elements = [[word, 1] for word in Elements]
print(elements)

# BT_ENV.from_elements(LIST, SCHEMA)
# Create Table
t1 = BT_ENV.from_elements(elements, ["word", "count"])
t1.execute().print()

t2 = t1.filter("word=='pyflink'")
t2.execute().print()


