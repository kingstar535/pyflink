# 使用from_element方法，生成Table
# 调用Table api select筛选出特定列。
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

t2 = t1.select("word")
t2.execute().print()
t3 = t1.select("word, count+1 as count")
# 打印t3的内容
t3.execute().print()

