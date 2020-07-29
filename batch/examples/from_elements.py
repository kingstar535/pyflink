# 使用from_element方法，生成Table
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

# 查看类型
print(type(t1))
#
# 打印t1的Schema
t1.print_schema()

# 打印执行结果
# SUCCESS = 0
# SUCCESS_WITH_CONTENT = 1
print(t1.execute().get_result_kind())
# 打印t1的内容
t1.execute().print()

