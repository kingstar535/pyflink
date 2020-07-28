#-*- coding:utf-8 -*-
import tempfile
import os
import shutil
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, CsvTableSink, WriteMode
from pyflink.table.descriptors import Schema, FileSystem, OldCsv

# 定义输出路径
sink_path = os.getcwd() + os.sep + "output" + os.sep + "streaming.csv"
# 创建StreamEnvironment
s_env = StreamExecutionEnvironment.get_execution_environment()
# StreamEnvironment 设置并行度
# StreamEnvironment 设置时间属性
s_env.set_parallelism(4)
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
# 创建StreamTableEnvironment
st_env = StreamTableEnvironment.create(s_env) 
# 调用from_elements从list生成Table
t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])

# 注册TableSink
st_env.register_table_sink("stream_sink",
                            CsvTableSink(["a", "b", "c"],
                                         [DataTypes.BIGINT(),
                                          DataTypes.STRING(),
                                          DataTypes.STRING()
                                         ],
                                         sink_path,
                                         ",",
                                         1,
                                         WriteMode.OVERWRITE))

 
t.select("a + 1, b, c").insert_into("stream_sink")
 
st_env.execute("stream_job")
