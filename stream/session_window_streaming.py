import os

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, CsvTableSource, CsvTableSink, DataTypes, WriteMode
from pyflink.table.descriptors import Schema, Rowtime, FileSystem, OldCsv
from pyflink.table.window import Session, Tumble, Slide, Over
from pyflink.table.table_schema import TableSchema
from pyflink.table.types import TimestampType
from pyflink.table.udf import udf
# 对日期格式进行变换
from datetime import datetime

input_file = os.getcwd() + os.sep + "input" + os.sep + "streaming_data.csv"

# Flink SQL connector
# watermark可以容忍30秒的乱序
sql_dml = "CREATE TABLE MySource ( \
 a VARCHAR, \
 b INT, \
 c INT, \
 row_time TIMESTAMP(3),\
 proctime AS PROCTIME(), \
WATERMARK FOR row_time AS row_time - INTERVAL '30' SECOND \
) WITH (\
'connector.type' = 'filesystem', \
'connector.path' = '{0}',\
'format.type' = 'csv'\
)".format(input_file) 

csv_source_ddl = sql_dml

# 会话窗口示例
# 统计每窗口时间（5分钟）内，每个用户购买了id为xxx的产品的数量。
def session_window_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    # 
    s_env.set_parallelism(1)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    #
    st_env = StreamTableEnvironment.create(s_env)

    result_file = os.getcwd() + os.sep + "output" + os.sep + "table_session_window_agg_streaming_004.csv"

    st_env.register_table_sink("results",
                               CsvTableSink(["a", "b", "sum", "start_time", "end_time"],
                                            [DataTypes.STRING(),
                                             DataTypes.INT(),
                                             DataTypes.INT(),
                                             DataTypes.TIMESTAMP(3),
                                             DataTypes.TIMESTAMP(3)
                                            ],
                                            result_file, 
                                            ",",
                                            1, 
                                            WriteMode.OVERWRITE))  

 
    st_env.sql_update(csv_source_ddl)
    # 
    t1 = st_env.from_path("MySource")
    t1.print_schema()
    # 测试t1
    #t1.select("a, b").insert_into("results")
    # Session Window
    w1 = Session.with_gap("5.minutes")
    w2 = w1.on("row_time")
    w3 = w2.alias("w")
    # t1.window(Session.with_gap("10.minutes").on("row_time").alias("w")) 
    #GroupWindowedTable
    print(type(w1))
    print(type(w2))
    print(type(w3))
    
    # Table window api
    # Table ------> GroupWindowedTable
    t2 = t1.window(w3) # GroupWindowedTable
    
    #Table通过window操作变成GroupWindowedTable。
    #GroupWindowedTable支持group_by api，变成WindowGroupedTable。
    #WindowGroupedTable只支持select api，变成Table。
    
    # GroupWindowedTable ------> WindowGroupedTable
    #GroupBy must contain exactly one window alias.
    t3 = t2.group_by("w, a, b") # 按字段a, b分组 
    # WindowGroupedTable ------> Table
    
    # 统计每窗口时间（5分钟）内，每个用户购买了id为xxx的产品的数量。
    t4 = t3.select("a, b, sum(c) as sum, w.start as start_time, w.end as end_time")
    t4.print_schema()
    t4.insert_into("results")
    # A limit operation on unbounded tables is currently not supported. order_by
    
    #StreamOperator.execute()
    st_env.execute("session window streaming")
    
    
if __name__ == '__main__':
    session_window_streaming()
