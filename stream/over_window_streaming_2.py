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

# Over Window
# 实时统计每个用户，购买的商品的id和商品的数量。
def over_window_streaming():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    # 
    s_env.set_parallelism(4)
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    #
    st_env = StreamTableEnvironment.create(s_env)

    result_file = os.getcwd() + os.sep + "output" + os.sep + "table_over_window_agg_streaming_005.csv"

    st_env.register_table_sink("results",
                               CsvTableSink(["a", "b", "sum"],
                                            [DataTypes.STRING(),
                                             DataTypes.BIGINT(),
                                             DataTypes.INT()
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
    w1 = Over.partition_by("a, b") #可以按多个元素进行partition, 多个key使用,分割 
    w2 = w1.order_by("row_time") # order_by必须是时间属性的参数
    w3 = w2.preceding("unbounded_range")
    w4 = w3.alias("w")
    # Over.partition_by("a").order_by("rowtime").preceding("unbounded_range").alias("w") 
    #GroupWindowedTable
    print(type(w1))
    print(type(w2))
    print(type(w3))
    print(type(w4))
    
    # Table over_window api
    # Table ------> OverWindowedTable
    t2 = t1.over_window(w4) # OverWindowedTable
    #Table.over_window(OverWindow) 
    #OverWindow通过Table over_window api操作变成OverWindowedTable。

    #OverWindowedTable只支持select api，select之后变成Table。
   
    # count返回类型为BIGINT
    t3 = t2.select("a, b, c.sum over w as sum")
    t3.print_schema()
    # WindowGroupedTable ------> Table

    t3.insert_into("results")
    # A limit operation on unbounded tables is currently not supported. order_by
    
    #StreamOperator.execute()
    st_env.execute("over window streaming")
    
    
if __name__ == '__main__':
    over_window_streaming()
