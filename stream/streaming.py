import tempfile
import os
import shutil
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, CsvTableSink, WriteMode
from pyflink.table.descriptors import Schema, FileSystem, OldCsv

sink_path = os.getcwd() + os.sep + "output" + os.sep + "streaming.csv"

"""
if os.path.exists(sink_path):
    if os.path.isfile(sink_path):
        os.remove(sink_path)
    else:
        shutil.rmtree(sink_path)
"""
s_env = StreamExecutionEnvironment.get_execution_environment()
# 
s_env.set_parallelism(4)
s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
#
st_env = StreamTableEnvironment.create(s_env) 

t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])

"""
st_env.connect(FileSystem().path(sink_path)).with_format(OldCsv()
                 .field_delimiter(',')
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())).with_schema(Schema()
                 .field("a", DataTypes.BIGINT())
                 .field("b", DataTypes.STRING())
                 .field("c", DataTypes.STRING())).in_append_mode().create_temporary_table("stream_sink")
"""
# 
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
