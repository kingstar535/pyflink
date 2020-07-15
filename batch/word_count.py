################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# 导入python lib
import logging
import os
import shutil
import sys
import tempfile

from pyflink.dataset import ExecutionEnvironment
from pyflink.table import BatchTableEnvironment, TableConfig
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pyflink.table.types import DataTypes

# 定义job
def word_count():
    # 定义数据源，要进行wordcount的字符串。
    content = "line Licensed to the Apache Software Foundation ASF under one " \
              "line or more contributor license agreements See the NOTICE file " \
              "line distributed with this work for additional information " \
              "line regarding copyright ownership The ASF licenses this file " \
              "to you under the Apache License Version the " \
              "License you may not use this file except in compliance " \
              "with the License"
    # 设定flink执行环境
    # 调用pyflink.table.TableConfig实例化t_config，调用t_config方法设置或者获取Table的参数。
    t_config = TableConfig()
    # 调用pyflink.dataset.ExecutionEnvironment.get_execution_environment
    env = ExecutionEnvironment.get_execution_environment()
    # 创建执行环境
    t_env = BatchTableEnvironment.create(env, t_config)

    # register Results table in table environment
    # 指定计算结果输出位置
    tmp_dir = tempfile.gettempdir()
    result_path = tmp_dir + '/result'
    # 检测result_path是否存在，存在可能会出现冲突，需要进行预处理
    if os.path.exists(result_path):
        try:
            # 定义的result_path是文件的话，需要删除
            # 否则无法创建同名的目录
            if os.path.isfile(result_path):
                os.remove(result_path)
            else:
                # 递归删除result_path下的所有子文件夹和子文件。
                shutil.rmtree(result_path)
        except OSError as e:
            logging.error("Error removing directory: %s - %s.", e.filename, e.strerror)

    logging.info("Results directory: %s", result_path)
    # t.env通过table connector连接外部系统，将批量或者流数据从外部系统获取到Flink系统中，或者从Flink系统中将数据发送到外部系统中。这里table connect与FileSystem连接，将Flink数据写入到外部的FileSystem，路径是result_path，格式由with_format定义，这里是OldCsv,结构由with_schema定义[包含字段名称，字段类型信息]。
    # t_env连接Descriptor FileSystem, 注册table sink，table name-Results。
    t_env.connect(FileSystem().path(result_path)) \
        .with_format(OldCsv()
                     .field_delimiter(',')
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .create_temporary_table("Results")

    # 定义临时表Temp1，存放中间结果
    t_env.connect(FileSystem().path("/tmp/table1")) \
                     .with_format(OldCsv() #
                     .field_delimiter(',') #
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .create_temporary_table("Temp1")

    # 定义临时表Temp2，存放中间结果
    t_env.connect(FileSystem().path("/tmp/table2")) \
        .with_format(OldCsv()
                     .field_delimiter(',')
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .create_temporary_table("Temp2")

    # 定义临时表Temp3，存放中间结果
    t_env.connect(FileSystem().path("/tmp/table3")) \
        .with_format(OldCsv()
                     .field_delimiter(',')
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .with_schema(Schema()
                     .field("word", DataTypes.STRING())
                     .field("count", DataTypes.BIGINT())) \
        .create_temporary_table("Temp3")

    # 创建或加载数据
    # 使用space split content生成list对象，元素为set(word, 1)
    elements = [(word, 1) for word in content.split(" ")]
    print(elements)
    table_data_1 = t_env.from_elements(elements, ["word", "count"])
    print(table_data_1)
    print("********************")
    table_data_1.select("*").insert_into("Temp1")
    print("********************")
    print(type(table_data_1)) # <class 'pyflink.table.table.Table'>
    print(dir(table_data_1)) 
    # 对数据集进行转换操作，将结果输出
    """
    t_env.from_elements(elements, ["word", "count"]) \
         .group_by("word") \
         .select("word, count(1) as count") \
         .insert_into("Results")
    """
    table_data_2 = table_data_1.group_by("word") 
    print(table_data_2) #<class 'pyflink.table.table.GroupedTable'>
    # A table that has been grouped on a set of grouping keys
    table_data_2.select("word, count(1) as count").insert_into("Temp2")
    table_data_3 = table_data_2.select("word, count(1) as count")
    print(table_data_3) #<class 'pyflink.table.table.Table'>
    #print(type(table_data_3))
    table_data_3.select("*").insert_into("Temp3")
    table_data_4 = table_data_3.insert_into("Results")
    #print(table_data_4)
    #print(type(table_data_4))
    # 调用execute方法触发执行
    t_env.execute("word_count")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    word_count()
