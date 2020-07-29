
from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import TableConfig
from pyflink.table import DataTypes
from pyflink.table import BatchTableEnvironment
from pyflink.table import StreamTableEnvironment

# 设定flink执行环境
# 调用pyflink.table.TableConfig实例化t_config，调用t_config方法设置或者获取Table的参数。
T_CONFIG = TableConfig()

# Batch Environment
# 调用pyflink.dataset.ExecutionEnvironment.get_execution_environment
B_EXEC_ENV = ExecutionEnvironment.get_execution_environment()
B_EXEC_ENV.set_parallelism(1)

# Stream Environment
S_EXEC_ENV = StreamExecutionEnvironment.get_execution_environment()
S_EXEC_ENV.set_parallelism(1)

# 创建Table执行环境
# Table Environment
ST_ENV = StreamTableEnvironment.create(S_EXEC_ENV, T_CONFIG)
BT_ENV = BatchTableEnvironment.create(B_EXEC_ENV, T_CONFIG)

if __name__ == "__main__":
    print(B_EXEC_ENV)
    print(S_EXEC_ENV)
    print(BT_ENV)
    print(ST_ENV)
