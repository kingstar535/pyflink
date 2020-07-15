from pyflink.dataset import ExecutionEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import BatchTableEnvironment, StreamTableEnvironment

# Batch Environment
b_env = ExecutionEnvironment.get_execution_environment()
# Stream Environment
s_env = StreamExecutionEnvironment.get_execution_environment()

# Table Environment
st_env = StreamTableEnvironment.create(s_env)
bt_env = BatchTableEnvironment.create(b_env)

print(type(ExecutionEnvironment))
print(dir(ExecutionEnvironment))
print(type(StreamExecutionEnvironment))
print(dir(StreamExecutionEnvironment))

print(type(b_env))
print(dir(b_env))
print(type(bt_env))
print(dir(bt_env))
print(type(s_env))
print(dir(s_env))
print(type(st_env))
print(dir(st_env))

