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

"""
<class 'type'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 'add_default_kryo_serializer', 'execute', 'get_config', 'get_default_local_parallelism', 'get_execution_environment', 'get_execution_plan', 'get_parallelism', 'get_restart_strategy', 'register_type', 'register_type_with_kryo_serializer', 'set_default_local_parallelism', 'set_parallelism', 'set_restart_strategy']
<class 'type'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', 'add_default_kryo_serializer', 'disable_operator_chaining', 'enable_checkpointing', 'execute', 'get_buffer_timeout', 'get_checkpoint_config', 'get_checkpoint_interval', 'get_checkpointing_mode', 'get_config', 'get_default_local_parallelism', 'get_execution_environment', 'get_execution_plan', 'get_max_parallelism', 'get_parallelism', 'get_restart_strategy', 'get_state_backend', 'get_stream_time_characteristic', 'is_chaining_enabled', 'register_type', 'register_type_with_kryo_serializer', 'set_buffer_timeout', 'set_default_local_parallelism', 'set_max_parallelism', 'set_parallelism', 'set_restart_strategy', 'set_state_backend', 'set_stream_time_characteristic']
<class 'pyflink.dataset.execution_environment.ExecutionEnvironment'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_j_execution_environment', 'add_default_kryo_serializer', 'execute', 'get_config', 'get_default_local_parallelism', 'get_execution_environment', 'get_execution_plan', 'get_parallelism', 'get_restart_strategy', 'register_type', 'register_type_with_kryo_serializer', 'set_default_local_parallelism', 'set_parallelism', 'set_restart_strategy']
<class 'pyflink.table.table_environment.BatchTableEnvironment'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__metaclass__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_add_jars_to_j_env_config', '_before_execute', '_from_elements', '_get_function_catalog', '_get_j_env', '_is_aggregate_function', '_is_blink_planner', '_is_table_function', '_j_tenv', '_judge_blink_planner', '_register_aggregate_function', '_register_table_function', '_serializer', '_set_python_executable_for_local_executor', 'add_python_archive', 'add_python_file', 'connect', 'create', 'create_statement_set', 'create_temporary_view', 'drop_temporary_table', 'drop_temporary_view', 'execute', 'execute_sql', 'explain', 'explain_sql', 'from_elements', 'from_pandas', 'from_path', 'from_table_source', 'get_catalog', 'get_config', 'get_current_catalog', 'get_current_database', 'insert_into', 'list_catalogs', 'list_databases', 'list_functions', 'list_modules', 'list_tables', 'list_temporary_tables', 'list_temporary_views', 'list_user_defined_functions', 'list_views', 'register_catalog', 'register_function', 'register_java_function', 'register_table', 'register_table_sink', 'register_table_source', 'scan', 'set_python_requirements', 'sql_query', 'sql_update', 'use_catalog', 'use_database']
<class 'pyflink.datastream.stream_execution_environment.StreamExecutionEnvironment'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_j_stream_execution_environment', 'add_default_kryo_serializer', 'disable_operator_chaining', 'enable_checkpointing', 'execute', 'get_buffer_timeout', 'get_checkpoint_config', 'get_checkpoint_interval', 'get_checkpointing_mode', 'get_config', 'get_default_local_parallelism', 'get_execution_environment', 'get_execution_plan', 'get_max_parallelism', 'get_parallelism', 'get_restart_strategy', 'get_state_backend', 'get_stream_time_characteristic', 'is_chaining_enabled', 'register_type', 'register_type_with_kryo_serializer', 'set_buffer_timeout', 'set_default_local_parallelism', 'set_max_parallelism', 'set_parallelism', 'set_restart_strategy', 'set_state_backend', 'set_stream_time_characteristic']
<class 'pyflink.table.table_environment.StreamTableEnvironment'>
['__class__', '__delattr__', '__dict__', '__dir__', '__doc__', '__eq__', '__format__', '__ge__', '__getattribute__', '__gt__', '__hash__', '__init__', '__le__', '__lt__', '__metaclass__', '__module__', '__ne__', '__new__', '__reduce__', '__reduce_ex__', '__repr__', '__setattr__', '__sizeof__', '__str__', '__subclasshook__', '__weakref__', '_add_jars_to_j_env_config', '_before_execute', '_from_elements', '_get_function_catalog', '_get_j_env', '_is_aggregate_function', '_is_blink_planner', '_is_table_function', '_j_tenv', '_judge_blink_planner', '_register_aggregate_function', '_register_table_function', '_serializer', '_set_python_executable_for_local_executor', 'add_python_archive', 'add_python_file', 'connect', 'create', 'create_statement_set', 'create_temporary_view', 'drop_temporary_table', 'drop_temporary_view', 'execute', 'execute_sql', 'explain', 'explain_sql', 'from_elements', 'from_pandas', 'from_path', 'from_table_source', 'get_catalog', 'get_config', 'get_current_catalog', 'get_current_database', 'insert_into', 'list_catalogs', 'list_databases', 'list_functions', 'list_modules', 'list_tables', 'list_temporary_tables', 'list_temporary_views', 'list_user_defined_functions', 'list_views', 'register_catalog', 'register_function', 'register_java_function', 'register_table', 'register_table_sink', 'register_table_source', 'scan', 'set_python_requirements', 'sql_query', 'sql_update', 'use_catalog', 'use_database']
"""
