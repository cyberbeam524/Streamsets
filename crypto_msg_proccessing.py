
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

from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings
from pyflink.table.udf import udf
from datetime import datetime

provinces = ("Beijing", "Shanghai", "Hangzhou", "Shenzhen", "Jiangxi", "Chongqing", "Xizang")


@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def province_id_to_name(id):
    return provinces[id]

# @udf(input_types=[DataTypes.TIMESTAMP(3)], result_type=DataTypes.TIMESTAMP(3))
# def to_time(timestr):
#     res = datetime.strptime(timestr, '%Y-%m-%d %H:%M:%S').time()
#     print("this is the timestr:", timestr, " and ", res)
#     return res

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)
    t_env.get_config().get_configuration().set_boolean("python.fn-execution.memory.managed", True)


    create_kafka_source_ddl = """
            CREATE TABLE payment_msg(
                S VARCHAR,
                T VARCHAR,
                i DOUBLE,
                p DOUBLE,
                s DOUBLE,
                ts VARCHAR,
                tks VARCHAR,
                x VARCHAR
            ) WITH (
              'connector' = 'kafka',
              'topic' = 'crypto_msg',
              'properties.bootstrap.servers' = 'kafka:9092',
              'properties.group.id' = 'test_3',
              'scan.startup.mode' = 'latest-offset',
              'format' = 'json'
            )
            """

    create_es_sink_ddl = """
            CREATE TABLE es_sink(
                S VARCHAR,
                T VARCHAR,
                i DOUBLE,
                p DOUBLE,
                s DOUBLE,
                ts_1 VARCHAR,
                tks VARCHAR,
                x VARCHAR
            ) with (
                'connector' = 'elasticsearch-7',
                'hosts' = 'http://elasticsearch:9200',
                'index' = 'crypto_1',
                'document-id.key-delimiter' = '$',
                'sink.bulk-flush.max-size' = '42mb',
                'sink.bulk-flush.max-actions' = '32',
                'sink.bulk-flush.interval' = '1000',
                'sink.bulk-flush.backoff.delay' = '1000',
                'format' = 'json'
            )
    """

    t_env.execute_sql(create_kafka_source_ddl)
    t_env.execute_sql(create_es_sink_ddl)
    # t_env.register_function('province_id_to_name', province_id_to_name)
    # t_env.register_function('to_time', to_time)

    t_env.from_path("payment_msg") \
        .select("S, T, i, p, s, ts, tks, x") \
        .execute_insert("es_sink")


if __name__ == '__main__':
    log_processing()

