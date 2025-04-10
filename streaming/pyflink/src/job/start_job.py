from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp TIMESTAMP
        ) WITH (
            'connector' = 'jdbc',                                   -- JDBC sink connector
            'url' = 'jdbc:postgresql://postgres:5432/postgres',     -- PostgreSQL JDBC URL
            'table-name' = '{table_name}',                          -- Table name in PostgreSQL
            'username' = 'postgres',                                -- PostgreSQL username
            'password' = 'postgres',                                -- PostgreSQL password
            'driver' = 'org.postgresql.Driver'                      -- PostgreSQL JDBC driver
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    table_name = "events"
    pattern = "yyyy-MM-dd HH:mm:ss.SSS"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),                -- Use the event timestamp as the watermark
            WATERMARK for event_watermark as event_watermark - INTERVAL '5' SECOND  -- Watermark strategy
        ) WITH (
            'connector' = 'kafka',                                  -- Kafka source connector
            'properties.bootstrap.servers' = 'redpanda-1:29092',    -- Kafka bootstrap servers
            'topic' = 'test-topic',                                 -- Kafka topic to read from
            'scan.startup.mode' = 'latest-offset',                  -- Start reading from the latest offset
            'properties.auto.offset.reset' = 'latest',              -- Auto offset reset policy
            'format' = 'json'                                       -- Data format (JSON in this case)                           
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()    # Get the current execution environment
    env.enable_checkpointing(10 * 1000)                             # Set checkpoint interval to 10 seconds (value in ms)
    # env.set_parallelism(1)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build() # Create a new environment settings instance
    t_env = StreamTableEnvironment.create(env, environment_settings=settings) # Create a new table environment
    try:
        # Create Kafka table
        source_table = create_events_source_kafka(t_env)                # Create Kafka source table
        postgres_sink = create_processed_events_sink_postgres(t_env)    # Create PostgreSQL sink table
        # write records to postgres too!
        t_env.execute_sql(
            f"""
                    INSERT INTO {postgres_sink}
                    SELECT
                        test_data,
                        TO_TIMESTAMP_LTZ(event_timestamp, 3) as event_timestamp
                    FROM {source_table}
                    """
        ).wait() # Wait for the job to finish

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()
