from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy
import json

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_topics("f1-telemetry") \
    .set_group_id("flink-consumer") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Source"
)

def parse(event):
    data = json.loads(event)
    data["speed_kmh"] = data["speed"]
    return json.dumps(data)

processed = stream.map(parse)

processed.print()

env.execute("F1 Telemetry Streaming Job")
