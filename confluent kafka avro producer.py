# Ref. https://github.com/confluentinc/confluent-kafka-python
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

# Confluent Kafka Avro Producer

value_schema_str = """
{
   "name": "patient",
   "type": "record",
   "fields" : [
     {
       "name" : "pname",
       "type" : "string",
       "default" : ""
     },
     {
       "name" : "pulse",
       "type" : "int",
       "default" : 72
     },
     {
       "name" : "pressure",
       "type" : "string",
       "default" : "120/80"
     }
   ]
}
"""

key_schema_str = """
{
   "name": "key",
   "type": "record",
   "fields" : [
     {
       "name" : "keyname",
       "type" : "long"
     }
   ]
}
"""

value_schema = avro.loads(value_schema_str)
key_schema = avro.loads(key_schema_str)
value = {"pname": "Sam Doe", "pulse": 100, "pressure": "130/80"}
key = {"keyname": 1000}

avroProducer = AvroProducer({
    'bootstrap.servers': '129.213.148.168:6667',
    'schema.registry.url': 'http://132.145.0.0:8081/schemaregistry/'
    }, default_value_schema=value_schema)

avroProducer.produce(topic='PulseStreamAvroPy', value=value)
avroProducer.flush()
