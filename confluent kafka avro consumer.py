# Ref. https://github.com/confluentinc/confluent-kafka-python
from confluent_kafka import KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


# Confluent Kafka Avro Consumer
c = AvroConsumer({
    'bootstrap.servers': '129.213.148.168:6667',
    'group.id': 'group0',
    'session.timeout.ms': 60000,
    'default.topic.config': {'auto.offset.reset': 'latest'},
    'schema.registry.url': 'http://132.145.0.0:8081/schemaregistry'})


# Subscribe to topic
c.subscribe(['PulseStreamAvroPy'])

print('Consuming Avro Messages ...')

while True:
    try:
        msg = c.poll(10)
    except SerializerError as e:
        print("Message deserialization failed : {}".format(e))
        break

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            c.close()
            break

    print(msg.value())

c.close()




