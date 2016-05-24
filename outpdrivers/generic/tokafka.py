# This is the output driver, which sends message to Kafka
# This function takes only two arguments, topic name and
# the message itself, and sends it to Kafka.

# By convention the topic name should be damstream.dplname

from pykafka import KafkaClient

def sendtokafka(dplid, msg):

    client = KafkaClient(hosts="DSambari.novalocal:6667")
    topic = client.topics['damstream.'+dplid]
    with topic.get_sync_producer() as producer:
        producer.produce(msg)
