# This is the output driver, which sends message to Kafka
# This function takes only two arguments, topic name and
# the message itself, and sends it to Kafka.

# By convention the topic name should be damstream.dplname

from connections.kafkaconn import getkafkaclient

def sendtokafka(topicname, msg):

    client = getkafkaclient()
    topic = client.topics['damstream.' + topicname]
    with topic.get_sync_producer() as producer:
        producer.produce(msg)
