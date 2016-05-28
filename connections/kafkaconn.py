from pykafka import KafkaClient

def getkafkaclient():
    client = KafkaClient(hosts="DSambari.novalocal:6667")
    return client

def zookeeperaddress():
    return "DSambari.novalocal:6667"