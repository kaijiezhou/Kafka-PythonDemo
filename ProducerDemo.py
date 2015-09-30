from kafka import SimpleProducer, KafkaClient, KeyedProducer, RoundRobinPartitioner
from time import sleep
import sys
import JsonParser
from kafka.common import LeaderNotAvailableError

__author__ = 'kaijiezhou'


class DemoProducer(object):
    def __init__(self, configs):
        self.configs=configs

    def produce(self,topic, msg):
        kafka=KafkaClient(self.configs["broker_list"].split(","))
        producer=SimpleProducer(kafka)
        producer.send_messages(topic,msg)

    def produce(self,topic, key, value):
        kafka=KafkaClient(self.configs["broker_list"].split(","))
        keyedProducer=KeyedProducer(kafka)
        undone=True
        while(undone):
            try:
                keyedProducer.send_messages(topic, key, value)
                undone=False
            except LeaderNotAvailableError:
                pass
            #keyedProducer.send_messages(topic, key, value)


if __name__=="main":
    args=sys.argv
    topic=args[0]
    msg=args[1]
    configs=JsonParser.parseJson("config.json")
    producer=DemoProducer(configs)
    producer.produce(topic,msg)