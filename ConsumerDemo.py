__author__ = 'kaijiezhou'
from kafka import KafkaConsumer
import sys,JsonParser,json

class DemoConsumer(object):
    def __init__(self, configs):
        self.configs=configs

    def consume(self, topic):
        consumer=KafkaConsumer(topic,group_id=self.configs["group_id"],bootstrap_servers=self.configs["zookeeper"].split(","), auto_commit_enable=True)
        for message in consumer:
           print("[%s.consumer] %s:%d:%d: key=%s value=%s" % (self.configs["group_id"],message.topic, message.partition,message.offset, message.key,message.value))

if __name__=="main":
    args=sys.argv
    configFile=open(args[0])
    configs=json.loads(configFile.read())
    consumer=DemoConsumer(configs)
    DemoConsumer.consume(args[1])

