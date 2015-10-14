__author__ = 'kaijiezhou'
from kafka.consumer.simple import SimpleConsumer
from kafka import KafkaClient
import sys,JsonParser,json

class DemoConsumer(object):
    def __init__(self, configs):
        self.configs=configs

    def consume(self, topic):
        #consumer=KafkaConsumer(topic,group_id=self.configs["group_id"],bootstrap_servers=self.configs["zookeeper"].split(","), auto_commit_enable=False)
        client=KafkaClient(self.configs["broker_list"].split(","))
        consumer=SimpleConsumer(topic=topic,group=self.configs["group_id"],client=client, auto_commit=False)
        while(True):
            # message is (partition, msg). for msg, it has key, value as param.
            for message in consumer.get_messages(10):
                #print("[%s.consumer] %s-part-%d: value=%s" % (self.configs["group_id"],topic,message[0],message[1]))
                print("[%s.consumer] %s-part-%d: key=%s value=%s" % (self.configs["group_id"],topic, message[0], message[1].key,message[1].value))

class GrabConsumer(object):
    def __init__(self,configs):
        self.client=KafkaClient(configs["broker_list"].split(","))
        self.group_id=configs["group_id"]

    def consumeLatest(self,topic):
        consumer = SimpleConsumer(topic=topic,group=self.group_id,client=self.client,auto_commit=True)
        consumer.get_message(timeout=1)
        consumer.stop()



if __name__=="main":
    args=sys.argv
    configFile=open(args[0])
    configs=json.loads(configFile.read())
    consumer=DemoConsumer(configs)
    DemoConsumer.consume(args[1])

