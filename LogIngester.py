import tailer
import ProducerDemo

__author__ = 'kaijiezhou'

class LogIngester():
    def __init__(self, path=str, producerConfigs=[]):
        self.path=path
        self.producer=ProducerDemo.DemoProducer(producerConfigs)

    #Return [topic, msg]
    def parseLine(self,line):
        return line.split(",",2)

    def ingest(self):
        for line in tailer.follow(open(self.path)):
            resSet=self.parseLine(line)
            self.producer.keyedProduce(resSet[0],resSet[1],resSet[2])