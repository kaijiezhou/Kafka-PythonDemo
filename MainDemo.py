import ConsumerDemo, ProducerDemo, sys, threading, random, JsonParser, LogIngester
__author__ = 'kaijiezhou'



def produceDemo(configs):
    producer=ProducerDemo.DemoProducer(configs)
    keys=["CPU","MEM"]
    while(True):
        topic="computer"+str(random.randint(0,1))
        key=keys[random.randint(0,1)]
        pec=str(random.randint(0,100))
        producer.keyedProduce(topic,key,pec)
        #producer.produce(topic,pec)
        print("Produced: "+topic+"."+key+"="+pec)

def consumeDemo(configs):
    consumer=ConsumerDemo.DemoConsumer(configs)
    t1=threading.Thread(target=consumer.consume,args=("computer1",))
    t2=threading.Thread(target=consumer.consume, args=("computer0",))
    t1.start()
    t2.start()
args=sys.argv
if args[1]=="producer":
    configs=JsonParser.parseJson("producer.json")
    produceDemo(configs)
elif args[1]=="consumer":
    configs1= JsonParser.parseJson("consumer1.json")
    configs2= JsonParser.parseJson("consumer2.json")
    consumeDemo(configs1)
    consumeDemo(configs2)
elif args[1]=="ingest":
    configs=JsonParser.parseJson("producer.json")
    LogIngester.LogIngester(args[2],configs).ingest()
else:
    print("Choose a Mode, producer or consumer")