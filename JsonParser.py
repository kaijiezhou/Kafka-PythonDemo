__author__ = 'kaijiezhou'
import json

def parseJson(path):
    file=open(path)
    confStr=file.read()
    return json.loads(confStr)