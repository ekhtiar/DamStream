from hdfs import InsecureClient

def gethdfsclient():
    client = InsecureClient('http://172.17.32.201:50070', user='admin')
    return client