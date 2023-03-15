# -*- coding: utf8 -*-
# 参考地址: http://bottlepy.org
from bottle import Bottle, route, run, request, response
import json
from kafka3 import KafkaConsumer, KafkaProducer, TopicPartition
import time
from functools import wraps
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import re

baseDir = os.path.dirname(os.path.abspath(__file__))
logPath = 'logs/myapp.log'

if not os.path.isdir(os.path.dirname(logPath)):
    # os.makedirs 可以视为os.mkdir的升级版本，它以递归的方式创建文件夹
    # 设置exist_ok = True， 就不会引发FileExistsError
    os.makedirs(os.path.dirname(logPath), exist_ok=True)

logger = logging.getLogger('myapp')

logger.setLevel(logging.INFO)
# file_handler = logging.FileHandler()
# interval 滚动周期，
# when="MIDNIGHT", interval=1 表示每天0点为更新点，每天生成一个文件
# backupCount  表示日志保存个数
file_handler = TimedRotatingFileHandler(filename=logPath, when="MIDNIGHT", interval=1, backupCount=30)
# filename="mylog" suffix设置，会生成文件名为mylog.2020-02-25.log
file_handler.suffix = "%Y-%m-%d.log"
# extMatch是编译好正则表达式，用于匹配日志文件名后缀
# 需要注意的是suffix和extMatch一定要匹配的上，如果不匹配，过期日志不会被删除。
file_handler.extMatch = re.compile(r"^\d{4}-\d{2}-\d{2}.log$")
# 定义日志输出格式
formatter = logging.Formatter('%(msg)s')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
retryNum = 5
confName = os.path.join(baseDir, 'kafka.conf')


def log_to_logger(fn):
    '''
    Wrap a Bottle request so that a log line is emitted after it's handled.
    (This decorator can be extended to take the desired logger as a param.)
    '''
    @wraps(fn)
    def _log_to_logger(*args, **kwargs):
        request_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        actual_response = fn(*args, **kwargs)
        # modify this to log exactly what you need:
        logger.info('%s %s %s %s %s' % (request.remote_addr,
                                        request_time,
                                        request.method,
                                        request.url,
                                        response.status))
        return actual_response
    return _log_to_logger


def getKafkaInfo(confName):
    if os.path.exists(confName):
        with open(confName, 'r') as f:
            data = f.read()
        kafkaInfo = json.loads(data)
        return kafkaInfo
    else:
        logger.info('%s not found' % confName)
        exit(1)


def getConsumer(kafkaInfo):
    try:
        consumer = KafkaConsumer(group_id=kafkaInfo['consumer']['group_id'],
                                 bootstrap_servers=kafkaInfo['consumer']['bootstrap_servers'],
                                 security_protocol=kafkaInfo['consumer']['security_protocol'],
                                 sasl_mechanism=kafkaInfo['consumer']['sasl_mechanism'],
                                 sasl_plain_username=kafkaInfo['consumer']['sasl_plain_username'],
                                 sasl_plain_password=kafkaInfo['consumer']['sasl_plain_password'],
                                 fetch_max_bytes=kafkaInfo['consumer']['fetch_max_bytes'],
                                 auto_offset_reset=kafkaInfo['consumer']['auto_offset_reset'],
                                 enable_auto_commit=False,
                                 api_version=(0, 10),
                                 )

        msg = ("%s kafka consumer ok: connect to kafka server %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), kafkaInfo['consumer']['bootstrap_servers']))
        logger.info(msg)
    except Exception as e:
        msg = ("%s kafka consumer error %s: unable to connect kafka server %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e, kafkaInfo['consumer']['bootstrap_servers']))
        logger.info(msg)
        global retryNum
        if retryNum > 0:
            retryNum -= 1
            return getConsumer(kafkaInfo)
        else:
            msg = ("%s consumer server error: server is stopped" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            logger.info(msg)
            exit(1)
    return consumer


def getProducer(kafkaInfo):
    try:
        producer = KafkaProducer(bootstrap_servers=kafkaInfo['producer']['bootstrap_servers'],
                                 security_protocol=kafkaInfo['producer']['security_protocol'],
                                 sasl_mechanism=kafkaInfo['producer']['sasl_mechanism'],
                                 sasl_plain_username=kafkaInfo['producer']['sasl_plain_username'],
                                 sasl_plain_password=kafkaInfo['producer']['sasl_plain_password'],
                                 api_version=(0, 10),
                                 )
        msg = ("%s kafka producer ok: connect to kafka server %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), kafkaInfo['producer']['bootstrap_servers']))
        logger.info(msg)
    except Exception as e:
        msg = ("%s kafka producer error %s: unable to connect kafka server %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e, kafkaInfo['producer']['bootstrap_servers']))
        logger.info(msg)
        global retryNum
        if retryNum > 0:
            retryNum -= 1
            return getProducer(kafkaInfo)
        else:
            msg = ("%s producer server error: server is stopped" % time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            logger.info(msg)
            exit(1)
    return producer


app = Bottle()
app.install(log_to_logger)
kafkaInfo = getKafkaInfo(confName)
consumer = getConsumer(kafkaInfo)
producer = getProducer(kafkaInfo)


# 读取kafka的数据
@app.route('/kafka/<topic>')
def getKafka(topic):
    result = []
    try:
        consumer.subscribe([topic])
        # 使用poll函数手动获取kafak数据
        data = consumer.poll(timeout_ms=100)
        for k, v in data.items():
            for item in v:
                recv = "%s:%d:%d key=%s value=%s" % (item.topic, item.partition, item.offset, item.key, item.value)
                result.append(recv)
        if len(result) > 0:
            # 手动提交offset
            consumer.commit()
    except Exception as e:
        result.append('None')
        msg = ("%s get kafka consumer error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e))
        logger.info(msg)
    return json.dumps({'data': result})


# 向kafka中写入数据
# data为form-data格式 key分别为topic和msg
@app.route('/kafka', method='POST')
def kafkaProducer():
    try:
        topic = request.forms.get('topic')
        msg = request.forms.get('msg')
        producer.send(topic, msg.encode('utf8'))
        producer.flush()
        result = "data: %s written successfully" % msg
        return json.dumps({'success': result})
    except Exception as e:
        msg = ("%s get kafka producer error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e))
        logger.info(msg)
        result = "topic or msg is not exist"
        return json.dumps({'error': result})


@app.route('/kafka/reload', method='POST')
def kafkaReload():
    try:
        authkey = request.forms.get('authkey')
        if authkey != "7eWdrGoSiMm4Kfhj":
            msg = "authkey error: access denied"
            return json.dumps({'error': msg})
        else:
            global kafkaInfo, consumer, producer
            kafkaInfo = getKafkaInfo(confName)
            consumer.close()
            producer.close()
            consumer = getConsumer(kafkaInfo)
            producer = getProducer(kafkaInfo)
            msg = "reload kafka successfully"
            return json.dumps({'error': msg})
    except Exception as e:
        msg = ("%s get kafka producer error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e))
        logger.info(msg)
        result = "topic or msg is not exist"
        return json.dumps({'error': result})


@app.route('/kafka/topic')
def getKafkaTopic():
    result = []
    try:
        topics = list(consumer.topics())
        for topic in topics:
            partitions = [TopicPartition(topic, p) for p in consumer.partitions_for_topic(topic)]
            toff = consumer.end_offsets(partitions)
            toff = [(key.partition, toff[key]) for key in toff.keys()]
            toff.sort()
            coff = [(x.partition, consumer.committed(x)) for x in partitions]
            coff.sort()
            toffSum = sum([x[1] for x in toff])
            curSum = sum([x[1] for x in coff if x[1] is not None])
            leftSum = toffSum - curSum
            result.append({"topic": topic, "total offset": toff, "current offset": coff, "diff offset": leftSum})
        return json.dumps({'data': result})
    except Exception as e:
        msg = ("%s get topics error %s" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), e))
        logger.info(msg)
        result = "get topics error"
        return json.dumps({'error': result})


# 与uwsgi一起使用时需要使用application = app
# application = app
# 单独运行时使用app.run()
# app.run(host='0.0.0.0', port=8888, debug=True)
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8888, debug=True)
else:
    application = app
