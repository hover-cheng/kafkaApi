### 使用Bottle框架处理Kafka数据
1、读取数据\
    url:http://127.0.0.1:9090/kafka/<topic>\
    method:GET
    
2、写入数据\
    url:http://127.0.0.1:9090/kafka\
    method:POST\
    data:{"topic": "topic", "msg": "message"}

3、reload配置文件\
    url:http://127.0.0.1:9090/kafka/reload\
    method:POST\
    data:{"authkey": "7eWdrGoSiMm4Kfhj"}

4、获取kafka的topic信息，并且获取topic的堆积信息\
    url:http://127.0.0.1:9090/kafka/topic\
    method:GET

4、kafka.conf为连接kafka所需的配置信息

### Requirements
python==3.6\
kafka-python3==3.0.0\
uWSGI==2.0.21

### 启动方式
uwsgi --ini uwsgi.ini 
