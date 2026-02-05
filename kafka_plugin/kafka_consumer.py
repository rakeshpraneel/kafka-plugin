import json
import logging
import os
import queue
import threading
import time

from confluent_kafka import Consumer


class KafkaConsumer:
    '''
    class to create consumer instance with all the configs
    subscribes to topic and consume all the messages
    stores in the local queue from which the data can be consumed without
    data loss and efficiently.

    input: app_config object returned from Config class from load_manifest
    and auto_start which is True by default, it can be passed as false if you need
    control over starting the consumer

    if load_manifest is not used, follow the below sample format
    app_config = {
    "app_constants": {
    "kafka_topics": [],
    "kafka_principal":,
    "kafka_servicename":,
    "kafka_keytab":,
    "kafka_sslca":,
    "kafka_groupid":,
    "kafka_servers":,
    "client_id": ,(optional; "default")
    "cache_path":,(optional; "tmp//tmp/krb5cc_{pid}")
    "queue_size":,(optional; 10000)
    "poll_timeout":,(optional; 5.0)
    "session_timeout":,(optional; 6000)
    "auto_commit":,(optional;False)
    }
    }

    '''

    def __init__(self, app_config, auto_start=True):
        self.logger = logging.getLogger("kafka_plugin")
        self.main_thread = True
        try:
            self.kafka_configs = app_config["app_constants"]
            self.topics = self.kafka_configs["kafka_topics"]
            self.consumer_conf = {
                "sasl.mechanism": "GSSAPI",
                "security.protocol": "SASL_SSL",
                "client.id": self.kafka_configs.get("client_id","default"),
                "sasl.kerberos.service.name": self.kafka_configs["kafka_servicename"],
                "sasl.kerberos.principal": self.kafka_configs["kafka_principal"],
                "sasl.kerberos.keytab": self.kafka_configs["kafka_keytab"],
                "ssl.ca.location": self.kafka_configs["kafka_sslca"],
                "sasl.kerberos.kinit.cmd": 'kinit -k -t "%{sasl.kerberos.keytab}" "%{sasl.kerberos.principal}"',
                "enable.auto.commit": self.kafka_configs.get("auto_commit",False),
                "session.timeout.ms": self.kafka_configs.get("session_timeout",6000),
                "bootstrap.servers": self.kafka_configs["kafka_servers"],
                "group.id": self.kafka_configs["kafka_groupid"]
            }
            cache_path = self.kafka_configs.get("cache_path","/tmp/krb5cc_")
            self.krb5_cache = cache_path + str(os.getpid())
            os.environ["KRB5CCNAME"] = self.krb5_cache

            self.local_queue = queue.Queue(maxsize=self.kafka_configs.get("queue_size",10000))

            if auto_start:
                self.start_consumer()
        except Exception as e:
            self.logger.error("Exception while initiating the consumer")
            self.logger.error(str(e))

    def start_consumer(self):
        '''
        function to start the consumer instance by subscribing to required topic
        '''
        try:
            self.consumer = Consumer(self.consumer_conf)
            self.consumer.subscribe(self.topics)
        except Exception as e:
            self.consumer = None
            self.logger.error(f"Exception while spinning up the consumer: {str(e)}")

    def consume_msg(self):
        '''
        function to consume message through the initiated consumer
        instance and add them to local queue
        stops the consumer if there's any exception
        '''
        nomsg_identify = True
        self.consumer_paused = False
        try:
            while self.main_thread:
                if self.local_queue.full():
                    if not self.consumer_paused:
                        self.consumer.pause(self.consumer.assignment())
                        self.consumer_paused = True
                    self.consumer.poll(timeout=1.0)
                    self.logger.warning("Local queue is full, so pausing consumer")
                    continue

                if self.consumer_paused:
                    self.consumer.resume(self.consumer.assignment())
                    self.consumer_paused = False

                time.sleep(0.2)
                self.msg = self.consumer.poll(timeout=5.0)
                if self.msg is None:
                    if nomsg_identify:
                        self.logger.info("No message found")
                        nomsg_identify = False
                    continue
                elif self.msg.error():
                    self.logger.error(f"Kafka Message Error: {self.msg.error()}")
                    nomsg_identify = True
                    continue
                try:
                    msg_decoded = json.loads(self.msg.value().decode("utf-8"))
                except Exception as e:
                    self.logger.error("Message received is not in json form")
                    self.logger.error(str(e))
                    continue
                try:
                    self.local_queue.put(msg_decoded, timeout=2)
                    self.consumer.commit(self.msg)
                except queue.Full:
                    continue

                nomsg_identify = True
        except Exception as e:
            self.logger.error("Exception while consuming message")
            self.logger.error(str(e))
            self.stop_consumer()
            self.main_thread = False

    def stop_consumer(self):
        '''
        function to stop consumer
        '''
        if self.consumer:
            self.consumer.close()

    def start_read_msg(self):
        '''
        function to run consume msg in seperate thread and
        it runs in the background without any interruption
        if not it will log the error and return none
        '''
        if self.consumer:
            consumer_thread = threading.Thread(target=self.consume_msg, daemon=True)
            consumer_thread.start()
        else:
            self.logger.error("Consumer is down, starting thread is of no use...")
            return

    def get_message(self):
        '''
        function is used to get message from local queue
        :return: first message from the queue and None if it is empty
        '''
        if self.local_queue.qsize() > 0:
            return self.local_queue.get()
        return None