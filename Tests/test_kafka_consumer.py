import os
from pathlib import Path

import pytest

from kafka_plugin import kafka_consumer, load_manifest

# Loading data from user provided dictionary
app_config = {"app_constants": {
    "kafka_topics": ["SAMPLE.TOPIC"],
    "kafka_principal":"sample_principal",
    "kafka_servicename":"sample_servicename",
    "kafka_keytab":"./path/samplefile.keytab",
    "kafka_sslca":"./path/sample.pem",
    "kafka_groupid":"SAMPLE_GROUPID",
    "kafka_servers":"server1:9092 server2:9092",
    "client_id": "sample_client",
    "cache_path":"./path/tmp/sample",
    "queue_size": 100,
    "poll_timeout":10.0,
    "session_timeout":5000,
    "auto_commit":True,
    }
}

# Loading data from manifest
path = os.path.join(Path(__file__).parent.parent, "Sample_file", "app_conf_sample.yaml")
app_conf = load_manifest.Config(path)

def test_consumer_queue():
    kafka_agent = kafka_consumer.KafkaConsumer(app_config)
    assert kafka_agent.local_queue.qsize() == 0

def test_initiate_consumer():
    kafka_agent = kafka_consumer.KafkaConsumer(app_config)
    assert kafka_agent.consumer_conf.get("sasl.kerberos.service.name") == app_config["app_constants"]["kafka_servicename"]
    assert kafka_agent.consumer_conf.get("group.id") == app_config["app_constants"]["kafka_groupid"]
    assert kafka_agent.consumer_conf.get("client.id") == app_config["app_constants"]["client_id"]
    assert kafka_agent.main_thread

def test_initiate_consumer_yaml():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.kafka_configs == values["app_constants"]
    assert kafka_local_agent.main_thread

def test_stop_consumer():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.stop_consumer() is None

def test_start_consumer():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.start_consumer() is None

def test_start_read_msg():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.start_read_msg() is None

def test_consume_msg():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.consume_msg() is None
    assert kafka_local_agent.main_thread is False

def test_check_optional_parameters():
    values = app_conf.get_config()
    kafka_local_agent = kafka_consumer.KafkaConsumer(values)
    assert kafka_local_agent.consumer_conf.get("enable.auto.commit") is False
    assert kafka_local_agent.consumer_conf.get("session.timeout.ms") == 6000
    assert kafka_local_agent.consumer_conf.get("client.id") == "default"
    assert "/tmp/krb5cc_" in kafka_local_agent.krb5_cache

def test_check_optional_parameters_with_value():
    kafka_agent = kafka_consumer.KafkaConsumer(app_config)
    assert kafka_agent.consumer_conf.get("enable.auto.commit") == app_config["app_constants"]["auto_commit"]
    assert kafka_agent.consumer_conf.get("session.timeout.ms") == app_config["app_constants"]["session_timeout"]
    assert kafka_agent.consumer_conf.get("client.id") == app_config["app_constants"]["client_id"]
    assert app_config["app_constants"]["cache_path"] in kafka_agent.krb5_cache

def test_disable_auto_start():
    kafka_agent = kafka_consumer.KafkaConsumer(app_config,auto_start=False)
    with pytest.raises(AttributeError):
        kafka_agent.consumer

def test_enable_auto_start():
    kafka_agent = kafka_consumer.KafkaConsumer(app_config)
    assert kafka_agent.consumer is None