import os
from pathlib import Path

from kafka_plugin import load_manifest

path = os.path.join(Path(__file__).parent.parent,"Sample_file","app_conf_sample.yaml")
app_conf = load_manifest.Config(path)

def test_load_config():
    assert app_conf.app_conf

def test_get_config_type():
    config_data = app_conf.get_config()
    assert type(config_data.get("app_constants")) is dict

def test_get_config_sub_type():
    config_data = app_conf.get_config()
    data = config_data["app_constants"]
    assert type(data.get("kafka_topics")) is list