import yaml
import logging

class Config:
    '''
    class to initiate yaml config file load and get data

    input: relative file config file path
    output: config data
    '''
    def __init__(self, file_path):
        self.logger = logging.getLogger("kafka_plugin")
        try:
            with open(file_path,'r', encoding="utf-8") as config_file:
                self.app_conf = yaml.safe_load(config_file)
        except Exception as e:
            self.logger.error("Exception occured while loading config")
            self.logger.error(str(e))
            self.app_conf = None

    def get_config(self):
        return self.app_conf
