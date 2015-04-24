import ConfigParser
import thread
from threading import currentThread
import time

class Config(thread):
    config = None
    
    def __init__(self, interval, configPath):
        self.configPath = configPath
        self.interval = interval
        
    def run(self):
        while True:
            Config.config = ConfigParser.ConfigParser()
            Config.config.read(configPath)
            time.sleep(interval)