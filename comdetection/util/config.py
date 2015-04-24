import ConfigParser

class Config:
    def __init__(self, interval, configPath):
        self.configPath = configPath
        self.interval = interval
        
    def load(self):
        Config.config = ConfigParser.ConfigParser()
        Config.config.read(configPath)
     Config.config = ConfigParser.ConfigParser()
        Config.config.read(configPath)