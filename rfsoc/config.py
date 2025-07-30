"""
Handles the process of loading and creating the relative configuration to the readout system.
Users should

credit: Alexandra Zaharia on Python Configuration and Data Classes.
    https://alexandra-zaharia.github.io/posts/python-configuration-and-dataclasses/

    Python Documentation
    https://docs.python.org/3.11/library/configparser.html

"""
import configparser
from dataclasses import dataclass, asdict
import logging
import uuid

__all__ = []
__version__ = "0.2.7"
__author__ = "Cody Roberson"
__email__ = "carobers@asu.edu"


log = logging.getLogger(__name__)


@dataclass
class ConfigData:
    """
    Serves as the struct/container of our config.
    """

    rfsocName: str = "devrfsoc"
    redis_host: str = "192.168.2.10"
    redis_port: str = "6379"


class GeneralConfig(object):
    """Kidpy General Config Handler.
    This is a **SINGLETON** class and as such there can only be one instance of this class runnig at a time.
    This is to ensure unity across the library. parameters are accessed through
    GeneralConfig.cfg.<parameter_here>

    The configurator blindly reads all input from the configuration file
    into the ConfigData object (cfg). This allows users
    to set custom parameters however, they will not be regenerated in the event the config file is
    deleted. All config parameters will be interpreted as strings and must be checked/parsed independantly.

    Additionally, custom attributes added to the cfg object will be recorded in the config file. All configuration
    parameters shall be left in the [DEFAULT] section, any other section will be ignored.

    In the event that the config is extended, the best practice would be to update the cfg class to include
    any desired defaults.

    """

    def __init__(self, path: str):
        log.getChild("GeneralConfig.__init__").info("Using config file {}".format(path))
        self.cfg = ConfigData()
        self.file_name = path
        self.__config_parser = configparser.ConfigParser()
        self.__read_cfg()

    def __read_cfg(self):
        """
        reads config data into the GeneralConfig instance.

        The configurator blindly reads all input in the configuration into the cfg class object. This lets custom params
        to be set and used in the config file however, they will not be regenerated in the event the config file is
        deleted. All config parameters will be interpreted as strings and must be checked/parsed independantly.
        :param path: path to the config file to read
        """
        read_files = self.__config_parser.read(self.file_name)
        if self.file_name not in read_files:
            log.getChild("GeneralConfig.read_cfg").warning(
                "failed to read config file specified, defaults will be used"
            )
            setattr(self.cfg, "uuid", str(uuid.uuid4()))
        else:
            # This will greedily read any property specified in the config file's DEFAULT section
            for attr, val in self.__config_parser["DEFAULT"].items():
                log.getChild("GeneralConfig.read_config").debug(
                    "read attr: {}, val: {} from config file".format(attr, val)
                )
                setattr(self.cfg, attr, val)

    def write_config(self):
        """
        Write the current configuration to a file. Even if no configuration is specified, a default will be set.
        """
        cfg = asdict(self.cfg)
        self.__config_parser["DEFAULT"] = cfg

        try:
            with open(self.file_name, "w") as cfg_file:
                self.__config_parser.write(cfg_file)
        except PermissionError as PE:
            log.getChild("GeneralConfig.write_config").error(
                "permission error: failed to write configuration to file specified"
            )
