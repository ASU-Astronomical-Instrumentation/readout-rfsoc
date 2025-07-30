"""
@author: Cody Roberson
@date: Apr 2025
@file: redisControl.py
@version: 0.2.8
@description:
    This file is the main control loop for the rfsoc. It listens for commands from the redis server and executes them.
    A dictionary is used to map commands to functions in order to create a dispatch table".

    This application outputs to a log file in /var/log/kidpyControl.log. Once the log reaches ~ 20 MB, a new log file
    is created and old one will be renamed to kidpyControl.log.1, kidpyControl.log.2 ... and so on up to 10. Following this, rollover occurs.

    This program is intended to run as a daemon...

"""

# Set up logging
import logging
from logging.handlers import RotatingFileHandler
import os
import traceback
__LOGFMT = "%(asctime)s|%(levelname)s|%(filename)s|%(lineno)d|%(funcName)s|   %(message)s"
logging.basicConfig(format=__LOGFMT, level=logging.DEBUG)
log = logging.getLogger(__name__)
logh = RotatingFileHandler("/var/log/kidpyControl.log", mode = 'a', maxBytes=20_971_520, backupCount=10)
log.addHandler(logh)
logh.setFormatter(logging.Formatter(__LOGFMT))

log.info("Starting redisControl.py; loading libraries")

# rfsocInterface uses the 'PYNQ' library which requires root priviliges+
import getpass

if getpass.getuser() != "root":
    log.error("rfsocInterface.py: root priviliges are required, please run as root.")
    exit()


import redis
import numpy as np
import json
from time import sleep
import config
import ipaddress

import rfsocInterfaceDual as ri

last_tonelist_chan1 = []
last_amplitudes_chan1 = []
last_tonelist_chan2 = []
last_amplitudes_chan2 = []


def create_response(
    status: bool,
    uuid: str,
    data: dict = {},
    error: str = "",
):
    rdict = {
        "status": "OK" if status else "ERROR",
        "uuid": uuid,
        "error": error,
        "data": data,
    }
    response = json.dumps(rdict)
    return response


#################### Command Functions ###########################
def upload_bitstream(uuid, data: dict):
    status = False
    err = ""
    bitstream = "/placeholder/path/to/nowhere"
    try:
        bitstream = data["abs_bitstream_path"]
    except KeyError:
        err = "missing required parameters"
        log.exception("Key error exception caught while parsing upload bitstream command")
        return create_response(status, uuid, error=err)
    if not os.path.exists(bitstream):
        err = "Bitstream does not exist."
        log.error(err)
        return create_response(status, uuid, error=err)
    try:
        ri.uploadOverlay(bitstream)
    except Exception:
        log.exception("Exception occurred while attempting to upload the bitstream")
        return create_response(status, uuid, error="Exception occurred while attempting to upload the bitstream")
    status = True
    return create_response(status, uuid, error=err)

def config_hardware_chan1(uuid, data: dict):
    _ = uuid
    _ = data
    raise Exception("Not implemented")

def config_hardware_chan2(uuid, data: dict):
    _ = uuid
    _ = data
    raise Exception("Not implemented")


def config_hardware(uuid, data: dict):
    """
    :param uuid:
    :param data:
    :return:
    """
    data_a_srcip = "0.0.0.0"
    data_b_srcip = "0.0.0.0"
    data_a_dstip = "0.0.0.0"
    data_b_dstip = "0.0.0.0"
    dstmac_a_msb = "00:00:00:00"
    dstmac_a_lsb = "00:00"
    dstmac_b_msb = "00:00:00:00"
    dstmac_b_lsb = "00:00"
    porta = 0
    portb = 0
    status = False
    err = ""
    try:
        log.debug(f"config_hardware, {data}")
        data_a_srcip = int(ipaddress.ip_address(data["data_a_srcip"]))
        data_b_srcip = int(ipaddress.ip_address(data["data_b_srcip"]))
        data_a_dstip = int(ipaddress.ip_address(data["data_a_dstip"]))
        data_b_dstip = int(ipaddress.ip_address(data["data_b_dstip"]))
        dstmac_a_msb = int(data["destmac_a_msb"], 16)
        dstmac_a_lsb = int(data["destmac_a_lsb"], 16)
        dstmac_b_msb = int(data["destmac_b_msb"], 16)
        dstmac_b_lsb = int(data["destmac_b_lsb"], 16)
        porta = int(data["port_a"])
        portb = int(data["port_b"])
    except KeyError:
        err = "missing required parameters"
        log.exception(err)
        return create_response(status, uuid, error=err)
    except ValueError:
        err = "invalid parameter data type"
        log.exception(err)
        return create_response(status, uuid, error=err)

    try:
        ri.configure_registers(data_a_srcip, data_b_srcip, data_a_dstip, data_b_dstip, dstmac_a_msb, dstmac_a_lsb,
                               dstmac_b_msb, dstmac_b_lsb, porta, portb)
    except:
        err = "An error occured while attempting to set registers."
        log.exception(err)
        return create_response(status, uuid, error=err)
    
    status = True
    return create_response(status, uuid, error=err)


def set_tone_list(uuid, data: dict):
    global last_tonelist_chan1
    global last_tonelist_chan2
    global last_amplitudes_chan1
    global last_amplitudes_chan2
    chan = 0
    strtonelist = ""
    amplitudes = ""

    status = False
    err = ""
    try:
        strtonelist = data["tone_list"]
        chan = int(data["channel"])
        amplitudes = data["amplitudes"]
    except KeyError:  
        err = "missing required parameters, double check that tone list and amplitude list are present"
        log.exception(err)
        return create_response(status, uuid, error=err)
    except ValueError:
        err = "invalid parameter data type"
        log.exception(err)
        return create_response(status, uuid, error=err)

    if chan == 1:
        last_tonelist_chan1 = strtonelist
        last_amplitudes_chan1 = amplitudes
    elif chan == 2:
        last_tonelist_chan2 = strtonelist
        last_amplitudes_chan2 = amplitudes
    try:
        tonelist = np.array(strtonelist)
        x, phi, freqactual = ri.generate_wave_ddr4(tonelist, amplitudes)
        ri.load_bin_list(chan, freqactual)
        wave_r, wave_i = ri.norm_wave(x)
        ri.load_ddr4(chan, wave_r, wave_i, phi)
        ri.reset_accum_and_sync(chan, freqactual)
    except:
        err = "Exception has occured while attempting to upload the waveform"
        log.exception(err)
        return create_response(status, uuid, error=err)
    status = True
    return create_response(status, uuid, error=err)


def get_tone_list(uuid, data: dict):
    global last_tonelist_chan1
    global last_tonelist_chan2
    global last_amplitudes_chan1
    global last_amplitudes_chan2
    status = False
    err = ""
    try:
        chan = int(data["channel"])
        data['channel'] = chan
        if chan == 1:
            data['tone_list'] = last_tonelist_chan1
            data['amplitudes'] = last_amplitudes_chan1
            status = True
        elif chan == 2:
            data['tone_list'] = last_tonelist_chan2
            data['amplitudes'] = last_amplitudes_chan2
            status = True
        else:
            err = "bad channel number"
            log.error(err)
            return create_response(status, uuid, error=err, data = data)
    except KeyError:
        err = "missing required parameters"
        log.error(err)
        return create_response(status, uuid, error=err)
    except ValueError:
        err = "invalid parameter data type"
        log.error(err)
        return create_response(status, uuid, error=err)

    return create_response(status, uuid, error=err, data = data)

############ end of command functions #############
def load_config() -> config.GeneralConfig:
    """Grab config from a file or make it if it doesn't exist."""
    c = config.GeneralConfig("rfsoc_config.cfg")
    c.write_config()
    return c


def main():
    """
    main daemon for rfsoc control.
    """
    conf = load_config()

    name = conf.cfg.rfsocName
    connection = RedisConnection(name, conf.cfg.redis_host, port=conf.cfg.redis_port)
    while 1:
        msg = connection.grab_command_msg()
        if msg is None:
            sleep(3)
            log.warning("No message received from redis server after timeout")
            continue
        else:
            log.debug("received a message from redis server")
        if msg["type"] == "message":
            try:
                command = json.loads(msg["data"].decode())
            except json.JSONDecodeError:
                err = "Could not decode JSON from command"
                log.exception(err)
                connection.sendmsg(create_response(False, "000000000000", error = err))
                continue
            except KeyError:
                err = "no data field in command message"
                log.exception(err)
                connection.sendmsg(create_response(False, "000000000000", error = err))
                continue
            if command["command"] in COMMAND_DICT:
                function = COMMAND_DICT[command["command"]]
                args = {}
                uuid = "no uuid"
                try:
                    args = command["data"]
                except KeyError:
                    err = f"data key was empty for the following command {command['command']}"
                    log.exception(err)
                    connection.sendmsg(create_response(False, "000000000000", error = err))
                    continue
                try:
                    uuid = command['uuid']
                except KeyError:
                    err = f"uuid key was empty for the following command {command['command']}"
                    log.exception(err)
                    connection.sendmsg(create_response(False, "000000000000", error = err))
                    continue
                log.info(f"Executing command: {command['command']} with args: {args}")
                response_str = function(uuid, args)
                connection.sendmsg(response_str)
            else:
                err = "Error, unknown command received"
                log.error(err)
                connection.sendmsg(create_response(False, "000000000000", error = err))
        else:
            continue


class RedisConnection:
    def __init__(self, name, host, port) -> None:
        self.r = redis.Redis(host=host, port=port)
        log.debug("Attempting to connect to redis server")
        if self.check_connection():
            self.pubsub = self.r.pubsub()
            logging.debug(f"subscribing to {name}")
            self.pubsub.subscribe(name)
            log.info("Connected to readout server's redis")
        else:
            log.error(f"Could not connect to redis server. HOST={host}, PORT={port}")
             # This is here because we don't need to constantly retry (as systemd will try to relaunch this app)
            sleep(5)
            exit(1)
                                
    def check_connection(self):
        """Check if the RFSOC is connected to the redis server

        :return: true if connected, false if not
        :rtype: bool
        """
        log.debug("Checking connection to redis server")
        is_connected = False
        try:
            self.r.ping()  # Doesn't just return t/f, it throws an exception if it can't connect.. y tho?
            is_connected = True
        except redis.ConnectionError:
            is_connected = False
            log.exception("Redis Connection Error")
        except redis.TimeoutError:
            is_connected = False
            log.exception("Redis Connection Timeout")
        finally:
            return is_connected

    def grab_command_msg(self):
        """Wait (indefinitely) for a message from the redis server

        :return: the message
        :rtype: str
        """
        if self.check_connection():
            return self.pubsub.get_message(timeout=None)
        else:
            return None

    def sendmsg(self, response):
        if self.check_connection():
            self.r.publish("REPLY", response)
            return


COMMAND_DICT = {
    "config_hardware": config_hardware,
    "upload_bitstream": upload_bitstream,
    "set_tone_list": set_tone_list,
    "get_tone_list": get_tone_list,
}

if __name__ == "__main__":
    main()
