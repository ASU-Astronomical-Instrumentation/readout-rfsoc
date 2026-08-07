"""
@author: Cody Roberson
@date: Jan 15, 2026
@file: redisControl.py
@description: Main control loop for rfsoc. Listens for Redis commands, validates overlay state,
              executes hardware functions, and returns structured JSON responses.
"""

import logging
from logging.handlers import RotatingFileHandler
import os
import getpass
import redis
import numpy as np
import json
from time import sleep
import ipaddress
import config
import rfsocInterfaceDual as ri

__LOGFMT = "%(asctime)s|%(levelname)s|%(filename)s|%(lineno)d|%(funcName)s|   %(message)s"
logging.basicConfig(format=__LOGFMT, level=logging.INFO)
log = logging.getLogger(__name__)
logh = RotatingFileHandler("/var/log/kidpyControl.log", mode='a', maxBytes=20_971_520, backupCount=10)
log.addHandler(logh)
logh.setFormatter(logging.Formatter(__LOGFMT))

log.info("Starting redisControl.py; loading libraries")

if getpass.getuser() != "root":
    log.error("rfsocInterface.py: root privileges are required, please run as root.")
    exit()

# Volatile state (lost on crash/restart by design)
last_tonelist_chan1 = []
last_amplitudes_chan1 = []
last_tonelist_chan2 = []
last_amplitudes_chan2 = []

def create_response(status: bool, uuid: str, data: dict = None, error: str = ""):
    rdict = {
        "status": "OK" if status else "ERROR",
        "uuid": uuid,
        "error": error,
        "data": data if data is not None else {}
    }
    return json.dumps(rdict)

# ------------------- Command Functions -------------------
def upload_bitstream(uuid, data: dict):
    status, err = False, ""
    bitstream = ""
    try:
        bitstream = data["abs_bitstream_path"]
    except KeyError:
        err = "missing required parameters"
        log.exception(err)
        return create_response(status, uuid, error=err)
    if not os.path.exists(bitstream):
        err = "Bitstream does not exist."
        log.error(err)
        return create_response(status, uuid, error=err)
    try:
        ri.uploadOverlay(bitstream)
        status = True
    except Exception as e:
        err = f"Exception occurred while attempting to upload the bitstream: {e}"
        log.exception(err)
    return create_response(status, uuid, error=err)

def config_hardware(uuid, data: dict):
    status, err = False, ""
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
    except (KeyError, ValueError) as e:
        err = f"missing or invalid required parameters: {e}"
        log.exception(err)
        return create_response(status, uuid, error=err)
    try:
        ri.configure_registers(data_a_srcip, data_b_srcip, data_a_dstip, data_b_dstip,
                               dstmac_a_msb, dstmac_a_lsb, dstmac_b_msb, dstmac_b_lsb, porta, portb)
        status = True
    except Exception as e:
        err = f"An error occurred while attempting to set registers: {e}"
        log.exception(err)
    return create_response(status, uuid, error=err)

def set_tone_list(uuid, data: dict):
    global last_tonelist_chan1, last_tonelist_chan2, last_amplitudes_chan1, last_amplitudes_chan2
    status, err = False, ""
    try:
        strtonelist = data["tone_list"]
        chan = int(data["channel"])
        amplitudes = data["amplitudes"]
    except (KeyError, ValueError) as e:
        err = f"missing or invalid required parameters: {e}"
        log.exception(err)
        return create_response(status, uuid, error=err)

    if chan == 1:
        last_tonelist_chan1 = strtonelist
        last_amplitudes_chan1 = amplitudes
    elif chan == 2:
        last_tonelist_chan2 = strtonelist
        last_amplitudes_chan2 = amplitudes
    else:
        err = "bad channel number"
        log.error(err)
        return create_response(status, uuid, error=err)

    try:
        tonelist = np.array(strtonelist)
        x, phi, freqactual = ri.generate_wave_ddr4(tonelist, amplitudes)
        ri.load_bin_list(chan, freqactual)
        wave_r, wave_i = ri.norm_wave(x)
        ri.load_ddr4(chan, wave_r, wave_i, phi)
        ri.reset_accum_and_sync(chan, freqactual)
        status = True
    except Exception as e:
        err = f"Exception occurred while attempting to upload the waveform: {e}"
        log.exception(err)
    return create_response(status, uuid, error=err)

def get_tone_list(uuid, data: dict):
    global last_tonelist_chan1, last_tonelist_chan2, last_amplitudes_chan1, last_amplitudes_chan2
    status, err = False, ""
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
            return create_response(status, uuid, error=err, data=data)
    except (KeyError, ValueError) as e:
        err = f"missing or invalid required parameters: {e}"
        log.error(err)
        return create_response(status, uuid, error=err)
    return create_response(status, uuid, error=err, data=data)

def check_overlay_status(uuid, data: dict):
    """Query current PYNQ overlay state on the PL."""
    if ri.firmware is None:
        return create_response(False, uuid, error="Overlay not initialized")
    if ri.firmware.is_loaded():
        return create_response(True, uuid, data={"status": "loaded", "bitfile": ri.firmware.bitfile_name})
    return create_response(False, uuid, error="Overlay unloaded or timestamp mismatched")

# Granular setters (unchanged signature, rely on interface layer validation)
def set_data_a_src_ip(uuid, data): return _wrap_setter(uuid, data, ri.setDataASrcIp, "data A source IP")
def set_data_a_dst_ip(uuid, data): return _wrap_setter(uuid, data, ri.setDataADstIp, "data A destination IP")
def set_data_a_mac_msb(uuid, data): return _wrap_setter(uuid, data, ri.setDataAMacMsb, "data A MAC MSB")
def set_data_a_mac_lsb(uuid, data): return _wrap_setter(uuid, data, ri.setDataAMacLsb, "data A MAC LSB")
def set_data_a_port(uuid, data): return _wrap_setter(uuid, data, ri.setDataAPort, "data A port")
def set_data_b_src_ip(uuid, data): return _wrap_setter(uuid, data, ri.setDataBSrcIp, "data B source IP")
def set_data_b_dst_ip(uuid, data): return _wrap_setter(uuid, data, ri.setDataBDstIp, "data B destination IP")
def set_data_b_mac_msb(uuid, data): return _wrap_setter(uuid, data, ri.setDataBMacMsb, "data B MAC MSB")
def set_data_b_mac_lsb(uuid, data): return _wrap_setter(uuid, data, ri.setDataBMacLsb, "data B MAC LSB")
def set_data_b_port(uuid, data): return _wrap_setter(uuid, data, ri.setDataBPort, "data B port")

def _wrap_setter(uuid, data, func, desc):
    status, err = False, ""
    try:
        val = int(data["value"])
    except (KeyError, ValueError, TypeError) as e:
        err = f"missing or invalid parameter 'value': {e}"
        log.exception(err)
        return create_response(status, uuid, error=err)
    try:
        func(val)
        status = True
    except Exception as e:
        err = f"Failed to set {desc}: {e}"
        log.exception(err)
    return create_response(status, uuid, error=err)

COMMAND_DICT = {
    "config_hardware": config_hardware,
    "upload_bitstream": upload_bitstream,
    "set_tone_list": set_tone_list,
    "get_tone_list": get_tone_list,
    "check_overlay": check_overlay_status,
    "set_data_a_src_ip": set_data_a_src_ip,
    "set_data_a_dst_ip": set_data_a_dst_ip,
    "set_data_a_mac_msb": set_data_a_mac_msb,
    "set_data_a_mac_lsb": set_data_a_mac_lsb,
    "set_data_a_port": set_data_a_port,
    "set_data_b_src_ip": set_data_b_src_ip,
    "set_data_b_dst_ip": set_data_b_dst_ip,
    "set_data_b_mac_msb": set_data_b_mac_msb,
    "set_data_b_mac_lsb": set_data_b_mac_lsb,
    "set_data_b_port": set_data_b_port,
}

def load_config() -> config.GeneralConfig:
    c = config.GeneralConfig("rfsoc_config.cfg")
    c.write_config()
    return c

def _startup_bitstream_load(conf):
    """Attempt to load default bitstream on fresh service start. Fails gracefully on restart/crash."""
    default_bs = getattr(conf.cfg, 'default_bitstream', None)
    if not default_bs:
        log.info("Startup: No default_bitstream configured. Skipping automatic overlay load.")
        return
    try:
        log.info(f"Startup: Loading default bitstream: {default_bs}")
        ri.uploadOverlay(default_bs)
        log.info("Startup: Bitstream loaded successfully.")
    except Exception as e:
        # On crash/restart, the FPGA may already have the bitstream or be in an unknown state.
        # We log the warning but continue. Hardware commands will properly report overlay state.
        log.warning(f"Startup: Could not load default bitstream: {e}. "
                    f"Daemon will continue. Hardware commands will validate overlay state dynamically.")

def main():
    conf = load_config()
    _startup_bitstream_load(conf)

    name = conf.cfg.rfsocName
    connection = RedisConnection(name, conf.cfg.redis_host, port=conf.cfg.redis_port)

    log.info("Daemon started. Awaiting commands...")
    while True:
        msg = connection.grab_command_msg()
        if msg is None:
            sleep(3)
            log.warning("No message received from redis server after timeout")
            continue

        if msg["type"] != "message":
            continue

        try:
            command = json.loads(msg["data"].decode())
        except json.JSONDecodeError:
            log.error("Could not decode JSON from command")
            connection.sendmsg(create_response(False, "000000000000", error="Invalid JSON payload"))
            continue

        if "data" not in command or "uuid" not in command:
            err = "Missing 'data' or 'uuid' field in command message"
            log.error(err)
            connection.sendmsg(create_response(False, "000000000000", error=err))
            continue

        cmd_name = command["command"]
        if cmd_name not in COMMAND_DICT:
            err = f"Unknown command received: {cmd_name}"
            log.error(err)
            connection.sendmsg(create_response(False, command["uuid"], error=err))
            continue

        try:
            log.info(f"Executing command: {cmd_name}")
            response_str = COMMAND_DICT[cmd_name](command["uuid"], command["data"])
        except RuntimeError as e:
            err = f"Hardware state error: {e}"
            log.error(err)
            response_str = create_response(False, command["uuid"], error=err)
        except Exception as e:
            err = f"Unhandled command execution failure: {e}"
            log.exception(err)
            response_str = create_response(False, command["uuid"], error=err)

        connection.sendmsg(response_str)

class RedisConnection:
    def __init__(self, name, host, port) -> None:
        self.r = redis.Redis(host=host, port=port)
        if self.check_connection():
            self.pubsub = self.r.pubsub()
            self.pubsub.subscribe(name)
            log.info("Connected to Redis and subscribed to channel: %s", name)
        else:
            log.error("Could not connect to Redis server. HOST=%s, PORT=%s", host, port)
            sleep(5)
            exit(1)

    def check_connection(self):
        try:
            self.r.ping()
            return True
        except (redis.ConnectionError, redis.TimeoutError):
            log.exception("Redis connection failed")
            return False

    def grab_command_msg(self):
        return self.pubsub.get_message(timeout=None) if self.check_connection() else None

    def sendmsg(self, response):
        if self.check_connection():
            self.r.publish("REPLY", response)

if __name__ == "__main__":
    main()
