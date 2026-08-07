import ipaddress
import logging
import getpass
import sys
import numpy as np
from time import sleep

log = logging.getLogger(__name__)

if getpass.getuser() != "root":
    log.error("rfsocInterface.py: root privileges are required, please run as root.")
    sys.exit()

import pynq
from pynq import Overlay
from pynq import MMIO
import xrfclk

# Global state tracking - initialized to None to force explicit loading
firmware = None

# Configure Ethernet Reg Map
ethRegMap = {
    "srcip": 0x10,
    "dstip": 0x18,
    "dstmaclsb": 0x20,
    "dstmacmsb": 0x24,
    "ports": 0x2c,
    "timemsb": 0x34,  # Not Implemented
    "timelsb": 0x38   # Not Implemented
}

def _ensure_firmware_loaded():
    """Validate that the overlay is actively loaded on the PL. Raises if not."""
    global firmware
    if firmware is None or not firmware.is_loaded():
        raise RuntimeError(
            "PYNQ overlay is not loaded or has been evicted/mismatched. "
            "Send 'upload_bitstream' command first."
        )
    return firmware

def uploadOverlay(overlayPath: str):
    """Upload an overlay to the RFSoC with state validation and crash recovery."""
    global firmware
    try:
        bit_path = overlayPath if overlayPath else "bram_lutwave_wrapper_2406031500.bit"
        firmware = Overlay(bit_path, ignore_version=True)
        xrfclk.set_all_ref_clks(409.6)

        if not firmware.is_loaded():
            raise RuntimeError("Overlay instantiated but failed to load on PL.")

        log.info(f"Successfully loaded overlay: {bit_path}")
    except Exception as e:
        firmware = None  # Reset to safe state on any failure
        log.exception(f"Failed to upload overlay: {e}")
        raise RuntimeError(f"Overlay upload failed: {e}") from e

# --- Port A Setters ---
def setDataASrcIp(ip_int32):
    _ensure_firmware_loaded().ethWrapPort0.EthernetControl_0.write(ethRegMap['srcip'], ip_int32)

def setDataADstIp(ip_int32):
    _ensure_firmware_loaded().ethWrapPort0.EthernetControl_0.write(ethRegMap['dstip'], ip_int32)

def setDataAMacMsb(mac_msb_int16):
    _ensure_firmware_loaded().ethWrapPort0.EthernetControl_0.write(ethRegMap['dstmacmsb'], mac_msb_int16)

def setDataAMacLsb(mac_lsb_int32):
    _ensure_firmware_loaded().ethWrapPort0.EthernetControl_0.write(ethRegMap['dstmaclsb'], mac_lsb_int32)

def setDataAPort(port):
    val = (port << 16) | port
    _ensure_firmware_loaded().ethWrapPort0.EthernetControl_0.write(ethRegMap['ports'], val)

# --- Port B Setters ---
def setDataBSrcIp(ip_int32):
    _ensure_firmware_loaded().ethWrapPort1.EthernetControl_0.write(ethRegMap['srcip'], ip_int32)

def setDataBDstIp(ip_int32):
    _ensure_firmware_loaded().ethWrapPort1.EthernetControl_0.write(ethRegMap['dstip'], ip_int32)

def setDataBMacMsb(mac_msb_int16):
    _ensure_firmware_loaded().ethWrapPort1.EthernetControl_0.write(ethRegMap['dstmacmsb'], mac_msb_int16)

def setDataBMacLsb(mac_lsb_int32):
    _ensure_firmware_loaded().ethWrapPort1.EthernetControl_0.write(ethRegMap['dstmaclsb'], mac_lsb_int32)

def setDataBPort(port):
    val = (port << 16) | port
    _ensure_firmware_loaded().ethWrapPort1.EthernetControl_0.write(ethRegMap['ports'], val)

def configure_registers(dataA_srcip, dataB_srcip, dataA_dstip, dataB_dstip,
                        dstmac_a_msb, dstmac_a_lsb, dstmac_b_msb, dstmac_b_lsb,
                        portA, portB):
    fw = _ensure_firmware_loaded()
    def ethRegsPortWrite(eth_regs, src_ip_int32, dst_ip_int32, dst_mac1_int32, dst_mac0_int16, port):
        eth_regs.write(ethRegMap['srcip'], src_ip_int32)
        eth_regs.write(ethRegMap['dstip'], dst_ip_int32)
        eth_regs.write(ethRegMap['dstmacmsb'], dst_mac0_int16)
        eth_regs.write(ethRegMap['dstmaclsb'], dst_mac1_int32)
        eth_regs.write(ethRegMap['ports'], (port << 16) | port)

    ethRegsPortWrite(fw.ethWrapPort0.EthernetControl_0, dataA_srcip, dataA_dstip, dstmac_a_lsb, dstmac_a_msb, portA)
    ethRegsPortWrite(fw.ethWrapPort1.EthernetControl_0, dataB_srcip, dataB_dstip, dstmac_b_lsb, dstmac_b_msb, portB)

def norm_wave(wave, max_amp=2**15 - 1):
    wave = np.asarray(wave, dtype=complex)
    if wave.size == 0:
        raise ValueError("Input wave array cannot be empty.")
    norm = np.max(np.abs(wave))
    if norm == 0:
        return wave.real, wave.imag
    wave_real = ((wave.real / norm) * max_amp).astype("int16")
    wave_imag = ((wave.imag / norm) * max_amp).astype("int16")
    return wave_real, wave_imag

def generate_wave_ddr4(freq_list, amp_list):
    freq_arr = np.asarray(freq_list)
    amp_arr = np.asarray(amp_list)
    if freq_arr.size == 0 or amp_arr.size == 0:
        raise ValueError("freq_list and amp_list cannot be empty.")
    fs = 512e6
    lut_len = 2**20
    fft_len = 1024
    k = np.int64(np.round(freq_arr / (fs / lut_len)))
    freq_actual = k * (fs / lut_len)
    X = np.zeros(lut_len, dtype="complex")
    phi = np.random.uniform(-np.pi, np.pi, freq_arr.size)
    X[k] = np.exp(-1j * phi) * amp_arr
    x = np.fft.ifft(X) * lut_len / np.sqrt(2)
    bin_num = np.int64(np.round(freq_actual / (fs / fft_len)))
    f_beat = (bin_num) * fs / fft_len - freq_actual
    dphi0 = f_beat / (fs / fft_len) * 2**16
    if dphi0.size > 1:
        dphi = np.concatenate((dphi0, np.zeros(fft_len - dphi0.size)))
    else:
        dphi = np.zeros(fft_len)
        if dphi0.size == 1:
            dphi[0] = dphi0.item()
    return x, dphi, freq_actual

def load_bin_list(chan, freq_list):
    fw = _ensure_firmware_loaded()
    freq_arr = np.asarray(freq_list)
    if freq_arr.size == 0:
        log.warning("load_bin_list: freq_list is empty. All bins will be zeroed.")
        bin_list = np.zeros(1024, dtype=np.int64)
    else:
        fs = 512e6
        fft_len = 1024
        lut_len = 2**20
        k = np.int64(np.round(-freq_arr / (fs / lut_len)))
        freq_actual = k * (fs / lut_len)
        bin_list = np.int64(np.round(freq_actual / (fs / fft_len)))
        pos_bin_idx = np.where(bin_list > 0)
        if pos_bin_idx[0].size > 0:
            bin_list[pos_bin_idx] = 1024 - bin_list[pos_bin_idx]
        bin_list = np.abs(bin_list)

    dsp_regs = fw.chan1.dsp_regs_0 if chan == 1 else fw.chan2.dsp_regs_0 if chan == 2 else None
    if dsp_regs is None:
        raise ValueError(f"Invalid channel: {chan}")

    for addr in range(1024):
        val = int(bin_list[addr]) if addr < bin_list.size else 0
        dsp_regs.write(0x04, val)
        dsp_regs.write(0x00, ((addr << 1) + 1) << 12)
        dsp_regs.write(0x00, 0)

def reset_accum_and_sync(chan, freqs):
    fw = _ensure_firmware_loaded()
    dsp_regs = fw.chan1.dsp_regs_0 if chan == 1 else fw.chan2.dsp_regs_0 if chan == 2 else None
    if dsp_regs is None:
        raise ValueError(f"Invalid channel: {chan}")

    dsp_regs.write(0x0C, 182)
    sync_in = 2**26
    accum_rst = 2**24
    accum_length = (2**19) - 1
    fft_shift = 511
    dsp_regs.write(0x00, fft_shift)
    dsp_regs.write(0x08, accum_length | sync_in)
    sleep(0.5)
    dsp_regs.write(0x08, accum_length | accum_rst | sync_in)

def load_ddr4(chan, wave_real, wave_imag, dphi):
    fw = _ensure_firmware_loaded()
    dphi = np.asarray(dphi)
    wave_real = np.asarray(wave_real)
    wave_imag = np.asarray(wave_imag)
    if dphi.size == 0:
        raise ValueError("dphi array cannot be empty.")
    if wave_real.size == 0 or wave_imag.size == 0:
        raise ValueError("Waveform arrays cannot be empty.")

    base_addr_dphis = 0xA004C000 if chan == 1 else 0xA0040000 if chan == 2 else None
    if base_addr_dphis is None:
        raise ValueError(f"Invalid channel: {chan}")

    if dphi.size % 2 != 0:
        dphi = np.append(dphi, 0)
    dphi_16b = dphi.astype("uint16")
    dphi_stacked = ((np.uint32(dphi_16b[1::2]) << 16) + dphi_16b[0::2]).astype("uint32")

    mem_size = 512 * 4
    mmio_bram_phis = MMIO(base_addr_dphis, mem_size)
    slice_len = min(512, dphi_stacked.size)
    mmio_bram_phis.array[0:slice_len] = dphi_stacked[0:slice_len]

    Q0, Q1, Q2, Q3 = wave_real[0::4], wave_real[1::4], wave_real[2::4], wave_real[3::4]
    I0, I1, I2, I3 = wave_imag[0::4], wave_imag[1::4], wave_imag[2::4], wave_imag[3::4]
    data0 = ((np.int32(I1) << 16) + I0).astype("int32")
    data1 = ((np.int32(Q1) << 16) + Q0).astype("int32")
    data2 = ((np.int32(I3) << 16) + I2).astype("int32")
    data3 = ((np.int32(Q3) << 16) + Q2).astype("int32")

    ddr4mux = fw.axi_ddr4_mux
    ddr4mux.write(8, 0)
    ddr4mux.write(0, 0)
    base_addr_ddr4 = 0x4_0000_0000
    depth_ddr4 = 2**32
    mmio_ddr4 = MMIO(base_addr_ddr4, depth_ddr4)
    max_len = 4194304
    offset = (chan - 1) * 4

    mmio_ddr4.array[0:max_len][0 + offset :: 16] = data0
    mmio_ddr4.array[0:max_len][1 + offset :: 16] = data1
    mmio_ddr4.array[0:max_len][2 + offset :: 16] = data2
    mmio_ddr4.array[0:max_len][3 + offset :: 16] = data3

    ddr4mux.write(8, 1)
    ddr4mux.write(0, 1)
