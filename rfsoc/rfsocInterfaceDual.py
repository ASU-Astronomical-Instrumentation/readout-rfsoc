import logging

log = logging.getLogger(__name__)

import getpass
import sys

if getpass.getuser() != "root":
    log.error("rfsocInterface.py: root priviliges are required, please run as root.")
    sys.exit()

import pynq
from pynq import Overlay
from pynq import MMIO
import xrfclk
from time import sleep
import numpy as np


firmware = pynq.Overlay


def uploadOverlay(overlayPath: str):
    """Upload an overlay to the RFSoC

    :param overlayPath: Path to the overlay to upload
    :type overlayPath: str
    """
    global firmware
    if overlayPath is None:
        firmware = Overlay("bram_lutwave_wrapper_2406031500.bit", ignore_version=True)
    else:
        firmware = Overlay(overlayPath, ignore_version=True)
    xrfclk.set_all_ref_clks(409.6)


def configure_registers(dataA_srcip: int, dataB_srcip: int, dataA_dstip: int, dataB_dstip: int, dstmac_a_msb: int,
                        dstmac_a_lsb: int,dstmac_b_msb: int,dstmac_b_lsb: int, portA: int, portB: int):
    # SET ETHERNET IPS and MACS
    def ethRegsPortWrite(
        eth_regs,
        src_ip_int32=int("c0a80335", 16),
        dst_ip_int32=int("c0a80328", 16),
        src_mac0_int32=int("eec0ffee", 16),
        src_mac1_int16=int("c0ff", 16),
        dst_mac0_int16=int("00F2", 16),
        dst_mac1_int32=int("3CECEFBB", 16),
        port=4096
    ):  # f
        eth_regs.write(0x00, src_mac0_int32)
        eth_regs.write(0x04, (dst_mac0_int16 << 16) + src_mac1_int16)
        eth_regs.write(0x08, dst_mac1_int32)
        eth_regs.write(0x0C, src_ip_int32)
        eth_regs.write(0x10, dst_ip_int32)
        eth_regs.write(0x14, (port<<16) | port)

    ethRegsPortWrite(
        firmware.ethWrapPort0.eth_regs_0,
        src_ip_int32=dataA_srcip,
        dst_ip_int32=dataA_dstip,
        dst_mac1_int32=dstmac_a_msb,
        dst_mac0_int16=dstmac_a_lsb,
        port=portA
    )  # OPSERO PORT 3, CHAN 1
    ethRegsPortWrite(
        firmware.ethWrapPort1.eth_regs_0,
        src_ip_int32=dataB_srcip,
        dst_ip_int32=dataB_dstip,
        dst_mac1_int32=dstmac_b_msb,
        dst_mac0_int16=dstmac_b_lsb,
        port=portB
    )  # OPSERO PORT 2, CHAN 2


def norm_wave(wave, max_amp=2**15 - 1) -> np.ndarray:
    norm = np.max(np.abs(wave))
    if norm == 0:
        return wave.real, wave.imag
    wave_real = ((wave.real / norm) * max_amp).astype("int16")
    wave_imag = ((wave.imag / norm) * max_amp).astype("int16")
    return wave_real, wave_imag


def generate_wave_ddr4(freq_list, amp_list):
    fs = 512e6
    lut_len = 2**20
    fft_len = 1024
    k = np.int64(np.round(freq_list / (fs / lut_len)))
    freq_actual = k * (fs / lut_len)
    X = np.zeros(lut_len, dtype="complex")
    phi = np.random.uniform(-np.pi, np.pi, np.size(freq_list))
    X[k] = np.exp(-1j * phi) * amp_list
    x = np.fft.ifft(X) * lut_len / np.sqrt(2)
    bin_num = np.int64(np.round(freq_actual / (fs / fft_len)))
    f_beat = (bin_num) * fs / fft_len - (freq_actual)
    dphi0 = f_beat / (fs / fft_len) * 2**16
    if np.size(dphi0) > 1:
        dphi = np.concatenate((dphi0, np.zeros(fft_len - np.size(dphi0))))
    else:
        z = np.zeros(fft_len)
        z[0] = dphi0
        dphi = z
    return x, dphi, freq_actual


def load_bin_list(chan, freq_list):
    fs = 512e6
    fft_len = 1024
    lut_len = 2**20
    k = np.int64(np.round(-freq_list / (fs / lut_len)))
    freq_actual = k * (fs / lut_len)
    bin_list = np.int64(np.round(freq_actual / (fs / fft_len)))
    pos_bin_idx = np.where(bin_list > 0)
    if np.size(pos_bin_idx) > 0:
        bin_list[pos_bin_idx] = 1024 - bin_list[pos_bin_idx]
    bin_list = np.abs(bin_list)
    # DSP REGS
    if chan == 1:
        dsp_regs = firmware.chan1.dsp_regs_0
    elif chan == 2:
        dsp_regs = firmware.chan2.dsp_regs_0
    else:
        return "Does not compute"
    for addr in range(1024):
        if addr < (np.size(bin_list)):
            # print("addr = {}, bin# = {}".format(addr, bin_list[addr]))
            dsp_regs.write(0x04, int(bin_list[addr]))
            dsp_regs.write(0x00, ((addr << 1) + 1) << 12)
            dsp_regs.write(0x00, 0)
        else:
            dsp_regs.write(0x04, 0)
            dsp_regs.write(0x00, ((addr << 1) + 1) << 12)
            dsp_regs.write(0x00, 0)


def reset_accum_and_sync(chan, freqs):
    if chan == 1:
        dsp_regs = firmware.chan1.dsp_regs_0
        dsp_regs.write(0x0C, 181)
    elif chan == 2:
        dsp_regs = firmware.chan2.dsp_regs_0
        dsp_regs.write(0x0C, 181)
    else:
        return "Does not compute"

    sync_in = 2**26
    accum_rst = 2**24  # (active rising edge)
    accum_length = (2**19) - 1  # (2**19)-1 # (2**18)-1

    fft_shift = 0
    if len(freqs) < 400:
        fft_shift = 511  # 2**9-1
    else:
        fft_shift = (2**9) - 1
    dsp_regs.write(0x00, fft_shift)  # set fft shift
    dsp_regs.write(0x08, accum_length | sync_in)
    sleep(0.5)
    dsp_regs.write(0x08, accum_length | accum_rst | sync_in)


def load_ddr4(chan, wave_real, wave_imag, dphi):
    if chan == 1:
        base_addr_dphis = 0xA004C000
    elif chan == 2:
        base_addr_dphis = 0xA0040000
    else:
        return "Does not compute"

    # write dphi to bram
    dphi_16b = dphi.astype("uint16")
    dphi_stacked = ((np.uint32(dphi_16b[1::2]) << 16) + dphi_16b[0::2]).astype("uint32")
    mem_size = 512 * 4  # 32 bit address slots
    mmio_bram_phis = MMIO(base_addr_dphis, mem_size)
    mmio_bram_phis.array[0:512] = dphi_stacked[
        0:512
    ]  # the [0:512] indexing is necessary on .array

    # slice waveform for uploading to ddr4
    Q0, Q1, Q2, Q3 = (
        wave_real[0::4],
        wave_real[1::4],
        wave_real[2::4],
        wave_real[3::4],
    )
    I0, I1, I2, I3 = (
        wave_imag[0::4],
        wave_imag[1::4],
        wave_imag[2::4],
        wave_imag[3::4],
    )
    data0 = ((np.int32(I1) << 16) + I0).astype("int32")
    data1 = ((np.int32(Q1) << 16) + Q0).astype("int32")
    data2 = ((np.int32(I3) << 16) + I2).astype("int32")
    data3 = ((np.int32(Q3) << 16) + Q2).astype("int32")
    # write waveform to DDR4 memory
    ddr4mux = firmware.axi_ddr4_mux
    ddr4mux.write(8, 0)  # set read valid
    ddr4mux.write(0, 0)  # mux switch

    base_addr_ddr4 = 0x4_0000_0000  # 0x5_0000_0000
    depth_ddr4 = 2**32
    mmio_ddr4 = MMIO(base_addr_ddr4, depth_ddr4)
    mmio_ddr4.array[0:4194304][0 + (chan - 1) * 4 :: 16] = data0
    mmio_ddr4.array[0:4194304][1 + (chan - 1) * 4 :: 16] = data1
    mmio_ddr4.array[0:4194304][2 + (chan - 1) * 4 :: 16] = data2
    mmio_ddr4.array[0:4194304][3 + (chan - 1) * 4 :: 16] = data3
    ddr4mux.write(8, 1)  # set read valid
    ddr4mux.write(0, 1)  # mux switch
