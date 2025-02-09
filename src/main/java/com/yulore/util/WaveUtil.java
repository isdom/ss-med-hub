package com.yulore.util;

import lombok.extern.slf4j.Slf4j;

import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

@Slf4j
public class WaveUtil {
    public static byte[] genWaveHeader(final int sampleRate, final int numChannels) {
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final DataOutputStream dos = new DataOutputStream(bos);
        /*
        typedef struct {
            char        chunk_id[4]; //内容为"RIFF"
            uint32_t    chunk_size;  //存储文件的字节数（不包含ChunkID和ChunkSize这8个字节）
            char        format[4];  //内容为"WAVE“
        } wave_header_t;

        typedef struct {
            char        subchunk1_id[4]; //内容为"fmt"
            uint32_t    subchunk1_size;  //存储该子块的字节数（不含前面的 subchunk1_id 和 subchunk1_size 这8个字节）
            uint16_t    audio_format;    //存储音频文件的编码格式，例如若为PCM则其存储值为1。
            uint16_t    num_channels;    //声道数，单声道(Mono)值为1，双声道(Stereo)值为2，等等
            uint32_t    sample_rate;     //采样率，如8k，44.1k等
            uint32_t    byte_rate;       //每秒存储的bit数，其值 = sample_rate * num_channels * bits_per_sample / 8
            uint16_t    block_align;     //块对齐大小，其值 = num_channels * bits_per_sample / 8
            uint16_t    bits_per_sample;  //每个采样点的bit数，一般为8,16,32等。
        } wave_fmt_t;

        typedef struct {
            char        subchunk2_id[4]; //内容为“data”
            uint32_t    subchunk2_size;  //接下来的正式的数据部分的字节数，其值 = num_samples * num_channels * bits_per_sample / 8
        } wave_data_t;

        vfs->vfs_append_func(&wave_hdr, sizeof(wave_hdr), wav_file);
        vfs->vfs_append_func(&wave_fmt, sizeof(wave_fmt), wav_file);
        vfs->vfs_append_func(&wave_data, sizeof(wave_data), wav_file);
        */

        try {
            // wave_hdr
            /*
            wave_header_t wave_hdr = {
                    {'R', 'I', 'F', 'F'},
                    2147483583,
                    {'W', 'A', 'V', 'E'},
            }; */
            dos.writeByte('R');
            dos.writeByte('I');
            dos.writeByte('F');
            dos.writeByte('F');
            writeInt32(dos, 2147483583);
            dos.writeByte('W');
            dos.writeByte('A');
            dos.writeByte('V');
            dos.writeByte('E');
            // wave_fmt
            /*
            wave_fmt_t wave_fmt = {
                    {'f', 'm', 't', ' '},
                    16,
                    1,
                    1,
                    16000,
                    32000,
                    2,
                    16
            };*/
            dos.writeByte('f');
            dos.writeByte('m');
            dos.writeByte('t');
            dos.writeByte(' ');
            writeInt32(dos, 16); // uint32_t    subchunk1_size;  //存储该子块的字节数（不含前面的 subchunk1_id 和 subchunk1_size 这8个字节）
            writeInt16(dos, 1); //uint16_t    audio_format;    //存储音频文件的编码格式，例如若为PCM则其存储值为1。
            writeInt16(dos, numChannels); //uint16_t    num_channels;    //声道数，单声道(Mono)值为1，双声道(Stereo)值为2，等等
            writeInt32(dos, sampleRate);//uint32_t    sample_rate;     //采样率，如8k，44.1k等
            writeInt32(dos, sampleRate * numChannels * 2);//uint32_t    byte_rate;       //每秒存储的bit数，其值 = sample_rate * num_channels * bits_per_sample / 8
            writeInt16(dos, 2); //uint16_t    block_align;     //块对齐大小，其值 = num_channels * bits_per_sample / 8
            writeInt16(dos, 16);//uint16_t    bits_per_sample;  //每个采样点的bit数，一般为8,16,32等。
            // wave_data
            /*
            wave_data_t wave_data = {
                    {'d', 'a', 't', 'a'},
                    2147483547
            };*/
            dos.writeByte('d');
            dos.writeByte('a');
            dos.writeByte('t');
            dos.writeByte('a');
            writeInt32(dos, 2147483547);// uint32_t    subchunk2_size;  //接下来的正式的数据部分的字节数，其值 = num_samples * num_channels * bits_per_sample / 8
            dos.flush();
            final byte[] bytes = bos.toByteArray();
            log.info("WaveUtil.genWaveHeader: gen wave header: {} bytes", bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeInt32(DataOutputStream dos, int data) throws IOException {
        int i0 = data & 0x000000ff;
        int i1 = (data & 0x0000ff00) >> 8;
        int i2 = (data & 0x00ff0000) >> 16;
        int i3 = (data & 0xff000000) >> 24;
        dos.writeByte(i0);
        dos.writeByte(i1);
        dos.writeByte(i2);
        dos.writeByte(i3);
    }

    private static void writeInt16(DataOutputStream dos, int data) throws IOException {
        int i0 = data & 0x000000ff;
        int i1 = (data & 0x0000ff00) >> 8;
        dos.writeByte(i0);
        dos.writeByte(i1);
    }

    // 转换采样率
    public static byte[] resamplePCM(final byte[] pcm, final int sourceSampleRate, final int targetSampleRate) {
        final AudioFormat sf = new AudioFormat(sourceSampleRate, 16, 1, true, false);
        final AudioFormat tf = new AudioFormat(targetSampleRate, 16, 1, true, false);
        try(// 创建原始音频格式
            final AudioInputStream ss = new AudioInputStream(new ByteArrayInputStream(pcm), sf, pcm.length);
            // 创建目标音频格式
            final AudioInputStream ts = AudioSystem.getAudioInputStream(tf, ss);
            // 读取转换后的数据
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
        ) {
            ts.transferTo(os);
            return os.toByteArray();
        } catch (Exception ex) {
            log.warn("resamplePCM: failed, detail: {}", ExceptionUtil.exception2detail(ex));
            return null;
        }
    }
}
