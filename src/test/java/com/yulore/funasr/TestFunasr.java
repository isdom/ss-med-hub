package com.yulore.funasr;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClientBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import javax.sound.sampled.AudioSystem;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class TestFunasr {
    public static void main(String[] args) throws Exception {
        final Properties props = new Properties();

        try (var configStream = TestFunasr.class.getClassLoader().getResourceAsStream("test.properties")) {
            props.load(configStream);
        } catch (Exception e) {
            throw new RuntimeException("加载配置文件失败，请确保 test.properties 存在且路径正确", e);
        }

        String ossEndpoint = props.getProperty("oss.endpoint");
        String ossAccessKeyId = props.getProperty("oss.accessKeyId");
        String ossAccessKeySecret = props.getProperty("oss.accessKeySecret");
        String bucketName = props.getProperty("oss.bucketName");
        String objectName = props.getProperty("oss.objectName");
        String url = props.getProperty("funasr.url");

        final CountDownLatch cdl = new CountDownLatch(1);
        final AtomicReference<FunasrClient.AsrOp> ref = new AtomicReference<>();

        final EventLoopGroup nettyGroup = new NioEventLoopGroup(1);
        final OSS oss = new  OSSClientBuilder().build(ossEndpoint, ossAccessKeyId, ossAccessKeySecret);
        try (final var is = oss.getObject(bucketName, objectName).getObjectContent();
             final ByteArrayOutputStream bos = new ByteArrayOutputStream();) {
            is.transferTo(bos);
            final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            final var ais = AudioSystem.getAudioInputStream(bis);
            final var af = ais.getFormat();
            final int samples = (int)ais.getFrameLength();
            final int sampleRate = (int)af.getSampleRate();

            final var funasr = new FunasrClient(nettyGroup,
                    null,
                    url,
                    vo -> {
                        vo.setAudio_fs(sampleRate);
                    },
                    asr -> {
                        ref.set(asr);
                        cdl.countDown();
                    },
                    text -> {
                        System.out.printf("rbt: %s\n", text);
                    },
                    last_text -> {
                        System.out.printf("last rbt: %s\n", last_text);
                    });

            System.out.println("start to connect funasr");
            cdl.await();
            System.out.println("connected funasr");
            FunasrClient.AsrOp op = ref.get();
            // send pcm data every 20ms
            int packetSize = (sampleRate / (1000 / 20) * (af.getSampleSizeInBits() / 8)) * af.getChannels();
            System.out.printf("packet size: %d\n", packetSize);
            final byte[] slice = new byte[packetSize];
            int read;
            int read_count = 0;
            while ((read = ais.read(slice)) >= 0) {
                read_count++;
                op.send(slice);
                // sleep for 20 ms
                Thread.sleep(20);
            }
            funasr.shutdown();
            System.out.printf("read %d times while samples is %d\n", read_count, samples);
        } finally {
            nettyGroup.shutdownGracefully();
            oss.shutdown();
        }
    }
}
