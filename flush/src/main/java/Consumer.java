import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.header.Header;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author SummCoder
 * @desc 消费远端kafka数据
 * @date 2024/5/24 22:39
 */

public class Consumer {

    private static BufferedWriter fileWriter = null;
    private static BufferedWriter additionalFileWriter = null;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "36.134.119.108:31092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        // GROUP_ID请使用学号，不同组必须使用不同的 GROUP_ID。
        // 原因参考：https://blog.csdn.net/daiyutage/article/details/70599433
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "211250026");
        // 原因参考：https://blog.csdn.net/matrix_google/article/details/88658234
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"student\" password=\"nju2024\";");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("transaction"));

        // 初始化writer
        initWriter();

        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down...");
            //TODO: 关闭 writer, 确保在程序结束时调用，避免丢失 writer 缓冲区数据
            closeWriters();
        }));

        // 会从最新数据开始消费
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                // 获取消息头
                Header groupNoHeader = record.headers().lastHeader("groupNo");
                if (groupNoHeader == null) {
                    // 获取消息数据
                    // TODO: 见下文示例 writeToFile method; filewriter; additionalFileWriter
                    writeToFile(fileWriter, record.value());
                } else {
                    byte[] groupNo = groupNoHeader.value();
                    // 此处yourGroupNo替换成你们组的组号, 1-18
                    if (Arrays.equals("15".getBytes(), groupNo)) {
                        // 额外记录这条数据
                        writeToFile(additionalFileWriter, record.value());
                    }
                }
            }
        }

    }

    private static void initWriter() {
        String filePath = "/data/flush/normal.txt";
        String additionalFilePath = "/data/flush/special.txt";
        try {
            File file = new File(filePath);
            File additionalFile = new File(additionalFilePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            if (!additionalFile.exists()) {
                additionalFile.createNewFile();
            }
            fileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true), StandardCharsets.UTF_8));
            additionalFileWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(additionalFile, true), StandardCharsets.UTF_8));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile(Writer writer, String data) {
        try {
            writer.write(data);
            writer.write(System.lineSeparator());
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void closeWriters() {
        try {
            if (fileWriter != null) {
                fileWriter.close();
            }
            if (additionalFileWriter != null) {
                additionalFileWriter.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
