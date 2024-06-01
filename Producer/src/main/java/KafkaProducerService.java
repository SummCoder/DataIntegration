import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * @author SummCoder
 * @desc Kafka进行消息生产
 * @date 2024/5/30 17:29
 */
// kafka生产者代码
public class KafkaProducerService {

    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka 集群，broker-list
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "0");
        //重试次数
        props.put("retries", 1);
        //批次大小
        props.put("batch.size", 16384);
        //等待时间
        props.put("linger.ms", 1);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader br = new BufferedReader(new FileReader("/data/flush/normal.txt"))) {
            String line;
            while ((line = br.readLine()) != null) {
                ProducerRecord<String, String> record = new ProducerRecord<>("kafka", line);
                producer.send(record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
