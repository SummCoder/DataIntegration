package org.example;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

/**
 * @author SummCoder
 * @desc Flink进行对于Kafka数据的实时消费并写入ClickHouse
 */
public class FlinkKafkaToClickhouse {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setParallelism(1); // 将并行度设置为 1 来控制读取速度

//        env.enableCheckpointing(60000); // 将 checkpoint 间隔设置为 60 秒

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "211250001");

        // 创建 Kafka 消费者
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "kafka", // Kafka topic
                new SimpleStringSchema(), // 反序列化 schema
                properties); // Kafka 配置

        // 从最早记录开始消费
        consumer.setStartFromEarliest();


        // 添加 Kafka 数据源
        DataStream<String> stream = env.addSource(consumer);

        stream.map(jsonStr -> {
                    if (jsonStr == null || jsonStr.isEmpty()) {
                        // 处理无效输入，可以选择抛出自定义异常，或者返回一个默认值
                        return null;
                    }

                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        JsonNode jsonNode = mapper.readTree(jsonStr);

                        JsonNode eventTypeNode = jsonNode.get("eventType");
                        if (eventTypeNode == null || eventTypeNode.isNull()) {
                            // 处理缺失 eventType 的情况
                            return null;
                        }
                        String eventType = eventTypeNode.asText();

                        JsonNode eventBodyNode = jsonNode.get("eventBody");
                        if (eventBodyNode == null || eventBodyNode.isNull()) {
                            // 处理缺失 eventBody 的情况
                            return null;
                        }

                        return new Event(eventType, eventBodyNode);
                    } catch (IOException e) {
                        e.printStackTrace();
                        return null;
                    }
                }).filter(Objects::nonNull) // 过滤掉解析失败的记录，因为我们已经处理了null情况
                .addSink(new ClickHouseSink());

        try {
            env.execute("Kafka to ClickHouse");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}