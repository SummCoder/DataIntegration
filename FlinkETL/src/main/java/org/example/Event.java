package org.example;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

/**
 * @author SummCoder
 * @desc 事件对象类
 * @date 2024/5/30 21:24
 */

@Data
public class Event {
    public String eventType;
    public JsonNode eventBody;

    public Event(String eventType, JsonNode eventBody) {
        this.eventType = eventType;
        this.eventBody = eventBody;
    }
}
