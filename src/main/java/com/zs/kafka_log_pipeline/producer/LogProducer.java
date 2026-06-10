package com.zs.kafka_log_pipeline.producer;

import com.zs.kafka_log_pipeline.event.LogEvent;
import com.zs.kafka_log_pipeline.event.LogLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Random;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogProducer {

    private static final String TOPIC = "raw-logs";

    private final KafkaTemplate<String, LogEvent> kafkaTemplate;
    private final Random random = new Random();

    private static final List<String> SERVICES = List.of(
            "order-service", "payment-service", "stock-service", "notification-service"
    );

    private static final List<String> INFO_MESSAGES = List.of(
            "Request processed successfully",
            "User authenticated",
            "Cache hit for key",
            "Database connection established",
            "Scheduled task completed"
    );

    private static final List<String> WARN_MESSAGES = List.of(
            "Response time above threshold",
            "Retrying failed request",
            "Cache miss, falling back to database",
            "High memory usage detected",
            "Slow query detected"
    );

    private static final List<String> ERROR_MESSAGES = List.of(
            "Failed to process payment",
            "Database connection timeout",
            "NullPointerException in order processing",
            "Service unavailable",
            "Failed to publish event to Kafka"
    );

    @Scheduled(fixedDelay = 500)
    public void produceLog() {
        LogLevel level = randomLevel();
        String service = SERVICES.get(random.nextInt(SERVICES.size()));
        String message = randomMessage(level);

        LogEvent event = LogEvent.of(level, service, message);

        kafkaTemplate.send(TOPIC, event.id(), event);
        log.info("[{}] [{}] {}", level, service, message);
    }

    private LogLevel randomLevel() {
        // 60% INFO, 25% WARN, 15% ERROR
        int roll = random.nextInt(100);
        if (roll < 60) return LogLevel.INFO;
        if (roll < 85) return LogLevel.WARN;
        return LogLevel.ERROR;
    }

    private String randomMessage(LogLevel level) {
        return switch (level) {
            case INFO -> INFO_MESSAGES.get(random.nextInt(INFO_MESSAGES.size()));
            case WARN -> WARN_MESSAGES.get(random.nextInt(WARN_MESSAGES.size()));
            case ERROR -> ERROR_MESSAGES.get(random.nextInt(ERROR_MESSAGES.size()));
        };
    }
}
