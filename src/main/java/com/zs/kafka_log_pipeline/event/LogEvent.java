package com.zs.kafka_log_pipeline.event;

import java.time.LocalDateTime;
import java.util.UUID;

public record LogEvent(
        String id,
        LogLevel level,
        String service,
        String message,
        LocalDateTime timestamp
) {
    public static LogEvent of(LogLevel level, String service, String message) {
        return new LogEvent(
                UUID.randomUUID().toString(),
                level,
                service,
                message,
                LocalDateTime.now()
        );
    }
}
