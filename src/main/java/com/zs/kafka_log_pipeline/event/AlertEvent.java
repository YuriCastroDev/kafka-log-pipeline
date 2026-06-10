package com.zs.kafka_log_pipeline.event;

import java.time.LocalDateTime;

public record AlertEvent(
        String service,
        long errorCount,
        int threshold,
        String windowSizeSeconds,
        LocalDateTime triggeredAt
) {
    public static AlertEvent of(String service, long errorCount, int threshold, int windowSizeSeconds) {
        return new AlertEvent(
                service,
                errorCount,
                threshold,
                windowSizeSeconds + "s",
                LocalDateTime.now()
        );
    }
}
