package com.zs.kafka_log_pipeline.consumer;

import com.zs.kafka_log_pipeline.event.AlertEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AlertConsumer {

    @KafkaListener(topics = "alerts", groupId = "alert-group")
    public void consume(AlertEvent alert) {
        log.error("ALERT RECEIVED — service: '{}' had {} errors in a {}s window (threshold: {}). Triggered at: {}",
                alert.service(),
                alert.errorCount(),
                alert.windowSizeSeconds(),
                alert.threshold(),
                alert.triggeredAt()
        );

        // Ex: slackNotifier.send(alert);
        //     pagerDutyClient.triggerIncident(alert);
    }
}
