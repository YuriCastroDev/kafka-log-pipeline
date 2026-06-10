package com.zs.kafka_log_pipeline.consumer;

import com.zs.kafka_log_pipeline.event.AlertEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"alerts", "raw-logs", "error-logs", "info-logs", "warn-logs"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class AlertConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, AlertEvent> kafkaTemplate;

    @SpyBean
    private AlertConsumer alertConsumer;

    @Test
    void shouldConsumeAlertEvent() {
        AlertEvent alert = AlertEvent.of("payment-service", 5, 3, 60);

        kafkaTemplate.send("alerts", "payment-service", alert);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                verify(alertConsumer, atLeastOnce()).consume(any(AlertEvent.class))
        );
    }
}
