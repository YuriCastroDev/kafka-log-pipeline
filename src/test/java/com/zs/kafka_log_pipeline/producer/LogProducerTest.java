package com.zs.kafka_log_pipeline.producer;

import com.zs.kafka_log_pipeline.event.LogEvent;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class LogProducerTest {

    @Mock
    private KafkaTemplate<String, LogEvent> kafkaTemplate;

    @InjectMocks
    private LogProducer logProducer;

    @Test
    void shouldPublishToRawLogsTopic() {
        logProducer.produceLog();

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<LogEvent> eventCaptor = ArgumentCaptor.forClass(LogEvent.class);

        verify(kafkaTemplate).send(topicCaptor.capture(), keyCaptor.capture(), eventCaptor.capture());

        assertThat(topicCaptor.getValue()).isEqualTo("raw-logs");
        assertThat(keyCaptor.getValue()).isEqualTo(eventCaptor.getValue().id());
    }

    @Test
    void shouldPublishEventWithAllFieldsPopulated() {
        logProducer.produceLog();

        ArgumentCaptor<LogEvent> eventCaptor = ArgumentCaptor.forClass(LogEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        LogEvent event = eventCaptor.getValue();
        assertThat(event.id()).isNotNull();
        assertThat(event.level()).isNotNull();
        assertThat(event.service()).isNotNull();
        assertThat(event.message()).isNotNull();
        assertThat(event.timestamp()).isNotNull();
    }

    @RepeatedTest(20)
    void shouldPublishValidLogLevels() {
        logProducer.produceLog();

        ArgumentCaptor<LogEvent> eventCaptor = ArgumentCaptor.forClass(LogEvent.class);
        verify(kafkaTemplate).send(anyString(), anyString(), eventCaptor.capture());

        assertThat(eventCaptor.getValue().level()).isNotNull();
        reset(kafkaTemplate);
    }
}
