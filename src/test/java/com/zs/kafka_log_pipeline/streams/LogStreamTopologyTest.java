package com.zs.kafka_log_pipeline.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zs.kafka_log_pipeline.event.LogEvent;
import com.zs.kafka_log_pipeline.event.LogLevel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class LogStreamTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, LogEvent> inputTopic;
    private TestOutputTopic<String, LogEvent> infoOutputTopic;
    private TestOutputTopic<String, LogEvent> warnOutputTopic;
    private TestOutputTopic<String, LogEvent> errorOutputTopic;
    private Serde<LogEvent> logEventSerde;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        Serializer<LogEvent> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        Deserializer<LogEvent> deserializer = (topic, data) -> {
            try { return mapper.readValue(data, LogEvent.class); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        logEventSerde = Serdes.serdeFrom(serializer, deserializer);

        // Monta a mesma topology da aplicação
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, LogEvent> rawStream = builder.stream(
                "raw-logs", Consumed.with(Serdes.String(), logEventSerde)
        );

        rawStream
                .filter((key, event) -> event != null && event.level() != null)
                .split()
                .branch(
                        (key, event) -> event.level() == LogLevel.INFO,
                        Branched.withConsumer(s -> s.to("info-logs", Produced.with(Serdes.String(), logEventSerde)))
                )
                .branch(
                        (key, event) -> event.level() == LogLevel.WARN,
                        Branched.withConsumer(s -> s.to("warn-logs", Produced.with(Serdes.String(), logEventSerde)))
                )
                .branch(
                        (key, event) -> event.level() == LogLevel.ERROR,
                        Branched.withConsumer(s -> s.to("error-logs", Produced.with(Serdes.String(), logEventSerde)))
                );

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-log-pipeline");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic("raw-logs",
                Serdes.String().serializer(), logEventSerde.serializer());
        infoOutputTopic = testDriver.createOutputTopic("info-logs",
                Serdes.String().deserializer(), logEventSerde.deserializer());
        warnOutputTopic = testDriver.createOutputTopic("warn-logs",
                Serdes.String().deserializer(), logEventSerde.deserializer());
        errorOutputTopic = testDriver.createOutputTopic("error-logs",
                Serdes.String().deserializer(), logEventSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldRouteInfoLogToInfoTopic() {
        LogEvent event = LogEvent.of(LogLevel.INFO, "order-service", "Request processed");
        inputTopic.pipeInput(event.id(), event);

        assertThat(infoOutputTopic.isEmpty()).isFalse();
        assertThat(warnOutputTopic.isEmpty()).isTrue();
        assertThat(errorOutputTopic.isEmpty()).isTrue();

        LogEvent result = infoOutputTopic.readValue();
        assertThat(result.level()).isEqualTo(LogLevel.INFO);
        assertThat(result.service()).isEqualTo("order-service");
    }

    @Test
    void shouldRouteWarnLogToWarnTopic() {
        LogEvent event = LogEvent.of(LogLevel.WARN, "payment-service", "Slow query detected");
        inputTopic.pipeInput(event.id(), event);

        assertThat(warnOutputTopic.isEmpty()).isFalse();
        assertThat(infoOutputTopic.isEmpty()).isTrue();
        assertThat(errorOutputTopic.isEmpty()).isTrue();

        LogEvent result = warnOutputTopic.readValue();
        assertThat(result.level()).isEqualTo(LogLevel.WARN);
    }

    @Test
    void shouldRouteErrorLogToErrorTopic() {
        LogEvent event = LogEvent.of(LogLevel.ERROR, "stock-service", "Database timeout");
        inputTopic.pipeInput(event.id(), event);

        assertThat(errorOutputTopic.isEmpty()).isFalse();
        assertThat(infoOutputTopic.isEmpty()).isTrue();
        assertThat(warnOutputTopic.isEmpty()).isTrue();

        LogEvent result = errorOutputTopic.readValue();
        assertThat(result.level()).isEqualTo(LogLevel.ERROR);
    }

    @Test
    void shouldRouteMultipleEventsToCorrectTopics() {
        inputTopic.pipeInput(null, LogEvent.of(LogLevel.INFO, "svc-a", "ok"));
        inputTopic.pipeInput(null, LogEvent.of(LogLevel.ERROR, "svc-b", "fail"));
        inputTopic.pipeInput(null, LogEvent.of(LogLevel.WARN, "svc-c", "slow"));
        inputTopic.pipeInput(null, LogEvent.of(LogLevel.INFO, "svc-d", "ok"));

        assertThat(infoOutputTopic.readValuesToList()).hasSize(2);
        assertThat(errorOutputTopic.readValuesToList()).hasSize(1);
        assertThat(warnOutputTopic.readValuesToList()).hasSize(1);
    }

    @Test
    void shouldFilterOutNullEvents() {
        inputTopic.pipeInput("key-null", null);

        assertThat(infoOutputTopic.isEmpty()).isTrue();
        assertThat(warnOutputTopic.isEmpty()).isTrue();
        assertThat(errorOutputTopic.isEmpty()).isTrue();
    }
}
