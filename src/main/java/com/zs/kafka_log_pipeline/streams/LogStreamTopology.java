package com.zs.kafka_log_pipeline.streams;

import com.zs.kafka_log_pipeline.event.LogEvent;
import com.zs.kafka_log_pipeline.event.LogLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class LogStreamTopology {

    private static final String RAW_LOGS_TOPIC   = "raw-logs";
    private static final String INFO_LOGS_TOPIC  = "info-logs";
    private static final String WARN_LOGS_TOPIC  = "warn-logs";
    private static final String ERROR_LOGS_TOPIC = "error-logs";

    private final Serde<LogEvent> logEventSerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        KStream<String, LogEvent> rawStream = builder.stream(
                RAW_LOGS_TOPIC,
                Consumed.with(Serdes.String(), logEventSerde)
        );

        // Filtra eventos inválidos (sem level)
        KStream<String, LogEvent> validStream = rawStream
                .filter((key, event) -> event != null && event.level() != null);

        // Roteia para topics diferentes por severidade
        validStream
                .split()
                .branch(
                        (key, event) -> event.level() == LogLevel.INFO,
                        Branched.withConsumer(stream ->
                                stream.to(INFO_LOGS_TOPIC, Produced.with(Serdes.String(), logEventSerde)))
                )
                .branch(
                        (key, event) -> event.level() == LogLevel.WARN,
                        Branched.withConsumer(stream ->
                                stream.to(WARN_LOGS_TOPIC, Produced.with(Serdes.String(), logEventSerde)))
                )
                .branch(
                        (key, event) -> event.level() == LogLevel.ERROR,
                        Branched.withConsumer(stream ->
                                stream.to(ERROR_LOGS_TOPIC, Produced.with(Serdes.String(), logEventSerde)))
                );

        log.info("Log stream topology built: raw-logs → [info-logs | warn-logs | error-logs]");
    }
}
