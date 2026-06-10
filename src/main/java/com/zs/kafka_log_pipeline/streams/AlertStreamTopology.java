package com.zs.kafka_log_pipeline.streams;

import com.zs.kafka_log_pipeline.event.AlertEvent;
import com.zs.kafka_log_pipeline.event.LogEvent;
import com.zs.kafka_log_pipeline.event.LogLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertStreamTopology {

    private static final String ERROR_LOGS_TOPIC = "error-logs";
    private static final String ALERTS_TOPIC     = "alerts";

    @Value("${app.kafka.alert.error-threshold:10}")
    private int errorThreshold;

    @Value("${app.kafka.alert.window-size-seconds:60}")
    private int windowSizeSeconds;

    private final Serde<LogEvent> logEventSerde;
    private final Serde<AlertEvent> alertEventSerde;

    @Autowired
    public void buildAlertTopology(StreamsBuilder builder) {

        builder.stream(ERROR_LOGS_TOPIC, Consumed.with(Serdes.String(), logEventSerde))

                // Agrupa por nome do serviço
                .groupBy(
                        (key, event) -> event.service(),
                        Grouped.with(Serdes.String(), logEventSerde)
                )

                // Janela tumbling de 60 segundos
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(windowSizeSeconds)))

                // Conta os erros por serviço dentro da janela
                .count(Materialized.as("error-counts-store"))

                // Converte para stream para poder filtrar e publicar
                .toStream()

                // Só dispara alerta se passou do threshold
                .filter((windowedKey, count) -> count >= errorThreshold)

                // Transforma em AlertEvent
                .mapValues((windowedKey, count) -> {
                    String service = windowedKey.key();
                    log.warn("ALERT: service '{}' exceeded {} errors in {}s window (count: {})",
                            service, errorThreshold, windowSizeSeconds, count);
                    return AlertEvent.of(service, count, errorThreshold, windowSizeSeconds);
                })

                // Republica com a chave sem a janela
                .selectKey((windowedKey, alert) -> windowedKey.key())

                // Publica no topic de alertas
                .to(ALERTS_TOPIC, Produced.with(Serdes.String(), alertEventSerde));

        log.info("Alert stream topology built: error-logs → [windowing {}s] → alerts", windowSizeSeconds);
    }
}
