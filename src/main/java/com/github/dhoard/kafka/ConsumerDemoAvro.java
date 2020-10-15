package com.github.dhoard.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAvro {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAvro.class);

    private static final String BOOTSTRAP_SERVERS = "cp-6-0-x.address.cx:9092";

    private static final String SCHEMA_REGISTRY_URL = "http://cp-6-0-x.address.cx:8081";

    private static final String TOPIC = "test";

    public static void main(String[] args) throws Exception {
        new ConsumerDemoAvro().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaConsumer<String, GenericRecord> kafkaConsumer = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(
                ConsumerConfig.CLIENT_ID_CONFIG, getClass().getName());

            logger.info(ConsumerConfig.CLIENT_ID_CONFIG + " = [" + getClass().getName() + "]");

            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                KafkaAvroDeserializer.class.getName());

            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                getClass().getName());

            properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

            /*
            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());
             */

            // Build the sasl.jaas.config String
            /*
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"");
            stringBuilder.append("test"); // If username has double quotes, they will need to be escaped
            stringBuilder.append("\" password=\"");
            stringBuilder.append("test123"); // If password has double quotes, they will need to be escaped
            stringBuilder.append("\";");

            // Set the security properties
            properties.setProperty(
                "sasl.jaas.config", stringBuilder.toString());

            properties.setProperty(
                "security.protocol", "SASL_PLAINTEXT"); // Or correct protocol

            properties.setProperty("sasl.mechanism", "PLAIN");
             */

            properties.put("schema.registry.url", SCHEMA_REGISTRY_URL);

            kafkaConsumer = new KafkaConsumer<String, GenericRecord>(properties);
            kafkaConsumer.subscribe(Collections.singleton(TOPIC));

            while (true) {
                ConsumerRecords<String, GenericRecord> consumerRecords =
                    kafkaConsumer.poll(Duration.ofMillis(250));

                if (consumerRecords.count() > 0) {
                    logger.info("message count = [" + consumerRecords.count() + "]");

                    for (ConsumerRecord<String, GenericRecord> consumerRecord : consumerRecords) {
                        String key = consumerRecord.key();
                        GenericRecord genericRecord = consumerRecord.value();

                        logger.info("Received message, key = [" + key + "] value = [" + genericRecord.get("currentValue") + "]");
                    }

                    kafkaConsumer.commitAsync();
                }
            }
        } finally {
            if (null != kafkaConsumer) {
                try {
                    kafkaConsumer.close();
                } catch (Throwable t) {
                    // DO NOTHING
                }
            }
        }
    }
}
