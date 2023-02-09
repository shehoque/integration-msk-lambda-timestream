package com.amazonaws.kafka.samples;

import com.amazonaws.services.lambda.runtime.events.KafkaEvent;
import com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import samples.workspaceevent.avro.WorkspaceEvent;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import java.time.Duration;
import java.util.*;

class ProcessRecords {

    private static final Logger logger = LogManager.getLogger(ProcessRecords.class);

    private byte[] base64Decode(KafkaEvent.KafkaEventRecord kafkaEventRecord) {
        return Base64.getDecoder().decode(kafkaEventRecord.getValue().getBytes());
    }

    private long getKafkaEventRecordsSize(KafkaEvent kafkaEvent) {
        long size = 0L;
        for (Map.Entry<String, List<KafkaEvent.KafkaEventRecord>> kafkaEventEntry : kafkaEvent.getRecords().entrySet()) {
            size += kafkaEventEntry.getValue().size();
        }
        return size;
    }

    private Map<String, Object> getGSRConfigs() {
        Map<String, Object> gsrConfigs = new HashMap<>();
        gsrConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        gsrConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AWSKafkaAvroDeserializer.class.getName());
        gsrConfigs.put(AWSSchemaRegistryConstants.AWS_REGION, System.getenv("AWS_REGION"));
        gsrConfigs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.SPECIFIC_RECORD.getName());
        if (System.getenv("SECONDARY_DESERIALIZER") != null) {
            if (Boolean.parseBoolean(System.getenv("SECONDARY_DESERIALIZER"))) {
                gsrConfigs.put(AWSSchemaRegistryConstants.SECONDARY_DESERIALIZER, KafkaAvroDeserializer.class.getName());
                gsrConfigs.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, System.getenv("SCHEMA_REGISTRY_URL"));
                gsrConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
            }
        }
        return gsrConfigs;
    }

    private Map<String, Object> getCSRConfigs() {
        Map<String, Object> csrConfigs = new HashMap<>();
        csrConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        csrConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        csrConfigs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");
        return csrConfigs;
    }

    private void deserializeWriteToTimestream(Deserializer deserializer, KafkaEvent kafkaEvent, String requestId, TimestreamWriteClient timestreamWriteClient) {
        String databaseName = System.getenv("DatabaseName") != null?System.getenv("DatabaseName"):"anaplan-poc-db";
        String tableName = System.getenv("TableName") != null?System.getenv("TableName"):"workspace-tbl";
        TimestreamSimpleIngest timestreamSimpleIngest = new TimestreamSimpleIngest(timestreamWriteClient, databaseName, tableName);
        kafkaEvent.getRecords().forEach((key, value) -> value.forEach(v -> {

            WorkspaceEvent workspaceEvent = null;
            boolean csr = false;

            if (System.getenv("CSR") != null) {
                if (Boolean.parseBoolean(System.getenv("CSR"))) {
                    csr = true;
                    try {
                        GenericRecord rec = (GenericRecord) deserializer.deserialize(v.getTopic(), base64Decode(v));
                        workspaceEvent = (WorkspaceEvent) SpecificData.get().deepCopy(WorkspaceEvent.SCHEMA$, rec);

                    } catch (Exception e) {
                        logger.error(Util.stackTrace(e));
                    }
                }
            }

            if (!csr) {
                workspaceEvent = (WorkspaceEvent) deserializer.deserialize(v.getTopic(), base64Decode(v));
            }

            if (workspaceEvent != null){
                logger.info("workspace event {}",workspaceEvent);
                timestreamSimpleIngest.writeRecord(workspaceEvent);
            }
        }));

    }

    void processRecords(KafkaEvent kafkaEvent, String requestId) {
        logger.info("Processing batch with {} records for Request ID {} \n", getKafkaEventRecordsSize(kafkaEvent), requestId);
        Deserializer deserializer = null;
        if (System.getenv("CSR") != null) {
            if (Boolean.parseBoolean(System.getenv("CSR"))) {
                SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(System.getenv("SCHEMA_REGISTRY_URL"), 10, getCSRConfigs());
                deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
            }
        }
        if (deserializer == null) {
            deserializer = new AWSKafkaAvroDeserializer(getGSRConfigs());
        }
        TimestreamWriteClient writeClient = buildWriteClient();
        deserializeWriteToTimestream(deserializer, kafkaEvent, requestId, writeClient);
    }

    /**
     * Recommended Timestream write client SDK configuration:
     *  - Set SDK retry count to 10.
     *  - Use SDK DEFAULT_BACKOFF_STRATEGY
     *  - Set RequestTimeout to 20 seconds .
     *  - Set max connections to 5000 or higher.
     */
    public static TimestreamWriteClient buildWriteClient() {
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.US_EAST_2)
                .build();
    }
}
