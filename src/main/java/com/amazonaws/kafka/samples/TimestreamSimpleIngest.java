package com.amazonaws.kafka.samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;


public class TimestreamSimpleIngest {
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;

    TimestreamWriteClient timestreamWriteClient;
    private String databaseName;
    private String tableName;
    private static final Logger logger = LogManager.getLogger(TimestreamSimpleIngest.class);
    public TimestreamSimpleIngest(TimestreamWriteClient client, String databaseName, String tableName) {
        this.timestreamWriteClient = client;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public void writeRecords(List<ClickEvent> clickEvents) {
        Random random = new Random();
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        List<Dimension> dimensions = null;
        for(ClickEvent clickEvent : clickEvents){
            final long time = System.currentTimeMillis();
            dimensions = new ArrayList<>();
            Dimension eventType = Dimension.builder().name("event-type").value(clickEvent.getEventType().toString()).build();
            Dimension deviceType= Dimension.builder().name("device-type").value(clickEvent.getDevicetype().toString()).build();
            dimensions.add(deviceType);
            dimensions.add(eventType);
            Record viewCount = Record.builder()
                    .dimensions(dimensions)
                    .measureValueType(MeasureValueType.BIGINT)
                    .measureName("view_count")
                    .measureValue(String.valueOf(random.nextInt(100)*90+10))
                    .time(String.valueOf(clickEvent.getEventtimestamp())).build();

            Record clickCount = Record.builder()
                    .dimensions(dimensions)
                    .measureValueType(MeasureValueType.BIGINT)
                    .measureName("click_count")
                    .measureValue(String.valueOf(random.nextInt(100)*90+10))
                    .time(String.valueOf(clickEvent.getEventtimestamp())).build();
            records.add(viewCount);
            records.add(clickCount);
        }


        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            logger.info("WriteRecords Status: {}" , writeRecordsResponse.sdkHttpResponse().statusCode());
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            logger.error("Error: " , e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        logger.error("RejectedRecords: " + e);
        e.rejectedRecords().forEach(System.out::println);
    }
}
