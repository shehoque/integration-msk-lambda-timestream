package com.amazonaws.kafka.samples;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import samples.clickstream.avro.ClickEvent;
import samples.workspaceevent.avro.WorkspaceEvent;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;


public class TimestreamSimpleIngest {
    public static final long HT_TTL_HOURS = 24L;
    public static final long CT_TTL_DAYS = 7L;

    private final TimestreamWriteClient timestreamWriteClient;

    private final Random random = new Random();
    private final String databaseName;
    private final String tableName;
    private static final Logger logger = LogManager.getLogger(TimestreamSimpleIngest.class);
    public TimestreamSimpleIngest(TimestreamWriteClient client, String databaseName, String tableName) {
        this.timestreamWriteClient = client;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public void writeRecord(WorkspaceEvent workspaceEvent) {
        // Specify repeated values for all records
        List<Record> records = new ArrayList<>();
        List<Dimension> dimensions = new ArrayList<>();
        dimensions = new ArrayList<>();
        Dimension workspaceId = Dimension.builder().name("WorkspaceId").value(workspaceEvent.getWorkspaceId().toString()).build();
        Dimension modelId= Dimension.builder().name("ModelId").value(workspaceEvent.getModelId().toString()).build();
        Dimension state = Dimension.builder().name("State").value(workspaceEvent.getState().toString()).build();
        Dimension version = Dimension.builder().name("Version").value(workspaceEvent.getVersion().toString()).build();
        dimensions.add(workspaceId);
        dimensions.add(modelId);
        dimensions.add(state);
        dimensions.add(version);
        Collection<MeasureValue> measureValues = new ArrayList<>();
        MeasureValue memoryUtilMV = MeasureValue.builder().name("Mem_Util").value(String.valueOf(workspaceEvent.getMemUtil())).type(MeasureValueType.DOUBLE).build();
        MeasureValue cpuUtilMV = MeasureValue.builder().name("Cpu_Util").value(String.valueOf(workspaceEvent.getCpuUtl())).type(MeasureValueType.DOUBLE).build();
        measureValues.add(memoryUtilMV);
        measureValues.add(cpuUtilMV);
        Record workspaceMV = Record.builder()
                .dimensions(dimensions)
                .measureValueType(MeasureValueType.MULTI)
                .measureName("Metrics")
                .measureValues(measureValues)
                .time(String.valueOf(workspaceEvent.getEventTimestamp())).timeUnit(TimeUnit.SECONDS).build();

        records.add(workspaceMV);


        WriteRecordsRequest writeRecordsRequest = WriteRecordsRequest.builder()
                .databaseName(databaseName).tableName(tableName).records(records).build();

        try {
            WriteRecordsResponse writeRecordsResponse = timestreamWriteClient.writeRecords(writeRecordsRequest);
            logger.info("WriteRecords Status: {}" , writeRecordsResponse);
        } catch (RejectedRecordsException e) {
            printRejectedRecordsException(e);
        } catch (Exception e) {
            logger.error("Error: " , e);
        }
    }

    private void printRejectedRecordsException(RejectedRecordsException e) {
        logger.error("RejectedRecords: " , e);
        e.rejectedRecords().forEach(System.out::println);
    }
}
