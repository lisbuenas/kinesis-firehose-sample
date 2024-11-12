package service;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import model.Event;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class KinesisService {

   static final String PROVIDER = "AWS KINESIS FIREHOSE";
   static final String BLOG = "Coffee and Tips";
   static final String KNS_DELIVERY_NAME = "kns-delivery-event";
   static final String RECORD_ID = "Record ID ";
   static final String EVENT = "Event ";

   public static AmazonKinesisFirehose kinesisFirehoseClient() {
       return AmazonKinesisFirehoseClientBuilder.standard()
               .withRegion(Regions.US_EAST_1.getName())
               .build();
   }

   @SneakyThrows
   public static void sendDataWithPutRecordBatch(int maxRecords) {
       PutRecordBatchRequest putRecordBatchRequest = new PutRecordBatchRequest()
               .withDeliveryStreamName(KNS_DELIVERY_NAME);

       List<com.amazonaws.services.kinesisfirehose.model.Record> records = new ArrayList<>();

       while (maxRecords > 0) {
           String line = getData();
           String data = line + "\n";
           System.out.println(data);

           // Create Record using the AWS SDK
           com.amazonaws.services.kinesisfirehose.model.Record record = new com.amazonaws.services.kinesisfirehose.model.Record()
                   .withData(ByteBuffer.wrap(data.getBytes()));
           records.add(record);
           maxRecords--;
       }

       putRecordBatchRequest.setRecords(records);
       PutRecordBatchResult putRecordResult = kinesisFirehoseClient()
               .putRecordBatch(putRecordBatchRequest);

       putRecordResult.getRequestResponses().forEach(result -> System.out
               .println(RECORD_ID + result.getRecordId()));
   }

   @SneakyThrows
   public static void sendDataWithPutRecord(int maxRecords) {
       while (maxRecords > 0) {
           PutRecordRequest putRecordRequest = new PutRecordRequest()
                   .withDeliveryStreamName(KNS_DELIVERY_NAME);

           String line = getData();
           String data = line + "\n";
           System.out.println(EVENT + data);

           // Create Record using the AWS SDK
           com.amazonaws.services.kinesisfirehose.model.Record record = new com.amazonaws.services.kinesisfirehose.model.Record()
                   .withData(ByteBuffer.wrap(data.getBytes()));
           putRecordRequest.setRecord(record);

           PutRecordResult putRecordResult = kinesisFirehoseClient()
                   .putRecord(putRecordRequest);

           System.out.println(RECORD_ID + putRecordResult.getRecordId());

           maxRecords--;
       }
   }

   @SneakyThrows
   public static String getData() {
       Event event = new Event();
       event.setEventId(UUID.randomUUID());
       event.setPostId(UUID.randomUUID());
       event.setBlog(BLOG);
       event.setEventDate(LocalDateTime.now().toString());
       event.setProvider(PROVIDER);
       ObjectMapper objectMapper = new ObjectMapper();
       return objectMapper.writeValueAsString(event);
   }

   public static void main(String[] args) {
       // sendDataWithPutRecordBatch(500);
        sendDataWithPutRecord(2000);
   }
}
