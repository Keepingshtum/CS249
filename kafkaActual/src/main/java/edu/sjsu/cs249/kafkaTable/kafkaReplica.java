package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class kafkaReplica {
    static Logger log = Logger.getLogger(kafkaReplica.class.getName());

    Map<String, Integer> mainMap;

    Map<String, Integer> clientCounters;
    String kafkaHost;
    String replicaName;
    String grpcPort;
    int messageThreshold;

    String topicPrefix;

    public static final String ANANT_TOPIC_NAME = "anantBasicTest";
    public static final String VT_TOPIC_NAME = "VT-operations";

    public static final String MY_GROUP_ID = "anant1";

    public static final String OPERATIONS = "operations";

    public static final String SNAPSHOT = "snapshot";

    public static final String SNAPSHOT_ORDERING = "snapshotOrdering";

    public static String OPERATIONS_TOPIC_NAME;
    public static String SNAPSHOT_TOPIC_NAME;
    public static String SNAPSHOT_ORDERING_TOPIC_NAME;

    public final Semaphore opSem = new Semaphore(1);

    public long snapshotOrderingOffset;
    public long operationsOffset;
    public String snapshotReplicaId;


    // To pass around in the consumer driver
    KafkaConsumer<String, byte[]> operationsConsumer;
    KafkaConsumer<String, byte[]> snapshotOrderingConsumer;
    KafkaConsumer<String, byte[]> snapshotConsumer;

    // Keeping track of grpcs

    Map<ClientXid, StreamObserver<IncResponse>> clientIncRequests;
    Map<ClientXid,StreamObserver<GetResponse>> clientGetRequests;

    public kafkaReplica(String kafkaHost, String replicaName, String grpcPort, int messageThreshold, String topicPrefix) {
        this.mainMap = new HashMap<>();
        this.clientCounters = new HashMap<>();
        this.clientIncRequests = new HashMap<>();
        this.clientGetRequests = new HashMap<>();
        this.grpcPort = grpcPort;
        this.kafkaHost = kafkaHost;
        this.replicaName = replicaName;
        this.messageThreshold = messageThreshold;
        this.topicPrefix = topicPrefix;
        OPERATIONS_TOPIC_NAME = topicPrefix + OPERATIONS;
        SNAPSHOT_TOPIC_NAME = topicPrefix + SNAPSHOT;
        SNAPSHOT_ORDERING_TOPIC_NAME = topicPrefix + SNAPSHOT_ORDERING;
        snapshotOrderingOffset = -1;
        operationsOffset = -1;
        snapshotReplicaId = "";
    }

    public void startup() throws IOException, InterruptedException {
        //Attach GRPC services
        MainKafkaGrpcService mainKafkaGrpcServer = new MainKafkaGrpcService(this);
        KafkaDebugGrpcService kafkaDebugServer = new KafkaDebugGrpcService(this);

        Server server = ServerBuilder.forPort(Integer.parseInt(grpcPort))
                .addService(mainKafkaGrpcServer)
                .addService(kafkaDebugServer)
                .build();
        server.start();

        log.info(String.format("listening on port %s\n", server.getPort()));

        //Shutdown logic
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                //Stop further operations
                opSem.acquire();
            } catch (Exception e) {
                log.info("Error while closing Kafka instance or acquiring semaphore");
                e.printStackTrace();
            }
            server.shutdown();
            log.info("Successfully stopped server");
        }));


        snapshotOrderingConsumer = createConsumer(SNAPSHOT_ORDERING_TOPIC_NAME, 0L, MY_GROUP_ID+"1");
        snapshotConsumer = createConsumer(SNAPSHOT_TOPIC_NAME, 0L, MY_GROUP_ID+"2");


        consumeLatestSnapshot();
        //TODO: Enqueue yourself in the snapshot ordering topic

//        Thread consumerThread = new Thread(() -> {
//            try {
//                createConsumer(VT_TOPIC_NAME,MY_GROUP_ID);
//            } catch (InterruptedException | InvalidProtocolBufferException e) {
//                log.info("Error in consumer thread");
//                e.printStackTrace();
//            }
//        });
//        consumerThread.start();
        ConsumerDriver consumerDriver = new ConsumerDriver(this);
        consumerDriver.start();
        server.awaitTermination();

    }

    public KafkaConsumer<String, byte[]> createConsumer(String topicName, long offset, String groupID) throws InterruptedException, InvalidProtocolBufferException {
        var properties = new Properties();
        //TODO: Suffix fix - avoid that multiple topic problem with same groupID
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        var consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer());
        log.info("Starting at " + new Date());
        var sem = new Semaphore(0);
        consumer.subscribe(List.of(topicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                log.info("Didn't expect the revoke!");
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                log.info("Partition assigned");
                collection.stream().forEach(t -> consumer.seek(t, offset));
                sem.release();
            }
        });
        log.info("first poll count: " + consumer.poll(0).count());
        sem.acquire();
        log.info("Ready to consume at " + new Date());
        return consumer;
//        while (true) {
//            var records = consumer.poll(Duration.ofSeconds(20));
//            log.info("Got: " + records.count());
//            for (var record: records) {
//                log.info(record.headers().toString());
//                log.info(String.valueOf(record.timestamp()));
//                log.info(String.valueOf(record.timestampType()));
//                log.info(String.valueOf(record.offset()));
//                // If you want to read plaintext messages, use below line instead
////                var message = SimpleMessage.parseFrom(record.value());
//                var publishedItem = PublishedItem.parseFrom(record.value());
//                log.info(publishedItem.toString());
//        }
    }

    public void produceOperationsMessage(String topicName, PublishedItem message) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);

        // Create a Kafka producer instance
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps,new StringSerializer(),new ByteArraySerializer());

        //Create record to publish
        var record = new ProducerRecord<String, byte[]>(topicName, message.toByteArray());

        //Create a new thread to publish message
        Thread producerThread = new Thread(() -> {
            producer.send(record);
            log.info("Published to " + topicName);
        });

        producerThread.start();

    }

    //TODO: Validate
    public void consumeLatestSnapshot() {
        //Assumption: consumer is already subscribed to the topic
        // Poll for new records
        var records = snapshotConsumer.poll(Duration.ofSeconds(1));

        // Find the latest snapshot message
        Snapshot latestSnapshotRecord = null;
        long currentOffset = -1;
        for (var record : records) {
            try {

                if (latestSnapshotRecord == null || record.offset() > currentOffset) {
                    latestSnapshotRecord = Snapshot.parseFrom(record.value());
                    currentOffset = record.offset();
                }
            } catch (InvalidProtocolBufferException e) {
                log.info("Error occured during consuming snapshot");
                e.printStackTrace();
            }

        }

        // Process the latest snapshot message
        if (latestSnapshotRecord != null) {

            log.info("Snapshot details");
            log.info(latestSnapshotRecord.toString());
            // Update state variables according to the snapshot
            snapshotReplicaId = latestSnapshotRecord.getReplicaId();
            //Set to 0 if offsets are -1, otherwise assign as normal
            operationsOffset = (latestSnapshotRecord.getOperationsOffset() == -1) ? 0 : latestSnapshotRecord.getOperationsOffset();
            snapshotOrderingOffset = (latestSnapshotRecord.getSnapshotOrderingOffset() == -1) ? 0 : latestSnapshotRecord.getSnapshotOrderingOffset();
            //TODO: REPLACE THIS LOGIC TO GO THROUGH MAP ENTRIES SEQUENTIALLY
            // IS THE ROOT CAUSE OF UNMODIFIABLE MAP EXCEPTION
//            mainMap = latestSnapshotRecord.getTableMap();
//            clientCounters = latestSnapshotRecord.getClientCountersMap();

        } else {
            log.info("ALERT! SNAPSHOT IS NULL!");
        }
    }


    public void checkSnapshotOrdering(
            KafkaProducer<String, byte[]> producer,
            String topicName,
            String replicaName,
            long startingOffset
    ) throws InvalidProtocolBufferException {

        // Seek to the starting offset
        snapshotOrderingConsumer.seek(new TopicPartition(topicName, 0), startingOffset);

        // Poll for new records
        var records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1));

        // Check for replicaName in the messages
        boolean replicaNameExists = false;
        for (var record : records) {
            SnapshotOrdering snapshotOrderingMessage = SnapshotOrdering.parseFrom(record.value());
            if (snapshotOrderingMessage.getReplicaId().contains(replicaName)) {
                replicaNameExists = true;
                break;
            }
        }

        //TODO: CONFIRM YOU DON'T NEED TO CREATE A NEW PRODUCER

        // If replicaName does not exist, publish a message with replicaName
        if (!replicaNameExists) {
            SnapshotOrdering message = SnapshotOrdering.newBuilder()
                    .setReplicaId(replicaName)
                    .build();

            producer.send(new ProducerRecord<>(topicName, message.toByteArray()));
        }
    }


}
