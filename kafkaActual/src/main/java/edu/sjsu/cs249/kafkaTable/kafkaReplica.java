package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.kafka.clients.consumer.*;
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

    int groupIDSuffix;

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
    Map<ClientXid, StreamObserver<GetResponse>> clientGetRequests;

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
        groupIDSuffix = 0;
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

                //For faster shutdown and rejoin
                this.operationsConsumer.unsubscribe();
                this.snapshotConsumer.unsubscribe();
                this.snapshotOrderingConsumer.unsubscribe();
            } catch (Exception e) {
                log.info("Error while closing Kafka instance or acquiring semaphore");
                e.printStackTrace();
            }
            server.shutdown();
            log.info("Successfully stopped server");
        }));



        snapshotConsumer = createConsumer(SNAPSHOT_TOPIC_NAME, 0L, MY_GROUP_ID + "2");


        //Consume the latest snapshot
        consumeLatestSnapshot();

        snapshotOrderingConsumer = createConsumer(SNAPSHOT_ORDERING_TOPIC_NAME, snapshotOrderingOffset+1, MY_GROUP_ID + "1");

        //TODO: Enqueue yourself in the snapshot ordering topic
        checkSnapshotOrdering();

        //Create new thread to handle consuming of operations topic
        ConsumerDriver consumerDriver = new ConsumerDriver(this);
        consumerDriver.start();
        server.awaitTermination();

    }

    public KafkaConsumer<String, byte[]> createConsumer(String topicName, long offset, String groupID) throws InterruptedException, InvalidProtocolBufferException {
        var properties = new Properties();
        //Suffix fix - avoid that multiple topic problem with same groupID
        groupIDSuffix += 1;
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID + groupIDSuffix);
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
        //Prepoll to set up connection
        log.info("first poll count: " + consumer.poll(0).count());
        sem.acquire();
        log.info("Ready to consume at " + new Date());
        return consumer;
    }

    public void checkAndTakeSnapshot() {
        // Increment the offset by one and check if it is your turn to snapshot now
        this.snapshotOrderingConsumer.seek(new TopicPartition(this.SNAPSHOT_ORDERING_TOPIC_NAME, 0), snapshotOrderingOffset+1);
        this.snapshotOrderingConsumer.poll(Duration.ofSeconds(0));

        //This time, we will actually define our records instead of using vars!
        ConsumerRecords<String, byte[]> records = this.snapshotOrderingConsumer.poll(Duration.ofSeconds(1));
        Iterator topicIterator = records.iterator();

        if (topicIterator.hasNext()) {
            ConsumerRecord<String, byte[]> record = (ConsumerRecord<String, byte[]>) topicIterator.next();
            log.info("Polling Snapshot Ordering to see if it's my turn");
            try {
                SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                //Update state info
                snapshotOrderingOffset = record.offset();
                log.info("snapshotOrderingOffset: " + snapshotOrderingOffset);
                log.info("Current name pulled: " + message.getReplicaId());
                if (message.getReplicaId().equals(replicaName)) {
                    log.info("It's a-me! Publishing snap");
                    Snapshot snap = Snapshot.newBuilder().setReplicaId(replicaName).putAllTable(mainMap).setOperationsOffset(operationsOffset)
                            .putAllClientCounters(clientCounters).setSnapshotOrderingOffset(snapshotOrderingOffset).build();
                    produceSnapshotMessage(SNAPSHOT_TOPIC_NAME, snap);
                    produceSnapshotOrderingMessage(SNAPSHOT_ORDERING_TOPIC_NAME, SnapshotOrdering.newBuilder().setReplicaId(replicaName).build());
                } else {
                    log.info("Not me this time. This guy should be publishing ->" + message.getReplicaId());
                }
            } catch (InvalidProtocolBufferException e) {
                log.info("Unable to parse value: " + e);
            }
        } else {
            log.info("No records lol");
        }
    }

    public void produceOperationsMessage(String topicName, PublishedItem message) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);

        // Create a Kafka producer instance
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());

        //Create record to publish
        var record = new ProducerRecord<String, byte[]>(topicName, message.toByteArray());

        //Create a new thread to publish message
        Thread producerThread = new Thread(() -> {
            producer.send(record);
            log.info("Published to " + topicName);
        });

        producerThread.start();

    }

    public void produceSnapshotMessage(String topicName, Snapshot message) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);

        // Create a Kafka producer instance
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());

        //Create record to publish
        var record = new ProducerRecord<String, byte[]>(topicName, message.toByteArray());

        //Create a new thread to publish message
        Thread producerThread = new Thread(() -> {
            producer.send(record);
            log.info("Published to " + topicName);
        });

        producerThread.start();

    }

    public void produceSnapshotOrderingMessage(String topicName, SnapshotOrdering message) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);

        // Create a Kafka producer instance
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps, new StringSerializer(), new ByteArraySerializer());

        //Create record to publish
        var record = new ProducerRecord<String, byte[]>(topicName, message.toByteArray());

        //Create a new thread to publish message
        Thread producerThread = new Thread(() -> {
            producer.send(record);
            log.info("Published to " + topicName);
        });

        producerThread.start();

    }

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
            operationsOffset =  latestSnapshotRecord.getOperationsOffset();
            snapshotOrderingOffset = latestSnapshotRecord.getSnapshotOrderingOffset();


            //We have to iterate through each record in the snapshot map individually
            //Otherwise the map becomes immutable and leads to errors!
            Iterator snapIterator = latestSnapshotRecord.getTableMap().keySet().iterator();
            String key;
            while (snapIterator.hasNext()) {
                key = (String) snapIterator.next();
                this.mainMap.put(key, latestSnapshotRecord.getTableMap().get(key));
            }

            this.operationsOffset = latestSnapshotRecord.getOperationsOffset();
            snapIterator = latestSnapshotRecord.getClientCountersMap().keySet().iterator();

            while (snapIterator.hasNext()) {
                key = (String) snapIterator.next();
                this.clientCounters.put(key, latestSnapshotRecord.getClientCountersMap().get(key));
            }

        } else {
            log.info("ALERT! SNAPSHOT IS NULL!");
        }
    }


    public void checkSnapshotOrdering() {
        //TODO: Modify to work with joining in the middle
        ConsumerRecords<String, byte[]> records = snapshotOrderingConsumer.poll(Duration.ofSeconds(1L));
        Iterator snapshotOrderingIterator = records.iterator();

        //Check if you are already queued up
        while (snapshotOrderingIterator.hasNext()) {
            ConsumerRecord<String, byte[]> record = (ConsumerRecord<String, byte[]>) snapshotOrderingIterator.next();
            try {
                SnapshotOrdering message = SnapshotOrdering.parseFrom(record.value());
                if (message.getReplicaId().equals(replicaName)) {
                    //ALready queued - do nothing!
                    log.info("Found myself - no need to do anything now");
                    return;
                }
            } catch (InvalidProtocolBufferException e) {
                log.info("Error occured during snapshot ordering processing: " + e);
                e.printStackTrace();
            }
        }
        //Otherwise, queue up
        log.info("Didn't find myself - queueing up");
        SnapshotOrdering snapshotOrderingMessage = SnapshotOrdering.newBuilder().setReplicaId(replicaName).build();
        produceSnapshotOrderingMessage(kafkaReplica.SNAPSHOT_ORDERING_TOPIC_NAME, snapshotOrderingMessage);
    }

    public boolean isValidRequest(ClientXid xid) {
        if (clientCounters.containsKey(xid.getClientid())) {
            if (clientCounters.get(xid.getClientid()) >= xid.getCounter()) {
                return false;
            }

        }
        return true;
    }
}
