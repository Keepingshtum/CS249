package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class kafkaReplica {
    static Logger log = Logger.getLogger(KafkaDebugGrpcService.class.getName());

    Map<String, Integer> mainMap;
    Set<ClientXid> seenXids;
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

    public kafkaReplica(String kafkaHost, String replicaName,String grpcPort, int messageThreshold, String topicPrefix) {
        this.mainMap = new HashMap<>();
        this.seenXids = new HashSet<>();
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


        snapshotOrderingConsumer = createConsumer(SNAPSHOT_ORDERING_TOPIC_NAME, 0L, "snapshotOrdering");
        snapshotConsumer = createConsumer(SNAPSHOT_TOPIC_NAME, 0L, "snapshot");


        //TODO: Consume the latest snapshot
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
        server.awaitTermination();

    }

    public KafkaConsumer<String, byte[]> createConsumer(String topicName,long offset, String groupID) throws InterruptedException, InvalidProtocolBufferException {
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

    public void produceMessage(String topicName,byte[] bytes) {
        // Set up Kafka producer properties
        Properties producerProps = new Properties();
        producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHost);
        producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create a Kafka producer instance
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

        //Create record to publish
        var record = new ProducerRecord<String, byte[]>(topicName, bytes);

        //Create a new thread to publish message
        Thread producerThread = new Thread(() -> {
            producer.send(record);
            log.info("Published to " + topicName);
        });

        producerThread.start();

    }


}
