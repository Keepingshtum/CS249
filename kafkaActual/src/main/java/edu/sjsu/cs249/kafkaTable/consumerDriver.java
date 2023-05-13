package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;

import java.time.Duration;
import java.util.logging.Logger;

import static edu.sjsu.cs249.kafkaTable.kafkaReplica.MY_GROUP_ID;
import static edu.sjsu.cs249.kafkaTable.kafkaReplica.OPERATIONS_TOPIC_NAME;


/**
 * Holds all the logic for consuming from operations topic
 * Will poll indefinitely and process get and inc requests once read from the Kafka queue
 */
public class consumerDriver extends Thread {

    kafkaReplica replicaInstance;

    static Logger log = Logger.getLogger(KafkaDebugGrpcService.class.getName());
    public consumerDriver(kafkaReplica replicaInstance) {
        this.replicaInstance = replicaInstance;
    }

    @Override
    public void run() {
        log.info("Spinning up consumers");

        try {
            replicaInstance.createConsumer(OPERATIONS_TOPIC_NAME,replicaInstance.operationsOffset,MY_GROUP_ID);
        } catch (InterruptedException | InvalidProtocolBufferException e) {
            log.info("Exception occured in consumerDriver");
            e.printStackTrace();
        }

        //Poll forever
        while(true){
            //Preserve ordering
            synchronized (replicaInstance) {
                try {
                    replicaInstance.opSem.acquire();
                    log.info("Polling");
                    var records = replicaInstance.operationsConsumer.poll(Duration.ofSeconds(1));
                    for (var record : records) {
                        log.info("Received message!");
                        // If you want to read plaintext messages, use below line instead
//                        var message = SimpleMessage.parseFrom(record.value());
                        log.info(String.valueOf(record.headers()));
                        log.info(String.valueOf(record.timestamp()));
                        log.info(String.valueOf(record.timestampType()));
                        long offset = record.offset();
                        log.info(String.valueOf(offset));
                        replicaInstance.operationsOffset = offset;
                        PublishedItem message;
                        try {
                            message = PublishedItem.parseFrom(record.value());
                            log.info(String.valueOf(message));
                            if (message.hasInc()) {
                                IncRequest incRequest = message.getInc();

                            }
                            if(message.hasGet()) {
                            }
                        } catch (InvalidProtocolBufferException e) {
                            log.info("Unable to parse value: " + e);
                        } catch (Exception e) {
                            log.info("Exception in message parsing block" + e);
                        }

                    }
                    replicaInstance.opSem.release();
                    log.info("Released semaphore");
                } catch (InterruptedException e) {
                    log.info("Problem acquiring semaphore");
                }
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

        }