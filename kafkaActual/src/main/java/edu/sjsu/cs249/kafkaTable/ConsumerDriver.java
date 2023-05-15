package edu.sjsu.cs249.kafkaTable;

import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.util.logging.Logger;

import static edu.sjsu.cs249.kafkaTable.kafkaReplica.MY_GROUP_ID;
import static edu.sjsu.cs249.kafkaTable.kafkaReplica.OPERATIONS_TOPIC_NAME;


/**
 * Holds all the logic for consuming from operations topic
 * Will poll indefinitely and process get and inc requests once read from the Kafka queue
 */
public class ConsumerDriver extends Thread {

    kafkaReplica replicaInstance;

    static Logger log = Logger.getLogger(ConsumerDriver.class.getName());

    public ConsumerDriver(kafkaReplica replicaInstance) {
        this.replicaInstance = replicaInstance;
    }

    @Override
    public void run() {
        log.info("Spinning up consumers");

        try {
            if(replicaInstance.operationsOffset == -1L){
                replicaInstance.operationsConsumer = replicaInstance.createConsumer(OPERATIONS_TOPIC_NAME, 0, MY_GROUP_ID+"3");
            }
            else {
                replicaInstance.operationsConsumer = replicaInstance.createConsumer(OPERATIONS_TOPIC_NAME, replicaInstance.operationsOffset, MY_GROUP_ID+"3");
            }

        } catch (InterruptedException | InvalidProtocolBufferException e) {
            log.info("Exception occurred in consumerDriver");
            e.printStackTrace();
        }

        //Poll forever
        while (true) {
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
                            if (message.hasInc() && replicaInstance.isValidRequest(message.getInc().getXid())) {
                                //After this point, request is valid
                                log.info("Processing Inc");
                                IncRequest incRequest = message.getInc();
                                if(replicaInstance.mainMap.containsKey(incRequest.getKey())){
                                    //Increment by incRequest.getIncValue()
                                    int currentValue = replicaInstance.mainMap.get(incRequest.getKey());
                                    int newValue = currentValue + incRequest.getIncValue();
                                    replicaInstance.mainMap.put(incRequest.getKey(), newValue);
                                } else {
                                    // Put incRequest.getIncValue() with key
                                    replicaInstance.mainMap.put(incRequest.getKey(), incRequest.getIncValue());
                                }
                                // If a client asked this replica to do this request, respond once it is processed
                                if(replicaInstance.clientIncRequests.containsKey(incRequest.getXid())) {
                                    IncResponse response = IncResponse.newBuilder().build();
                                    StreamObserver<IncResponse> responseObserver = replicaInstance.clientIncRequests.remove(incRequest.getXid());
                                    responseObserver.onNext(response);
                                    responseObserver.onCompleted();
                                }
                                // Update client counters
                                replicaInstance.clientCounters.put(incRequest.getXid().getClientid(), incRequest.getXid().getCounter());

                            }
                            else if(message.hasInc()) {
                                //Request is not valid. Respond if you got the request and exit
                                // If a client asked this replica to do this request, respond once it is processed
                                log.info("Duplicate message received from Kafka -ignoring");
                                IncRequest incRequest = message.getInc();
                                if(replicaInstance.clientIncRequests.containsKey(incRequest.getXid())) {
                                    IncResponse response = IncResponse.newBuilder().build();
                                    StreamObserver<IncResponse> responseObserver = replicaInstance.clientIncRequests.remove(incRequest.getXid());
                                    responseObserver.onNext(response);
                                    responseObserver.onCompleted();
                                }

                            }
                            if (message.hasGet() && replicaInstance.isValidRequest(message.getGet().getXid())) {
                                //After this point, request is valid
                                log.info("Processing Get");
                                GetRequest getRequest = message.getGet();

                                // If a client asked this replica to do this request, respond once it is processed
                                if(replicaInstance.clientGetRequests.containsKey(getRequest.getXid())) {


                                    StreamObserver<GetResponse> responseObserver = replicaInstance.clientGetRequests.remove(getRequest.getXid());

                                    if(replicaInstance.mainMap.containsKey(getRequest.getKey())){
                                        //Return the value that the main map contains
                                        GetResponse response = GetResponse.newBuilder().setValue(replicaInstance.mainMap.get(getRequest.getKey())).build();
                                        responseObserver.onNext(response);
                                        responseObserver.onCompleted();

                                    } else {
                                        // return a 0 since it is not present in the map
                                        GetResponse response = GetResponse.newBuilder().setValue(0).build();
                                        responseObserver.onNext(response);
                                        responseObserver.onCompleted();
                                    }

                                }

                                // Update client counters
                                replicaInstance.clientCounters.put(getRequest.getXid().getClientid(), getRequest.getXid().getCounter());


                                //If no client is waiting for this, do nothing!

                            }
                            else if(message.hasGet()){
                                GetRequest getRequest = message.getGet();
                                //Request is invalid, return an empty response if a client asked you for this request
                                log.info("Duplicate message received from Kafka -ignoring");
                                if(replicaInstance.clientGetRequests.containsKey(getRequest.getXid())) {
                                    StreamObserver<GetResponse> responseObserver = replicaInstance.clientGetRequests.remove(getRequest.getXid());
                                    //Empty response
                                    GetResponse response = GetResponse.newBuilder().build();
                                    responseObserver.onNext(response);
                                    responseObserver.onCompleted();
                                }
                            }

                            //After each message, check if it's time to snapshot
                            if (offset % (long)replicaInstance.messageThreshold == 0L) {
                                log.info("Offset "+offset+ " mod "+replicaInstance.messageThreshold+" equals zero."
                                +"Message Threshold hit - time to check for snapshots");
                                replicaInstance.checkAndTakeSnapshot();
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
            //Wait some time to allow to process debug requests, etc.
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}