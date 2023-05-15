package edu.sjsu.cs249.kafkaTable;

import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class MainKafkaGrpcService extends KafkaTableGrpc.KafkaTableImplBase {

    kafkaReplica replicaInstance;

    static Logger log = Logger.getLogger(MainKafkaGrpcService.class.getName());

    public MainKafkaGrpcService(kafkaReplica instance) {
        this.replicaInstance = instance;
    }

    /**
     * @param request          incRequest
     * @param responseObserver corresponding responseObserver
     *                         <p>
     *                         Accepts incRequests from clients
     *                         Publishes a message in the kafka operations queue if the client has no other requests outstanding
     */
    @Override
    public void inc(IncRequest request, StreamObserver<IncResponse> responseObserver) {
        //TODO: Synchronize if needed
        ClientXid xid = request.getXid();
        // if the replica has seen the request before, just ignore
        if (!replicaInstance.isValidRequest(xid)) {
                // ignore the inc request , just respond back
                log.info("Client already has a request outstanding, or we have already processed a newer request. Ignoring.");
                IncResponse response = IncResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;

        }

        //Otherwise, update the seen Xids list, and queue a message in the kafka topic
        replicaInstance.clientIncRequests.put(xid, responseObserver);


        // Produce a message for kafka
        PublishedItem incPubItem = PublishedItem.newBuilder().setInc(request).build();
        replicaInstance.produceOperationsMessage(kafkaReplica.OPERATIONS_TOPIC_NAME, incPubItem);

        // Don't Update client counters - we'll do this once we consume and process the message
    }

    /**
     * @param request          getRequest
     * @param responseObserver corresponding responseObserver
     *                         <p>
     *                         Accepts get requests from clients
     *                         Returns hashmap value (or 0, if key doesn't exist)
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        //TODO: Synchronize if needed
        ClientXid xid = request.getXid();
        if (!replicaInstance.isValidRequest(xid)) {
                // ignore the get request , just respond back
                log.info("Client already has a request outstanding, or we have already processed a newer request. Ignoring.");
                GetResponse response = GetResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();

        }
        //Otherwise, update the seen Xids list, and queue a message in the kafka topic
        replicaInstance.clientGetRequests.put(xid, responseObserver);

        // Produce a message for kafka
        PublishedItem getPubItem = PublishedItem.newBuilder().setGet(request).build();
        replicaInstance.produceOperationsMessage(kafkaReplica.OPERATIONS_TOPIC_NAME, getPubItem);

        //Don't Update client counters - we'll do this once we consume the message


    }
}
