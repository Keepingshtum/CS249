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
        ClientXid xid = request.getXid();
        // if the replica has seen the request before, just ignore
        if (replicaInstance.clientCounters.containsKey(xid.getClientid())) {
            //TODO: May be a bug, revisit correctness of condition
            if (replicaInstance.clientCounters.get(xid.getClientid()) >= xid.getCounter()) {
                // ignore the inc request , just respond back
                log.info("Client already has a request outstanding, or we have already processed a newer request. Ignoring.");
                IncResponse response = IncResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }

        } else {

            //Otherwise, update the seen Xids list, and queue a message in the kafka topic
            replicaInstance.clientIncRequests.put(xid, responseObserver);


            // Produce a message for kafka
            PublishedItem incPubItem = PublishedItem.newBuilder().setInc(request).build();
            replicaInstance.produceOperationsMessage(kafkaReplica.OPERATIONS_TOPIC_NAME, incPubItem);

            // Update client counters
            replicaInstance.clientCounters.put(xid.getClientid(), xid.getCounter());
        }
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
        ClientXid xid = request.getXid();
        if (replicaInstance.clientCounters.containsKey(xid.getClientid())) {
            //TODO: May be a bug, revisit correctness of condition
            if (replicaInstance.clientCounters.get(xid.getClientid()) >= xid.getCounter()) {
                // ignore the get request , just respond back
                log.info("Client already has a request outstanding, or we have already processed a newer request. Ignoring.");
                GetResponse response = GetResponse.newBuilder().build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            }

        } else {
            //Otherwise, update the seen Xids list, and queue a message in the kafka topic
            replicaInstance.clientGetRequests.put(xid, responseObserver);

            // Produce a message for kafka
            PublishedItem getPubItem = PublishedItem.newBuilder().setGet(request).build();
            replicaInstance.produceOperationsMessage(kafkaReplica.OPERATIONS_TOPIC_NAME, getPubItem);

            // Update client counters
            replicaInstance.clientCounters.put(xid.getClientid(), xid.getCounter());
        }

    }
}
