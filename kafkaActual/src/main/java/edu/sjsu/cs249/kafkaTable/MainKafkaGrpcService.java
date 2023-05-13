package edu.sjsu.cs249.kafkaTable;

import io.grpc.stub.StreamObserver;

import java.util.Map;

public class MainKafkaGrpcService extends KafkaTableGrpc.KafkaTableImplBase {

    kafkaReplica replicaInstance;
    public MainKafkaGrpcService(kafkaReplica instance) {
        this.replicaInstance = instance;
    }

    /**
     * @param request incRequest
     * @param responseObserver corresponding responseObserver
     *
     *  Accepts incRequests from clients
     *  Publishes a message in the kafka operations queue if the client has no other requests outstanding
     */
    @Override
    public void inc(IncRequest request, StreamObserver<IncResponse> responseObserver) {
        //TODO: Build out a main class so you can pass around the map and snapshot details, then complete this 
        ClientXid xid = request.getXid();

        //if the replica has seen the request before, just ignore
        if (replicaInstance.seenXids.contains(xid)) {
            return; // ignore the inc request
        }

        //Otherwise, update the seenXids list, and queue a message in the kafka topic
        //TODO: Validate
        replicaInstance.seenXids.add(xid);

        // Produce a message for kafka

        IncResponse response = IncResponse.newBuilder().build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * @param request getRequest
     * @param responseObserver corresponding responseObserver
     *
     * Accepts get requests from clients
     * Returns hashmap value (or 0, if key doesn't exist)
     */
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        super.get(request, responseObserver);
    }
}
