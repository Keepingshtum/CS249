package edu.sjsu.cs249.kafkaTable;

import io.grpc.stub.StreamObserver;

import java.util.Map;
import java.util.logging.Logger;

public class KafkaDebugGrpcService extends KafkaTableDebugGrpc.KafkaTableDebugImplBase {

    kafkaReplica replicaInstance;
    public KafkaDebugGrpcService(kafkaReplica instance) {
        this.replicaInstance = instance;
    }

    static Logger log = Logger.getLogger(KafkaDebugGrpcService.class.getName());

    /**
     * @param request debugRequest
     * @param responseObserver corresponding responseObserver
     */
    @Override
    public void debug(KafkaTableDebugRequest request, StreamObserver<KafkaTableDebugResponse> responseObserver) {
        KafkaTableDebugResponse.Builder builder = KafkaTableDebugResponse.newBuilder();
        Snapshot.Builder snapBuilder = Snapshot.newBuilder();
        //TODO:UPDATE
        snapBuilder.setReplicaId("0").setOperationsOffset(0L).build();
        builder.setSnapshot(snapBuilder);
    }

    /**
     * @param request exitRequest
     * @param responseObserver corresponding responseObserver
     */
    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        log.info("Exiting...");
        responseObserver.onNext(ExitResponse.newBuilder().build());
        responseObserver.onCompleted();
        log.info("Goodbye!");
        System.exit(0);

    }
}
