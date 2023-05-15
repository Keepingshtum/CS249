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
        //To avoid conccurrent access exceptions
        synchronized (this.replicaInstance) {
            Snapshot snap = Snapshot.newBuilder().setReplicaId(replicaInstance.replicaName)
                    .setOperationsOffset(this.replicaInstance.operationsOffset)
                    .setSnapshotOrderingOffset(this.replicaInstance.snapshotOrderingOffset)
                    .putAllTable(this.replicaInstance.mainMap)
                    .putAllClientCounters(this.replicaInstance.clientCounters).build();
            responseObserver.onNext(KafkaTableDebugResponse.newBuilder().setSnapshot(snap).build());
            responseObserver.onCompleted();
        }
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
