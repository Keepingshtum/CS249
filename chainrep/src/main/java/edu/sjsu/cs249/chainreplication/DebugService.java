package edu.sjsu.cs249.chainreplication;


import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class DebugService extends ChainDebugGrpc.ChainDebugImplBase {

    static Logger log = Logger.getLogger(DebugService.class.getName());
    ChainRepDriver chainRepDriver;

    public DebugService(ChainRepDriver chainRepDriver) {
        this.chainRepDriver = chainRepDriver;
    }

    /**
     * @param request: ChainDebugRequest received by replica
     * @param responseObserver : Corresponding responseObserver
     */
    @Override
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
        //Add the current hashmap state as seen by the replica
        ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();
        builder
                .setXid(chainRepDriver.lastAckXid)
                .putAllState(chainRepDriver.replicaMap);

        //Add the pending requests as seen by the replica
        for(int key: chainRepDriver.pendingUpdateRequests.keySet()) {
            builder.addSent(UpdateRequest.newBuilder()
                    .setXid(key)
                    .setKey(chainRepDriver.pendingUpdateRequests.get(key).getMapKey())
                    .setNewValue(chainRepDriver.pendingUpdateRequests.get(key).getMapVal())
                    .build());
        }
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * @param request :ExitRequest seen by replica
     * @param responseObserver : Corresponding responseObserver
     */
    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        //Do a "Graceful" exit
        synchronized (chainRepDriver) {
            try {
                chainRepDriver.ackSemaphore.acquire();
                log.info("Exiting...");
                responseObserver.onNext(ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                log.info("releasing semaphore");
                chainRepDriver.ackSemaphore.release();
                System.exit(0);
            } catch (InterruptedException e) {
                log.info("Problem acquiring semaphore");
                log.info(e.getMessage());
            }
        }
    }
}
