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
     * @param request:         ChainDebugRequest received by replica
     * @param responseObserver : Corresponding responseObserver
     */
    @Override
    public void debug(ChainDebugRequest request, StreamObserver<ChainDebugResponse> responseObserver) {
        //To avoid problems with --check-chain
        synchronized (chainRepDriver) {
            try {

                //Don't ack during debug because it might throw false errors
                chainRepDriver.ackSemaphore.acquire();

                //Add the current hashmap state as seen by the replica
                ChainDebugResponse.Builder builder = ChainDebugResponse.newBuilder();
                builder
                        .setXid(chainRepDriver.lastXidSeen)
                        .putAllState(chainRepDriver.replicaMap);

                //Add the pending requests as seen by the replica
                for (int key : chainRepDriver.pendingUpdateRequests.keySet()) {
                    builder.addSent(UpdateRequest.newBuilder()
                            .setXid(key)
                            .setKey(chainRepDriver.pendingUpdateRequests.get(key).getMapKey())
                            .setNewValue(chainRepDriver.pendingUpdateRequests.get(key).getMapVal())
                            .build());
                }
                log.info("xid: " + builder.getXid() + ", state: " + builder.getStateMap() + ", sent: " + builder.getSentList());

                responseObserver.onNext(builder.build());
                responseObserver.onCompleted();
            } catch (InterruptedException e) {
                log.info("Problem acquiring semaphore");
                log.info(e.getMessage());
            } finally {
                log.info("releasing semaphore for ack");
                chainRepDriver.ackSemaphore.release();
            }
        }
    }


    /**
     * @param request          :ExitRequest seen by replica
     * @param responseObserver : Corresponding responseObserver
     */
    @Override
    public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
        //Do a "Graceful" exit - if you're exiting, don't do any other task
        synchronized (chainRepDriver) {
            try {
                // Don't ack while exiting
                chainRepDriver.ackSemaphore.acquire();
                log.info("Exiting...");
                responseObserver.onNext(ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                log.info("Releasing Ack Semaphore - Goodbye!");
                chainRepDriver.ackSemaphore.release();
                System.exit(0);
            } catch (InterruptedException e) {
                log.info("Problem during exit");
                log.info(e.getMessage());
            }
        }
    }
}
