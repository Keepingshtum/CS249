package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import edu.sjsu.cs249.chain.HeadChainReplicaGrpc.HeadChainReplicaImplBase;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class HeadService extends HeadChainReplicaImplBase {

    static Logger log = Logger.getLogger(HeadService.class.getName());

    ChainRepDriver chainRepDriver;

    public HeadService(ChainRepDriver chainRepDriver) {
        this.chainRepDriver = chainRepDriver;
    }

    /**
     * @param request: Increment Request from Client
     * @param responseObserver : responseObserver to use for response
     */

    @Override
    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        /*
           Three cases to note:
           1.> Replica is head - process request as intended
           2.> Replica is head and tail - process request as head + also ack it since there are no other nodes in the chain
           3.> Replica is in the middle - return response with rc = 1
         */

        //TODO: REFACTOR
        synchronized (chainRepDriver) {
            log.info("increment grpc called");
            if (!chainRepDriver.isHead) {
                log.info("not head, cannot update");
                responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
                responseObserver.onCompleted();
                return;
            }
            String key = request.getKey();
            int incrementer = request.getIncValue();
            int newValue;
            if (chainRepDriver.replicaMap.containsKey(key)) {
                newValue = chainRepDriver.replicaMap.get(key) + incrementer;
                log.info("key: " + key + ", " + "oldValue: " + chainRepDriver.replicaMap.get(key) + ", " + "newValue: " + newValue);
            } else {
                newValue = incrementer;
                log.info("key: " + key + ", " + "oldValue: " + 0+ ", " + "newValue: " + newValue);
            }
            chainRepDriver.replicaMap.put(key, newValue);
            int xid = ++chainRepDriver.lastUpdateRequestXid;
            log.info("xid generated: " + xid);

            if (chainRepDriver.isTail) {
                chainRepDriver.lastAckXid = xid;
                responseObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                responseObserver.onCompleted();
            } else {
                chainRepDriver.pendingUpdateRequests.put(xid, new ChainRepDriver.Triple(key, newValue,responseObserver));

                if (!chainRepDriver.successorContacted) return;
                chainRepDriver.updateSuccessor(key, newValue, xid);
            }
            log.info("exiting increment synchronized block");
        }
    }
}
