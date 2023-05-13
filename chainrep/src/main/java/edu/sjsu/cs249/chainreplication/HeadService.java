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
     * @param request:         Increment Request from Client
     * @param responseObserver : responseObserver to use for response
     *                         <p>
     *                         Handles increment requests depending on position of replica (Head, Middle, or Head and Tail both)
     */

    @Override
    public void increment(IncRequest request, StreamObserver<HeadResponse> responseObserver) {
        /*
           Three cases to note:
           1.> Replica is head - process request as intended
           2.> Replica is head and tail - process request as head + also ack it since there are no other nodes in the chain
           3.> Replica is in the middle - return response with rc = 1
         */
        synchronized (chainRepDriver) {
            log.info("Increment request received");
            // Case 1: Replica is head. Can proceed with logic to increment
            if (chainRepDriver.isHead) {
                String requestKey = request.getKey();
                int requestIncValue = request.getIncValue();
                int newValue;

                //If value is in the map, increment, otherwise, put it the map with corresponding key
                if (chainRepDriver.replicaMap.containsKey(requestKey)) {
                    newValue = chainRepDriver.replicaMap.get(requestKey) + requestIncValue;
                    log.info("key: " + requestKey + ", " + "old value: " + chainRepDriver.replicaMap.get(requestKey) + ", " + "newValue: " + newValue);
                } else {
                    newValue = requestIncValue;
                    log.info("key: " + requestKey + ", " + "old value: " + 0 + ", " + "new value: " + newValue);
                }
                chainRepDriver.replicaMap.put(requestKey, newValue);
                int xid = ++chainRepDriver.lastXidSeen;
                log.info("xid generated: " + xid);

                // Case 2: Replica is head and tail; Ack the inc request since no other replica needs to see the request
                if (chainRepDriver.isTail) {
                    chainRepDriver.lastAckSeen = xid;
                    responseObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                    responseObserver.onCompleted();
                } else {
                    chainRepDriver.pendingUpdateRequests.put(xid, new ChainRepDriver.Triple(requestKey, newValue, responseObserver));

                    if (!chainRepDriver.successorContacted) return;
                    chainRepDriver.updateSuccessor(requestKey, newValue, xid);
                }
            }
            // Case 3:  If replica is in the middle, just return with RC=1
            else {
                log.info("Head Service called for non-head Replica");
                responseObserver.onNext(HeadResponse.newBuilder().setRc(1).build());
                responseObserver.onCompleted();
                return;
            }
            log.info("Increment Complete");
        }
    }
}
