package edu.sjsu.cs249.chainreplication;

import com.google.rpc.Code;
import edu.sjsu.cs249.chain.*;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.Objects;
import java.util.logging.Logger;

public class ReplicaService extends ReplicaGrpc.ReplicaImplBase {

    static Logger log = Logger.getLogger(ReplicaService.class.getName());

    ChainRepDriver chainRepDriver;

    public ReplicaService(ChainRepDriver chainRepDriver) {
        this.chainRepDriver = chainRepDriver;
    }

    /**
     * @param request          : Update request received by replica
     * @param responseObserver : Corresponding responseObserver
     *                         <p>
     *                         Defines Behaviour for how the update requests propagate down the replica chain
     */
    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        synchronized (chainRepDriver) {
            String requestKey = request.getKey();
            int newValue = request.getNewValue();
            int requestXid = request.getXid();
            log.info("Update request received");
            log.info("xid: " + requestXid + ", key: " + requestKey + ", Value: " + newValue);


            //Always update replica before sending the request onwards down the chain
            chainRepDriver.replicaMap.put(requestKey, newValue);
            chainRepDriver.lastXidSeen = requestXid;
            chainRepDriver.pendingUpdateRequests.put(requestXid, new ChainRepDriver.Triple(requestKey, newValue, null));

        /*
        Finish the whole UpdateRequest - Response loop
        So that the whole chain doesn't have to wait because of one request
         */
            log.info("Completing responseObserver with XID " + requestXid);
            responseObserver.onNext(UpdateResponse.newBuilder().build());
            responseObserver.onCompleted();
            log.info("...Done");

            log.info("Is Tail: " + chainRepDriver.isTail);

            //If you are tail, no need to send the message down further. Just ack back
            if (chainRepDriver.isTail) {
                log.info("Acking back");
                chainRepDriver.ack(requestXid);
            }
            //If you are not tail, send the message down the chain
            else if (chainRepDriver.successorContacted) {
                chainRepDriver.updateSuccessor(requestKey, newValue, requestXid);
            }

            log.info("Update completed");
        }
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     *
     * @param request           : newSucc request received by replica
     * @param responseObserver: corresponding responseObserver
     *
     *                          <post>
     *                          The newSucc Request informs the pred that there is a new Successor
     *                          Replicas will ignore requests from replicas with zk Views older than theirs
     *                          And will sync their views if the successor has a newer view before proceeding.
     *                          </post>
     */
    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        synchronized (chainRepDriver) {
            log.info("newSuccessor called");

            long lastZxidSeen = request.getLastZxidSeen();
            int lastXid = request.getLastXid();
            int lastAck = request.getLastAck();
            String znodeName = request.getZnodeName();

            log.info("lastZxidSeen: " + lastZxidSeen + ", lastXid: " + lastXid + ", lastAck: " + lastAck + ", znodeName: " + znodeName);
            log.info("Replica lastZxidSeen: " + chainRepDriver.lastZxidSeen);

            //If the zk view of the contacting replica is stale, ignore request
            if (lastZxidSeen < chainRepDriver.lastZxidSeen) {
                log.info("Replica contacting has a stale zk view, ignoring request");
                rejectNewSuccRequest(responseObserver);
            }
            //If the contacting replica has the same view as replica
            else if (lastZxidSeen == chainRepDriver.lastZxidSeen) {
                log.info("Successor Replica Name: " + chainRepDriver.successorZNode);

                //Check that the replica that has sent newSucc is supposed to be replica's successor
                //As seen from the oracle (zk view)
                if (Objects.equals(chainRepDriver.successorZNode, znodeName)) {
                    getSuccUpToSpeed(lastAck, lastXid, znodeName, responseObserver);
                }
                //If the replica doesn't match oracle view
                else {
                    log.info("Replica does not match replica seen in zk view , ignoring request");
                    rejectNewSuccRequest(responseObserver);
                }
            }
            // If the contacting replica has a more recent view of the chain than current replica
            // Call sync before you process the newSucc request
            else {
                log.info("Contacting replica has newer zk view, syncing request");
                chainRepDriver.zk.sync(chainRepDriver.zkDataDir, (result, extra1, extra2) -> {
                    if (result == Code.OK_VALUE && Objects.equals(chainRepDriver.successorZNode, znodeName)) {
                        getSuccUpToSpeed(lastAck, lastXid, znodeName, responseObserver);
                    }
                }, null);
            }
            log.info("exiting newSuccessor synchronized block");
        }
    }


    /**
     * <pre></pre>
     *
     * @param lastAck           : Last Ack'd transaction from candidate newSucc
     * @param lastXid           : Last Xid seen from candidate newSucc
     * @param znodeName         :Name of candidate newSucc
     * @param responseObserver: corresponding responseObserver for the newSucc request
     *
     * <p></p>
     *                          <post>
     *                          Driver logic for handling a new successor once we have determined that it is a valid
     *                          candidate for being replica's new successor
     *                          </post>
     */
    public void getSuccUpToSpeed(int lastAck, int lastXid, String znodeName, StreamObserver<NewSuccessorResponse> responseObserver) {

        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(1);

        //If lastXid is -1, do a state transfer
        if (lastXid == -1) {
            builder.setRc(0)
                    .putAllState(chainRepDriver.replicaMap);
        }

        // Otherwise, send successor all the requests it has missed
        for (int xid = lastXid + 1; xid <= chainRepDriver.lastXidSeen; xid += 1) {
            if (chainRepDriver.pendingUpdateRequests.containsKey(xid)) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(xid)
                        .setKey(chainRepDriver.pendingUpdateRequests.get(xid).getMapKey())
                        .setNewValue(chainRepDriver.pendingUpdateRequests.get(xid).getMapVal())
                        .build());
            }
        }

        // Ack requests that they have acked since they are your successor now
        for (int myAckXid = chainRepDriver.lastAckSeen + 1; myAckXid <= lastAck; myAckXid += 1) {
            chainRepDriver.ack(myAckXid);
        }

        builder.setLastXid(chainRepDriver.lastXidSeen);

        log.info("Values in the newSucc response:");
        log.info("rc: " + builder.getRc() + ", lastXid: " + builder.getLastXid() + ", state: " + builder.getStateMap() + ", sent: " + builder.getSentList());

        try {
            //Open a channel between replica and its successor for communication
            String zkData = new String(chainRepDriver.zk.getData(chainRepDriver.zkDataDir + "/" + znodeName, false, null));
            chainRepDriver.successorAddress = zkData.split("\n")[0];
            chainRepDriver.successorChannel = chainRepDriver.createChannel(chainRepDriver.successorAddress);
            log.info("New Successor Established");
            log.info("Successor Address: " + chainRepDriver.successorAddress);
            log.info("Successor Name: " + zkData.split("\n")[1]);
        } catch (InterruptedException | KeeperException e) {
            log.info("error in getting successor address from zookeeper");
        }
        //Important!
        chainRepDriver.successorContacted = true;
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * @param request          : AckRequest received by replica
     * @param responseObserver : Corresponding requestObserver
     */
    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        try {
            //Use a semaphore to work around the sync block of the request loop in inc, etc
            log.info("Ack called. Trying to acquire ack semaphore");
            chainRepDriver.ackSemaphore.acquire();
            int requestXid = request.getXid();
            log.info("xid: " + requestXid);

            log.info("Completing responseObserver with XID " + requestXid);
            responseObserver.onNext(AckResponse.newBuilder().build());
            responseObserver.onCompleted();
            log.info("Done");


            //If replica is head, send a response back to client
            if (chainRepDriver.isHead) {
                chainRepDriver.lastAckSeen = requestXid;
                log.info("sending response back to client");
                StreamObserver<HeadResponse> headResponseStreamObserver = chainRepDriver.pendingUpdateRequests.get(requestXid).getObserver();
                chainRepDriver.pendingUpdateRequests.remove(requestXid);
                headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                headResponseStreamObserver.onCompleted();
            }
            //Otherwise, ack the request back to predecessor
            else {
                chainRepDriver.ack(requestXid);
            }


        } catch (InterruptedException e) {
            log.info("Problem acquiring semaphore");
            log.info(e.getMessage());
        } finally {
            log.info("releasing semaphore for ack");
            chainRepDriver.ackSemaphore.release();
        }
    }

    // Util methods

    private static void rejectNewSuccRequest(StreamObserver<NewSuccessorResponse> responseObserver) {
        responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(-1).build());
        responseObserver.onCompleted();
    }
}

