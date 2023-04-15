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
     * @param request : Update request received by replica
     * @param responseObserver : Corresponding responseObserver
     *
     *                         Defines Behaviour for how the update requests propagate down the replica chain
     */
    @Override
    public void update(UpdateRequest request, StreamObserver<UpdateResponse> responseObserver) {
        log.info("Update request received");

        String key = request.getKey();
        int newValue = request.getNewValue();
        int xid = request.getXid();

        log.info("xid: " + xid + ", key: " + key + ", Value: " + newValue);


        //We always update our map before sending the request onwards down the chain
        chainRepDriver.replicaMap.put(key, newValue);

        chainRepDriver.lastUpdateRequestXid = xid;
        chainRepDriver.pendingUpdateRequests.put(xid, new ChainRepDriver.Triple(key,newValue,null));

        log.info("Is Tail: " + chainRepDriver.isTail);

        //If you are tail, no need to send the message down further. Just ack back
        if (chainRepDriver.isTail) {
            log.info("Acking back");
            chainRepDriver.ack(xid);
        }
        //If you are not tail, send the message down the chain
        else if (chainRepDriver.successorContacted) {
            chainRepDriver.updateSuccessor(key, newValue, xid);
        }
        responseObserver.onNext(UpdateResponse.newBuilder().build());
        responseObserver.onCompleted();
        log.info("Update completed");
    }

    /**
     * <pre>
     * will be called by a new successor to this replica
     * </pre>
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void newSuccessor(NewSuccessorRequest request, StreamObserver<NewSuccessorResponse> responseObserver) {
        //TODO: REFACTOR
        synchronized (chainRepDriver) {
            log.info("newSuccessor grpc called");

            long lastZxidSeen = request.getLastZxidSeen();
            int lastXid = request.getLastXid();
            int lastAck = request.getLastAck();
            String znodeName = request.getZnodeName();

            log.info("request params");
            log.info("lastZxidSeen: " + lastZxidSeen +
                    ", lastXid: " + lastXid +
                    ", lastAck: " + lastAck +
                    ", znodeName: " + znodeName);
            log.info("my lastZxidSeen: " + chainRepDriver.lastZxidSeen);

            if (lastZxidSeen < chainRepDriver.lastZxidSeen) {
                log.info("replica has older view of zookeeper than me, ignoring request");
                responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(-1).build());
                responseObserver.onCompleted();
            }
            else if (lastZxidSeen == chainRepDriver.lastZxidSeen) {
                log.info("my successorReplicaName: " + chainRepDriver.successorZNode);
                if (Objects.equals(chainRepDriver.successorZNode, znodeName)) {
                    successorProcedure(lastAck, lastXid, znodeName, responseObserver);
                } else {
                    log.info("replica is not the replica i saw in my view of zookeeper");
                    responseObserver.onNext(NewSuccessorResponse.newBuilder().setRc(-1).build());
                    responseObserver.onCompleted();
                }
            }
            else {
                log.info("replica has newer view of zookeeper than me, syncing request");
                chainRepDriver.zk.sync(chainRepDriver.controlPath, (i, s, o) -> {
                    if (i == Code.OK_VALUE && Objects.equals(chainRepDriver.successorZNode, znodeName)) {
                        successorProcedure(lastAck, lastXid, znodeName, responseObserver);
                    }
                }, null);
            }
            log.info("exiting newSuccessor synchronized block");
        }
    }

    public void successorProcedure(int lastAck, int lastXid, String znodeName, StreamObserver<NewSuccessorResponse> responseObserver) {
        //TODO:REFACTOR
        NewSuccessorResponse.Builder builder = NewSuccessorResponse.newBuilder();
        builder.setRc(1);

        //If lastXid is -1, send all state
        if (lastXid == -1) {
            builder.setRc(0)
                    .putAllState(chainRepDriver.replicaMap);
        }

        // send update request starting from their lastXid + 1 till your lastXid
        for (int xid = lastXid + 1; xid <= chainRepDriver.lastUpdateRequestXid; xid += 1) {
            if (chainRepDriver.pendingUpdateRequests.containsKey(xid)) {
                builder.addSent(UpdateRequest.newBuilder()
                        .setXid(xid)
                        .setKey(chainRepDriver.pendingUpdateRequests.get(xid).getMapKey())
                        .setNewValue(chainRepDriver.pendingUpdateRequests.get(xid).getMapVal())
                        .build());
            }
        }

        // ack back request start from your lastAck till their last ack
        for (int myAckXid = chainRepDriver.lastAckXid + 1; myAckXid <= lastAck; myAckXid += 1) {
            chainRepDriver.ack(myAckXid);
        }

        builder.setLastXid(chainRepDriver.lastUpdateRequestXid);

        log.info("response values:");
        log.info(
                "rc: " + builder.getRc() +
                        ", lastXid: " + builder.getLastXid() +
                        ", state: " + builder.getStateMap() +
                        ", sent: " + builder.getSentList());

        try {
            String data = new String(chainRepDriver.zk.getData(chainRepDriver.controlPath + "/" + znodeName, false, null));
            chainRepDriver.successorAddress = data.split("\n")[0];
            chainRepDriver.successorChannel = chainRepDriver.createChannel(chainRepDriver.successorAddress);
            log.info("new successor");
            log.info("successorAddress: " + chainRepDriver.successorAddress);
            log.info("successor name: " + data.split("\n")[1]);
        } catch (InterruptedException | KeeperException e) {
            log.info("error in getting successor address from zookeeper");
        }
        chainRepDriver.successorContacted = true;
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        //TODO: REFACTOR
        try {
            log.info("trying to acquire semaphore in ack");
            chainRepDriver.ackSemaphore.acquire();
            log.info("ack grpc called");
            int xid = request.getXid();

            log.info("xid: " + xid);

            if (chainRepDriver.isHead) {
                chainRepDriver.lastAckXid = xid;
                log.info("sending response back to client");
                StreamObserver<HeadResponse> headResponseStreamObserver = chainRepDriver.pendingUpdateRequests.get(xid).getObserver();
                chainRepDriver.pendingUpdateRequests.remove(xid);
                headResponseStreamObserver.onNext(HeadResponse.newBuilder().setRc(0).build());
                headResponseStreamObserver.onCompleted();
            } else {
                chainRepDriver.ack(xid);
            }
            responseObserver.onNext(AckResponse.newBuilder().build());
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

