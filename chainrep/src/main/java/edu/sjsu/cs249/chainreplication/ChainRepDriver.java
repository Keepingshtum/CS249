package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class ChainRepDriver {

    static Logger log = Logger.getLogger(ChainRepDriver.class.getName());

    String name;
    String grpcHostPort;
    String serverList;
    String controlPath;
    ZooKeeper zk;
    boolean isHead;
    boolean isTail;
    long lastZxidSeen;
    int lastUpdateRequestXid;
    int lastAckXid;
    String myZNodeName;
    String predecessorAddress;
    String successorAddress;
    String successorZNode;
    boolean successorContacted;

    HashMap <String, Integer> replicaMap;
    List<String> replicas;

    HashMap<Integer, StreamObserver<HeadResponse>> pendingHeadStreamObserver;

    HashMap <Integer, Triple> pendingUpdateRequests;

    ManagedChannel successorChannel;
    ManagedChannel predecessorChannel;

    final Semaphore ackSemaphore;


    ChainRepDriver(String name, String grpcHostPort, String zookeeper_server_list, String control_path) {
        this.name = name;
        this.grpcHostPort = grpcHostPort;
        this.serverList = zookeeper_server_list;
        this.controlPath = control_path;
        isHead = false;
        isTail = false;
        lastZxidSeen = -1;
        lastUpdateRequestXid = -1;
        lastAckXid = -1;
        successorAddress = "";
        successorZNode = "";
        pendingHeadStreamObserver = new HashMap<>();
        replicaMap = new HashMap<>();
        successorContacted = false;
        pendingUpdateRequests = new HashMap<>();
        ackSemaphore = new Semaphore(1);
    }
    void start () throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(serverList, 10000, System.out::println);
        myZNodeName = zk.create(controlPath + "/replica-", (grpcHostPort + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        log.info("Created znode name: " + myZNodeName);

        //Remove control path from name
        myZNodeName = myZNodeName.replace(controlPath + "/", "");

        this.getChildrenInPath();
        this.checkMembership();

        HeadService headServer = new HeadService(this);
        TailService tailServer = new TailService(this);
        ReplicaService replicaServer = new ReplicaService(this);
        DebugService debugServer = new DebugService(this);

        Server server = ServerBuilder.forPort(Integer.parseInt(grpcHostPort.split(":")[1]))
                .addService(headServer)
                .addService(tailServer)
                .addService(replicaServer)
                .addService(debugServer)
                .build();
        server.start();
        log.info(String.format("will listen on port %s\n", server.getPort()));
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.info("Error while closing zookeeper instance");
                e.printStackTrace();
            }
            server.shutdown();
            log.info("Successfully stopped the server");
        }));
        server.awaitTermination();
    }

    //TODO: REFACTOR
    private Watcher childrenWatcher() {
        return watchedEvent -> {
            log.info("childrenWatcher triggered");
            log.info("WatchedEvent: " + watchedEvent.getType() + " on " + watchedEvent.getPath());
            try {
                ChainRepDriver.this.getChildrenInPath();
                ChainRepDriver.this.checkMembership();
            } catch (InterruptedException | KeeperException e) {
                log.info("Error getting children with getChildrenInPath()");
            }
        };
    }

    //Set up a watcher on the path to watch children changes
    private void getChildrenInPath() throws InterruptedException, KeeperException {
        Stat replicaPath = new Stat();
        List<String> children = zk.getChildren(controlPath, childrenWatcher(), replicaPath);
        lastZxidSeen = replicaPath.getPzxid();
        replicas = children.stream().filter(child -> child.contains("replica-")).toList();
        log.info("Current replicas: " + replicas + ", lastZxidSeen: " + lastZxidSeen);
    }

    //To track head and tail status
    void checkMembership() throws InterruptedException, KeeperException {

        //Sort replicas currently in the path
        List<String> replicaList = replicas.stream().sorted(Comparator.naturalOrder()).toList();

        //Check if you are head or tail
        isHead = replicaList.get(0).equals(myZNodeName);
        isTail = replicaList.get(replicaList.size() - 1).equals(myZNodeName);

        log.info("isHead: " + isHead + ", isTail: " + isTail);

        callPredecessor(replicaList);
        setSuccessorData(replicaList);
    }

    void callPredecessor(List<String> sortedReplicas) throws InterruptedException, KeeperException {
        //TODO: Refactor
        //Don't need to call predecessor if you're head! Reset predecessor values
        if (isHead) {
            if (predecessorChannel != null) {
                predecessorChannel.shutdownNow();
            }
            predecessorAddress = "";
            return;
        }

        log.info("calling pred");

        int index = sortedReplicas.indexOf(myZNodeName);
        String predecessorReplicaName = sortedReplicas.get(index - 1);

        String data = new String(zk.getData(controlPath + "/" + predecessorReplicaName, false, null));

        String newPredecessorAddress = data.split("\n")[0];
        String newPredecessorName = data.split("\n")[1];

        // If last predecessor is not the same as the last one, then call the new one!
        if (!newPredecessorAddress.equals(predecessorAddress)) {
            log.info("new predecessor");
            log.info("newPredecessorAddress: " + newPredecessorAddress);
            log.info("newPredecessorName: " + newPredecessorName);

            log.info("calling newSuccessor of new predecessor.");
            log.info("params:" +
                    ", lastZxidSeen: " + lastZxidSeen +
                    ", lastXid: " + lastUpdateRequestXid +
                    ", lastAck: " + lastAckXid +
                    ", myReplicaName: " + myZNodeName);

            predecessorAddress = newPredecessorAddress;
            predecessorChannel = this.createChannel(predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(predecessorChannel);
            var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                    .setLastZxidSeen(lastZxidSeen)
                    .setLastXid(lastUpdateRequestXid)
                    .setLastAck(lastAckXid)
                    .setZnodeName(myZNodeName).build();
            NewSuccessorResponse newSuccessorResponse = stub.newSuccessor(newSuccessorRequest);
            long rc = newSuccessorResponse.getRc();
            log.info("Response received");
            log.info("rc: " + rc);
            if (rc == -1) {
                try {
                    getChildrenInPath();
                    checkMembership();
                } catch (InterruptedException | KeeperException e) {
                    log.info("Error getting children with getChildrenInPath()");
                }
            } else if (rc == 0) {
                lastUpdateRequestXid = newSuccessorResponse.getLastXid();
                log.info("lastUpdateRequestXid: " + lastUpdateRequestXid);
                log.info("state value:");
                for (String key : newSuccessorResponse.getStateMap().keySet()) {
                    replicaMap.put(key, newSuccessorResponse.getStateMap().get(key));
                    log.info(key + ": " + newSuccessorResponse.getStateMap().get(key));
                }
                addPendingUpdateRequests(newSuccessorResponse);
            } else {
                lastUpdateRequestXid = newSuccessorResponse.getLastXid();
                log.info("lastUpdateRequestXid: " + lastUpdateRequestXid);
                addPendingUpdateRequests(newSuccessorResponse);
            }
        }
    }

    void setSuccessorData(List<String> sortedReplicas) {
        //TODO: REFACTOR
        if (isTail) {
            successorZNode = "";
            successorAddress = "";
            successorContacted = false;
            return;
        }

        int index = sortedReplicas.indexOf(myZNodeName);
        String newSuccessorZNode = sortedReplicas.get(index + 1);

        // If the curr successor replica name matches the new one,
        // then hasSuccessorContacted should be the old value of hasSuccessorContacted
        // else it should be false
        if (!newSuccessorZNode.equals(successorZNode)) {
            successorZNode = newSuccessorZNode;
            successorContacted = false;
            log.info("new successor");
            log.info("successorZNode: " + successorZNode);
        }
    }
    private void addPendingUpdateRequests(NewSuccessorResponse result) {
        //TODO:REFAACTOR
        List<UpdateRequest> sent = result.getSentList();
        log.info("sent requests: ");
        for (UpdateRequest request : sent) {
            String key = request.getKey();
            int newValue = request.getNewValue();
            int xid = request.getXid();
            pendingUpdateRequests.put(xid, new Triple(key, newValue,null));
            log.info("xid: " + xid + ", key: " + key + ", value: " + newValue);
        }

        if (isTail && pendingUpdateRequests.size() > 0) {
            log.info("I am tail, have to ack back all pending requests!");
            for (int xid: pendingUpdateRequests.keySet()) {
                ack(xid);
            }
        }
    }



    public ManagedChannel createChannel(String serverAddress){
        var lastColon = serverAddress.lastIndexOf(':');
        var host = serverAddress.substring(0, lastColon);
        var port = Integer.parseInt(serverAddress.substring(lastColon+1));
        return ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();
    }

    public void ack (int xid) {
        log.info("Sending ack to predecessor: " + predecessorAddress);

        //Update the last acknowledged Xid and remove it from pending requests
        lastAckXid = xid;
        pendingUpdateRequests.remove(xid);
        log.info("lastAckXid: " + lastAckXid);

        //Send ack across the channel
        var channel = createChannel(predecessorAddress);
        var stub = ReplicaGrpc.newBlockingStub(channel);
        var ackRequest = AckRequest.newBuilder()
                .setXid(xid).build();
        stub.ack(ackRequest);
        channel.shutdownNow();
    }

    public void updateSuccessor(String key, int newValue, int xid) {
        synchronized (this) {
            log.info("making update call to successor: " + successorAddress);
            log.info("params:" + ", xid: " + xid + ", key: " + key + ", newValue: " + newValue);
            var channel = createChannel(successorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var updateRequest = UpdateRequest.newBuilder()
                    .setXid(xid)
                    .setKey(key)
                    .setNewValue(newValue)
                    .build();
            stub.update(updateRequest);
            channel.shutdownNow();
        }
    }

    //Stores Key, Val, responseObserver for the HeadResponse class
    // Used in pendingRequestsMap as {Xid, Key,Val,responseObserver}
    // To ack responses once ack is seen
    static class Triple {
        String mapKey;
        int mapVal;

        StreamObserver<HeadResponse> observer;

        public Triple(String mapKey, int mapVal, StreamObserver<HeadResponse> observer) {
            this.mapKey = mapKey;
            this.mapVal = mapVal;
            this.observer = observer;
        }

        public StreamObserver<HeadResponse> getObserver() {
            return observer;
        }

        public void setObserver(StreamObserver<HeadResponse> observer) {
            this.observer = observer;
        }

        public String getMapKey() {
            return mapKey;
        }

        public void setMapKey(String mapKey) {
            this.mapKey = mapKey;
        }

        public int getMapVal() {
            return mapVal;
        }

        public void setMapVal(int mapVal) {
            this.mapVal = mapVal;
        }
    }

}

