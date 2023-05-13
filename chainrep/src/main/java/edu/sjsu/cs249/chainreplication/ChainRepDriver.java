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
import java.util.Arrays;
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
    String zkDataDir;
    ZooKeeper zk;
    boolean isHead;
    boolean isTail;
    long lastZxidSeen;
    int lastXidSeen;
    int lastAckSeen;
    String replicaZkName;
    String predecessorAddress;
    String successorAddress;
    String successorZNode;
    boolean successorContacted;

    HashMap <String, Integer> replicaMap;
    List<String> replicas;

    HashMap<Integer, StreamObserver<HeadResponse>> pendingHeadRequestStreamObserverMap;

    HashMap <Integer, Triple> pendingUpdateRequests;

    ManagedChannel successorChannel;
    ManagedChannel predecessorChannel;

    final Semaphore ackSemaphore;


    ChainRepDriver(String name, String grpcHostPort, String serverList, String ZkDataDir) {
        this.name = name;
        this.grpcHostPort = grpcHostPort;
        this.serverList = serverList;
        this.zkDataDir = ZkDataDir;
        isHead = false;
        isTail = false;
        lastZxidSeen = -1;
        lastXidSeen = -1;
        lastAckSeen = -1;
        successorAddress = "";
        successorZNode = "";
        pendingHeadRequestStreamObserverMap = new HashMap<>();
        replicaMap = new HashMap<>();
        successorContacted = false;
        pendingUpdateRequests = new HashMap<>();
        ackSemaphore = new Semaphore(1);
    }
    void startReplica() throws IOException, InterruptedException, KeeperException {
        zk = new ZooKeeper(serverList, 10000, System.out::println);
        replicaZkName = zk.create(zkDataDir + "/replica-", (grpcHostPort + "\n" + name).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        log.info("Created znode name: " + replicaZkName);

        //Remove control path from name
        replicaZkName = replicaZkName.replace(zkDataDir + "/", "");

        //Crucial Steps: Check path and check whether replica is head or tail
        this.getChildrenInPath();
        this.checkMembership();

        //Attach GRPC services
        HeadService headServer = new HeadService(this);
        ReplicaService replicaServer = new ReplicaService(this);
        TailService tailServer = new TailService(this);
        DebugService debugServer = new DebugService(this);

        Server server = ServerBuilder.forPort(Integer.parseInt(grpcHostPort.split(":")[1]))
                .addService(headServer)
                .addService(replicaServer)
                .addService(tailServer)
                .addService(debugServer)
                .build();
        server.start();

        log.info(String.format("listening on port %s\n", server.getPort()));

        //Shutdown logic
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                zk.close();
            } catch (InterruptedException e) {
                log.info("Error while closing zookeeper instance");
                e.printStackTrace();
            }
            server.shutdown();
            log.info("Successfully stopped server");
        }));
        server.awaitTermination();
    }

    private Watcher datadirWatcher() {
        return watchedEvent -> {
            log.info("datadirWatcher triggered");
            log.info("WatchedEvent: " + watchedEvent.getType() + " path " + watchedEvent.getPath());
            try {
                ChainRepDriver.this.getChildrenInPath();
                ChainRepDriver.this.checkMembership();
            } catch (InterruptedException | KeeperException e) {
                log.info("Error getting children with getChildrenInPath() or checkMembership()");
            }
        };
    }

    //Set up a watcher on the path to watch children changes
    private void getChildrenInPath() throws InterruptedException, KeeperException {
        Stat datadirPath = new Stat();
        List<String> children = zk.getChildren(zkDataDir, datadirWatcher(), datadirPath);
        lastZxidSeen = datadirPath.getPzxid();

        //Update list of replicas that the replica knows of
        replicas = children.stream().filter(child -> child.contains("replica-")).toList();
        log.info("Current replicas seen: " + replicas + ", lastZxidSeen: " + lastZxidSeen);
    }

    //To track head and tail status
    void checkMembership() throws InterruptedException, KeeperException {

        //Sort replicas currently in the path
        List<String> replicaList = replicas.stream().sorted(Comparator.naturalOrder()).toList();

        //Check if you are head or tail
        synchronized (this) {
            isHead = replicaList.get(0).equals(replicaZkName);
            isTail = replicaList.get(replicaList.size() - 1).equals(replicaZkName);

            log.info("isHead: " + isHead + ", isTail: " + isTail);

            callPredecessor(replicaList);
            checkSuccs(replicaList);
        }
    }

    void callPredecessor(List<String> sortedReplicas) throws InterruptedException, KeeperException {
        //If you are head, you don't have a predecessor

        //If you are freshly made head,
        if (isHead) {
            if (predecessorChannel != null) {
                predecessorChannel.shutdownNow();
            }
            predecessorAddress = "";
            return;
        }

        log.info("calling pred");

        int index = sortedReplicas.indexOf(replicaZkName);
        String predecessorReplicaName = sortedReplicas.get(index - 1);

        String data = new String(zk.getData(zkDataDir + "/" + predecessorReplicaName, false, null));

        String newPredecessorAddress = data.split("\n")[0];
        String newPredecessorName = data.split("\n")[1];

        // If last predecessor is not the same as the last one, then call the new one!
        if (!newPredecessorAddress.equals(predecessorAddress)) {
            for (String s : Arrays.asList("Calling New Predecessor at", "newPredecessorAddress: " + newPredecessorAddress, "newPredecessorName: " + newPredecessorName)) {
                log.info(s);
            }

            log.info("Calling newSuccessor with values");
            log.info( "lastZxidSeen: " + lastZxidSeen + ", lastXid: " + lastXidSeen + ", lastAck: " + lastAckSeen + ", myReplicaName: " + replicaZkName);

            predecessorAddress = newPredecessorAddress;
            predecessorChannel = this.createChannel(predecessorAddress);
            var stub = ReplicaGrpc.newBlockingStub(predecessorChannel);
            var newSuccessorRequest = NewSuccessorRequest.newBuilder()
                    .setLastZxidSeen(lastZxidSeen)
                    .setLastXid(lastXidSeen)
                    .setLastAck(lastAckSeen)
                    .setZnodeName(replicaZkName).build();
            NewSuccessorResponse newSuccessorResponse = stub.newSuccessor(newSuccessorRequest);
            long rc = newSuccessorResponse.getRc();
            log.info("Response received");
            log.info("rc: " + rc);
            if (rc == -1) {
                try {
                    getChildrenInPath();
                    checkMembership();
                } catch (InterruptedException | KeeperException e) {
                    log.info("Error getting children with getChildrenInPath() or checkMembership");
                }
            } else if (rc == 0) {
                lastXidSeen = newSuccessorResponse.getLastXid();
                log.info("Last Xid Seen: " + lastXidSeen);
                log.info("Replica Map is:");
                for (String key : newSuccessorResponse.getStateMap().keySet()) {
                    replicaMap.put(key, newSuccessorResponse.getStateMap().get(key));
                    log.info(key + ": " + newSuccessorResponse.getStateMap().get(key));
                }
                attachPendingUpdateRequests(newSuccessorResponse);
            } else {
                lastXidSeen = newSuccessorResponse.getLastXid();
                log.info("lastUpdateRequestXid: " + lastXidSeen);
                attachPendingUpdateRequests(newSuccessorResponse);
            }
        }
    }

    void checkSuccs(List<String> sortedReplicas) {
        //If replica isn't tail, only then it will have a successor
        if(!isTail) {
            int index = sortedReplicas.indexOf(replicaZkName);
            String newSuccessorZNode = sortedReplicas.get(index + 1);

            if (!newSuccessorZNode.equals(successorZNode)) {
                successorZNode = newSuccessorZNode;
                successorContacted = false;
                log.info("new successor");
                log.info("successorZNode: " + successorZNode);
            }
        }

        //if replica is tail
        else {
            successorZNode = "";
            successorAddress = "";
            successorContacted = false;
        }
    }
    private void attachPendingUpdateRequests(NewSuccessorResponse result) {
        List<UpdateRequest> sentList = result.getSentList();
        log.info("Adding Sent Requests: ");
        for (UpdateRequest request : sentList) {
            String requestKey = request.getKey();
            int requestNewValue = request.getNewValue();
            int requestXid = request.getXid();
            pendingUpdateRequests.put(requestXid, new Triple(requestKey, requestNewValue,null));
            log.info("xid: " + requestXid + ", key: " + requestKey + ", value: " + requestNewValue);
        }

        if (isTail && pendingUpdateRequests.size() > 0) {
            log.info("Replica is tail - ack all pending requests");

            //To prevent Concurrent Access Exceptions, use a dummy object
            HashMap <Integer, Triple> dummyPendingRequests = new HashMap<>(pendingUpdateRequests);

            for (int xid: dummyPendingRequests.keySet()) {
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
        lastAckSeen = xid;
        pendingUpdateRequests.remove(xid);
        log.info("lastAckXid: " + lastAckSeen);

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
            log.info("Updating Successor: " + successorAddress);
            log.info(" xid: " + xid + ", key: " + key + ", newValue: " + newValue);
            var channel = createChannel(successorAddress);
            var stub = ReplicaGrpc.newBlockingStub(channel);
            var updateRequest = UpdateRequest.newBuilder().setXid(xid)
                    .setKey(key).setNewValue(newValue).build();
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

