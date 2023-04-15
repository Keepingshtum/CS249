package edu.sjsu.cs249.chainreplication;

import edu.sjsu.cs249.chain.GetRequest;
import edu.sjsu.cs249.chain.GetResponse;
import edu.sjsu.cs249.chain.TailChainReplicaGrpc;
import io.grpc.stub.StreamObserver;

import java.util.logging.Logger;

public class TailService extends TailChainReplicaGrpc.TailChainReplicaImplBase {


    static Logger log = Logger.getLogger(TailService.class.getName());


    ChainRepDriver chainRepDriver;

    public TailService(ChainRepDriver chainRepDriver) {
        this.chainRepDriver = chainRepDriver;
    }

    /**
     * @param request : The get request the tail receives
     * @param responseObserver : The corresponding responseObserver for the request
     */

    // The only additional thing the tail needs to do is respond to queries, so all we need to do is
    // Read the values stored in the hashmap already and send them back.
    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        log.info("get called");
        if (chainRepDriver.isTail) {
            String key = request.getKey();
            //Return 0 if the key is not found, as per the proto
            int value = chainRepDriver.replicaMap.getOrDefault(key, 0);
            log.info("get returned with value"+value);
            responseObserver.onNext(GetResponse.newBuilder().setValue(value).setRc(0).build());
            responseObserver.onCompleted();
        }

        //If you are not tail, return an empty response with rc = 1
        else{
            log.info("not tail. returning");
            responseObserver.onNext(GetResponse.newBuilder().setRc(1).build());
            responseObserver.onCompleted();
        }

    }
}
