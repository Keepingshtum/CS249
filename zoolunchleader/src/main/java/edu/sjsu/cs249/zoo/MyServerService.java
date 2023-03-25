package edu.sjsu.cs249.zoo;

import edu.sjsu.cs249.zooleader.Grpc;
import edu.sjsu.cs249.zooleader.ZooLunchGrpc;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Logger;

//public class ServerService {

//    @CommandLine.Command(subcommands = { ServerCli.class})
//    static class Cli {}
//    @CommandLine.Command(name = "server", mixinStandardHelpOptions = true, description = "abd server for class.")
//    static class ServerCli implements Callable<Integer> {
//
//        @CommandLine.Parameters(index = "0", description = "port to connect listen on.")
//        int port;

        public class MyServerService extends ZooLunchGrpc.ZooLunchImplBase {

            static Logger log = Logger.getLogger(MyServerService.class.getName());
            final String storageFilePath = "storage";
            Storage dataStore = readStorage(storageFilePath);
            /**
             * <pre>
             * request an audit of the last or current lunch situation
             * </pre>
             *
             * @param request
             * @param responseObserver
             */
            @Override
            public void goingToLunch(Grpc.GoingToLunchRequest request, StreamObserver<Grpc.GoingToLunchResponse> responseObserver) {
                log.info("GoingToLunchRequest Received");
                //get Rc from store
                //if rc =0 i.e you were leader
                if(dataStore.isLeader()){

                    responseObserver.onNext(Grpc.GoingToLunchResponse.newBuilder()
                                .setRestaurant("PainTrain")
                                .setRc(0)
                                .setLeader(dataStore.getLeaderName())
                                .addAllAttendees(dataStore.getAttendeesForEachZxid().get(dataStore.getAttendeesForEachZxid().size() - 1))
                                .build());

                }



                //if rc =1 i.e you attended the lunch

                //TODO: Revisit this
                else if(!dataStore.isSkip()) {

                    responseObserver.onNext(Grpc.GoingToLunchResponse.newBuilder()
                            .setRc(1)
                            .setLeader(dataStore.getLeaderName())
                            .build());

                }

                //otherwise, you skipped the lunch

                else {
                    responseObserver.onNext(Grpc.GoingToLunchResponse.newBuilder()
                            .setRc(2)
                            .build());
                }

                responseObserver.onCompleted();


            }

            /**
             * <pre>
             * request an audit of the last or current lunch situation
             * </pre>
             *
             * @param request
             * @param responseObserver
             */
            @Override
            public void lunchesAttended(Grpc.LunchesAttendedRequest request, StreamObserver<Grpc.LunchesAttendedResponse> responseObserver) {
                //Return zxids of all lunches attended so far
                log.info("LunchesAttendedRequest Received");
                if(dataStore.getAttendeesMap() != new HashMap<String,String>()) {
                    responseObserver.onNext(Grpc.LunchesAttendedResponse.newBuilder().addAllZxids(new ArrayList<Long>()).build());
                }
            }

            /**
             * <pre>
             * request an audit of the last or current lunch situation
             * </pre>
             *
             * @param request
             * @param responseObserver
             */
            @Override
            public void getLunch(Grpc.GetLunchRequest request, StreamObserver<Grpc.GetLunchResponse> responseObserver) {
                //query map for zxid
                //get rc

                //if rc ==0 i.e you were leader

                //if rc =1 i.e you attended the lunch

                //else


                //                List<String> attendees =
//                responseObserver.onNext(Grpc.GoingToLunchResponse.newBuilder()
//                                .setRestaurant("PainTrain")
//                                .setRc()
//                                .setLeader()
//                                .addAllAttendees()
//                                .build());
//                responseObserver.onCompleted();

                log.info("GetLunchRequest Received");
            }

            /**
             * <pre>
             * skip the next readyforlunch announcement
             * </pre>
             *
             * @param request
             * @param responseObserver
             */
            @Override
            public void skipLunch(Grpc.SkipRequest request, StreamObserver<Grpc.SkipResponse> responseObserver) {
                log.info("SkipRequest Received");
                dataStore.setSkip(true);
                writeStorage(storageFilePath,dataStore);
            }

            /**
             * <pre>
             * exit your process right away
             * </pre>
             *
             * @param request
             * @param responseObserver
             */
            @Override
            public void exitZoo(Grpc.ExitRequest request, StreamObserver<Grpc.ExitResponse> responseObserver) {
                log.info("ExitRequest Received");
                responseObserver.onNext(Grpc.ExitResponse.newBuilder().build());
                responseObserver.onCompleted();
                System.exit(0);
            }


            private static void writeStorage(String storageFilePath, Storage dataStore) {
                try {
                    // Write dataStore to file
                    FileOutputStream fileOutputStream = new FileOutputStream(storageFilePath);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
                    objectOutputStream.writeObject(dataStore);
                    objectOutputStream.close();
                    fileOutputStream.close();
                } catch (Exception e) {
                    // Handle exception
                }
            }

            private static Storage readStorage(String storageFilePath) {
                Storage dataStore;
                try {
                    // Try to read from file
                    FileInputStream fileInputStream = new FileInputStream(storageFilePath);
                    ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
                    dataStore = (Storage) objectInputStream.readObject();
                    objectInputStream.close();
                    fileInputStream.close();
                } catch (Exception e) {
                    // If file not found or other exception, initialize new Storage object
                    log.info("No file called storage found... initializing new datastore");
                    dataStore = new Storage(false, 0, new HashMap<>());
                }
                return dataStore;
            }
            ////            boolean read1 = true;
//            boolean read2 = true;
//            boolean write = true;
//
//            Map<Long, List<Long>> serverHashMap = new HashMap<>();
//
//
//
//            @Override
//            public void name(NameRequest request, StreamObserver<NameResponse> responseObserver) {
//                System.out.println("Name Request Received");
//                responseObserver.onNext(NameResponse.newBuilder().setName("Anant Shukla").build());
//                responseObserver.onCompleted();
//            }
//
//            @Override
//            public void enableRequests(EnableRequest request, StreamObserver<EnableResponse> responseObserver) {
//                this.read1 = request.getRead1();
//                this.read2 = request.getRead2();
//                this.write = request.getWrite();
//                responseObserver.onNext(EnableResponse.newBuilder().build());
//                responseObserver.onCompleted();
//            }
//
//            @Override
//            public void write(WriteRequest request, StreamObserver<WriteResponse> responseObserver){
//                System.out.println("Got Write Request");
//                if (this.write) {
//                    System.out.println("Write Requests Enabled. Proceeding...");
//                    Long address = request.getAddr();
//                    Long timestamp = request.getLabel();
//                    Long value = request.getValue();
//
//                    //Log to console
//                    System.out.println(address);
//                    System.out.println(timestamp);
//                    System.out.println(value);
//
//                    //commit to memory
//                    List<Long> timestampAndValue = new ArrayList<Long>();
//                    timestampAndValue.add(timestamp);
//                    timestampAndValue.add(value);
//
//                    serverHashMap.put(address,timestampAndValue);
//                    responseObserver.onNext(WriteResponse.newBuilder().build());
//                    responseObserver.onCompleted();
//                }
//            }
//            @Override
//            public void read1(Read1Request request, StreamObserver<Read1Response> responseObserver) {
//                if (this.read1) {
//                    Long address = request.getAddr();
//                    System.out.println(address);
//
//                    //pass
//                    if (serverHashMap.containsKey(address)){
//                        List<Long> innerList= serverHashMap.get(address);
//                        Long timestamp = innerList.get(0);
//                        Long value =  innerList.get(1);
//                        responseObserver.onNext(Read1Response.newBuilder()
//                                .setRc(0)
//                                .setLabel(timestamp)
//                                .setValue(value)
//                                .build());
//                    }
//                    //fail
//                    else {
//                        responseObserver.onNext(Read1Response.newBuilder().setRc(1)
//                                .build());
//                    }
//                    responseObserver.onCompleted();
//                }
//            }
//
//            @Override
//            public void read2(Read2Request request, StreamObserver<Read2Response> responseObserver) {
//                if(this.read2) {
//                    Long address = request.getAddr();
//                    Long timestamp = request.getLabel();
//                    Long value = request.getValue();
//
//                    //Log to console
//                    System.out.println(address);
//                    System.out.println(timestamp);
//                    System.out.println(value);
//
//                    if(serverHashMap.containsKey(address)){
//                        List<Long> innerList= serverHashMap.get(address);
//                        Long listTimestamp = innerList.get(0);
//
//                        if (listTimestamp<timestamp){
//                            innerList.set(0,timestamp);
//                            innerList.set(1,value);
//                            serverHashMap.put(address,innerList);
//                        }
//                    }
//
//                    responseObserver.onNext(Read2Response.newBuilder().build());
//                    responseObserver.onCompleted();
//                }
//            }
//
//            @Override
//            public void exit(ExitRequest request, StreamObserver<ExitResponse> responseObserver) {
//                responseObserver.onNext(ExitResponse.newBuilder().build());
//                responseObserver.onCompleted();
//                System.exit(0);
//            }



        static private Context.Key<SocketAddress> REMOTE_ADDR = Context.key("REMOTE_ADDR");
//        @Override
//        public Integer call() throws Exception {
//            System.out.printf("listening on %d\n", port);
//            var server = ServerBuilder.forPort(port).intercept(new ServerInterceptor() {
//                @Override
//                public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> sc, Metadata h,
//                                                                             ServerCallHandler<ReqT, RespT> next) {
//                    var remote = sc.getAttributes().get(TRANSPORT_ATTR_REMOTE_ADDR);
//                    return Contexts.interceptCall(Context.current().withValue(REMOTE_ADDR, remote),
//                            sc, h, next);
//                }
//            }).addService(new MyServerService()).build();
//            server.start();
//            server.awaitTermination();
//            return 0;
//        }
//    }
//
//    public static void main(String[] args) {
//        System.exit(new CommandLine(new Cli()).execute(args));
//    }
}