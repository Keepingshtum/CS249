//package edu.sjsu.cs249.zoo;
//
//import edu.sjsu.cs249.zoo.Grpc.Read1Response;
//import io.grpc.ManagedChannel;
//import io.grpc.ManagedChannelBuilder;
//import io.grpc.StatusRuntimeException;
//import io.grpc.stub.StreamObserver;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//
////public class ClientService {
////
////
////    @CommandLine.Command(subcommands = { ClientCli.class})
////    static class Cli {}
////    @CommandLine.Command(name = "client", mixinStandardHelpOptions = true, description = "abd client for class.")
////    static class ClientCli implements Callable<Integer> {
////        @CommandLine.Parameters(index = "0", description = "host:port to connect to.")
////        String serverPort;
//
//        public class MyClientService {
//
//            //Don't check for acks in read1
//
//            public void writeToServer(long register, long value, String[] serverList) {
//                //set the lock to wait until majority
//                //is achieved
//                final CountDownLatch lock = new CountDownLatch(serverList.length /2 +1);
//
//                for (String server: serverList) {
////                    System.out.printf("Going to write `%s` on %s\n", value, server);
//                    var channel = createChannel(server);
//                    var stub = this.createStub(channel);
//                    StreamObserver<Grpc.WriteResponse> streamObserver = new StreamObserver() {
//                        @Override
//                        public void onNext(Object o) {
//
//                        }
//
//                        @Override
//                        public void onError(Throwable throwable) {
//                            channel.shutdownNow();
//                            System.out.println("failed");
//                        }
//
//                        @Override
//                        public void onCompleted() {
//                            channel.shutdownNow();
//                            lock.countDown();
//                        }
//                    };
//                    stub.write(Grpc.WriteRequest.newBuilder().setValue(value).build(),streamObserver);
//                    try {
//                        if(!lock.await(3, TimeUnit.SECONDS)) {
//                            System.out.println("failed");
//                        }
//                        System.out.println("success");
////                        System.out.printf("Server %s OK ", server);
//
//                    } catch (StatusRuntimeException e) {
//                        System.out.printf("failed");
////                        System.out.println("Stack trace:");
//                        e.printStackTrace();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//
//            public void readFromServer(Long register, String[] serverList) {
//                //set the lock to wait until majority
//                //is achieved
//                final CountDownLatch lock = new CountDownLatch(serverList.length /2 +1);
//                List<Long> candidateVals = new ArrayList<>();
//                List<Long> candidateTimes = new ArrayList<>();
//                Long maxval = 1L;
//                Long maxTimestamp = 1L;
//
//                for (String server: serverList) {
////                    System.out.printf("Going to read from server %s on %s\n", register, server);
//                    var channel = createChannel(server);
//                    var stub = this.createStub(channel);
//                    StreamObserver<Read1Response> streamObserverRead1 = new StreamObserver<>() {
//                        @Override
//                        public void onNext(Read1Response read1Response) {
//                            int incomingRC = read1Response.getRc();
//                            if (incomingRC == 0) {
//                                Long incomingTimestamp = read1Response.getLabel();
//                                Long incomingValue = read1Response.getValue();
//                                candidateVals.add(incomingValue);
//                                candidateTimes.add(incomingTimestamp);
//                            }
//
//                        }
//
//                        @Override
//                        public void onError(Throwable throwable) {
//                            channel.shutdownNow();
//                            System.out.println("failed");
//                        }
//
//                        @Override
//                        public void onCompleted() {
//                            channel.shutdownNow();
//                            lock.countDown();
//                        }
//                    };
//                    stub.read1(Grpc.Read1Request.newBuilder().build(), streamObserverRead1);
//
//                }
//                    try {
//                        // Wait for a majority for these before returning value
//                        if(!lock.await(3, TimeUnit.SECONDS)) {
//                            System.out.println("failed");
//                        }
//                        else {
//                             maxval = Collections.max(candidateVals);
//                             maxTimestamp = Collections.max(candidateTimes);
//                            System.out.println(maxval+"("+maxTimestamp+")");
//                        }
//
//                    } catch (StatusRuntimeException e) {
//                        System.out.printf("failed");
////                        System.out.println("Stack trace:");
//                        e.printStackTrace();
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//
//                for (String server: serverList) {
//                    var channel = createChannel(server);
//                    var stub = this.createStub(channel);
//                    StreamObserver<Grpc.Read2Response> streamObserverRead2 = new StreamObserver<>() {
//                        @Override
//                        public void onNext(Grpc.Read2Response read2Response) {
//
//                        }
//
//                        @Override
//                        public void onError(Throwable throwable) {
//                            channel.shutdownNow();
//                            System.out.println("failed");
//                        }
//
//                        @Override
//                        public void onCompleted() {
//                            channel.shutdownNow();
//                            lock.countDown();
//                        }
//                    };
//                    stub.read2(Grpc.Read2Request.newBuilder().setLabel(maxTimestamp).setValue(maxval).build(),streamObserverRead2);
//                }
//
//
//                try {
//                    // Wait for a majority for these before returning value
//                    if(!lock.await(3, TimeUnit.SECONDS)) {
//                        System.out.println("failed");
//                    }
////                        System.out.println("success");
//                } catch (StatusRuntimeException e) {
//                    System.out.printf("failed");
////                        System.out.println("Stack trace:");
//                    e.printStackTrace();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//            }
//
//            private ABDServiceGrpc.ABDServiceStub createStub(ManagedChannel channel) {
//                return ABDServiceGrpc.newStub(channel);
//            }
//
//            private ManagedChannel createChannel(String serverAddress) {
//                var lastColon = serverAddress.lastIndexOf(':');
//                var host = serverAddress.substring(0, lastColon);
//                var port = Integer.parseInt(serverAddress.substring(lastColon+1));
//                var channel = ManagedChannelBuilder
//                        .forAddress(host, port)
//                        .usePlaintext()
//                        .build();
//                return channel;
//            }
//
//
//
//        }
////
////        @Override
////        public Integer call() throws Exception {
////            System.out.printf("will contact %s\n", serverPort);
////            var lastColon = serverPort.lastIndexOf(':');
////            var host = serverPort.substring(0, lastColon);
////            var port = Integer.parseInt(serverPort.substring(lastColon+1));
////            var channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
//////            var stub = HereServiceGrpc.newBlockingStub(channel);
//////            System.out.println(stub.hello(Grpc.HelloRequest.newBuilder().setName("ben").build()).getMessage());
////            return 0;
////        }
////    }
////
////}
