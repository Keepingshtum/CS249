package edu.sjsu.cs249.zoo;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class Main {

    static Logger mainLog = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        System.exit(new CommandLine(new ServerCli()).execute(args));
    }

    static class ServerCli implements Callable<Integer> {
        Logger log = Logger.getLogger(ServerCli.class.getName());
        @Parameters(index = "0", description = "ZooKeeper Name")
        String zkName;

        @Parameters(index = "1", description = "grpc host:port to listen on.")
        String zkServerList;

        @Parameters(index = "2", description = "zookeeper_server_list of host:ports to listen on.")
        String grpcHostPort;

        @Parameters(index = "3", description = "ZooKeeper Lunch Path")
        String lunchPath;

        @Override
        public Integer call() throws Exception {

            //Zookeper paths
            String readyPath = lunchPath+"/readyforlunch";
            String leaderPath = lunchPath+"/leader";
            String lunchtimePath = lunchPath+"/lunchtime";
            String employeePath = lunchPath+"/employee/zk-"+zkName;
            String lunchEmployeePath = lunchPath+"/zk-"+zkName;

            final String storageFilePath = "storage";

            var lastColon = zkServerList.lastIndexOf(':');
            var host = zkServerList.substring(0, lastColon);
            var serverPort = Integer.parseInt(zkServerList.substring(lastColon+1));
            ZooKeeper zk = new ZooKeeper(grpcHostPort, 10000, (e) -> {log.info(String.format("%s",e));});

            Storage dataStore = readStorage(storageFilePath);

            final int cooldown = dataStore.getCooldown();
//            writeStorage(storageFilePath, dataStore);





            //Anonymous watcher class to listen for /lunchtime deletion
            Watcher lunchtimeDeleteWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.info(event.getType()+":"+event.getPath());
                    resetWatcher(zk,lunchtimePath,this);
                    log.info("Created delete watch on " + lunchtimePath);

                    if (event.getType() == Event.EventType.NodeDeleted && event.getPath().equals(lunchtimePath)) {
                        try {


                            //If you are leader, remove leadernode
                            Stat leaderstat = zk.exists(leaderPath, false);
//                            log.info(leaderstat.toString());
                            if (leaderstat != null) {
                                zk.delete(leaderPath, -1);
                                log.info("Deleted leader node " + leaderPath);
                            }


                            // Remove node /lunch/zk-empname if it exists
                            Stat stat = zk.exists(lunchEmployeePath, false);
                            if (stat != null) {
                                zk.delete(lunchEmployeePath, -1);
                                log.info("Deleted node " + lunchEmployeePath);
                            }
                            log.info("Done");

                        } catch (KeeperException | InterruptedException e) {
                            // Handle exceptions
                            log.info("Exception occured"+ Arrays.toString(e.getStackTrace()));
                        }

                        finally {
                            // Re-register the watch on /lunch/lunchtime
//                            try {
//                                zk.setData(lunchtimePath,null,-1);
//                            } catch (KeeperException | InterruptedException e) {
//                                // Handle exceptions
//                            }
                            resetWatcher(zk,lunchtimePath,this);
                        }
                    }}
            };

            Watcher lunchtimeCreateWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.info(event.getType()+":"+event.getPath());
                    resetWatcher(zk,lunchtimePath,this);
                    log.info("Created create watch on " + lunchtimePath);

                    if (event.getType() == Event.EventType.NodeCreated && event.getPath().equals(lunchtimePath)) {
                        try {

                            //Get list of everything in lunch
                            List<String> children = zk.getChildren(lunchPath, false);
                            int numChildren = children.size();
                            log.info("Number of children in " + lunchPath + ": " + numChildren);

                            //Filter list on zx- and save to memory
                            List<String> filteredChildren = zk.getChildren(lunchPath, false)
                                    .stream()
                                    .filter(child -> child.startsWith("zk-"))
                                    .toList();

                            //Count no of zk-emps and update cooldown
                            int numZKs = filteredChildren.size();
                            if(dataStore.isLeader()){
                                if(numZKs>1) {
                                    dataStore.setCooldown(numZKs - 1);
                                }
                                else{
                                    dataStore.setCooldown(0);
                                }
                            }
                            log.info("zk-names are in total:"+numZKs);

                            //store lunch zxid
                            Stat statZxid = zk.exists(lunchtimePath,false);
                            log.info("Stat ZXID IS");
                            log.info(statZxid.toString());
                            long lunchZxid = statZxid.getCzxid(); // Problem line
                            log.info("zxid is:"+lunchZxid);

                            if(dataStore.isLeader()){
                                //TODO: Could be null... check later
                                dataStore.getAttendeesMap().put(Long.toString(lunchZxid), filteredChildren.toString());
                            }

                            //Save new info to memory
                            writeStorage(storageFilePath,dataStore);


                            //Set a delete watch if the path exists
                            zk.exists(lunchtimePath,lunchtimeDeleteWatcher);

                        } catch (KeeperException | InterruptedException e) {
                            // Handle exceptions
                        }

                    }}
            };

            //TODO: Create Watcher to check if leader dies


            //Anonymous watcher class to listen for /ready
            Watcher readyForLunchWatcher = new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    log.info(event.getType() +":"+event.getPath());
                    resetWatcher(zk,readyPath,this);

                    //Check for Skiprequest
                    if(!dataStore.isSkip()){
                        if (event.getType() == Event.EventType.NodeCreated && event.getPath().equals(readyPath)) {
                            try {

                                //Wait for the ready to lunch call
                                log.info(readyPath+" is created");
                                Stat leaderstat = zk.exists(leaderPath, false);
                                Stat lunchtimeStat = zk.exists(lunchtimePath,false);

                                //Check for cooldown
                                if(cooldown > 0) {
                                    log.info("Cooldown Enforced. Sleeping for "+cooldown+" seconds");
                                    Thread.sleep(cooldown* 1000L);
                                }

                                // Create the /lunch/zk-<empname> node to show you're attending lunch
                                Stat stat = zk.exists(lunchEmployeePath, false);
                                if (stat == null) {
                                    String createdPath = zk.create(lunchEmployeePath, ("zk-"+zkName).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                    log.info("Created ephemeral node " + createdPath);
                                }

                                if (leaderstat == null && lunchtimeStat == null) {
                                    String createdPath = zk.create(leaderPath, ("zk-"+zkName).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                                    log.info("Created ephemeral node " + createdPath);
                                    dataStore.setLeader(true);
                                }


//                            //Register in the /employee directory if you haven't already
//                            Stat stat = zk.exists(employeePath,false);
//                            if (stat == null) {
//                                String data = grpcHostPort.toString();
//                                String createdPath = zk.create(employeePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//                                log.info("Created ephemeral node " + createdPath);
//                            }
//



                            } catch (KeeperException | InterruptedException e) {
                                // Handle exceptions
                            } finally {
                                // Re-register the watch on /lunch/ready
                                resetWatcher(zk, readyPath, this);
                            }
                        }
                    }
                }
            };


            //Main logic starts here

            // Create the /lunch/employee/zk-<empname> node
            Stat stat = zk.exists(employeePath, false);
            if (stat == null) {
                String data = zkServerList;
                String createdPath = zk.create(employeePath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                log.info("Created ephemeral node " + createdPath);
            }

            try {

                // Check if the ready path exists
                stat = zk.exists(readyPath, readyForLunchWatcher);
                if (stat != null) {
                    // The ready path already exists, trigger the watcher manually
                    readyForLunchWatcher.process(new WatchedEvent(Watcher.Event.EventType.NodeCreated, null, readyPath));
                } else {
                    // The ready path does not exist yet, set the watcher
                    zk.exists(readyPath, readyForLunchWatcher);
                }
                //Remove leader node and lunch attending node after lunchtime goes away
                stat = zk.exists(lunchtimePath, lunchtimeCreateWatcher);

                if (stat != null) {
                    // The lunchtime path already exists, just watch for its deletion and clean up if necessary
                    zk.exists(lunchtimePath,lunchtimeDeleteWatcher);
                } else {
                    // The ready path does not exist yet, set the watcher
                    zk.exists(lunchtimePath,lunchtimeCreateWatcher);
                }


            } catch (KeeperException | InterruptedException e) {
                // Handle exceptions
            }

            //Create grpc server to handle requests
            Server server = ServerBuilder.forPort(serverPort)
                    .addService(new MyServerService())
                    .build();
            server.start();
            log.info(String.format("Listening on %s",grpcHostPort));
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
                try {
                    zk.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                log.info("Successfully stopped the server");
            }));
            server.awaitTermination();
            return 0;
        }
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
            mainLog.info("No file called storage found... initializing new datastore");
            //TODO: UPDATE AT THE END TO INIT EVERYTHING PROPERLY
            dataStore = new Storage(false, 0, new HashMap<>());
        }
        return dataStore;
    }

    private static void resetWatcher(ZooKeeper zk, String path, Watcher watcher) {
        try {
            mainLog.info("resetting watch for watcher" + path);
            zk.exists(path, watcher);
        } catch (KeeperException | InterruptedException e) {
            // Handle exceptions
        }
    }


        private static byte[] serialize(Object obj) {
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(obj);
                return bos.toByteArray();
            } catch (IOException e) {
                e.printStackTrace();
                return new byte[0];
            }
        }

        private static Object deserialize(byte[] bytes) {
            try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                 ObjectInput in = new ObjectInputStream(bis)) {
                return in.readObject();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


//
//    @Command(name = "client", mixinStandardHelpOptions = true, description = "Create an ADB client")
//    static class ClientCli {
//        @Parameters(index = "0", description = "comma separated list of servers to use.")
//        String serverPorts;
//
//        @Command
//        public void read(@Parameters(paramLabel = "register") long register) {
//            System.out.printf("Going to read %s from %s\n", register, serverPorts);
////            System.out.println("Read called");
//            String[] serversList = serverPorts.split(",");
//            new MyClientService().readFromServer(register, serversList);
//        }
//
//        @Command
//        public void write(@Parameters(paramLabel = "register") long register,
//                          @Parameters(paramLabel = "value") long value) {
//            System.out.printf("Going to write %s to %s on %s\n", value, register, serverPorts);
//            String[] serversList = serverPorts.split(",");
//            new MyClientService().writeToServer(register, value, serversList);
//        }
//    }
//}