//package edu.sjsu.cs249.zoo;
//
//public class eass {
//    private void leaderElection() throws InterruptedException, KeeperException {
//        ZookeeperHWMetaData metaData = getZookeeperMetaDataFromMemory();
//        int coolDown = metaData.getCoolDown();
//        writeZookeeperMetaDataToMemory(metaData, false);
//        System.out.println("Cooldown:  " + coolDown);
//        TimerTask delayTask = new TimerTask() {
//            @Override
//            public void run() {
//                try {
//                    Watcher leaderWatch = new Watcher() {
//                        @Override
//                        public void process(WatchedEvent watchedEvent) {
//                            System.out.println("WatchedEvent: " + watchedEvent.getType() + " " + watchedEvent.getPath());
//                            Stat readyForLunchPathExist;
//                            try {
//                                readyForLunchPathExist = ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/readyforlunch", false);
//                            } catch (KeeperException | InterruptedException e) {
//                                System.out.println("Error in checking for /readyforlunch");
//                                e.printStackTrace();
//                                throw new RuntimeException(e);
//                            }
//                            Stat lunchtimePathExist;
//                            try {
//                                lunchtimePathExist = ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/lunchtime", false);
//                            } catch (KeeperException | InterruptedException e) {
//                                System.out.println("Error in checking for /lunchtime");
//                                e.printStackTrace();
//                                throw new RuntimeException(e);
//                            }
//
//                            if (readyForLunchPathExist != null && lunchtimePathExist == null) {
//                                try {
//                                    System.out.println("Setting /leader listener...");
//                                    ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/leader", this);
//                                } catch (KeeperException | InterruptedException e) {
//                                    System.out.println("Error in setting /leader listener");
//                                    e.printStackTrace();
//                                    throw new RuntimeException(e);
//                                }
//                                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
//                                    try {
//                                        ZookeeperAsyncInstance.this.makeLeader();
//                                    } catch (InterruptedException e) {
//                                        System.out.println("Error in makeLeader");
//                                        e.printStackTrace();
//                                        throw new RuntimeException(e);
//                                    }
//                                }
//                            } else {
//                                System.out.println("Not a valid scenario to set leader and /leader listener");
//                            }
//                        }
//                    };
//                    System.out.println("Setting /leader listener...");
//                    ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/leader", leaderWatch);
//                    if (ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/leader", false) == null) {
//                        Stat readyForLunchPathExist;
//                        try {
//                            readyForLunchPathExist = ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/readyforlunch", false);
//                        } catch (KeeperException | InterruptedException e) {
//                            System.out.println("Error in checking for /readyforlunch");
//                            e.printStackTrace();
//                            throw new RuntimeException(e);
//                        }
//                        Stat lunchtimePathExist;
//                        try {
//                            lunchtimePathExist = ZookeeperAsyncInstance.this.zk.exists(ZookeeperAsyncInstance.this.lunch + "/lunchtime", false);
//                        } catch (KeeperException | InterruptedException e) {
//                            System.out.println("Error in checking for /lunchtime");
//                            e.printStackTrace();
//                            throw new RuntimeException(e);
//                        }
//                        if (readyForLunchPathExist != null && lunchtimePathExist == null) {
//                            ZookeeperAsyncInstance.this.makeLeader();
//                        } else {
//                            System.out.println("Not a valid scenario to set leader");
//                        }
//                    } else {
//                        System.out.println("waiting for /leader");
//                    }
//                } catch (KeeperException | InterruptedException e) {
//                    e.printStackTrace();
//                    throw new RuntimeException(e);
//                }
//            }
//        };
//        leaderTimer = new Timer();
//        leaderTimer.schedule(delayTask, coolDown * 1000L);
//    }
//}
