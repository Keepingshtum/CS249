package edu.sjsu.cs249.chainreplication;
import picocli.CommandLine;
import picocli.CommandLine.Parameters;

import java.util.concurrent.Callable;
import java.util.logging.Logger;

public class Main {
    static Logger mainLog = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        System.exit(new CommandLine(new ServerCli()).execute(args));
    }

    static class ServerCli implements Callable<Integer> {
        Logger log = Logger.getLogger(ServerCli.class.getName());
        @Parameters(index = "0", description = "ChainRep Name")
        String name;

        @Parameters(index = "1", description = "grpc host:port to listen on")
        String grpcHostPort;


        @Parameters(index = "2", description = "server to talk to")
        String serverList;

        @Parameters(index = "3", description = "ZooKeeper Path where replicas live")
        String controlPath;

        @Override
        public Integer call() throws Exception {
            new ChainRepDriver(name, grpcHostPort, serverList, controlPath).start();
            return 0;
        }
    }
}