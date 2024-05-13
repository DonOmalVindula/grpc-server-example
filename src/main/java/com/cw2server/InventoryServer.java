package com.cw2server;

import com.cw2client.DistributedLock;
import com.cw2client.DistributedTx;
import com.cw2client.DistributedTxCoordinator;
import com.cw2client.DistributedTxParticipant;
import com.grpc.generated.Item;
import com.grpc.generated.OperationStatus;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class InventoryServer {
    private DistributedLock leaderLock;
    private int serverPort;
    private AtomicBoolean isLeader = new
            AtomicBoolean(false);
    private byte[] leaderData;
    private ConcurrentHashMap<String, Item> inventory = new ConcurrentHashMap<>();
    DistributedTx transaction;
    AddItemServiceImpl addItemService;
    UpdateItemServiceImpl updateItemService;
    ListItemServiceImpl listItemService;
    DeleteItemServiceImpl deleteItemService;
    UpdateStockServiceImpl updateStockService;
    MakeReservationServiceImpl makeReservationService;
    PlaceOrderServiceImpl placeOrderService;


    public static void main(String[] args) throws Exception {
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181");
        // Provide port using args
        int serverPort;
        if (args.length < 1) {
            System.out.println("Usage InventoryServer <port>");
            System.exit(1);
        }
        serverPort = Integer.parseInt(args[0]);
        InventoryServer server = new InventoryServer("localhost", serverPort);
        server.startServer();
    }

    public InventoryServer(String host, int port) throws
            InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new
                DistributedLock("InventoryServerCluster",
                buildServerData(host, port));

        addItemService = new AddItemServiceImpl(this);
        updateItemService = new UpdateItemServiceImpl(this);
        listItemService = new ListItemServiceImpl(this);
        deleteItemService = new DeleteItemServiceImpl(this);
        updateStockService = new UpdateStockServiceImpl(this);
        makeReservationService = new MakeReservationServiceImpl(this);
        placeOrderService = new PlaceOrderServiceImpl(this);

        transaction = new DistributedTxParticipant(addItemService);
    }

    public void startServer() throws IOException,
            InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort(serverPort)
                .addService(addItemService)
                .addService(updateItemService)
                .addService(listItemService)
                .addService(deleteItemService)
                .addService(updateStockService)
                .addService(makeReservationService)
                .addService(placeOrderService)
                .build();
        server.start();
        System.out.println("InventoryServer Started and ready to accept requests on port " + serverPort);
        tryToBeLeader();
        server.awaitTermination();
    }
    public DistributedTx getTransaction() {
        return transaction;
    }

    private void beTheLeader() {
        System.out.println("I got the leader lock. Now acting as primary");
                isLeader.set(true);
        transaction = new DistributedTxCoordinator(addItemService);
    }

    public static String buildServerData(String IP, int
            port) {
        StringBuilder builder = new StringBuilder();
        builder.append(IP).append(":").append(port);
        return builder.toString();
    }

    public boolean isLeader() {
        return isLeader.get();
    }
    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    private void tryToBeLeader() throws KeeperException,
            InterruptedException {
        Thread leaderCampaignThread = new Thread(new
                LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    public void setInventory(ConcurrentHashMap<String, Item> inventory) {
        this.inventory = inventory;
    }

    public ConcurrentHashMap<String, Item> getInventory() {
        return inventory;
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }
    public List<String[]> getOthersData() throws
            KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for (byte[] data : othersData) {
            String[] dataStrings = new
                    String(data).split(":");
            result.add(dataStrings);
        }
        return result;
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;
        @Override
        public void run() {
            System.out.println("Starting the leader Campaign");
            try {
                boolean leader = leaderLock.tryAcquireLock();
                while (!leader) {
                    byte[] leaderData =
                            leaderLock.getLockHolderData();
                    if (currentLeaderData != leaderData) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData(currentLeaderData);
                    }
                    Thread.sleep(10000);
                    leader = leaderLock.tryAcquireLock();
                }
                currentLeaderData = null;
                beTheLeader();
            } catch (Exception e){
                e.printStackTrace();
            }
        }
    }

}


