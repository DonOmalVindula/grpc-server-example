package com.cw2server;

import com.grpc.generated.Concert;
import com.grpc.generated.Ticket;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class InventoryServer {

    private final DistributedLock leaderLock;
    private final int serverPort;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private byte[] leaderData;

    private final ConcurrentHashMap<String, Ticket> inventory = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Concert> concertStore = new ConcurrentHashMap<>();
    private DistributedTx transaction;
    private final TicketServiceImpl ticketService;
    private final ConcertServiceImpl concertService;
    private final MakeReservationServiceImpl makeReservationService;

    public static void main(String[] args) throws Exception {
        DistributedLock.setZooKeeperURL("localhost:2181");
        DistributedTx.setZooKeeperURL("localhost:2181");

        if (args.length < 1) {
            System.out.println("INVALID Input: Use InventoryServer <port>");
            System.exit(1);
        }

        int serverPort = Integer.parseInt(args[0]);
        InventoryServer server = new InventoryServer("localhost", serverPort);
        server.startServer();
    }

    public InventoryServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock("InventoryServerCluster", buildServerData(host, port));

        ticketService = new TicketServiceImpl(this);
        concertService = new ConcertServiceImpl(this);
        makeReservationService = new MakeReservationServiceImpl(this);

        transaction = new DistributedTxParticipant(makeReservationService);
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        seedDummyData();

        Server server = ServerBuilder.forPort(serverPort)
                .addService(ticketService)
                .addService(concertService)
                .addService(makeReservationService)
                .build();

        server.start();
        addShutdownHook(); // Add shutdown hook to release lock on exit
        System.out.println("InventoryServer started on port " + serverPort);
        tryToBeLeader();
        server.awaitTermination();
    }

    private void beTheLeader() {
        System.out.println("This server is now acting as the primary (leader).");
        isLeader.set(true);
        transaction = new DistributedTxCoordinator(makeReservationService);
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                if (isLeader()) {
                    leaderLock.releaseLock();
                    System.out.println("Leader lock released on shutdown.");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    public static String buildServerData(String IP, int port) {
        return IP + ":" + port;
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread(new LeaderCampaignThread());
        leaderCampaignThread.start();
    }

    public DistributedTx getTransaction() {
        return transaction;
    }

    public ConcurrentHashMap<String, Ticket> getInventory() {
        return inventory;
    }

    public ConcurrentHashMap<String, Concert> getConcertStore() {
        return concertStore;
    }

    public int getServerPort() {
        return serverPort;
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String(leaderData).split(":");
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for (byte[] data : othersData) {
            result.add(new String(data).split(":"));
        }
        return result;
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println("Starting leader election thread...");
            try {
                boolean leader = leaderLock.tryAcquireLock();

                while (!leader) {
                    byte[] newLeader = leaderLock.getLockHolderData();

                    if (currentLeaderData == null || !java.util.Arrays.equals(currentLeaderData, newLeader)) {
                        currentLeaderData = newLeader;
                        setCurrentLeaderData(newLeader);
                        System.out.println("Current leader: " + new String(currentLeaderData));
                    }

                    Thread.sleep(5000); // Re-check every 5s
                    leader = leaderLock.tryAcquireLock();
                }

                System.out.println("Acquired leadership.");
                beTheLeader();

            } catch (Exception e) {
                System.err.println("Error during leader election: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void seedDummyData() {
        // Seed Concert
        Concert concert = Concert.newBuilder()
                .setId("bns")
                .setName("Bathiya & Santhush Live in Concert")
                .setDate("2025-07-26")
                .build();

        Concert concert2 = Concert.newBuilder()
                .setId("westlife")
                .setName("Westlife Coast to Coast Tour")
                .setDate("2026-01-31")
                .build();

        concertStore.put(concert.getId(), concert);
        concertStore.put(concert2.getId(), concert2);

        // Seed Tickets
        Ticket vipTicket = Ticket.newBuilder()
                .setId("bns-vip")
                .setConcertId("bns")
                .setType("VIP")
                .setPrice(200.0)
                .setQuantity(50)
                .setIncludesAfterParty(true)
                .setAfterPartyQuantity(20)
                .setIsSentByPrimary(true)
                .build();

        Ticket regTicket = Ticket.newBuilder()
                .setId("bns-reg")
                .setConcertId("bns")
                .setType("Regular")
                .setPrice(100.0)
                .setQuantity(100)
                .setIncludesAfterParty(false)
                .setAfterPartyQuantity(0)
                .setIsSentByPrimary(true)
                .build();

        Ticket vipTicket2 = Ticket.newBuilder()
                .setId("westlife-vip")
                .setConcertId("westlife")
                .setType("VIP")
                .setPrice(300.0)
                .setQuantity(50)
                .setIncludesAfterParty(true)
                .setAfterPartyQuantity(20)
                .setIsSentByPrimary(true)
                .build();

        inventory.put(vipTicket.getId(), vipTicket);
        inventory.put(regTicket.getId(), regTicket);
        inventory.put(vipTicket2.getId(), vipTicket2);

        System.out.println("Dummy concert and tickets seeded.");
    }
}
