package com.cw2server;

import com.grpc.generated.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.List;


public class ConcertServiceImpl extends ConcertServiceGrpc.ConcertServiceImplBase {

    private final InventoryServer server;
    private ManagedChannel channel;

    public ConcertServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void addConcert(Concert request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader received addConcert request for: " + request.getId());
                applyAddConcert(request);
                propagateToFollowers(request);
                status = true;
                msg = "Concert and tickets added across cluster.";
            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower applying concert update from leader.");
                    applyAddConcert(request);
                    status = true;
                    msg = "Concert added by follower.";
                } else {
                    System.out.println("Forwarding addConcert request to leader...");
                    OperationStatus leaderResponse = callPrimary(request);
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error while adding concert: " + e.getMessage();
        }

        respondWithStatus(responseObserver, status, msg);
    }

    @Override
    public void deleteConcert(ConcertRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader deleting concert: " + request.getConcertId());
                boolean removed = applyDeleteConcert(request.getConcertId());
                propagateDeleteToFollowers(request);
                status = removed;
                msg = removed ? "Concert deleted across cluster." : "Concert not found.";
            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower applying concert deletion from leader.");
                    boolean removed = applyDeleteConcert(request.getConcertId());
                    status = removed;
                    msg = removed ? "Concert deleted by follower." : "Concert not found.";
                } else {
                    System.out.println("Forwarding deleteConcert request to leader...");
                    OperationStatus leaderResponse = callPrimary(request);
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error while deleting concert: " + e.getMessage();
        }

        respondWithStatus(responseObserver, status, msg);
    }

    @Override
    public void checkConcertExists(ConcertRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader checking if concert exists: " + request.getConcertId());
                status = applyCheckConcertExists(request.getConcertId());
                msg = status ? "Concert exists." : "Concert not found.";
            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower checking concert existence (from leader).");
                    status = applyCheckConcertExists(request.getConcertId());
                    msg = status ? "Concert exists (follower)." : "Concert not found (follower).";
                } else {
                    System.out.println("Forwarding checkConcertExists request to leader...");
                    OperationStatus leaderResponse = callPrimary(request);
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error checking concert existence: " + e.getMessage();
        }

        respondWithStatus(responseObserver, status, msg);
    }

    @Override
    public void listConcerts(Query request, StreamObserver<ConcertList> responseObserver) {
        String keyword = request.getSearchKeyword().toLowerCase();
        System.out.println("🔍 Listing concerts with keyword: " + keyword);

        ConcertList.Builder responseBuilder = ConcertList.newBuilder();

        for (Concert concert : server.getConcertStore().values()) {
            boolean matches = keyword.isEmpty() ||
                    concert.getId().toLowerCase().contains(keyword) ||
                    concert.getName().toLowerCase().contains(keyword);

            if (!matches) continue;

            Concert.Builder builder = concert.toBuilder();

            // Merge in tickets from inventory
            for (Ticket ticket : server.getInventory().values()) {
                if (ticket.getConcertId().equals(concert.getId())) {
                    builder.addTickets(ticket);
                }
            }

            responseBuilder.addConcerts(builder.build());
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }


    private void applyAddConcert(Concert request) {
        server.getConcertStore().put(request.getId(), request);
        for (Ticket ticket : request.getTicketsList()) {
            server.getInventory().put(ticket.getId(), ticket);
        }
    }

    private boolean applyDeleteConcert(String concertId) {
        boolean concertRemoved = server.getConcertStore().remove(concertId) != null;
        server.getInventory().entrySet().removeIf(entry ->
                entry.getValue().getConcertId().equals(concertId));

        return concertRemoved;
    }

    private boolean applyCheckConcertExists(String concertId) {
        return server.getConcertStore().containsKey(concertId);
    }

    private void propagateToFollowers(Concert request) throws KeeperException, InterruptedException {
        List<String[]> others = server.getOthersData();
        for (String[] node : others) {
            callServer(request, true, node[0], Integer.parseInt(node[1]));
        }
    }

    private void propagateDeleteToFollowers(ConcertRequest request) throws KeeperException, InterruptedException {
        List<String[]> others = server.getOthersData();
        for (String[] node : others) {
            callServer(request, true, node[0], Integer.parseInt(node[1]));
        }
    }

    private OperationStatus callPrimary(Concert request) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(request, false, leader[0], Integer.parseInt(leader[1]));
    }

    private OperationStatus callPrimary(ConcertRequest request) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(request, false, leader[0], Integer.parseInt(leader[1]));
    }

    private OperationStatus callServer(Concert request, boolean isSentByPrimary, String ip, int port) {
        channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        ConcertServiceGrpc.ConcertServiceBlockingStub stub = ConcertServiceGrpc.newBlockingStub(channel);

        Concert updated = Concert.newBuilder(request).setIsSentByPrimary(isSentByPrimary).build();
        OperationStatus response = stub.addConcert(updated);

        channel.shutdown();
        return response;
    }

    private OperationStatus callServer(ConcertRequest request, boolean isSentByPrimary, String ip, int port) {
        channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        ConcertServiceGrpc.ConcertServiceBlockingStub stub = ConcertServiceGrpc.newBlockingStub(channel);

        ConcertRequest updated = ConcertRequest.newBuilder(request).setIsSentByPrimary(isSentByPrimary).build();
        OperationStatus response = stub.deleteConcert(updated);

        channel.shutdown();
        return response;
    }

    private void respondWithStatus(StreamObserver<OperationStatus> responseObserver, boolean success, String message) {
        responseObserver.onNext(OperationStatus.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build());
        responseObserver.onCompleted();
    }
}
