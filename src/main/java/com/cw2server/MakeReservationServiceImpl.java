package com.cw2server;

import com.grpc.generated.MakeReservationServiceGrpc;
import com.grpc.generated.ReservationRequest;
import com.grpc.generated.OperationStatus;
import com.grpc.generated.Ticket;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class MakeReservationServiceImpl extends MakeReservationServiceGrpc.MakeReservationServiceImplBase
        implements DistributedTxListener {

    private final InventoryServer server;
    private ManagedChannel channel = null;
    private Pair<String, ReservationRequest> tempDataHolder;

    public MakeReservationServiceImpl(InventoryServer server) {
        this.server = server;
    }

    private void startDistributedTx(ReservationRequest request) throws IOException {
        server.getTransaction().start(request.getReservationDate(), UUID.randomUUID().toString());
        tempDataHolder = new Pair<>(request.getReservationDate(), request);
    }

    @Override
    public void onGlobalCommit() {
        commitReservation();
    }

    @Override
    public void onGlobalAbort() {
        System.out.println("Reservation aborted.");
        tempDataHolder = null;
    }

    @Override
    public void makeReservation(ReservationRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;

        try {
            System.out.println("Received reservation request for ticket: " + request.getTicketId()
                    + " | Qty: " + request.getQuantity()
                    + " | Includes After-Party: " + request.getIncludesAfterParty());


            if (server.isLeader()) {
                System.out.println("Acting as Leader (Primary Node)");

                DistributedLock reservationLock = new DistributedLock(
                        "ReservationLock-" + request.getTicketId(), server.buildServerData("localhost", server.getServerPort())
                );

                try {
                    reservationLock.acquireLock();
                    System.out.println("Acquired lock for reservation.");

                    startDistributedTx(request);
                    propagateToFollowers(request);

                    if (canReserve(request)) {
                        if (request.getIncludesAfterParty()) {
                            System.out.println("Combo reservation...");
                            ((DistributedTxCoordinator) server.getTransaction()).perform();
                        } else {
                            System.out.println("Regular ticket reservation (no after-party)...");
                            commitReservation();
                        }
                        status = true;
                    } else {
                        System.out.println("Reservation check failed — Not enough stock.");
                        if (request.getIncludesAfterParty()) {
                            ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                        }
                    }
                } finally {
                    reservationLock.releaseLock();
                    System.out.println("Released lock for reservation.");
                }
            } else {
                System.out.println("Acting as Secondary Node");

                if (request.getIsSentByPrimary()) {
                    System.out.println("Reservation forwarded by Primary...");
                    startDistributedTx(request);

                    if (canReserve(request)) {
                        if (request.getIncludesAfterParty()) {
                            System.out.println("Voting COMMIT as participant");
                            ((DistributedTxParticipant) server.getTransaction()).voteCommit();
                        } else {
                            System.out.println("Committing directly as secondary (regular ticket)");
                            commitReservation();
                        }
                        status = true;
                    } else {
                        System.out.println("Voting ABORT — insufficient stock");
                        if (request.getIncludesAfterParty()) {
                            ((DistributedTxParticipant) server.getTransaction()).voteAbort();
                        }
                    }

                } else {
                    System.out.println("Forwarding request to Primary node for processing...");
                    OperationStatus primaryResponse = callPrimary(request);
                    status = primaryResponse.getSuccess();
                }
            }
        } catch (Exception e) {
            System.out.println("Exception during reservation: " + e.getMessage());
            e.printStackTrace();
            try {
                if (request.getIncludesAfterParty() && server.isLeader()) {
                    ((DistributedTxCoordinator) server.getTransaction()).sendGlobalAbort();
                }
            } catch (Exception ex) {
                System.out.println("Failed to send global abort: " + ex.getMessage());
                ex.printStackTrace();
            }
        }

        OperationStatus response = OperationStatus.newBuilder()
                .setSuccess(status)
                .setMessage(status ? "Reservation successful." : "Reservation failed.")
                .build();

        System.out.println("Reservation Response: " + response.getMessage());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private boolean canReserve(ReservationRequest request) {
        Ticket ticket = server.getInventory().get(request.getTicketId());
        if (ticket == null) {
            System.out.println("Ticket not found in inventory.");
            return false;
        }

        boolean hasEnoughConcert = ticket.getQuantity() >= request.getQuantity();
        boolean hasEnoughAfterParty = !request.getIncludesAfterParty()
                || ticket.getAfterPartyQuantity() >= request.getQuantity();

        if (!hasEnoughConcert) {
            System.out.println("Not enough tickets available for concert.");
        }

        if (!hasEnoughAfterParty) {
            System.out.println("Not enough tickets available for after-party.");
        }

        return hasEnoughConcert && hasEnoughAfterParty;
    }

    private void commitReservation() {
        if (tempDataHolder == null) {
            System.out.println("No temp reservation data found during commit.");
            return;
        }

        ReservationRequest request = tempDataHolder.getValue();
        Ticket ticket = server.getInventory().get(request.getTicketId());

        Ticket.Builder updatedTicket = ticket.toBuilder()
                .setQuantity(ticket.getQuantity() - request.getQuantity());

        if (request.getIncludesAfterParty()) {
            updatedTicket.setAfterPartyQuantity(ticket.getAfterPartyQuantity() - request.getQuantity());
        }

        server.getInventory().put(request.getTicketId(), updatedTicket.build());
        System.out.println("Reservation committed for ticket " + request.getTicketId());
        tempDataHolder = null;
    }

    private void propagateToFollowers(ReservationRequest request) throws KeeperException, InterruptedException {
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            callServer(request, true, data[0], Integer.parseInt(data[1]));
        }
    }

    private OperationStatus callPrimary(ReservationRequest request) {
        String[] leaderData = server.getCurrentLeaderData();
        return callServer(request, false, leaderData[0], Integer.parseInt(leaderData[1]));
    }

    private OperationStatus callServer(ReservationRequest request, boolean isSentByPrimary, String ip, int port) {
        channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        MakeReservationServiceGrpc.MakeReservationServiceBlockingStub stub =
                MakeReservationServiceGrpc.newBlockingStub(channel);

        ReservationRequest updatedRequest = ReservationRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.makeReservation(updatedRequest);
        channel.shutdown();
        return response;
    }
}
