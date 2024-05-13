package com.cw2server;

import com.grpc.generated.MakeReservationServiceGrpc;
import com.grpc.generated.ReservationRequest;
import com.grpc.generated.OperationStatus;
import com.grpc.generated.Item;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class MakeReservationServiceImpl extends MakeReservationServiceGrpc.MakeReservationServiceImplBase {
    private ManagedChannel channel = null;
    private InventoryServer server;

    public MakeReservationServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void makeReservation(ReservationRequest request, StreamObserver<OperationStatus> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Making reservation as Primary");
                Item item = server.getInventory().get(request.getItemId());
                if (item != null && item.getQuantity() >= request.getQuantity() && item.getAvailable()) {
                    Item updatedItem = item.toBuilder()
                            .setQuantity(item.getQuantity() - request.getQuantity())
                            .build();
                    server.getInventory().replace(item.getId(), updatedItem);
                    updateSecondaryServers(request);
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Reservation made successfully")
                            .build());
                } else {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Insufficient quantity or item unavailable")
                            .build());
                }
            } catch (Exception e) {
                System.out.println("Error while making reservation: " + e.getMessage());
                e.printStackTrace();
                responseObserver.onNext(OperationStatus.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to make reservation due to an error")
                        .build());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Making reservation on secondary, on Primary's command");
                performReservation(request, responseObserver);
            } else {
                OperationStatus primaryResponse = callPrimary(request);
                responseObserver.onNext(primaryResponse);
            }
        }
        responseObserver.onCompleted();
    }

    private void performReservation(ReservationRequest request, StreamObserver<OperationStatus> responseObserver) {
        Item item = server.getInventory().get(request.getItemId());
        if (item != null && item.getQuantity() >= request.getQuantity() && item.getAvailable()) {
            Item updatedItem = item.toBuilder()
                    .setQuantity(item.getQuantity() - request.getQuantity())
                    .build();
            server.getInventory().replace(item.getId(), updatedItem);
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(true)
                    .setMessage("Reservation made successfully on secondary")
                    .build());
        } else {
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(false)
                    .setMessage("Item not found or insufficient quantity on secondary")
                    .build());
        }
    }

    private OperationStatus callPrimary(ReservationRequest request) {
        System.out.println("Calling Primary server for reservation");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, IPAddress, port);
    }

    private OperationStatus callServer(ReservationRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port + " to make reservation");
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        MakeReservationServiceGrpc.MakeReservationServiceBlockingStub stub = MakeReservationServiceGrpc.newBlockingStub(channel);

        ReservationRequest updatedRequest = ReservationRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.makeReservation(updatedRequest);
        channel.shutdown();
        return response;
    }

    private void updateSecondaryServers(ReservationRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers with reservation command");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, IPAddress, port);
        }
    }
}
