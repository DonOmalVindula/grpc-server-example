package com.cw2server;

import com.grpc.generated.PlaceOrderServiceGrpc;
import com.grpc.generated.OrderRequest;
import com.grpc.generated.OperationStatus;
import com.grpc.generated.Item;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class PlaceOrderServiceImpl extends PlaceOrderServiceGrpc.PlaceOrderServiceImplBase {
    private ManagedChannel channel = null;
    private InventoryServer server;

    public PlaceOrderServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void placeOrder(OrderRequest request, StreamObserver<OperationStatus> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Placing order as Primary");
                Item item = server.getInventory().get(request.getItemId());
                if (item != null && item.getQuantity() >= request.getQuantity()) {
                    Item updatedItem = item.toBuilder()
                            .setQuantity(item.getQuantity() - request.getQuantity())
                            .build();
                    server.getInventory().replace(item.getId(), updatedItem);
                    updateSecondaryServers(request);
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Order placed successfully")
                            .build());
                } else {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Insufficient stock to complete order")
                            .build());
                }
            } catch (Exception e) {
                System.out.println("Error while placing order: " + e.getMessage());
                e.printStackTrace();
                responseObserver.onNext(OperationStatus.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to place order due to an error")
                        .build());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Placing order on secondary, on Primary's command");
                performOrder(request, responseObserver);
            } else {
                OperationStatus primaryResponse = callPrimary(request);
                responseObserver.onNext(primaryResponse);
            }
        }
        responseObserver.onCompleted();
    }

    private void performOrder(OrderRequest request, StreamObserver<OperationStatus> responseObserver) {
        Item item = server.getInventory().get(request.getItemId());
        if (item != null && item.getQuantity() >= request.getQuantity()) {
            Item updatedItem = item.toBuilder()
                    .setQuantity(item.getQuantity() - request.getQuantity())
                    .build();
            server.getInventory().replace(item.getId(), updatedItem);
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(true)
                    .setMessage("Order placed successfully on secondary")
                    .build());
        } else {
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(false)
                    .setMessage("Insufficient stock on secondary to complete order")
                    .build());
        }
    }

    private OperationStatus callPrimary(OrderRequest request) {
        System.out.println("Calling Primary server for order placement");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, IPAddress, port);
    }

    private OperationStatus callServer(OrderRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port + " to place order");
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        PlaceOrderServiceGrpc.PlaceOrderServiceBlockingStub stub = PlaceOrderServiceGrpc.newBlockingStub(channel);

        OrderRequest updatedRequest = OrderRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.placeOrder(updatedRequest);
        channel.shutdown();
        return response;
    }

    private void updateSecondaryServers(OrderRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers with order placement command");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, IPAddress, port);
        }
    }
}
