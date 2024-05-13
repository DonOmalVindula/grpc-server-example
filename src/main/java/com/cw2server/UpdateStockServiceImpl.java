package com.cw2server;

import com.grpc.generated.UpdateStockServiceGrpc;
import com.grpc.generated.StockUpdateRequest;
import com.grpc.generated.OperationStatus;
import com.grpc.generated.Item;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class UpdateStockServiceImpl extends UpdateStockServiceGrpc.UpdateStockServiceImplBase {
    private ManagedChannel channel = null;
    private InventoryServer server;

    public UpdateStockServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void updateStock(StockUpdateRequest request, StreamObserver<OperationStatus> responseObserver) {
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Updating stock as Primary");
                Item item = server.getInventory().get(request.getItemId());
                if (item != null) {
                    Item updatedItem = item.toBuilder()
                            .setQuantity(item.getQuantity() + request.getAdditionalQuantity())
                            .build();
                    server.getInventory().replace(item.getId(), updatedItem);
                    updateSecondaryServers(request);
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Stock updated successfully")
                            .build());
                } else {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Item not found")
                            .build());
                }
            } catch (Exception e) {
                System.out.println("Error while updating stock: " + e.getMessage());
                e.printStackTrace();
                responseObserver.onNext(OperationStatus.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to update stock due to an error")
                        .build());
            }
        } else {
            // Act as Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating stock on secondary, on Primary's command");
                performUpdate(request, responseObserver);
            } else {
                OperationStatus primaryResponse = callPrimary(request);
                responseObserver.onNext(primaryResponse);
            }
        }
        responseObserver.onCompleted();
    }

    private void performUpdate(StockUpdateRequest request, StreamObserver<OperationStatus> responseObserver) {
        Item item = server.getInventory().get(request.getItemId());
        if (item != null) {
            Item updatedItem = item.toBuilder()
                    .setQuantity(item.getQuantity() + request.getAdditionalQuantity())
                    .build();
            server.getInventory().replace(item.getId(), updatedItem);
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(true)
                    .setMessage("Stock updated successfully on secondary")
                    .build());
        } else {
            responseObserver.onNext(OperationStatus.newBuilder()
                    .setSuccess(false)
                    .setMessage("Item not found on secondary")
                    .build());
        }
    }

    private OperationStatus callPrimary(StockUpdateRequest request) {
        System.out.println("Calling Primary server for stock update");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, IPAddress, port);
    }

    private OperationStatus callServer(StockUpdateRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port + " to update stock");
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        UpdateStockServiceGrpc.UpdateStockServiceBlockingStub stub = UpdateStockServiceGrpc.newBlockingStub(channel);

        StockUpdateRequest updatedRequest = StockUpdateRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.updateStock(updatedRequest);
        channel.shutdown();
        return response;
    }

    private void updateSecondaryServers(StockUpdateRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers with stock update command");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, IPAddress, port);
        }
    }
}
