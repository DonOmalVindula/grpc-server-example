package com.cw2server;

import com.grpc.generated.DeleteItemServiceGrpc;
import com.grpc.generated.ItemRequest;
import com.grpc.generated.OperationStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class DeleteItemServiceImpl extends DeleteItemServiceGrpc.DeleteItemServiceImplBase {
    private ManagedChannel channel = null;
    private InventoryServer server;

    public DeleteItemServiceImpl(InventoryServer server){
        this.server = server;
    }

    @Override
    public void deleteItem(ItemRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        if (server.isLeader()) {
            // Act as primary
            try {
                System.out.println("Attempting to delete item as Primary");
                if (server.getInventory().remove(request.getId()) != null) {
                    updateSecondaryServers(request);
                    status = true;
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Item deleted successfully")
                            .build());
                } else {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Item not found")
                            .build());
                }
            } catch (Exception e) {
                System.out.println("Error while deleting the item: " + e.getMessage());
                e.printStackTrace();
                responseObserver.onNext(OperationStatus.newBuilder()
                        .setSuccess(false)
                        .setMessage("Failed to delete item due to an error")
                        .build());
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Deleting item on secondary, on Primary's command");
                if (server.getInventory().remove(request.getId()) != null) {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Item deleted successfully")
                            .build());
                } else {
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Item not found")
                            .build());
                }
            } else {
                OperationStatus primaryResponse = callPrimary(request);
                responseObserver.onNext(primaryResponse);
            }
        }
        responseObserver.onCompleted();
    }

    private OperationStatus callPrimary(ItemRequest request) {
        System.out.println("Calling Primary server for deletion");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, IPAddress, port);
    }

    private OperationStatus callServer(ItemRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port + " to delete item");
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        DeleteItemServiceGrpc.DeleteItemServiceBlockingStub stub = DeleteItemServiceGrpc.newBlockingStub(channel);

        ItemRequest updatedRequest = ItemRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.deleteItem(updatedRequest);
        channel.shutdown();
        return response;
    }

    private void updateSecondaryServers(ItemRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers with delete command");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, IPAddress, port);
        }
    }
}
