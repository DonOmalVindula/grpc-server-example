package com.cw2server;

import com.grpc.generated.Item;
import com.grpc.generated.OperationStatus;
import com.grpc.generated.UpdateItemServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class UpdateItemServiceImpl extends UpdateItemServiceGrpc.UpdateItemServiceImplBase {
    private InventoryServer server;
    private ManagedChannel channel = null;
    public UpdateItemServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void updateItem(Item request, StreamObserver<OperationStatus> responseObserver) {
        if (server.isLeader()) {
            // Leader node handles the update
            if (server.getInventory().containsKey(request.getId())) {
                server.getInventory().replace(request.getId(), request);
                try {
                    updateSecondaryServers(request); // Propagate the update to secondary servers
                    OperationStatus status = OperationStatus.newBuilder()
                            .setSuccess(true)
                            .setMessage("Item updated successfully by Leader")
                            .build();
                    responseObserver.onNext(status);
                } catch (KeeperException | InterruptedException e) {
                    System.out.println("Error while updating secondary servers: " + e.getMessage());
                    e.printStackTrace();
                    responseObserver.onNext(OperationStatus.newBuilder()
                            .setSuccess(false)
                            .setMessage("Failed to update secondary servers")
                            .build());
                }
            } else {
                OperationStatus status = OperationStatus.newBuilder()
                        .setSuccess(false)
                        .setMessage("Item not found")
                        .build();
                responseObserver.onNext(status);
            }
        } else {
            // Secondary node forwards the update to the leader only if it has not been sent by the primary
            if (!request.getIsSentByPrimary()) {
                OperationStatus status = callPrimary(request);
                responseObserver.onNext(status);
            } else {
                // If already sent by primary, just confirm the operation
                responseObserver.onNext(OperationStatus.newBuilder()
                        .setSuccess(true)
                        .setMessage("Update confirmed on secondary")
                        .build());
            }
        }
        responseObserver.onCompleted();
    }

    private OperationStatus callPrimary(Item request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(request, false, IPAddress, port);
    }

    private OperationStatus callServer(Item request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        UpdateItemServiceGrpc.UpdateItemServiceBlockingStub stub = UpdateItemServiceGrpc.newBlockingStub(channel);

        Item updatedRequest = Item.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.updateItem(updatedRequest);
        channel.shutdown();
        return response;
    }

    private void updateSecondaryServers(Item request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(request, true, IPAddress, port);
        }
    }
}
