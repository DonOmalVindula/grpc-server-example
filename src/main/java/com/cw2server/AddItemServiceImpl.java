package com.cw2server;

import com.grpc.generated.AddItemServiceGrpc;
import com.grpc.generated.Item;
import com.grpc.generated.OperationStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;
import java.util.List;

public class AddItemServiceImpl extends AddItemServiceGrpc.AddItemServiceImplBase {
    private ManagedChannel channel = null;
    private InventoryServer server;

    public AddItemServiceImpl(InventoryServer server){
        this.server = server;
    }

    @Override
    public void addItem(Item request, StreamObserver<OperationStatus> responseObserver) {

        boolean status = false;
        if (server.isLeader()){
            // Act as primary
            try {
                System.out.println("Updating account balance as Primary");
                server.getInventory().put(request.getId(), request);
                updateSecondaryServers(request);
                status = true;
            } catch (Exception e) {
                System.out.println("Error while updating the account balance" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating account balance on secondary, on Primary's command");
                server.getInventory().put(request.getId(), request);
            } else {
                OperationStatus response = callPrimary(request);
                if (response.getSuccess()) {
                    status = true;
                }
            }
        }

        OperationStatus response = OperationStatus.newBuilder()
                .setSuccess(status)
                .setMessage(status ? "Item added successfully" : "Failed to add item")
                .build();

        responseObserver.onNext(response);
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
        AddItemServiceGrpc.AddItemServiceBlockingStub stub = AddItemServiceGrpc.newBlockingStub(channel);

        Item updatedRequest = Item.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();

        OperationStatus response = stub.addItem(updatedRequest);
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