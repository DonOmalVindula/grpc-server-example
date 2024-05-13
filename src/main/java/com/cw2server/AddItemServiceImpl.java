package com.cw2server;

import com.cw2client.DistributedTxCoordinator;
import com.cw2client.DistributedTxListener;
import com.cw2client.DistributedTxParticipant;
import com.grpc.generated.AddItemServiceGrpc;
import com.grpc.generated.Item;
import com.grpc.generated.OperationStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public class AddItemServiceImpl extends AddItemServiceGrpc.AddItemServiceImplBase implements DistributedTxListener {
    private ManagedChannel channel = null;
    private InventoryServer server;
    private Pair<String, Item> tempDataHolder;
    private boolean transactionStatus = false;
    public AddItemServiceImpl(InventoryServer server){
        this.server = server;
    }

    private void startDistributedTx(Item request) {
        try {
            server.getTransaction().start(request.getId(), String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<String, Item>(request.getId(), request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void onGlobalCommit() {
        addItemCommit();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }

    @Override
    public void addItem(Item request, StreamObserver<OperationStatus> responseObserver) {

        boolean status = false;
        if (server.isLeader()){
            // Act as primary
            try {
                System.out.println("Updating account balance as Primary");
                startDistributedTx(request);
                updateSecondaryServers(request);
                System.out.println("Going to perform");
                // If item is valid and quantity is greater than 0 then perform the transaction
                if (request.getQuantity() > 0) {
                    System.out.println("Performing transaction");
                    ((DistributedTxCoordinator)server.getTransaction()).perform();
                } else {
                    System.out.println("Transaction failed");
                    ((DistributedTxCoordinator)server.getTransaction()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while adding item" + e.getMessage());
                e.printStackTrace();
            }
        } else {
            // Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("add item on secondary, on Primary's command");
                startDistributedTx(request);

                if (request.getQuantity() > 0) {
                    System.out.println("Performing transaction as secondary");
                    ((DistributedTxParticipant)server.getTransaction()).voteCommit();
                } else {
                    System.out.println("Transaction failed as secondary");
                    ((DistributedTxParticipant)server.getTransaction()).voteAbort();
                }
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

    private void addItemCommit() {
        if (tempDataHolder != null) {
            String accountId = tempDataHolder.getKey();
            Item value = tempDataHolder.getValue();
            server.getInventory().put(value.getId(), value);
            System.out.println("Item " + value.getId() + " added successfully and committed");
            tempDataHolder = null;
        }
    }
}