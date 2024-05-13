package com.cw2server;

import com.grpc.generated.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class ListItemServiceImpl extends ListItemsServiceGrpc.ListItemsServiceImplBase {
    private ManagedChannel channel = null;
    private ListItemsServiceGrpc.ListItemsServiceBlockingStub clientStub = null;
    private InventoryServer server;

    public ListItemServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void listItems(Query request, StreamObserver<ItemList> responseObserver) {
        if (server.isLeader()) {
            try {
                ItemList.Builder itemList = ItemList.newBuilder();
                // Check if a search keyword is provided and filter accordingly
                String searchKeyword = request.getSearchKeyword();
                server.getInventory().values().stream()
                        .filter(item -> searchKeyword == null || searchKeyword.isEmpty() || item.getId().contains(searchKeyword))
                        .forEach(itemList::addItems);
                responseObserver.onNext(itemList.build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                System.err.println("Error processing list items: " + e.getMessage());
                responseObserver.onError(e);
            }
        } else {
            // If not the leader, forward the request to the leader
            forwardToListLeader(request, responseObserver);
        }
    }

    private void forwardToListLeader(Query request, StreamObserver<ItemList> responseObserver) {
        System.out.println("Forwarding list items request to leader");
        String[] leaderData = server.getCurrentLeaderData();
        String IPAddress = leaderData[0];
        int port = Integer.parseInt(leaderData[1]);

        // Establishing connection to the leader
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = ListItemsServiceGrpc.newBlockingStub(channel);

        // Forwarding the query and handling response
        try {
            ItemList response = clientStub.listItems(request);
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } finally {
            channel.shutdown();
        }
    }
}
