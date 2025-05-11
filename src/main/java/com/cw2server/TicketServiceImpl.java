package com.cw2server;

import com.grpc.generated.TicketServiceGrpc;
import com.grpc.generated.Ticket;
import com.grpc.generated.TicketRequest;
import com.grpc.generated.Query;
import com.grpc.generated.TicketList;
import com.grpc.generated.OperationStatus;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.zookeeper.KeeperException;

import java.util.List;

public class TicketServiceImpl extends TicketServiceGrpc.TicketServiceImplBase {

    private final InventoryServer server;
    private ManagedChannel channel;

    public TicketServiceImpl(InventoryServer server) {
        this.server = server;
    }

    @Override
    public void addTicket(Ticket request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader adding ticket: " + request.getId());

                if (!server.getInventory().containsKey(request.getId())) {
                    server.getInventory().put(request.getId(), request);
                    propagateToFollowers(request, "add");
                    status = true;
                    msg = "Ticket added across cluster.";
                } else {
                    msg = "Ticket already exists.";
                }

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower adding ticket on leader's command.");
                    if (!server.getInventory().containsKey(request.getId())) {
                        server.getInventory().put(request.getId(), request);
                        status = true;
                        msg = "Ticket added by follower.";
                    } else {
                        msg = "Ticket already exists.";
                    }
                } else {
                    System.out.println("Forwarding addTicket to leader...");
                    OperationStatus leaderResponse = callPrimary(request, "add");
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error during ticket addition.";
        }

        responseObserver.onNext(OperationStatus.newBuilder()
                .setSuccess(status)
                .setMessage(msg)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void updateTicket(Ticket request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader updating ticket: " + request.getId());

                if (server.getInventory().containsKey(request.getId())) {
                    server.getInventory().put(request.getId(), request);
                    propagateToFollowers(request, "update");
                    status = true;
                    msg = "Ticket updated across cluster.";
                } else {
                    msg = "Ticket not found.";
                }

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower updating ticket on leader's command.");
                    if (server.getInventory().containsKey(request.getId())) {
                        server.getInventory().put(request.getId(), request);
                        status = true;
                        msg = "Ticket updated by follower.";
                    } else {
                        msg = "Ticket not found.";
                    }
                } else {
                    System.out.println("Forwarding updateTicket to leader...");
                    OperationStatus leaderResponse = callPrimary(request, "update");
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error during ticket update.";
        }

        responseObserver.onNext(OperationStatus.newBuilder()
                .setSuccess(status)
                .setMessage(msg)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void deleteTicket(TicketRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean status = false;
        String msg;

        try {
            if (server.isLeader()) {
                System.out.println("Leader deleting ticket: " + request.getTicketId());

                if (server.getInventory().remove(request.getTicketId()) != null) {
                    propagateToFollowers(request);
                    status = true;
                    msg = "Ticket deleted across cluster.";
                } else {
                    msg = "Ticket not found.";
                }

            } else {
                if (request.getIsSentByPrimary()) {
                    System.out.println("Follower deleting ticket on leader's command.");
                    if (server.getInventory().remove(request.getTicketId()) != null) {
                        status = true;
                        msg = "Ticket deleted by follower.";
                    } else {
                        msg = "Ticket not found.";
                    }
                } else {
                    System.out.println("Forwarding deleteTicket to leader...");
                    OperationStatus leaderResponse = callPrimary(request);
                    status = leaderResponse.getSuccess();
                    msg = leaderResponse.getMessage();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            msg = "Error during ticket deletion.";
        }

        responseObserver.onNext(OperationStatus.newBuilder()
                .setSuccess(status)
                .setMessage(msg)
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void listTickets(Query request, StreamObserver<TicketList> responseObserver) {
        request.getSearchKeyword();
        String keyword = request.getSearchKeyword().toLowerCase();

        TicketList.Builder builder = TicketList.newBuilder();
        for (Ticket ticket : server.getInventory().values()) {
            if (keyword.isEmpty() || ticket.getId().toLowerCase().contains(keyword) || ticket.getType().toLowerCase().contains(keyword)) {
                builder.addTickets(ticket);
            }
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void checkTicketExists(TicketRequest request, StreamObserver<OperationStatus> responseObserver) {
        boolean exists = server.getInventory().containsKey(request.getTicketId());

        OperationStatus response = OperationStatus.newBuilder()
                .setSuccess(exists)
                .setMessage(exists ? "Ticket exists." : "Ticket not found.")
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    // Helper methods

    private void propagateToFollowers(Ticket ticket, String action) throws KeeperException, InterruptedException {
        List<String[]> others = server.getOthersData();
        for (String[] node : others) {
            callServer(ticket, true, action, node[0], Integer.parseInt(node[1]));
        }
    }

    private void propagateToFollowers(TicketRequest request) throws KeeperException, InterruptedException {
        List<String[]> others = server.getOthersData();
        for (String[] node : others) {
            callServer(request, true, node[0], Integer.parseInt(node[1]));
        }
    }

    private OperationStatus callPrimary(Ticket ticket, String action) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(ticket, false, action, leader[0], Integer.parseInt(leader[1]));
    }

    private OperationStatus callPrimary(TicketRequest request) {
        String[] leader = server.getCurrentLeaderData();
        return callServer(request, false, leader[0], Integer.parseInt(leader[1]));
    }

    private OperationStatus callServer(Ticket ticket, boolean isSentByPrimary, String action, String ip, int port) {
        channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        TicketServiceGrpc.TicketServiceBlockingStub stub = TicketServiceGrpc.newBlockingStub(channel);

        Ticket updated = Ticket.newBuilder(ticket).setIsSentByPrimary(isSentByPrimary).build();
        OperationStatus response;

        if ("add".equals(action)) {
            response = stub.addTicket(updated);
        } else {
            response = stub.updateTicket(updated);
        }

        channel.shutdown();
        return response;
    }

    private OperationStatus callServer(TicketRequest request, boolean isSentByPrimary, String ip, int port) {
        channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
        TicketServiceGrpc.TicketServiceBlockingStub stub = TicketServiceGrpc.newBlockingStub(channel);

        TicketRequest updated = TicketRequest.newBuilder(request).setIsSentByPrimary(isSentByPrimary).build();
        OperationStatus response = stub.deleteTicket(updated);

        channel.shutdown();
        return response;
    }
}
