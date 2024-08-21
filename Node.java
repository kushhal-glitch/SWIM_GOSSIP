import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Node {
    private InetAddress address;
    private int port;
    private boolean isFailed;
    private Map<Node, NodeStatus> membershipList = new ConcurrentHashMap<>();
    private DatagramSocket socket;
    private Random random = new Random();
    private ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
    private static Node[] nodes;
    public static Map<Integer, Node> portref = new ConcurrentHashMap<>();
    private boolean isLeader;
    private int clusterId;
    private static Map<Integer, Node> clusterLeaders = new ConcurrentHashMap<>();
    private Map<String, String> localDataStore = new ConcurrentHashMap<>(); // Simulated data store
    private static boolean recoveryInProgress = false;
    private Iterator<Node> roundRobinIterator;
    private MetricsTracker metrics;

    public Node(InetAddress address, int port, int clusterId, boolean isLeader, MetricsTracker metrics) throws Exception {
        this.address = address;
        this.port = port;
        this.clusterId = clusterId;
        this.isLeader = isLeader;
        this.metrics = metrics;
        this.socket = new DatagramSocket(port);
        startListeningForMessages();
        roundRobinIterator = membershipList.keySet().iterator();
    }

    public InetAddress getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public boolean isFailed() {
        return isFailed;
    }

    public void setFailed(boolean failed) {
        isFailed = failed;
    }

    public void addMember(Node node) {
        membershipList.put(node, NodeStatus.ALIVE);
    }

    public void removeMember(Node node) {
        membershipList.remove(node);
    }

    public Map<Node, NodeStatus> getMembershipList(Node node) {
        return node.membershipList;
    }

    public void startPinging() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                Node target = getRandomMember(null);
                if (target != null) {
                    sendPing(target);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void startGossiping() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                gossipMembershipList();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 3000, 2000, TimeUnit.MILLISECONDS);
    }

    public void startListeningForMessages() {
        new Thread(() -> {
            try {
                byte[] buffer = new byte[1024];
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
                while (true) {
                    socket.receive(packet);
                    String message = new String(packet.getData(), 0, packet.getLength());
                    InetAddress senderAddress = packet.getAddress();
                    int senderPort = packet.getPort();

                    if (message.startsWith("MEMBERS")) {
                        receiveGossipMessage(message);
                    } else if (message.equals("PING")) {
                        sendAck(senderAddress, senderPort);
                    } else if (message.equals("ACK")) {
                        System.out.println("Received ACK from " + senderAddress + ":" + senderPort);
                    } else if (message.startsWith("SUMMARY")) {
                        receiveSummaryMessage(message);
                    } else if (message.startsWith("DATA_REQUEST")) {
                        handleDataRequest(senderAddress, senderPort, message);
                    } else if (message.startsWith("DATA_RESPONSE")) {
                        handleDataResponse(message, senderPort);
                    } else if (message.startsWith("ELECTION")) {
                        handleElectionMessage(senderAddress, senderPort, message);
                    } else if (message.startsWith("COORDINATOR")) {
                        handleCoordinatorMessage(message);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    private void shareSummaryWithLeader(Node leader) {
        StringBuilder summaryMessage = new StringBuilder("SUMMARY ");
        for (Map.Entry<Node, NodeStatus> entry : membershipList.entrySet()) {
            summaryMessage.append(entry.getKey().getAddress().getHostAddress())
                    .append(":").append(entry.getKey().getPort())
                    .append("=").append(entry.getValue()).append(";");
        }
        try {
            DatagramPacket packet = new DatagramPacket(summaryMessage.toString().getBytes(),
                    summaryMessage.length(), leader.getAddress(), leader.getPort());
            socket.send(packet);
            metrics.incrementMessagesSent();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void handleDataRequest(InetAddress senderAddress, int senderPort, String message) {
        // Only respond if no other cluster has responded
        if (!recoveryInProgress) {
            recoveryInProgress = true;
            // Simulate sending local data for recovery
            String requestedData = "DATA_RESPONSE " + localDataStore.toString(); // Simplified example
            try {
                DatagramPacket packet = new DatagramPacket(requestedData.getBytes(), requestedData.length(), senderAddress, senderPort);
                socket.send(packet);
                metrics.incrementMessagesSent();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleDataResponse(String message, int failedNodePort) {
        // Handle data response and simulate data restoration
        String data = message.substring("DATA_RESPONSE ".length());
        System.out.println("Received data for recovery: " + data);
        // Simulate updating local data store with received data
        // This should be more sophisticated in a real implementation
        replaceFailedNodeWithNew(portref.get(failedNodePort), data);
        localDataStore.put("recoveredData", data);
    }

    private void receiveSummaryMessage(String message) {
        // Handle summary message from another cluster leader
        System.out.println("Received summary message: " + message);
        // Parse and integrate the summary information
        // For example, start recovery procedures if a failure is detected
    }

    public void sendPing(Node target) throws Exception {
        long startTime = System.currentTimeMillis();
        String message = "PING";
        DatagramSocket sendSocket = socket;
        DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), target.getAddress(), target.getPort());
        System.out.println("Node:" + socket.getLocalPort() + " sending the PING to target node:" + target.getPort());
        sendSocket.send(packet);
        metrics.incrementMessagesSent();

        // Wait for the ACK
        sendSocket.setSoTimeout(5000);
        try {
            byte[] buffer = new byte[1024];
            DatagramPacket response = new DatagramPacket(buffer, buffer.length);
            sendSocket.receive(response);
            String responseMessage = new String(response.getData(), 0, response.getLength());
            if (responseMessage.equals("ACK")) {
                System.out.println(target + " is alive");
                membershipList.put(target, NodeStatus.ALIVE);
                metrics.incrementMessagesReceived();
            }
        } catch (Exception e) {
            System.out.println(target + " did not respond. Marking as SUSPECT and sending indirect probes...");
            membershipList.put(target, NodeStatus.SUSPECT);
            sendIndirectProbes(target);
        }
        long endTime = System.currentTimeMillis();
        metrics.recordPingLatency(endTime - startTime);
    }

    public void sendAck(InetAddress address, int port) throws Exception {
        String message = "ACK";
        DatagramSocket sendSocket = new DatagramSocket();
        DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), address, port);
        sendSocket.send(packet);
        metrics.incrementMessagesSent();
        sendSocket.close();
    }

    public void sendIndirectProbes(Node target) throws Exception {
        int successfulProbes = 0;
        int probesSent = 0;

        while (probesSent < 3 && roundRobinIterator.hasNext()) {
            Node probeNode = roundRobinIterator.next();
            if (probeNode != target && probeNode != this) {
                probesSent++;
                String message = "PING_REQ " + target.getAddress().getHostAddress() + ":" + target.getPort();
                DatagramSocket sendSocket = new DatagramSocket();
                DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), probeNode.getAddress(), probeNode.getPort());
                sendSocket.send(packet);
                metrics.incrementMessagesSent();

                // Wait for the ACK from the indirect probe
                sendSocket.setSoTimeout(5000);
                try {
                    byte[] buffer = new byte[1024];
                    DatagramPacket response = new DatagramPacket(buffer, buffer.length);
                    sendSocket.receive(response);
                    String responseMessage = new String(response.getData(), 0, response.getLength());
                    if (responseMessage.equals("ACK")) {
                        successfulProbes++;
                        metrics.incrementMessagesReceived();
                        break;
                    }
                } catch (Exception e) {
                    System.out.println("Indirect probe from " + probeNode + " to " + target + " failed.");
                } finally {
                    sendSocket.close();
                }
            }
        }

        if (!roundRobinIterator.hasNext()) {
            roundRobinIterator = membershipList.keySet().iterator();
        }

        if (successfulProbes == 0) {
            System.out.println(target + " is marked as failed after indirect probes.");
            membershipList.put(target, NodeStatus.FAILED);

            // If the leader detects the failure, request recovery
            if (isLeader) {
                System.out.println("Leader detected failure of node " + target.getPort() + ". Requesting recovery.");
                requestDataForRecovery(target);
            }
        }
    }

    public void gossipMembershipList() throws Exception {
        long startTime = System.currentTimeMillis();
        StringBuilder gossipMessage = new StringBuilder("MEMBERS ");
        for (Map.Entry<Node, NodeStatus> entry : membershipList.entrySet()) {
            gossipMessage.append(entry.getKey().getAddress().getHostAddress()).append(":").append(entry.getKey().getPort()).append("=").append(entry.getValue()).append(";");
        }

        for (Node member : membershipList.keySet()) {
            DatagramPacket packet = new DatagramPacket(gossipMessage.toString().getBytes(), gossipMessage.length(), member.getAddress(), member.getPort());
            socket.send(packet);
            metrics.incrementMessagesSent();
        }

        System.out.println("Gossiped membership list to all members.");
        long endTime = System.currentTimeMillis();
        metrics.recordGossipLatency(endTime - startTime);
    }

    private Node getRandomMember(Node exclude) {
        Node[] membersArray = membershipList.keySet().toArray(new Node[0]);
        if (membersArray.length == 0) return null;
        Node selected;
        do {
            selected = membersArray[random.nextInt(membersArray.length)];
        } while (selected.equals(exclude));
        return selected;
    }

    public void receiveGossipMessage(String message) throws Exception {
        String[] parts = message.split(" ");
        String[] parts1 = parts[1].split(";");
        for (String nodeInfoStr : parts1) {
            String[] nodeInfo = nodeInfoStr.split("=");
            String[] addressInfo = nodeInfo[0].split(":");
            InetAddress nodeAddress;
            try {
                nodeAddress = InetAddress.getByName(addressInfo[0]);
                int nodePort = Integer.parseInt(addressInfo[1]);
                NodeStatus status = NodeStatus.valueOf(nodeInfo[1]);
                membershipList.put(portref.get(nodePort), status);

                // If the leader detects the failure, request recovery
                if (isLeader && status == NodeStatus.FAILED) {
                    System.out.println("Leader detected failure of node " + nodePort + " from gossip. Requesting recovery.");
                    requestDataForRecovery(portref.get(nodePort));
                }
            } catch (NumberFormatException | UnknownHostException e) {
                e.printStackTrace();
            }
        }
    }

    public void joinNetwork(Node introducer) throws Exception {
        if (introducer != null) {
            introducer.gossipMembershipList();
            for (Node member : introducer.membershipList.keySet()) {
                addMember(member);
                member.addMember(this);
            }
            addMember(introducer);
            introducer.addMember(this);
        } else {
            addMember(this); // First node in the network adds itself
        }
    }

    public static void initializeMembershipLists(Node[] nodes) {
        for (Node node : nodes) {
            for (Node member : nodes) {
                if (node != member) {
                    node.addMember(member);
                    portref.put(node.getPort(), node);
                }
            }
        }
    }

    public void startInterClusterCommunication() {
        if (isLeader) {
            scheduler.scheduleAtFixedRate(() -> {
                for (Node leader : clusterLeaders.values()) {
                    if (leader.clusterId != this.clusterId) {
                        shareSummaryWithLeader(leader);
                    }
                }
            }, 0, 5000, TimeUnit.MILLISECONDS); // Adjust interval as needed
        }
    }

    private void requestDataForRecovery(Node failedNode) {
        for (Node leader : clusterLeaders.values()) {
            if (leader.clusterId != this.clusterId) {
                String dataRequestMessage = "DATA_REQUEST " + failedNode.getAddress().getHostAddress() + ":" + failedNode.getPort();
                try {
                    DatagramPacket packet = new DatagramPacket(dataRequestMessage.getBytes(), dataRequestMessage.length(), leader.getAddress(), leader.getPort());
                    socket.send(packet);
                    metrics.incrementMessagesSent();
                    break; // Only one data request
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void replaceFailedNodeWithNew(Node failedNode, String recoveryData) {
        try {
            Node newNode = new Node(InetAddress.getByName("localhost"), 5030, failedNode.clusterId, false, metrics);
            newNode.localDataStore.put("recoveredData", recoveryData);
            newNode.membershipList.putAll(failedNode.membershipList);
            portref.put(5030, newNode);
            addMember(newNode); // Add new node to the leader's membership list
            gossipMembershipList();
            System.out.println("Replaced failed node " + failedNode.getPort() + " with new node.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startElection() {
        System.out.println("Starting election by node " + port);
        for (Node member : membershipList.keySet()) {
            if (member.getPort() > this.port) {
                sendElectionMessage(member.getAddress(), member.getPort());
            }
        }

        // Wait for a coordinator message for some time, then become leader if none is received
        scheduler.schedule(() -> {
            if (!isLeader) {
                System.out.println("No coordinator message received. Node " + port + " becoming the leader.");
                isLeader = true;
                clusterLeaders.put(clusterId, this);
                sendCoordinatorMessage();
            }
        }, 10, TimeUnit.SECONDS); // Wait for 5 seconds
    }

    private void sendElectionMessage(InetAddress address, int port) {
        String message = "ELECTION " + this.port;
        try {
            DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), address, port);
            socket.send(packet);
            metrics.incrementMessagesSent();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void sendCoordinatorMessage() {
        String message = "COORDINATOR " + this.port;
        for (Node member : membershipList.keySet()) {
            try {
                DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), member.getAddress(), member.getPort());
                socket.send(packet);
                metrics.incrementMessagesSent();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleElectionMessage(InetAddress senderAddress, int senderPort, String message) {
        int senderPortNum = Integer.parseInt(message.split(" ")[1]);
        if (senderPortNum > this.port) {
            // Acknowledge the election message and let the higher port node continue
            sendElectionMessage(senderAddress, senderPort);
        } else {
            // If the sender has a lower port number, start a new election
            startElection();
        }
    }

    private void handleCoordinatorMessage(String message) {
        int leaderPort = Integer.parseInt(message.split(" ")[1]);
        if (leaderPort != this.port) {
            isLeader = false;
            clusterLeaders.put(clusterId, portref.get(leaderPort));
            System.out.println("Node " + leaderPort + " is the new leader.");
        }
    }

    public static void main(String[] args) throws Exception {
        MetricsTracker metrics = new MetricsTracker();
        List<Node> cluster1Nodes = new ArrayList<>();
        List<Node> cluster2Nodes = new ArrayList<>();
        List<Node> cluster3Nodes = new ArrayList<>();

        // Create nodes and assign to clusters
        for (int i = 0; i < 10; i++) {
            cluster1Nodes.add(new Node(InetAddress.getByName("localhost"), 5000 + i, 1, i == 0, metrics));
            cluster2Nodes.add(new Node(InetAddress.getByName("localhost"), 5010 + i, 2, i == 0, metrics));
            cluster3Nodes.add(new Node(InetAddress.getByName("localhost"), 5020 + i, 3, i == 0, metrics));
        }

        // Initialize membership lists and start gossiping/pinging
        Node.initializeMembershipLists(cluster1Nodes.toArray(new Node[0]));
        Node.initializeMembershipLists(cluster2Nodes.toArray(new Node[0]));
        Node.initializeMembershipLists(cluster3Nodes.toArray(new Node[0]));

        // Start the leader election in each cluster
        cluster1Nodes.get(0).startElection();
        cluster2Nodes.get(0).startElection();
        cluster3Nodes.get(0).startElection();

        // Start node activities
        for (Node node : cluster1Nodes) {
            node.startPinging();
            node.startGossiping();
            node.startInterClusterCommunication();
        }

        for (Node node : cluster2Nodes) {
            node.startPinging();
            node.startGossiping();
            node.startInterClusterCommunication();
        }

        for (Node node : cluster3Nodes) {
            node.startPinging();
            node.startGossiping();
            node.startInterClusterCommunication();
        }

        // Simulate node failure and coordinated recovery in cluster 1
        int targetNodeIndex = 1; // Changed to 1 to avoid the leader
        Node targetNode = cluster1Nodes.get(targetNodeIndex);

        // Measure time to detect failure and replace node
        ScheduledExecutorService failureScheduler = Executors.newScheduledThreadPool(1);
        failureScheduler.schedule(() -> {
            try {
                System.out.println("Simulating failure of node " + targetNode.getPort());
                targetNode.setFailed(true);
                targetNode.getMembershipList(targetNode).put(targetNode, NodeStatus.FAILED);
                cluster1Nodes.get(0).sendIndirectProbes(targetNode); // Leader handles the failure detection and recovery
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 10, TimeUnit.SECONDS); // Fail after 10 seconds

        // Keep the program running
        while (true) {
            Thread.sleep(1000);
            metrics.printMetrics();
        }
    }
}

enum NodeStatus {
    ALIVE, SUSPECT, FAILED
}