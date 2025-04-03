//
//
//
//
//
//
//

import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;

interface NodeInterface {
    void setNodeName(String nodeName) throws Exception;
    void openPort(int portNumber) throws Exception;
    void handleIncomingMessages(int delay) throws Exception;
    boolean isActive(String nodeName) throws Exception;
    void pushRelay(String nodeName) throws Exception;
    void popRelay() throws Exception;
    boolean exists(String key) throws Exception;
    String read(String key) throws Exception;
    boolean write(String key, String value) throws Exception;
    boolean CAS(String key, String currentValue, String newValue) throws Exception;
}

// IN2011 Computer Networks
// Coursework 2024/2025
//



public class Node implements NodeInterface {

    private String nodeId;
    private DatagramSocket socket;
    private final Map<String, String> dataStore = new HashMap<>();
    private final Stack<String> relayStack = new Stack<>();
    private final Map<String, InetSocketAddress> nodeDirectory = new HashMap<>();
    private final Map<String, String> nearbyNodes = new HashMap<>();
    private final Set<String> processedTx = Collections.newSetFromMap(new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > 1000;
        }
    });

    private final Random random = new Random();
    private String lastReadValue = null;
    private boolean lastExistenceCheck = false;
    private final boolean debug = false;
    private Thread listenerThread;

    @Override
    public void setNodeName(String nodeName) {
        if (!nodeName.startsWith("N:")) {
            throw new IllegalArgumentException("Node name must start with 'N:'");
        }
        this.nodeId = nodeName;
    }

    @Override
    public void openPort(int port) throws Exception {
        socket = new DatagramSocket(port);
        if (debug) System.out.println("Port opened on: " + port);
        startBackgroundListener();
    }

    @Override
    public void handleIncomingMessages(int timeout) throws Exception {
        socket.setSoTimeout(100);
        byte[] buffer = new byte[2048];
        long startTime = System.currentTimeMillis();

        while (timeout == 0 || System.currentTimeMillis() - startTime < timeout) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                if (debug) System.out.println("📩 Received: " + message);
                processMessage(message, packet.getAddress(), packet.getPort());
            } catch (SocketTimeoutException ignored) {
            }
        }
    }

    private void processMessage(String msg, InetAddress address, int port) {
        try {
            if (Math.random() < 0.1) return;

            String[] parts = msg.trim().split(" ", 3);
            if (parts.length < 2) return;

            String txId = parts[0];
            String command = parts[1];

            if (!processedTx.add(txId)) return;

            switch (command) {
                case "G" -> sendResponse(address, port, txId + " H " + wrap(nodeId));
                case "H" -> {
                    String incoming = unwrap(parts.length > 2 ? parts[2] : "");
                    if (incoming != null) {
                        nodeDirectory.put(incoming, new InetSocketAddress(address, port));
                    }
                }
                case "W" -> handleWrite(txId, parts.length > 2 ? parts[2] : "", address, port);
                case "R" -> handleRead(txId, parts.length > 2 ? parts[2] : "", address, port);
                case "S" -> {
                    if (parts.length > 2) {
                        String[] content = parts[2].split(" ", 2);
                        if ("Y".equals(content[0]) && content.length > 1) {
                            lastReadValue = unwrap(content[1]);
                        }
                    }
                }
                case "E" -> {
                    String key = unwrap(parts.length > 2 ? parts[2] : "");
                    sendResponse(address, port, txId + " F " + (key != null && dataStore.containsKey(key) ? "Y" : "N"));
                }
                case "F" -> {
                    if (parts.length > 2 && "Y".equals(parts[2].trim())) {
                        lastExistenceCheck = true;
                    }
                }
                case "N" -> handleNearestNodes(txId, parts.length > 2 ? parts[2] : "", address, port);
                case "O" -> {
                    if (parts.length > 2) {
                        nearbyNodes.put(txId, parts[2]);
                    }
                }
                case "V" -> forwardMessage(parts.length > 2 ? parts[2] : "", address, port);
                case "I" -> {
                } // Optional ping/heartbeat
            }
        } catch (Exception e) {
            if (debug) System.err.println("⚠ Error: " + e.getMessage());
        }
    }

    private void handleWrite(String tx, String payload, InetAddress addr, int port) {
        String[] kv = extractKeyValue(payload);
        if (kv != null) {
            dataStore.put(kv[0], kv[1]);
            if (kv[0].startsWith("N:")) {
                try {
                    String[] ipPort = kv[1].split(":");
                    if (ipPort.length == 2) {
                        nodeDirectory.put(kv[0], new InetSocketAddress(ipPort[0], Integer.parseInt(ipPort[1])));
                    }
                } catch (Exception ignored) {
                }
            }
            sendResponse(addr, port, tx + " X A");
        }
    }

    private void handleRead(String tx, String payload, InetAddress addr, int port) {
        String key = unwrap(payload);
        if (key != null) {
            String value = dataStore.get(key);
            sendResponse(addr, port, tx + (value != null ? " S Y " + wrap(value) : " S N"));
        }
    }

    private void handleNearestNodes(String tx, String hash, InetAddress addr, int port) throws Exception {
        List<String> nodeList = new ArrayList<>(nodeDirectory.keySet());
        nodeList.removeIf(name -> !name.startsWith("N:"));
        nodeList.sort(Comparator.comparingInt(name -> {
            try {
                return computeDistance(hash, hashify(name));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }));

        StringBuilder reply = new StringBuilder(tx + " O");
        for (int i = 0; i < Math.min(3, nodeList.size()); i++) {
            String name = nodeList.get(i);
            InetSocketAddress info = nodeDirectory.get(name);
            reply.append(" ").append(wrap(name)).append(wrap(info.getAddress().getHostAddress() + ":" + info.getPort()));
        }

        sendResponse(addr, port, reply.toString());
    }

    private void forwardMessage(String data, InetAddress addr, int port) {
        String[] content = data.split(" ", 2);
        if (content.length < 2) return;

        String target = unwrap(content[0]);
        String innerMsg = content[1];

        if (target != null) {
            if (target.equals(nodeId)) {
                processMessage(innerMsg, addr, port);
            } else if (nodeDirectory.containsKey(target)) {
                InetSocketAddress next = nodeDirectory.get(target);
                sendResponse(next.getAddress(), next.getPort(), "V " + wrap(target) + innerMsg);
            }
        }
    }

    private void sendResponse(InetAddress address, int port, String msg) {
        try {
            for (int i = relayStack.size() - 1; i >= 0; i--) {
                msg = "V " + wrap(relayStack.get(i)) + msg;
            }
            byte[] data = msg.getBytes();
            socket.send(new DatagramPacket(data, data.length, address, port));
            if (debug) System.out.println("📤 Sent: " + msg);
        } catch (IOException e) {
            if (debug) System.err.println("❌ Send error: " + e.getMessage());
        }
    }

    private String wrap(String value) {
        return value.chars().filter(c -> c == ' ').count() + " " + value + " ";
    }

    private String unwrap(String wrapped) {
        int space = wrapped.indexOf(' ');
        return (space != -1 && wrapped.length() > space + 1) ? wrapped.substring(space + 1, wrapped.length() - 1) : null;
    }

    private String[] extractKeyValue(String input) {
        try {
            String[] parts = input.trim().split(" ", 4);
            if (parts.length == 4) return new String[]{parts[1], parts[3]};
        } catch (Exception ignored) {
        }
        return null;
    }

    @Override
    public boolean isActive(String nodeName) {
        return nodeDirectory.containsKey(nodeName);
    }

    @Override
    public void pushRelay(String nodeName) {
        relayStack.push(nodeName);
    }

    @Override
    public void popRelay() {
        if (!relayStack.isEmpty()) relayStack.pop();
    }

    @Override
    public boolean exists(String key) throws Exception {
        return attemptLookup(key, true) != null;
    }

    @Override
    public String read(String key) throws Exception {
        return attemptLookup(key, false);
    }

    private String attemptLookup(String key, boolean existenceCheck) throws Exception {
        if (dataStore.containsKey(key)) return dataStore.get(key);

        String hash = hashify(key);
        Set<String> visited = new HashSet<>();
        Queue<String> toQuery = new LinkedList<>(nodeDirectory.keySet());

        if (nodeDirectory.isEmpty()) {
            nodeDirectory.put("N:azure", new InetSocketAddress("10.200.51.19", 20114));
            toQuery.add("N:azure");
        }

        while (!toQuery.isEmpty()) {
            String node = toQuery.poll();
            if (visited.contains(node) || !nodeDirectory.containsKey(node)) continue;
            visited.add(node);

            InetSocketAddress target = nodeDirectory.get(node);
            String txId = generateTxnId();

            if (existenceCheck) {
                lastExistenceCheck = false;
                sendResponse(target.getAddress(), target.getPort(), txId + " E " + wrap(key));
            } else {
                lastReadValue = null;
                sendResponse(target.getAddress(), target.getPort(), txId + " R " + wrap(key));
            }

            long start = System.currentTimeMillis();
            while (System.currentTimeMillis() - start < 1000) {
                handleIncomingMessages(100);
                if ((existenceCheck && lastExistenceCheck) || (!existenceCheck && lastReadValue != null)) {
                    return existenceCheck ? "YES" : lastReadValue;
                }
            }

            String lookupTx = generateTxnId();
            sendResponse(target.getAddress(), target.getPort(), lookupTx + " N " + hash);

            long lookupStart = System.currentTimeMillis();
            while (!nearbyNodes.containsKey(lookupTx) && System.currentTimeMillis() - lookupStart < 1000) {
                handleIncomingMessages(100);
            }

            String response = nearbyNodes.get(lookupTx);
            if (response == null) continue;

            String[] items = response.trim().split(" ");
            for (int i = 0; i + 3 < items.length; i += 4) {
                String name = items[i + 1];
                String hostPort = items[i + 3];
                if (name.startsWith("N:") && hostPort.contains(":")) {
                    String[] parts = hostPort.split(":");
                    nodeDirectory.put(name, new InetSocketAddress(parts[0], Integer.parseInt(parts[1])));
                    if (!visited.contains(name)) toQuery.add(name);
                }
            }
        }

        return null;
    }

    @Override
    public boolean write(String key, String value) {
        dataStore.put(key, value);
        return true;
    }

    @Override
    public boolean CAS(String key, String currentValue, String newValue) {
        if (!dataStore.containsKey(key)) {
            dataStore.put(key, newValue);
            return true;
        } else if (dataStore.get(key).equals(currentValue)) {
            dataStore.put(key, newValue);
            return true;
        }
        return false;
    }

    private String hashify(String value) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = digest.digest(value.getBytes("UTF-8"));
        StringBuilder hash = new StringBuilder();
        for (byte b : hashBytes) hash.append(String.format("%02x", b));
        return hash.toString();
    }

    private int computeDistance(String h1, String h2) {
        for (int i = 0; i < h1.length(); i++) {
            int d1 = Integer.parseInt(h1.substring(i, i + 1), 16);
            int d2 = Integer.parseInt(h2.substring(i, i + 1), 16);
            int xor = d1 ^ d2;
            for (int bit = 3; bit >= 0; bit--) {
                if (((xor >> bit) & 1) == 1) return i * 4 + (3 - bit);
            }
        }
        return 256;
    }

    private String generateTxnId() {
        return "" + (char) ('A' + random.nextInt(26)) + (char) ('A' + random.nextInt(26));
    }

    public Set<String> getKnownNodeNames() {
        return nodeDirectory.keySet();
    }

    private void startBackgroundListener() {
        listenerThread = new Thread(() -> {
            try {
                while (true) handleIncomingMessages(0);
            } catch (Exception e) {
                if (debug) System.err.println("🧵 Listener error: " + e.getMessage());
            }
        });
        listenerThread.setDaemon(true);
        listenerThread.start();
    }
}
