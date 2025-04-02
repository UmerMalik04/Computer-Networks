import java.net.*;
import java.io.*;
import java.util.*;
import java.security.*;

// Coursework 2024/2025
//
// Submission by
//  YOUR_NAME_GOES_HERE
//  YOUR_STUDENT_ID_NUMBER_GOES_HERE
//  YOUR_EMAIL_GOES_HERE


// DO NOT EDIT starts
// This gives the interface that your code must implement.
// These descriptions are intended to help you understand how the interface
// will be used. See the RFC for how the protocol works.

interface NodeInterface {

    /* These methods configure your node.
     * They must both be called once after the node has been created but
     * before it is used. */
    
    // Set the name of the node.
    public void setNodeName(String nodeName) throws Exception;

    // Open a UDP port for sending and receiving messages.
    public void openPort(int portNumber) throws Exception;


    /*
     * These methods query and change how the network is used.
     */

    // Handle all incoming messages.
    // If you wait for more than delay miliseconds and
    // there are no new incoming messages return.
    // If delay is zero then wait for an unlimited amount of time.
    public void handleIncomingMessages(int delay) throws Exception;
    
    // Determines if a node can be contacted and is responding correctly.
    // Handles any messages that have arrived.
    public boolean isActive(String nodeName) throws Exception;

    // You need to keep a stack of nodes that are used to relay messages.
    // The base of the stack is the first node to be used as a relay.
    // The first node must relay to the second node and so on.
    
    // Adds a node name to a stack of nodes used to relay all future messages.
    public void pushRelay(String nodeName) throws Exception;

    // Pops the top entry from the stack of nodes used for relaying.
    // No effect if the stack is empty
    public void popRelay() throws Exception;
    

    /*
     * These methods provide access to the basic functionality of
     * CRN-25 network.
     */

    // Checks if there is an entry in the network with the given key.
    // Handles any messages that have arrived.
    public boolean exists(String key) throws Exception;
    
    // Reads the entry stored in the network for key.
    // If there is a value, return it.
    // If there isn't a value, return null.
    // Handles any messages that have arrived.
    public String read(String key) throws Exception;

    // Sets key to be value.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean write(String key, String value) throws Exception;

    // If key is set to currentValue change it to newValue.
    // Returns true if it worked, false if it didn't.
    // Handles any messages that have arrived.
    public boolean CAS(String key, String currentValue, String newValue) throws Exception;

}
// DO NOT EDIT ends

// Complete this!
public class Node implements NodeInterface {

    private String id;
    private DatagramSocket commSocket;
    private final Map<String, String> kvStore = new HashMap<>();
    private final Stack<String> relayPath = new Stack<>();
    private final Map<String, InetSocketAddress> knownNodes = new HashMap<>();
    private final Map<String, String> nearestNodeResponses = new HashMap<>();
    private final Set<String> seenTransactions = Collections.newSetFromMap(new LinkedHashMap<>() {
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > 1000;
        }
    });
    private final Random rng = new Random();
    private String recentRead = null;
    private boolean recentExistence = false;
    private final boolean debugLogs = false;
    private Thread backgroundListener;

    @Override
    public void setNodeName(String nodeName) {
        if (!nodeName.startsWith("N:")) throw new IllegalArgumentException("Invalid node name.");
        this.id = nodeName;
    }

    @Override
    public void openPort(int port) throws Exception {
        commSocket = new DatagramSocket(port);
        if (debugLogs) System.out.println("Socket active on port " + port);
        initiateListener();
    }

    @Override
    public void handleIncomingMessages(int timeout) throws Exception {
        commSocket.setSoTimeout(100);
        byte[] packetBuffer = new byte[2048];
        long tStart = System.currentTimeMillis();
        while (timeout == 0 || System.currentTimeMillis() - tStart < timeout) {
            DatagramPacket datagram = new DatagramPacket(packetBuffer, packetBuffer.length);
            try {
                commSocket.receive(datagram);
                String received = new String(datagram.getData(), 0, datagram.getLength());
                if (debugLogs) System.out.println("📩 " + received);
                parseMessage(received, datagram.getAddress(), datagram.getPort());
            } catch (SocketTimeoutException ignored) {}
        }
    }

    private void parseMessage(String msg, InetAddress addr, int port) {
        try {
            if (Math.random() < 0.1) return;

            String[] tokens = msg.trim().split(" ", 3);
            if (tokens.length < 2) return;
            String tx = tokens[0], kind = tokens[1];
            if (seenTransactions.contains(tx)) return;
            seenTransactions.add(tx);

            switch (kind) {
                case "G" -> respond(addr, port, tx + " H " + wrap(id));
                case "H" -> {
                    if (tokens.length > 2) {
                        String incoming = unwrap(tokens[2]);
                        if (incoming != null)
                            knownNodes.put(incoming, new InetSocketAddress(addr, port));
                    }
                }
                case "W" -> {
                    String[] kv = extractKeyValue(tokens.length > 2 ? tokens[2] : "");
                    if (kv != null && kv[0] != null && kv[1] != null) {
                        kvStore.put(kv[0], kv[1]);
                        if (kv[0].startsWith("N:")) {
                            try {
                                String[] ipInfo = kv[1].split(":");
                                if (ipInfo.length == 2)
                                    knownNodes.put(kv[0], new InetSocketAddress(ipInfo[0], Integer.parseInt(ipInfo[1])));
                            } catch (Exception ignored) {}
                        }
                        respond(addr, port, tx + " X A");
                    }
                }
                case "R" -> {
                    String query = unwrap(tokens.length > 2 ? tokens[2] : "");
                    if (query != null) {
                        String reply = kvStore.containsKey(query) ? kvStore.get(query) : null;
                        respond(addr, port, tx + (reply != null ? " S Y " + wrap(reply) : " S N "));
                    }
                }
                case "S" -> {
                    if (tokens.length > 2) {
                        String[] parts = tokens[2].split(" ", 2);
                        if (parts[0].equals("Y") && parts.length > 1)
                            recentRead = unwrap(parts[1]);
                    }
                }
                case "E" -> {
                    String k = unwrap(tokens.length > 2 ? tokens[2] : "");
                    boolean exist = k != null && kvStore.containsKey(k);
                    respond(addr, port, tx + " F " + (exist ? "Y" : "N"));
                }
                case "F" -> {
                    if (tokens.length > 2 && tokens[2].trim().equals("Y")) recentExistence = true;
                }
                case "N" -> {
                    if (tokens.length > 2) {
                        String hash = tokens[2].trim();
                        List<String> options = new ArrayList<>(knownNodes.keySet());
                        options.removeIf(x -> !x.startsWith("N:"));
                        options.sort(Comparator.comparingInt(n -> {
                            try {
                                return computeDistance(hash, hashify(n));
                            } catch (Exception e) {
                                return Integer.MAX_VALUE;
                            }
                        }));
                        StringBuilder resp = new StringBuilder(tx + " O");
                        for (int i = 0; i < Math.min(3, options.size()); i++) {
                            String name = options.get(i);
                            InetSocketAddress target = knownNodes.get(name);
                            if (target != null) {
                                String val = target.getAddress().getHostAddress() + ":" + target.getPort();
                                resp.append(" ").append(wrap(name)).append(wrap(val));
                            }
                        }
                        respond(addr, port, resp.toString());
                    }
                }
                case "O" -> {
                    if (tokens.length > 2) nearestNodeResponses.put(tx, tokens[2]);
                }
                case "V" -> {
                    if (tokens.length > 2) {
                        String[] inner = tokens[2].split(" ", 2);
                        if (inner.length == 2) {
                            String next = unwrap(inner[0]);
                            String payload = inner[1];
                            if (next != null) {
                                if (next.equals(id)) {
                                    parseMessage(payload, addr, port);
                                } else if (knownNodes.containsKey(next)) {
                                    InetSocketAddress forwardTo = knownNodes.get(next);
                                    String wrapped = "V " + wrap(next) + payload;
                                    respond(forwardTo.getAddress(), forwardTo.getPort(), wrapped);
                                }
                            }
                        }
                    }
                }
                case "I" -> {
                    // Optional heartbeat or hello
                }
            }
        } catch (Exception e) {
            if (debugLogs) System.err.println("⚠️ Error: " + e.getMessage());
        }
    }

    private void respond(InetAddress addr, int port, String msg) {
        try {
            for (int i = relayPath.size() - 1; i >= 0; i--) {
                msg = "V " + wrap(relayPath.get(i)) + msg;
            }
            byte[] data = msg.getBytes();
            DatagramPacket p = new DatagramPacket(data, data.length, addr, port);
            commSocket.send(p);
            if (debugLogs) System.out.println("📤 " + msg);
        } catch (IOException ex) {
            if (debugLogs) System.err.println("❌ Failed to send: " + ex.getMessage());
        }
    }

    private String wrap(String s) {
        long blanks = s.chars().filter(c -> c == ' ').count();
        return blanks + " " + s + " ";
    }

    private String unwrap(String s) {
        int space = s.indexOf(' ');
        return (space != -1 && s.length() > space + 1) ? s.substring(space + 1, s.length() - 1) : null;
    }

    private String[] extractKeyValue(String input) {
        try {
            String[] arr = input.trim().split(" ", 4);
            if (arr.length == 4) return new String[]{arr[1], arr[3]};
        } catch (Exception ignored) {}
        return null;
    }

    @Override
    public boolean isActive(String nodeName) {
        return knownNodes.containsKey(nodeName);
    }

    @Override
    public void pushRelay(String nodeName) {
        relayPath.push(nodeName);
    }

    @Override
    public void popRelay() {
        if (!relayPath.isEmpty()) relayPath.pop();
    }

    @Override
    public boolean exists(String key) throws Exception {
        return attemptLookup(key, true) != null;
    }

    @Override
    public String read(String key) throws Exception {
        return attemptLookup(key, false);
    }

    private String attemptLookup(String key, boolean checkExists) throws Exception {
        if (kvStore.containsKey(key)) return kvStore.get(key);

        String hash = hashify(key);
        Set<String> queried = new HashSet<>();
        Queue<String> queue = new LinkedList<>(knownNodes.keySet());

        if (knownNodes.isEmpty()) {
            knownNodes.put("N:azure", new InetSocketAddress("10.200.51.19", 20114));
            queue.add("N:azure");
        }

        while (!queue.isEmpty()) {
            String candidate = queue.poll();
            if (queried.contains(candidate) || !knownNodes.containsKey(candidate)) continue;
            queried.add(candidate);

            InetSocketAddress target = knownNodes.get(candidate);
            String txID = newTxn();
            if (checkExists) {
                recentExistence = false;
                respond(target.getAddress(), target.getPort(), txID + " E " + wrap(key));
            } else {
                recentRead = null;
                respond(target.getAddress(), target.getPort(), txID + " R " + wrap(key));
            }

            long waitStart = System.currentTimeMillis();
            while (System.currentTimeMillis() - waitStart < 1000) {
                handleIncomingMessages(100);
                if ((checkExists && recentExistence) || (!checkExists && recentRead != null)) {
                    return checkExists ? "YES" : recentRead;
                }
            }

            String nTx = newTxn();
            respond(target.getAddress(), target.getPort(), nTx + " N " + hash);

            long wait2 = System.currentTimeMillis();
            while (!nearestNodeResponses.containsKey(nTx) && System.currentTimeMillis() - wait2 < 1000) {
                handleIncomingMessages(100);
            }

            String raw = nearestNodeResponses.get(nTx);
            if (raw == null) continue;

            String[] lines = raw.trim().split(" ");
            for (int i = 0; i + 3 < lines.length; i += 4) {
                String nodeKey = lines[i + 1];
                String address = lines[i + 3];
                if (nodeKey.startsWith("N:") && address.contains(":")) {
                    String[] info = address.split(":");
                    InetSocketAddress peer = new InetSocketAddress(info[0], Integer.parseInt(info[1]));
                    knownNodes.put(nodeKey, peer);
                    if (!queried.contains(nodeKey)) queue.add(nodeKey);
                }
            }
        }
        return null;
    }

    @Override
    public boolean write(String key, String val) {
        kvStore.put(key, val);
        return true;
    }

    @Override
    public boolean CAS(String key, String oldVal, String newVal) {
        if (!kvStore.containsKey(key)) {
            kvStore.put(key, newVal);
            return true;
        } else if (kvStore.get(key).equals(oldVal)) {
            kvStore.put(key, newVal);
            return true;
        }
        return false;
    }

    private String hashify(String s) throws Exception {
        MessageDigest d = MessageDigest.getInstance("SHA-256");
        byte[] bytes = d.digest(s.getBytes("UTF-8"));
        StringBuilder out = new StringBuilder();
        for (byte b : bytes) out.append(String.format("%02x", b));
        return out.toString();
    }

    private int computeDistance(String a, String b) {
        for (int i = 0; i < a.length(); i++) {
            int d1 = Integer.parseInt(a.substring(i, i + 1), 16);
            int d2 = Integer.parseInt(b.substring(i, i + 1), 16);
            int diff = d1 ^ d2;
            for (int bit = 3; bit >= 0; bit--) {
                if (((diff >> bit) & 1) == 1) return i * 4 + (3 - bit);
            }
        }
        return 256;
    }

    private String newTxn() {
        return "" + (char) ('A' + rng.nextInt(26)) + (char) ('A' + rng.nextInt(26));
    }

    public Set<String> getKnownNodeNames() {
        return knownNodes.keySet();
    }

    private void initiateListener() {
        backgroundListener = new Thread(() -> {
            try {
                while (true) handleIncomingMessages(0);
            } catch (Exception e) {
                if (debugLogs) System.err.println("🧵 Listener crash: " + e.getMessage());
            }
        });
        backgroundListener.setDaemon(true);
        backgroundListener.start();
    }
}
