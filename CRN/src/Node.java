import java.net.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;// IN2011 Computer Networks
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

    private String nodeName;
    private DatagramSocket socket;
    private Map<String, String> keyValueStore = new HashMap<>();
    private List<String> relayStack = new ArrayList<>();

    public void setNodeName(String nodeName) throws Exception {
        this.nodeName = nodeName;
    }

    public void openPort(int portNumber) throws Exception {
        socket = new DatagramSocket(portNumber);
    }

    public void handleIncomingMessages(int delay) throws Exception {
        byte[] receiveData = new byte[1024];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);

        while (true) {
            socket.receive(receivePacket);
            String message = new String(receivePacket.getData(), 0, receivePacket.getLength());
            processMessage(message, receivePacket.getAddress(), receivePacket.getPort());

            // If delay is zero, wait indefinitely
            if (delay == 0) continue;
            // Otherwise, break if delay is reached
            Thread.sleep(delay);
        }
    }

    private void processMessage(String message, InetAddress address, int port) throws Exception {
        String[] parts = message.split(" ", 2);
        String transactionID = parts[0];
        String content = parts[1];

        if (content.startsWith("G")) {
            sendNameResponse(transactionID, address, port);
        } else if (content.startsWith("N")) {
            sendNearestResponse(transactionID, address, port, content.substring(2));
        } else if (content.startsWith("E")) {
            sendKeyExistenceResponse(transactionID, address, port, content.substring(2));
        } else if (content.startsWith("R")) {
            sendReadResponse(transactionID, address, port, content.substring(2));
        } else if (content.startsWith("W")) {
            String[] keyValue = content.substring(2).split(" ", 2);
            sendWriteResponse(transactionID, address, port, keyValue[0], keyValue[1]);
        } else if (content.startsWith("C")) {
            String[] keyValue = content.substring(2).split(" ", 3);
            sendCASResponse(transactionID, address, port, keyValue[0], keyValue[1], keyValue[2]);
        } else if (content.startsWith("V")) {
            handleRelayMessage(transactionID, content.substring(2), address, port);
        }
    }

    private void sendNameResponse(String transactionID, InetAddress address, int port) throws Exception {
        String response = transactionID + " H " + nodeName;
        sendMessage(response, address, port);
    }

    private void sendNearestResponse(String transactionID, InetAddress address, int port, String hashID) throws Exception {
        String response = transactionID + " O " + "0 N:test 0 127.0.0.1:20110 ";  // Example
        sendMessage(response, address, port);
    }

    private void sendKeyExistenceResponse(String transactionID, InetAddress address, int port, String key) throws Exception {
        String exists = keyValueStore.containsKey(key) ? "Y" : "N";
        String response = transactionID + " F " + exists;
        sendMessage(response, address, port);
    }

    private void sendReadResponse(String transactionID, InetAddress address, int port, String key) throws Exception {
        String value = keyValueStore.get(key);
        String response = value != null ? transactionID + " S Y " + value : transactionID + " S N ";
        sendMessage(response, address, port);
    }

    private void sendWriteResponse(String transactionID, InetAddress address, int port, String key, String value) throws Exception {
        keyValueStore.put(key, value);
        String response = transactionID + " X R ";
        sendMessage(response, address, port);
    }

    private void sendCASResponse(String transactionID, InetAddress address, int port, String key, String currentValue, String newValue) throws Exception {
        String storedValue = keyValueStore.get(key);
        String response;
        if (storedValue != null && storedValue.equals(currentValue)) {
            keyValueStore.put(key, newValue);
            response = transactionID + " D R ";
        } else {
            response = transactionID + " D N ";
        }
        sendMessage(response, address, port);
    }

    private void handleRelayMessage(String transactionID, String relayMessage, InetAddress address, int port) throws Exception {
        String[] relayParts = relayMessage.split(" ", 2);
        String targetNode = relayParts[0];
        sendMessage(transactionID + " V " + targetNode + " " + relayParts[1], address, port);
    }

    private void sendMessage(String message, InetAddress address, int port) throws Exception {
        DatagramPacket packet = new DatagramPacket(message.getBytes(), message.length(), address, port);
        socket.send(packet);
    }

    public boolean isActive(String nodeName) throws Exception {
        // Placeholder for checking if node is active
        return true;
    }

    public void pushRelay(String nodeName) throws Exception {
        relayStack.add(nodeName);
    }

    public void popRelay() throws Exception {
        if (!relayStack.isEmpty()) {
            relayStack.remove(relayStack.size() - 1);
        }
    }

    public boolean exists(String key) throws Exception {
        return keyValueStore.containsKey(key);
    }

    public String read(String key) throws Exception {
        return keyValueStore.get(key);
    }

    public boolean write(String key, String value) throws Exception {
        keyValueStore.put(key, value);
        return true;
    }

    public boolean CAS(String key, String currentValue, String newValue) throws Exception {
        if (keyValueStore.containsKey(key) && keyValueStore.get(key).equals(currentValue)) {
            keyValueStore.put(key, newValue);
            return true;
        }
        return false;
    }

    public static String computeHashID(String s) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        byte[] hashBytes = md.digest(s.getBytes(StandardCharsets.UTF_8));
        StringBuilder hexString = new StringBuilder();
        for (byte b : hashBytes) {
            hexString.append(String.format("%02x", b));
        }
        return hexString.toString();
    }
}
