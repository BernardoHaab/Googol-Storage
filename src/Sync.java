import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentHashMap.KeySetView;

public class Sync extends Thread {

  private final int BUFFER_SIZE = 1024;

  private String HOST_NAME;
  private int PORT;
  private MulticastSocket socket;
  private InetAddress group;

  private UUID storageId;
  private int messageId;

  // Palavra -> Lista de URLs
  private ConcurrentHashMap<String, HashSet<String>> storage = new ConcurrentHashMap<String, HashSet<String>>();

  // URL -> Lista de URLs que referenciam a URL
  private ConcurrentHashMap<String, HashSet<String>> urls = new ConcurrentHashMap<String, HashSet<String>>();

  // senderId -> ultima mensagem recebida
  private HashMap<UUID, Integer> lastMessages = new HashMap<UUID, Integer>();

  public Sync(String hostName, int port, ConcurrentHashMap<String, HashSet<String>> storage,
      ConcurrentHashMap<String, HashSet<String>> urls, HashMap<UUID, Integer> lastMessages, UUID storageId)
      throws Exception {
    this.HOST_NAME = hostName;
    this.PORT = port;
    this.storage = storage;
    this.urls = urls;
    this.lastMessages = lastMessages;

    socket = new MulticastSocket(PORT);
    group = InetAddress.getByName(this.HOST_NAME);

    this.storageId = storageId;
    this.messageId = 0;

    start();
  }

  @Override
  public void run() {

    try {
      sendLasMessages();
      sendIndex();
      sendUrls();
    } catch (Exception e) {
      // TODO: handle exception
    }

  }

  public void sendLasMessages() throws IOException {
    String message = createMulticastMessage("SYNC_MESSAGES");
    int standartSize = message.getBytes().length;
    int buffer_size = 0;

    Set<UUID> keys = lastMessages.keySet();

    for (UUID downloaderId : keys) {
      Integer lastMessage = lastMessages.get(downloaderId);

      String newDownloader = downloaderId + "|" + lastMessage;

      if (buffer_size + newDownloader.getBytes().length > BUFFER_SIZE) {
        sendMulticast(message);
        message = createMulticastMessage("SYNC_MESSAGES")
            + ";" + newDownloader;
      } else {
        message += ";" + newDownloader;
      }
      buffer_size = message.getBytes().length;
    }

    if (buffer_size > standartSize) {
      sendMulticast(message);
    }
  }

  public void sendIndex() throws IOException {
    String message = createMulticastMessage("SYNC_INDEX");
    int standartSize = message.getBytes().length;
    int buffer_size = 0;

    KeySetView<String, HashSet<String>> keySet = storage.keySet();

    Iterator<String> it = keySet.iterator();

    while (it.hasNext()) {
      String word = it.next();

      HashSet<String> urls = storage.get(word);

      for (String url : urls) {
        String newUrl = word + "|" + url;

        if (buffer_size + newUrl.getBytes().length > BUFFER_SIZE) {
          sendMulticast(message);
          message = createMulticastMessage("SYNC_INDEX")
              + ";" + newUrl;
        } else {
          message += ";" + newUrl;
        }
        buffer_size = message.getBytes().length;
      }

    }

    if (buffer_size > standartSize) {
      sendMulticast(message);
    }
  }

  public void sendUrls() throws IOException {
    String message = createMulticastMessage("SYNC_URLS");
    int standartSize = message.getBytes().length;
    int buffer_size = 0;

    KeySetView<String, HashSet<String>> keySet = urls.keySet();

    Iterator<String> it = keySet.iterator();

    while (it.hasNext()) {
      String page = it.next();

      HashSet<String> urls = storage.get(page);

      for (String url : urls) {
        String newUrl = page + "|" + url;

        if (buffer_size + newUrl.getBytes().length > BUFFER_SIZE) {
          sendMulticast(message);
          message = createMulticastMessage("SYNC_URLS")
              + ";" + newUrl;
        } else {
          message += ";" + newUrl;
        }
        buffer_size = message.getBytes().length;
      }

    }

    if (buffer_size > standartSize) {
      sendMulticast(message);
    }
  }

  private String createMulticastMessage(String type) {
    return storageId.toString() + "|" + messageId + ";TYPE|" + type;
  }

  private void sendMulticast(String message) throws IOException {
    // messageBuffer.put(messageId, message);
    byte[] buffer = message.getBytes();
    // System.out.println("\tTotal bytes: " + buffer.length);
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group, PORT);
    socket.send(packet);
    messageId++;

    // if (messageBuffer.keySet().size() > 25) {
    // messageBuffer.keySet().stream().sorted().limit(5).forEach(key -> {
    // messageBuffer.remove(key);
    // });
    // }
  }

}
