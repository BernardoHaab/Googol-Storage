import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StorageBarrel extends Thread {
  private String HOST_NAME;
  private int PORT;
  private int PORT_RETRIEVE;

  private MulticastSocket socket = null;
  private NetworkInterface networkInterface;
  private InetAddress mcastaddr;
  private UUID storageId;
  private int syncId;

  private boolean isSyncing = false;
  private boolean isReady = false;

  // Palavra -> Lista de URLs
  private ConcurrentHashMap<String, HashSet<String>> storage = new ConcurrentHashMap<String, HashSet<String>>();

  // URL -> Lista de URLs que referenciam a URL
  private ConcurrentHashMap<String, HashSet<String>> urls = new ConcurrentHashMap<String, HashSet<String>>();

  // ToDo: Clean received messages after a while
  // senderId -> conjunto de messageId
  private HashMap<UUID, Integer> lastMessages = new HashMap<UUID, Integer>();

  // senderId -> conjunto de messageId
  private HashMap<UUID, Integer> retrievingMessages = new HashMap<UUID, Integer>();
  private HashMap<UUID, Integer> attemptedRetrives = new HashMap<UUID, Integer>();

  // Buffer de mensagens aguardando para serem computadas
  // senderId -> (messageId -> Req)
  private HashMap<UUID, HashMap<Integer, Req>> messageBuffer = new HashMap<UUID, HashMap<Integer, Req>>();

  public StorageBarrel(String hostName, int port, int portRetrieve) {
    this.HOST_NAME = hostName;
    this.PORT = port;
    this.PORT_RETRIEVE = portRetrieve;
    this.storageId = UUID.randomUUID();
    this.syncId = 0;
  }

  @Override
  public void run() {

    System.out.println("Storage Barrel " + super.getId() + " running");

    try {
      socket = new MulticastSocket(PORT);
      networkInterface = NetworkInterface.getByIndex(0);
      mcastaddr = InetAddress.getByName(HOST_NAME);

      socket.joinGroup(new InetSocketAddress(mcastaddr, 0), networkInterface);

      isReady = true;
      byte[] buffer = new byte[1024];

      while (true) {
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);

        System.out.println("\n\n");
        System.out.println("Waiting for a message from the multicast group...");
        System.out.println("Address: " + packet.getAddress().getHostAddress());
        System.out.println("Address: " + packet.getSocketAddress());
        System.out.println("Port: " + packet.getPort());

        Req req = parseMessage(new String(buffer));
        processReq(req);
        buffer = new byte[1024];

        System.out.println("\t\t --> Tamanho do Index: " + storage.size());

      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private Req parseMessage(String message) {
    try {
      return new Req(message);
    } catch (Exception e) {
      System.out.println("Error parsing message");
      return null;
    }
  }

  /**
   * Se recebe a mensagem esperada, computa ela, incrementa o expectedMessageId e
   * ve se a próxima esta no buffer
   * - Se esta processa ela da mesma forma (Chama recursivamente o processMessage)
   * Se recebe mensagem com id menor que o esperado, ignora
   * Se recebe mensagem com id maior que o esperado, coloca ela no buffer e cria
   * timeout para enviar pedido de reenvio
   * - Quando timeout estourar, Se o expectedMessageId for < que o id da mensagem
   * no buffer, envia pedido de reenvio da expectedMessageId
   */
  private void processReq(Req req) {
    System.out.println("--->Processing message");
    boolean isNewMessage = true;

    Integer lastMessage = lastMessages.get(req.getSenderId());
    int expectedMessageId = 0;

    // System.out.println("MessageHistoric: " + lastMessage);

    if (lastMessage != null) {
      isNewMessage = true;
      expectedMessageId = lastMessage + 1;
    }

    if (!isNewMessage) {
      System.out.println("-->Message already received");
      return;
    }

    if (req.getMessageId() != expectedMessageId && isReady) {
      System.out.println("-->Unexpected message id");
      System.out.println("--->Expected: " + expectedMessageId);
      System.out.println("--->Received: " + req.getMessageId());
      addMessageToBuffer(req);

      Integer retrievingMessageId = retrievingMessages.get(req.getSenderId());

      if (retrievingMessageId == null || retrievingMessageId != expectedMessageId) {
        retrievingMessages.put(req.getSenderId(), expectedMessageId);
        requestRetrieve(req, expectedMessageId);
      }
      return;
    }
    expectedMessageId++;

    try {

      if (isSyncing) {
        switch (req.getType()) {
          case "SYNC_MESSAGES":
            reSyncLastMessages(req.getContent());
            break;
          case "SYNC_INDEX":
            reSyncIndex(req.getContent());
            break;
          case "SYNC_URLS":
            reSyncUrls(req.getContent());
            break;
          default:
            System.out.println("Invalid message type - Syncing");
            System.out.println("Type: " + req.getType());
            break;
        }
      }

      if (isReady) {
        switch (req.getType()) {
          case "WORD_LIST":
            updateStorage(req.getContent());

            System.out.println("---------------------Storage updated---------------------");
            break;

          case "REFERENCED_URLS":
            addReferencedUrls(req.getContent());

            System.out.println("---------------------Referenced URLs updated---------------------");
            break;
          case "REQ_SYNC":
            System.out.println("---------------------RECEBEU PEDIDO DE RESYNC---------------------");
            ConcurrentHashMap<String, HashSet<String>> storageCopy = new ConcurrentHashMap<String, HashSet<String>>();
            ConcurrentHashMap<String, HashSet<String>> urlsCopy = new ConcurrentHashMap<String, HashSet<String>>();
            HashMap<UUID, Integer> lastMessagesCopy = new HashMap<UUID, Integer>();

            for (String word : storage.keySet()) {
              HashSet<String> wordUrls = storage.get(word);
              storageCopy.put(word, wordUrls);
            }

            for (String url : urls.keySet()) {
              HashSet<String> page = urls.get(url);
              urlsCopy.put(url, page);
            }

            for (UUID senderId : lastMessages.keySet()) {
              Integer lastMessageId = lastMessages.get(senderId);
              lastMessagesCopy.put(senderId, lastMessageId);
            }

            new Sync(HOST_NAME, PORT, storageCopy, urlsCopy, lastMessagesCopy, storageId);

            break;
          default:
            System.out.println("Invalid message type");
            System.out.println("Type: " + req.getType());
            break;
        }
      }

      if (lastMessage != null) {
        lastMessages.put(req.getSenderId(), req.getMessageId());
      } else {
        System.out.println("===========================>Creating new message history");
        // HashSet<Integer> received = new HashSet<Integer>();
        // received.add(req.getMessageId());
        lastMessages.put(req.getSenderId(), req.getMessageId());
      }

      if (messageBuffer.containsKey(req.getSenderId())) {
        HashMap<Integer, Req> buffer = messageBuffer.get(req.getSenderId());
        if (buffer.containsKey(expectedMessageId)) {

          Req nextMessage = buffer.get(expectedMessageId);
          buffer.remove(expectedMessageId);
          processReq(nextMessage);
        }
      }

    } catch (Exception e) {
      System.out.println("Error parsing message");
    }
  }

  private void addMessageToBuffer(Req req) {
    UUID senderId = req.getSenderId();
    int messageId = req.getMessageId();

    if (messageBuffer.containsKey(senderId)) {
      HashMap<Integer, Req> buffer = messageBuffer.get(senderId);
      buffer.put(messageId, req);
    } else {
      HashMap<Integer, Req> buffer = new HashMap<Integer, Req>();
      buffer.put(messageId, req);
      messageBuffer.put(senderId, buffer);
    }
  }

  private void requestRetrieve(Req req, int messageToRetrieve) {
    new java.util.Timer().schedule(
        new java.util.TimerTask() {
          @Override
          public void run() {
            // Verifica se a mensagem esperada foi recebida durante o intervalo
            Integer lastMessage = 0;

            if (lastMessages.containsKey(req.getSenderId())) {
              lastMessage = lastMessages.get(req.getSenderId());
            }

            if (messageToRetrieve < lastMessage) {
              return;
            }

            Integer attempts = attemptedRetrives.get(req.getSenderId());

            if (attempts == null) {
              attempts = 0;
            }

            System.out.println("----------------------------------------------------------------------");
            System.out.println("Attempts: " + attempts);

            if (attempts >= 3) {
              try {
                System.out.println("===================>Syncing with other storages");
                String message = storageId + "|" + syncId + ";TYPE|REQ_SYNC;";
                byte[] buffer = message.getBytes();
                DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT);
                socket.send(packet);
                isSyncing = true;
                isReady = false;
                return;
              } catch (Exception e) {
                System.out.println("Error requesting sync");
              }
            } else {
              attempts++;
              attemptedRetrives.put(req.getSenderId(), attempts);
            }

            // Se não foi, pede ela
            try {
              System.out.println("Retrieving message: " + messageToRetrieve);
              String message = "TYPE|RETRIEVE; " + req.getSenderId() + "|" +
                  messageToRetrieve;
              sendRetrieveMessage(message);
            } catch (Exception e) {
              System.out.println("Error sending request");
            } finally {
              // Enquanto não receber a mensagem esperada, fica pedindo ela
              requestRetrieve(req, messageToRetrieve);
            }
          }
        },
        200);
  }

  private void updateStorage(String[] message) {
    List<String> newWords = new LinkedList<>();

    String url = message[0].trim().split("\\|")[1];

    String[] items = new String[message.length - 2];
    System.arraycopy(message, 2, items, 0, message.length - 2);

    for (String item : items) {
      String[] wordContent = item.trim().split("\\|");
      String word = wordContent[1];

      newWords.add(word);
    }

    addWords(url, newWords);
  }

  private void addWords(String url, List<String> words) {
    for (String word : words) {
      if (storage.containsKey(word)) {
        HashSet<String> urls = storage.get(word);
        urls.add(url);
      } else {
        HashSet<String> urls = new HashSet<String>();
        urls.add(url);
        storage.put(word, urls);

      }
    }
  }

  private void addReferencedUrls(String[] message) {
    String url = message[0].trim().split("\\|")[1];

    String[] items = new String[message.length - 2];
    System.arraycopy(message, 2, items, 0, message.length - 2);

    for (String item : items) {
      String[] linkContent = item.trim().split("\\|");
      String link = linkContent[1];

      HashSet<String> referencedBy = urls.get(link);

      if (referencedBy == null) {
        referencedBy = new HashSet<String>();
        referencedBy.add(url);
        urls.put(link, referencedBy);
      } else {
        referencedBy.add(url);
      }
    }
  }

  private void reSyncLastMessages(String[] messages) {
    for (String message : messages) {
      String[] parts = message.split("\\|");
      UUID senderId = UUID.fromString(parts[0].trim());
      int messageId = Integer.parseInt(parts[1].trim());

      lastMessages.put(senderId, messageId);
    }
    isReady = true;
  }

  private void reSyncIndex(String[] messages) {
    for (String message : messages) {
      String[] parts = message.split("\\|");
      String word = parts[0].trim();
      String url = parts[1].trim();

      if (storage.contains(word)) {
        HashSet<String> urls = storage.get(word);
        urls.add(url);
      } else {
        HashSet<String> urls = new HashSet<String>();
        urls.add(url);
        storage.put(word, urls);
      }
    }
  }

  private void reSyncUrls(String[] messages) {
    for (String message : messages) {
      String[] parts = message.split("\\|");
      String word = parts[0].trim();
      String url = parts[1].trim();

      if (urls.contains(url)) {
        HashSet<String> referencedBy = urls.get(url);
        referencedBy.add(word);
      } else {
        HashSet<String> referencedBy = new HashSet<String>();
        referencedBy.add(word);
        urls.put(url, referencedBy);
      }
    }
  }

  private void printStorage() {
    System.out.println("-----START - Printing storage-----");
    Iterator<String> it = storage.keySet().iterator();
    while (it.hasNext()) {
      String word = it.next();
      HashSet<String> urls = storage.get(word);

      System.out.println("Word: " + word);
      System.out.println("URLs: " + urls.toString());
    }
    System.out.println("-----END - Printing storage-----");
  }

  private void printUrls() {
    Iterator<String> it = urls.keySet().iterator();
    while (it.hasNext()) {
      String url = it.next();
      HashSet<String> referencedBy = urls.get(url);

      System.out.println("URL: " + url);
      System.out.println("Referenced by: " + referencedBy.toString());
    }
  }

  private void sendRetrieveMessage(String message) throws IOException {
    System.out.println("Sending retrieve message: " + message);
    byte[] buffer = message.getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT_RETRIEVE);
    socket.send(packet);
  }

}
