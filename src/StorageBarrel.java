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

  // Palavra -> Lista de URLs
  private ConcurrentHashMap<String, HashSet<String>> storage = new ConcurrentHashMap<String, HashSet<String>>();

  // URL -> Lista de URLs que referenciam a URL
  private ConcurrentHashMap<String, HashSet<String>> urls = new ConcurrentHashMap<String, HashSet<String>>();

  // ToDo: Clean received messages after a while
  // senderId -> conjunto de messageId
  private HashMap<UUID, HashSet<Integer>> receivedMessages = new HashMap<UUID, HashSet<Integer>>();

  // senderId -> conjunto de messageId
  private HashMap<UUID, Integer> retrievingMessages = new HashMap<UUID, Integer>();

  // Buffer de mensagens aguardando para serem computadas
  // senderId -> (messageId -> Req)
  private HashMap<UUID, HashMap<Integer, Req>> messageBuffer = new HashMap<UUID, HashMap<Integer, Req>>();

  public StorageBarrel(String hostName, int port, int portRetrieve) {
    this.HOST_NAME = hostName;
    this.PORT = port;
    this.PORT_RETRIEVE = portRetrieve;
  }

  @Override
  public void run() {

    System.out.println("Storage Barrel " + super.getId() + " running");

    try {
      socket = new MulticastSocket(PORT);
      networkInterface = NetworkInterface.getByIndex(0);
      mcastaddr = InetAddress.getByName(HOST_NAME);

      socket.joinGroup(new InetSocketAddress(mcastaddr, 0), networkInterface);

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

    HashSet<Integer> messagesHistoric = receivedMessages.get(req.getSenderId());
    int expectedMessageId = 0;

    // System.out.println("MessageHistoric: " + messagesHistoric);

    if (messagesHistoric != null) {
      isNewMessage = !messagesHistoric.contains(req.getMessageId());
      expectedMessageId = messagesHistoric.stream().max(Integer::compare).orElse(0) + 1;
    }

    if (!isNewMessage) {
      System.out.println("-->Message already received");
      return;
    }

    if (req.getMessageId() != expectedMessageId) {
      System.out.println("-->Unexpected message id");
      System.out.println("--->Expected: " + expectedMessageId);
      System.out.println("--->Received: " + req.getMessageId());
      addMessageToBuffer(req);

      Integer retrievingMessageId = retrievingMessages.get(req.getSenderId());

      if (retrievingMessageId == null || retrievingMessageId != expectedMessageId) {
        // System.out.println("--->Message " + req.getMessageId() + " requesting
        // retrieve of " + expectedMessageId);

        // retrievingMessageId = expectedMessageId;
        retrievingMessages.put(req.getSenderId(), expectedMessageId);
        requestRetrieve(req, expectedMessageId);
      }
      return;
    }
    expectedMessageId++;

    try {
      switch (req.getType()) {
        case "WORD_LIST":
          updateStorage(req.getContent());

          System.out.println("---------------------Storage updated---------------------");
          break;

        case "REFERENCED_URLS":
          addReferencedUrls(req.getContent());

          System.out.println("---------------------Referenced URLs updated---------------------");
          break;
        default:
          System.out.println("Invalid message type");
          break;
      }

      if (messagesHistoric != null) {
        messagesHistoric.add(req.getMessageId());
      } else {
        System.out.println("===========================>Creating new message history");
        HashSet<Integer> received = new HashSet<Integer>();
        received.add(req.getMessageId());
        receivedMessages.put(req.getSenderId(), received);
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
            Integer receivedMessageId = 0;

            if (receivedMessages.containsKey(req.getSenderId())) {
              receivedMessageId = receivedMessages.get(req.getSenderId()).stream().max(Integer::compare).orElse(0);
            }

            if (messageToRetrieve < receivedMessageId) {
              return;
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

  private void printStorage() {
    Iterator<String> it = storage.keySet().iterator();
    while (it.hasNext()) {
      String word = it.next();
      HashSet<String> urls = storage.get(word);

      System.out.println("Word: " + word);
      System.out.println("URLs: " + urls.toString());
    }
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
