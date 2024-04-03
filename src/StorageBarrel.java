import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.rmi.registry.LocateRegistry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class StorageBarrel extends Thread {
  private String HOST_NAME = "224.3.2.1";
  private int PORT = 4321;

  private MulticastSocket socket = null;
  private NetworkInterface networkInterface;
  private InetAddress mcastaddr;

  // Palavra -> Lista de URLs
  private ConcurrentHashMap<String, HashSet<String>> storage = new ConcurrentHashMap<String, HashSet<String>>();
  private ConcurrentHashMap<String, WordList> tempStorage = new ConcurrentHashMap<String, WordList>();

  // URL -> Lista de URLs que referenciam a URL
  private ConcurrentHashMap<String, HashSet<String>> urls = new ConcurrentHashMap<String, HashSet<String>>();
  // ToDo: Receber mensagem com URL que a página atual referencia

  public static void main(String[] args) {
    StorageBarrel gateway = new StorageBarrel();
    gateway.start();
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

        System.out.println("Waiting for a message from the multicast group...");
        System.out.println("Address: " + packet.getAddress().getHostAddress());
        System.out.println("Address: " + packet.getSocketAddress());
        System.out.println("Port: " + packet.getPort());

        System.out.println("\n\n");

        processMessage(new String(buffer, 0, packet.getLength()));
        buffer = new byte[1024];
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private String processMessage(String message) {
    System.out.println("--->Processing message");
    String req[] = message.split(";");
    String[] identifier = req[0].trim().split("\\|");

    if (!identifier[0].trim().toUpperCase().equals("TYPE")) {
      System.out.println("Does not have a type identifier");

      String res = "TYPE | ERROR; MESSAGE | Does not have a type identifier; req | " + message;
      return res;
    }

    String type = identifier[1].trim().toUpperCase();
    String[] messageContent = new String[req.length - 1];
    System.arraycopy(req, 1, messageContent, 0, req.length - 1);

    System.out.println("Type: " + type);

    try {
      switch (type) {
        case "WORD_LIST":
          updateStorage(messageContent);

          System.out.println("---------------------Storage updated---------------------");
          System.out.println("Storage size: " + storage.size());
          // printStorage();
          return "";

        case "REFERENCED_URLS":
          addReferencedUrls(messageContent);

          System.out.println("---------------------Referenced URLs updated---------------------");
          // printUrls();
          System.out.println("Referenced URLs size: " + urls.size());
          return "";
        default:
          System.out.println("Invalid message type");
          return "TYPE | ERROR; MESSAGE | Invalid message type; req | " + message;
      }
    } catch (Exception e) {
      e.printStackTrace();
      return "TYPE | ERROR; MESSAGE | Error processing message ; req | " + message;
    }
  }

  private void updateStorage(String[] message) {
    List<String> newWords = new LinkedList<>();

    String url = message[0].trim().split("\\|")[1];

    int qntWords = Integer.parseInt(message[1].split("\\|")[1].trim());

    String[] items = new String[message.length - 2];

    System.arraycopy(message, 2, items, 0, message.length - 2);

    Boolean isPartMessage = qntWords != items.length;

    for (String item : items) {
      String[] wordContent = item.trim().split("\\|");
      Integer wordNumber = Integer.parseInt(wordContent[0].trim());
      String word = wordContent[1];

      newWords.add(word);
    }

    if (isPartMessage) {
      if (tempStorage.containsKey(url)) {
        WordList wordList = tempStorage.get(url);
        wordList.addWord(newWords);
        if (wordList.getSize() == wordList.getWordList().size()) {
          addWords(url, wordList.getWordList());
          tempStorage.remove(url);
        }
      } else {
        WordList wordList = new WordList(url, qntWords, newWords);
        tempStorage.put(url, wordList);

        new java.util.Timer().schedule(
            new java.util.TimerTask() {
              @Override
              public void run() {
                if (tempStorage.containsKey(url)) {
                  try {
                    IUrlQueue urlQueue = (IUrlQueue) LocateRegistry.getRegistry(6666).lookup("urlQueue");

                    urlQueue.addUrlFirst(url);
                    tempStorage.remove(url);
                  } catch (Exception e) {
                    System.out.println("Exception in main: " + e);
                    e.printStackTrace();
                  }
                }
              }
            },
            5000);
        System.out.println("Timeout set");
      }
    } else {
      addWords(url, newWords);
    }
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
    int qntLinks = Integer.parseInt(message[1].split("\\|")[1].trim());

    String[] items = new String[message.length - 2];
    System.arraycopy(message, 2, items, 0, message.length - 2);
    Boolean isPartMessage = qntLinks != items.length;

    for (String item : items) {
      String[] linkContent = item.trim().split("\\|");
      // Integer wordNumber = Integer.parseInt(linkContent[0].trim());
      System.out.println("Link: " + Arrays.toString(linkContent));
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

    if (isPartMessage) {
      // ToDo: Tratar mensagem parcial
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

}
