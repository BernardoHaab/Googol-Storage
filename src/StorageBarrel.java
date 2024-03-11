import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.util.concurrent.ConcurrentLinkedDeque;

public class StorageBarrel extends Thread {
  private String HOST_NAME = "224.3.2.1";
  private int PORT = 4321;

  private MulticastSocket socket = null;
  private NetworkInterface networkInterface;
  private InetAddress mcastaddr;

  private ConcurrentLinkedDeque<String> urls;

  public static void main(String[] args) {
    StorageBarrel gateway = new StorageBarrel();
    gateway.start();
  }

  @Override
  public void run() {
    urls = new ConcurrentLinkedDeque<>();
    urls.add("www.uc.pt");

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
        System.out.println("Received a message from the multicast group: " + new String(buffer));

        multicastRouter(new String(buffer));
        System.out.println("Message processed");
        buffer = new byte[1024];
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void multicastRouter(String message) throws IOException {
    // Primeira parte da mensagem identifica o tipo, com demais valores opcionais
    // TYPE | <IDENTIFICADOR>; <CHAVE> | <VALOR>
    String[] req = message.split(";");
    String[] identifier = req[0].trim().split("\\|");

    System.out.println("Identifier: " + identifier[0] + " | " + identifier[1]);
    System.out.println(identifier[0].trim().toUpperCase());

    if (!identifier[0].trim().toUpperCase().equals("TYPE")) {
      System.out.println("Invalid message type");

      String res = "TYPE | Error; MESSAGE | Invalid message type; req | " + message;
      byte[] buffer = res.getBytes();

      DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT);
      socket.send(packet);
      return;
    }

    String type = identifier[1].trim().toUpperCase();

    switch (type) {
      case "GET_NEXT_URL": // TYPE | GET_NEXT_URL
        String newUrl = urls.poll();
        String res = "TYPE | RES; GET_NEXT_URL | " + newUrl; // Add IP and PORT to identify destination
        System.out.println("------> Vai enviar nova URL");
        sendResponse(res);
        break;

      default:
        break;
    }

  }

  private void sendResponse(String response) throws IOException {
    byte[] buffer = response.getBytes();
    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, mcastaddr, PORT);
    socket.send(packet);
  }

}
