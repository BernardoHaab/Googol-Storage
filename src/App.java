import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class App {

  private static String barrelHost;
  private static int barrelPortSend;
  private static int barrelPortRetrieve;

  public static void main(String[] args) {
    try {
      if (args.length == 0) {
        System.out.println("Usage: java App <properties file>");
        return;
      }

      String fileName = args[0];

      readFileProperties(fileName);

      StorageBarrel barrel = new StorageBarrel(barrelHost, barrelPortSend, barrelPortRetrieve);
      barrel.start();

      // downloader.start();
    } catch (Exception e) {
      System.out.println("Error on main: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private static void readFileProperties(String fileName) {
    File propFile = new File(fileName);

    try {
      Scanner myReader = new Scanner(propFile);
      while (myReader.hasNextLine()) {
        String line = myReader.nextLine();
        System.out.println(line);

        String[] parts = line.split(";");

        switch (parts[0]) {
          case "multicast":
            barrelHost = parts[1];
            barrelPortSend = Integer.parseInt(parts[2]);
            barrelPortRetrieve = Integer.parseInt(parts[3]);
            break;

          default:
            break;
        }

      }
      myReader.close();
    } catch (FileNotFoundException e) {
      System.out.println("Arquivo de propriedades não encontrado.");
      e.printStackTrace();
    } catch (IndexOutOfBoundsException e) {
      System.out.println("Arquivo de propriedades mal formatado.");
      e.printStackTrace();
    }
  }
}
