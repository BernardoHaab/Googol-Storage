package googol;

import java.io.File;
import java.io.FileNotFoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import jakarta.persistence.Persistence;

public class App {

  private static String barrelHost;
  private static int barrelPortSend;
  private static int barrelPortRetrieve;

  private static int servicePort;
  private static String serviceName;

  public static void main(String[] args) {

    EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("default");
    EntityManager em = entityManagerFactory.createEntityManager();

    try {
      if (args.length == 0) {
        System.out.println("Usage: java App <properties file>");
        return;
      }

      String fileName = args[0];

      readFileProperties(fileName);

      Service service = new Service();
      Registry registry = LocateRegistry.createRegistry(servicePort);
      registry.rebind(serviceName, service);

      StorageBarrel barrel = new StorageBarrel(barrelHost, barrelPortSend, barrelPortRetrieve, em);
      barrel.start();

      Set<String> terms = new HashSet<>();
      terms.add("your");
      terms.add("data");
//      List<PageDTO> pages = service.searchByTerms(terms, 1);

//      System.out.println(pages);
//
//      List<PageDTO> referencedBy = service.listReferencedBy("https://en.wikipedia.org/wiki/European_Economic_Area");
//
//      System.out.println(referencedBy);
//
//      List<AbstractMap.SimpleEntry<String, Integer>> topSearches = service.getTopSearch();
//
//      System.out.println(topSearches);

    } catch (Exception e) {
      System.out.println("Error on main: " + e.getMessage());
      e.printStackTrace();
    }
//    finally {
//      em.close();
//    }
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
          case "storageService":
            servicePort = Integer.parseInt(parts[2]);
            serviceName = parts[3];
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
