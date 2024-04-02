import java.rmi.RemoteException;

public interface IUrlQueue {

  public String getNextUrl() throws RemoteException;

  public void addUrls(java.util.List<String> urls) throws RemoteException;

  public int size() throws RemoteException;

  public void addUrlFirst(String url) throws RemoteException;
}
