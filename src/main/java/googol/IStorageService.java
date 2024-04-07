package googol;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Set;

public interface IStorageService extends Remote {

    public List<PageDTO> searchByTerms(Set<String> terms, int page) throws RemoteException;

    public List<PageDTO> listReferencedBy(String pageUrl) throws RemoteException;

    public List<AbstractMap.SimpleEntry<String, Integer>> getTopSearch() throws RemoteException;

}
