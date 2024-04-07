package googol;

import jakarta.persistence.*;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

public class Service extends UnicastRemoteObject implements IStorageService, Runnable {

    private final EntityManager em;

    public Service() throws RemoteException {
        super();

        EntityManagerFactory entityManagerFactory = Persistence.createEntityManagerFactory("default");
        this.em = entityManagerFactory.createEntityManager();
    }

    public List<PageDTO> searchByTerms(Set<String> terms, int page) throws RemoteException {
        List<PageDTO> pages = new LinkedList<>();

        System.out.println("Search terms: " + terms);

        Query q = em.createNativeQuery("Select p.PAGE_ID , p.URL, p.TITLE, p.QUOTE, (select count(pp.referencedBy_PAGE_ID) from page_page pp \n" +
                "join page p1 on p1.PAGE_ID = pp.referencePages_PAGE_ID\n" +
                "where p.URL = p1.URL) as recerencedBy  \n" +
                "FROM word_page wp\n" +
                "inner join page p on p.PAGE_ID = wp.PAGE_ID\n" +
                "inner join wordindex w on w.INDEX_ID = wp.INDEX_ID\n" +
                "WHERE w.WORD IN :terms\n" +
                "GROUP BY wp.PAGE_ID \n" +
                "HAVING COUNT(*) = :qnt_terms\n" +
                "order by recerencedBy desc\n" +
                "limit 10\n" +
                "offset :offset");

        q.setParameter("terms", terms);
        q.setParameter("qnt_terms", terms.size());
        q.setParameter("offset", page < 1 ? 0 : (page-1)*10);

        List<Object[]>  resultList = q.getResultList();

        for (Object[] row : resultList) {
            PageDTO p = new PageDTO();
            p.setId((int) row[0]);
            p.setUrl((String) row[1]);
            p.setTitle((String) row[2]);
            p.setQuote((String) row[3]);
            pages.add(p);
        }

        try {
            em.getTransaction().begin();
            SearchInfo searchInfo = new SearchInfo(terms);
            em.persist(searchInfo);
            em.getTransaction().commit();
        } catch (Exception e) {
            em.getTransaction().rollback();
        }

        return pages;
    }

    public List<PageDTO> listReferencedBy(String pageUrl) throws RemoteException {
        TypedQuery<Page> q = em.createNamedQuery("Pages.hasReferenceFor", Page.class);

        q.setParameter("referencedPage", pageUrl);

        List<Page> pages = q.getResultList();

        try {
            em.getTransaction().begin();
            SearchInfo searchInfo = new SearchInfo(pageUrl);
            em.persist(searchInfo);
            em.getTransaction().commit();
        } catch (Exception e) {
            em.getTransaction().rollback();
        }

        return pages.stream().map(PageDTO::new).toList();
    }

    public List<AbstractMap.SimpleEntry<String, Integer>> getTopSearch() {
        List<AbstractMap.SimpleEntry<String, Integer>> topSearches = new ArrayList<>();

        Query q = em.createNamedQuery("Search.getTop10");

        List<Object[]>  res = q.getResultList();

        System.out.println(res);

        for (Object[] row : res) {
            topSearches.add(new AbstractMap.SimpleEntry<>((String) row[0],(int) (long) row[1]));
            System.out.println(Arrays.toString(row));
        }
        return  topSearches;
    }

    @Override
    public void run() {

    }
}