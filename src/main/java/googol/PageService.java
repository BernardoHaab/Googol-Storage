package googol;

import jakarta.persistence.NoResultException;
import jakarta.persistence.TypedQuery;
import org.hibernate.Session;

public class PageService {

    public static Page getPageByUrl(String url, Session session) {
        Page page = null;
        try {
            TypedQuery<Page> q = session.createNamedQuery("Page.byUrl", Page.class);
            q.setParameter("url", url);
            page = q.getSingleResult();
        } catch (NoResultException e) {
            session.beginTransaction();
            page = new Page(url);
            session.persist(page);
            session.getTransaction().commit();
        } catch (Exception e) {
            System.out.println("Error persisting page");
            e.printStackTrace();
            session.beginTransaction();
            page = new Page(url);
            session.persist(page);
            session.getTransaction().commit();
        }
        return page;
    }


}
