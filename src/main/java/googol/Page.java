package googol;

import java.util.HashSet;
import java.util.Set;

import jakarta.persistence.*;
import org.hibernate.Session;

@Entity
@Table(name = "Page", uniqueConstraints = { @UniqueConstraint(columnNames = { "PAGE_ID" }) })
@NamedQuery( name = "Page.byUrl", query = "SELECT p FROM Page p WHERE p.url=:url" )
public class
Page {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "PAGE_ID", nullable = false, unique = true, length = 11)
  public int id;

  @Column(name = "URL", length = 255, nullable = false, unique = true)
  public String url;

  @ManyToMany
  public Set<Page> referencedBy;

  @ManyToMany( mappedBy = "referencedBy")
  public Set<Page> referencePages;

  public Page(String url, Set<Page> referencedBy, Set<Page> referencePages) {
    this.url = url;
    this.referencedBy = referencedBy;
    this.referencePages = referencePages;
  }

  public Page(Set<Page> referencedBy, String url) {
    this.referencedBy = referencedBy;
    this.url = url;
  }

  public Page(String url) {
    this.url = url;
    this.referencedBy = new HashSet<Page>();
    this.referencePages = new HashSet<Page>();
  }

  public Page() {

  }

  public void addReference(Page page) {
    page.referencedBy.add(this);
    referencePages.add(page);
  }

  public void addReferencedBy(Page page) {
    this.referencedBy.add(page);
  }

  @Override
  public String toString() {
    return "Page{" +
            "id=" + id +
            ", url='" + url + '\'' +
            ", referencedBy=" + referencedBy.size() +
            ", referencePages=" + referencePages.size() +
            '}';
  }

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
