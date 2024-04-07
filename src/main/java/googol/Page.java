package googol;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import jakarta.persistence.*;
import org.hibernate.Session;

@Entity
@Table(name = "Page", uniqueConstraints = { @UniqueConstraint(columnNames = { "PAGE_ID" }) })
@NamedQuery( name = "Page.byUrl", query = "SELECT p FROM Page p WHERE p.url=:url" )
@NamedQuery( name = "Pages.hasReferenceFor", query = "SELECT p FROM Page p join p.referencePages p1 where p1.url = :referencedPage" )
public class Page implements IPage {
  private static final long serialVersionUID = 1L;

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "PAGE_ID", nullable = false, unique = true, length = 11)
  private int id;

  @Column(name = "URL", nullable = false, unique = true)
  private String url;

  @Column(name = "TITLE")
  private String title;

  @Column(name = "QUOTE")
  private String quote;

  @ManyToMany
  private Set<Page> referencedBy;

  @ManyToMany( mappedBy = "referencedBy")
  private Set<Page> referencePages;

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
    this.referencedBy = new HashSet<>();
    this.referencePages = new HashSet<>();
  }

  public Page() {

  }

  public int getId() {
    return id;
  }

  public String getUrl() {
    return url;
  }

  public String getTitle() {
    return title;
  }

  public String getQuote() {
    return quote;
  }

  public Set<Page> getReferencedBy() {
    return referencedBy;
  }

  public Set<Page> getReferencePages() {
    return referencePages;
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
//
//  public static Page getPageByUrl(String url, Session session) {
//    Page page = null;
//    try {
//      TypedQuery<Page> q = session.createNamedQuery("Page.byUrl", Page.class);
//      q.setParameter("url", url);
//      page = q.getSingleResult();
//    } catch (NoResultException e) {
//      session.beginTransaction();
//      page = new Page(url);
//      session.persist(page);
//      session.getTransaction().commit();
//    } catch (Exception e) {
//      System.out.println("Error persisting page");
//      e.printStackTrace();
//      session.beginTransaction();
//      page = new Page(url);
//      session.persist(page);
//      session.getTransaction().commit();
//    }
//    return page;
//  }
  
}
