package googol;

import jakarta.persistence.*;

import java.util.Set;

@Entity
@Table(name = "serachinfo", uniqueConstraints = { @UniqueConstraint(columnNames = { "SEARCH_ID" }) })
@NamedQuery( name = "Search.getTop10", query = "select s.query, count(s.query) qntSearch  from SearchInfo s  group by s.query order by qntSearch desc  limit 10")
public class SearchInfo {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "SEARCH_ID", nullable = false, unique = true, length = 11)
    private int id;

        @Column( name = "QUERY", nullable = false )
    private String query;

    public SearchInfo() {
    }

    public SearchInfo(Set<String> terms) {
        this.query = terms.stream().sorted().reduce("", (a, b) -> a + " " + b);
    }

    public SearchInfo(String query) {
        this.query = query;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }
}
