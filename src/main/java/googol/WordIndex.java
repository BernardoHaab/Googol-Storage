package googol;

import jakarta.persistence.*;

import java.util.HashSet;
import java.util.Set;

@Entity
@Table( name = "wordindex", uniqueConstraints = { @UniqueConstraint(columnNames = { "WORD" }) } )
@NamedQuery( name = "WordIndex.word", query = "SELECT w FROM WordIndex w WHERE w.word=:word" )
public class WordIndex {

    @Id
    @GeneratedValue(strategy= GenerationType.IDENTITY)
    @Column(name="INDEX_ID", nullable=false, unique=true, length=11)
    private int id;

    @Column( name = "WORD", nullable = false )
    private String word;

    @ManyToMany
    @JoinTable(
            name = "word_page",
            joinColumns = { @JoinColumn(name = "INDEX_ID") },
            inverseJoinColumns = { @JoinColumn(name = "PAGE_ID") }
    )
    private Set<Page> page;

    public WordIndex(String word, Set<Page> page) {
        this.word = word;
        this.page = page;
    }

    public WordIndex(String word) {
        this.word = word;
        this.page = new HashSet<>();
    }

    public WordIndex() {
        this.page = new HashSet<>();
    }

    public void addPage(Page page) {
        this.page.add(page);
    }

    @Override
    public String toString() {
        return "WordIndex{" +
                "id=" + id +
                ", word='" + word + '\'' +
                ", page=" + page +
                '}';
    }
}
