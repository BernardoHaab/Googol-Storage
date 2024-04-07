package googol;

import java.io.Serializable;
import java.util.Set;

public interface IPage extends Serializable {

    public int getId();

    public String getUrl();

    public String getTitle();

    public String getQuote();

    public Set<Page> getReferencedBy();

    public Set<Page> getReferencePages();

}

