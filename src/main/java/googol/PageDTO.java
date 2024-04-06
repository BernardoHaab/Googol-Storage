package googol;

public class PageDTO {

    private int id;
    private String url;
    private String title;
    private String quote;

    public PageDTO(int id, String url, String title, String quote) {
        this.id = id;
        this.url = url;
        this.title = title;
        this.quote = quote;
    }

    public PageDTO() {
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

    public void setId(int id) {
        this.id = id;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }

    @Override
    public String toString() {
        return "PageDTO{" +
                "id=" + id +
                ", url='" + url + '\'' +
                ", title='" + title + '\'' +
                ", quote='" + quote + '\'' +
                '}';
    }
}
