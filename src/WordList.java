import java.util.List;

public class WordList {

  private String url;
  private int size;
  private List<String> wordList;

  public WordList(String url, int size, List<String> wordList) {
    this.url = url;
    this.size = size;
    this.wordList = wordList;
  }

  public void addWord(List<String> word) {
    wordList.addAll(word);
  }

  public String getUrl() {
    return url;
  }

  public int getSize() {
    return size;
  }

  public List<String> getWordList() {
    return wordList;
  }

}
