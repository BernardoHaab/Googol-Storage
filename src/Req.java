import java.util.Arrays;
import java.util.UUID;

public class Req {

  private UUID senderId;
  private int messageId;
  private String type;
  private String[] content;

  public Req(String message) throws Exception {
    try {
      String[] parts = message.split(";");

      String[] identifier = parts[0].split("\\|");

      this.senderId = UUID.fromString(identifier[0].trim());
      this.messageId = Integer.parseInt(identifier[1].trim());
      this.type = parts[1].split("\\|")[1].trim();
      this.content = new String[parts.length - 2];

      System.arraycopy(parts, 2, this.content, 0, parts.length - 2);
    } catch (Exception e) {
      System.err.println(e.getMessage());
      throw new Exception("Error parsing message");
    }
  }

  public UUID getSenderId() {
    return senderId;
  }

  public int getMessageId() {
    return messageId;
  }

  public String getType() {
    return type;
  }

  public String[] getContent() {
    return content;
  }

  @Override
  public String toString() {
    return "Req [senderId=" + senderId + ", messageId=" + messageId + ", type=" + type + ", content="
        + Arrays.toString(content) + "]";
  }

}
