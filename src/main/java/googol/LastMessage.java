package googol;

import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.util.UUID;

@Entity
@Table( name = "LastMessage" )
@NamedQuery( name = "LastMessage.bySenderId", query = "SELECT l FROM LastMessage l WHERE l.senderId=:senderId" )
public class LastMessage {

    @Id
    private UUID senderId;

    @Column( name = "LAST_MESSAGE_ID", nullable = false )
    private int lastMessageId;

    public LastMessage(UUID senderId) {
        this.senderId = senderId;
        this.lastMessageId = -1;
    }

    public LastMessage() {

    }

    public UUID getSenderId() {
        return senderId;
    }

    public void setLastMessageId(int lastMessageId) {
        this.lastMessageId = lastMessageId;
    }

    public int getLastMessageId() {
        return lastMessageId;
    }

    @Override
    public String toString() {
        return "LastMessage{" +
                "senderId=" + senderId +
                ", lastMessageId=" + lastMessageId +
                '}';
    }
}
