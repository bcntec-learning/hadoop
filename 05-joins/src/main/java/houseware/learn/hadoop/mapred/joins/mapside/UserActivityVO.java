package houseware.learn.hadoop.mapred.joins.mapside;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class UserActivityVO implements Writable {

    private int userId;
    private String userName;
    private String comments;
    private String postShared;


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        out.writeUTF(userName);
        out.writeUTF(comments);
        out.writeUTF(postShared);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readInt();
        userName = in.readUTF();
        comments = in.readUTF();
        postShared = in.readUTF();
    }

    @Override
    public String toString() {
        return "UserActivityVO [userId=" + userId + ", userName=" + userName + ", comments=" + comments
                + ", postShared=" + postShared + "]";
    }

}