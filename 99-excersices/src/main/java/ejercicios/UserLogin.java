package ejercicios;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections.comparators.ComparatorChain;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

@Getter @Setter
@NoArgsConstructor
public class UserLogin implements WritableComparable<UserLogin>{


    private String userId;
    private String ip;

    public UserLogin(String userId, String ip) {
        this.userId = userId;
        this.ip = ip;
    }



    @Override
    public int compareTo(UserLogin o) {
        return userId.compareTo(o.userId)+ip.compareTo(o.ip);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(ip);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        ip = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserLogin userLogin = (UserLogin) o;
        return Objects.equals(userId, userLogin.userId) &&
                Objects.equals(ip, userLogin.ip);
    }

    @Override
    public int hashCode() {

        return Objects.hash(userId, ip);
    }

    @Override
    public String toString() {
        return "{'" + userId + '\'' +
                ",'" + ip + '\'' +
                '}';
    }
}
