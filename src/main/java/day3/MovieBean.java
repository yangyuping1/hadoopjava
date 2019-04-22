package day3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MovieBean implements WritableComparable<MovieBean> {
    private String movie;
    private Integer rate;
    private Integer timeStamp;
    private Integer uid;

    public String getMovie() {
        return movie;
    }

    public void setMovie(String movie) {
        this.movie = movie;
    }

    public Integer getRate() {
        return rate;
    }

    public void setRate(Integer rate) {
        this.rate = rate;
    }

    public Integer getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Integer timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Integer getUid() {
        return uid;
    }

    public void setUid(Integer uid) {
        this.uid = uid;
    }

    @Override
    public String toString() {
        return "MovieBean{" +
                "movie=" + movie +
                ", rate=" + rate +
                ", timeStamp=" + timeStamp +
                ", uid=" + uid +
                '}';
    }

    @Override
    public int compareTo(MovieBean o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(movie);
        dataOutput.writeInt(rate);
        dataOutput.writeInt(timeStamp);
        dataOutput.writeInt(uid);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        movie = dataInput.readUTF();
        rate = dataInput.readInt();
        timeStamp = dataInput.readInt();
        uid = dataInput.readInt();
    }
}
