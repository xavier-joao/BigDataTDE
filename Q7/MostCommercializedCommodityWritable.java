package advanced.Q7;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MostCommercializedCommodityWritable implements Writable {
    private int n;

    public MostCommercializedCommodityWritable() {
    }

    public MostCommercializedCommodityWritable(int n) {
        this.n = n;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        n = in.readInt();
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(n);
    }

    public int getN(){
        return this.n;
    }

    public void setN(){
        this.n = n;
    }

    public void setN(int n) {
        this.n = n;
    }

    @Override
    public String toString() {
        return n + "";
    }
}
