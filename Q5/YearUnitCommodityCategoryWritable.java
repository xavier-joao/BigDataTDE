package advanced.TDE.Q5;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class YearUnitCommodityCategoryWritable implements Writable {
    private int n;
    private float price;

    public YearUnitCommodityCategoryWritable() {
    }

    public YearUnitCommodityCategoryWritable(int n, float price) {
        this.n = n;
        this.price = price;
    }

    public int getN() {
        return n;
    }

    public void setN(int n) {
        this.n = n;
    }

    public float getPrice() {
        return price;
    }

    public void setPrice(float price) {
        this.price = price;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeFloat(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        price = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "YearUnitCommodityCategoryWritable{" +
                "n=" + n +
                ", price=" + price +
                '}';
    }
}


