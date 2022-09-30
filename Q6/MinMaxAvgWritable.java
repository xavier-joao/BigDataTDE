package advanced.TDE.Q6;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxAvgWritable implements Writable {
    private int n;
    private float price;

    private float maxPrice;

    private float minPrice;

    public MinMaxAvgWritable() {
    }

    public MinMaxAvgWritable(int n, float price, float maxPrice, float minPrice) {
        this.n = n;
        this.price = price;
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
    }

    public MinMaxAvgWritable(float price, float maxPrice, float minPrice) {
        this.price = price;
        this.maxPrice = maxPrice;
        this.minPrice = minPrice;
    }

    public MinMaxAvgWritable(int n, float price) {
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

    public float getMaxPrice() {
        return maxPrice;
    }

    public void setMaxPrice(float maxPrice) {
        this.maxPrice = maxPrice;
    }

    public float getMinPrice() {
        return minPrice;
    }

    public void setMinPrice(float minPrice) {
        this.minPrice = minPrice;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(n);
        dataOutput.writeFloat(price);
        dataOutput.writeFloat(maxPrice);
        dataOutput.writeFloat(minPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        n = dataInput.readInt();
        price = dataInput.readFloat();
        maxPrice = dataInput.readFloat();
        minPrice = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return "\n" +
                "Average = " + price + "\n" +
                "MaxPrice = " + maxPrice + "\n" +
                "MinPrice = " + minPrice +"\n" +
                "\n";
        }
}


