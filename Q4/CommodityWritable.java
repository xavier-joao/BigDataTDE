package advanced.TDE.Q4;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CommodityWritable implements Writable {
    private int n;
    private float CommodityValue;
    private String CommodityName;


    //definir contrutor vazio
    public CommodityWritable(){

    }
    //definir contrutor atributos
    public CommodityWritable(int n, float CommodityValue){
        this.CommodityValue = CommodityValue;
        this.n = n;
    }

    //definir o metodo readFields
    @Override
    public void readFields(DataInput in) throws IOException {
        CommodityValue = in.readFloat();
        n = in.readInt();
    }

    //definir o metodo write
    @Override
    public void write(DataOutput out) throws  IOException {
        out.writeFloat(CommodityValue);
        out.writeInt(n);
    }

    //definir getters
    public int getN(){
        return this.n;
    }

    public float getCommodityValue(){
        return this.CommodityValue;
    }




    //definir settters
    public void setCommodityValue(float CommodityValue){
        this.CommodityValue = CommodityValue;
    }


    public void setN(){
        this.n = n;
    }

    @Override
    public String toString() {
        return " CommodityValue = " + (long)CommodityValue;
    }
}