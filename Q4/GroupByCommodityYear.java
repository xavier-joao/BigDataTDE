package advanced.TDE.Q4;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupByCommodityYear implements WritableComparable<GroupByCommodityYear> {
    public String year;
    public String commodityName;

    public GroupByCommodityYear(String year, String flowType) {
        this.year = year;
        this.commodityName = flowType;
    }

    public GroupByCommodityYear() {
    }

    @Override
    public int compareTo(GroupByCommodityYear obj) {
        if (obj == null) {
            return 0;
        }

        int val = year.compareTo(obj.year);

        return val == 0 ? commodityName.compareTo(obj.commodityName) : val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(commodityName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        commodityName = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return commodityName  + "-" + year;
    }
}