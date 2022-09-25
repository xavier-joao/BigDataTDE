package advanced.TDE.Q3;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupByWritable implements WritableComparable<GroupByWritable> {
    public String year;
    public String flowType;

    public GroupByWritable(String year, String flowType) {
        this.year = year;
        this.flowType = flowType;
    }

    public GroupByWritable() {
    }

    @Override
    public int compareTo(GroupByWritable obj) {
        if (obj == null) {
            return 0;
        }

        int val = year.compareTo(obj.year);

        return val == 0 ? flowType.compareTo(obj.flowType) : val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(flowType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        flowType = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return flowType  + "-" + year;
    }
}