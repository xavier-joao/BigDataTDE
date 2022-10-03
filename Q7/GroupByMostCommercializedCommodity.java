package advanced.Q7;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupByMostCommercializedCommodity implements WritableComparable<GroupByMostCommercializedCommodity> {
    private String flowType;
    private String commodityName;

    public GroupByMostCommercializedCommodity() {
    }

    public GroupByMostCommercializedCommodity(String flowType, String commodityName) {
        this.flowType = flowType;
        this.commodityName = commodityName;
    }

    @Override
    public int compareTo(GroupByMostCommercializedCommodity obj) {
        if (obj == null) {
            return 0;
        }

        int val = flowType.compareTo(obj.flowType);

        return val == 0 ? commodityName.compareTo(obj.commodityName) : val;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(flowType);
        dataOutput.writeUTF(commodityName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        flowType = dataInput.readUTF();
        commodityName = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return  "\n" +
                "flowType= " + flowType +
                "commodityName= " + commodityName;
    }
}
