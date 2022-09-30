package advanced.TDE.Q6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupByMinMaxAvg implements WritableComparable<GroupByMinMaxAvg> {
    public String year;
    public String unitType;

    public GroupByMinMaxAvg() {

    }

    public GroupByMinMaxAvg(String year, String unitType) {
        this.year = year;
        this.unitType = unitType;

    }

    @Override
    public int compareTo(GroupByMinMaxAvg o) {
        if (o == null) {
            return 0;
        }
        int ano = year.compareTo(o.year);

        return ano == 0 ? unitType.compareTo(o.unitType) : ano;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(unitType);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        unitType = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "GroupByMinMaxAvg{" +
                "year='" + year + '\'' +
                ", unitType='" + unitType + '\'' +
                '}';
    }
}

