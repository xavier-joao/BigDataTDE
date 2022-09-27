package advanced.TDE.Q5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GroupByYearUnitCommodityCategory implements WritableComparable<GroupByYearUnitCommodityCategory> {
    public String year;
    public String commodityName;
    public String unitType;
    public String category;

    public GroupByYearUnitCommodityCategory() {

    }

    public GroupByYearUnitCommodityCategory(String year, String commodityName, String unitType, String category) {
        this.year = year;
        this.commodityName = commodityName;
        this.unitType = unitType;
        this.category = category;
    }

    @Override
    public int compareTo(GroupByYearUnitCommodityCategory o) {
        if (o == null) {
            return 0;
        }
        int ano = year.compareTo(o.year);
        if (ano != 0){
            return ano;
        }
        int nome = commodityName.compareTo(o.commodityName);
        if(nome!= 0){
            return nome;
        }
        int tipoUnidade = unitType.compareTo(o.unitType);
        if(tipoUnidade != 0) {
            return tipoUnidade;
        }
        int categoria = category.compareTo(o.category);
        return categoria;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(year);
        dataOutput.writeUTF(commodityName);
        dataOutput.writeUTF(unitType);
        dataOutput.writeUTF(category);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        year = dataInput.readUTF();
        commodityName = dataInput.readUTF();
        unitType = dataInput.readUTF();
        category = dataInput.readUTF();
    }

    @Override
    public String toString() {

        return  '\n' +
                commodityName + '\n' +
                "year=" + year + '\n' +
                "unitType= " + unitType + '\n' +
                "category= " + category + '\n' +
                "avarage= ";
    }
}

