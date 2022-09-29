package advanced.TDE.Q6;

import advanced.TDE.Q5.GroupByYearUnitCommodityCategory;
import advanced.TDE.Q5.YearUnitCommodityCategory;
import advanced.TDE.Q5.YearUnitCommodityCategoryWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Objects;

public class MinMaxAvg {
    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "minMaxAvg");

        // registro das classes
        j.setJarByClass(MinMaxAvg.class);
        j.setMapperClass(MinMaxAvg.MapForMinMaxAvg.class);
        j.setReducerClass(MinMaxAvg.ReduceForMinMaxAvg.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(GroupByMinMaxAvg.class);
        j.setOutputValueClass(MinMaxAvgWritable.class);
        j.setMapOutputKeyClass(GroupByMinMaxAvg.class);
        j.setMapOutputValueClass(MinMaxAvgWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForMinMaxAvg extends Mapper<LongWritable, Text, GroupByMinMaxAvg, MinMaxAvgWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String texto = value.toString();
            if(texto.startsWith("country_or_area")){
                return;
            }
            //Quebra a linha em palavras
            String [] linhas = texto.split("\t");
            String [] valores = linhas[0].split(";");

            String year = valores[1];
            String unitType = valores[7];

            float sum = Float.parseFloat(valores[5]);

            GroupByMinMaxAvg groupby = new GroupByMinMaxAvg(year, unitType);
            // emitir <"media", (n=1, soma=valor)>
            MinMaxAvgWritable val = new MinMaxAvgWritable(1, sum);

            // emissao
            con.write(groupby, val);
        }
    }

    public static class ReduceForMinMaxAvg  extends Reducer<GroupByMinMaxAvg, MinMaxAvgWritable, GroupByMinMaxAvg, MinMaxAvgWritable> {
        public void reduce(GroupByMinMaxAvg groupby, Iterable<MinMaxAvgWritable> values, Context con)
                throws IOException, InterruptedException {
            float sumVals = 0;
            int sumN = 0;

            float maxPrice = 0;
            float minPrice = 0;

            for (MinMaxAvgWritable val : values) {
                sumN += val.getN();
                sumVals += val.getPrice();

                float price =  val.getPrice();

                if (price > maxPrice) {
                    maxPrice = price;
                }

                if (price < minPrice || minPrice == 0){
                    minPrice = price;
                }
            }

            float avg = sumVals/sumN;

            con.write(groupby, new MinMaxAvgWritable(avg, maxPrice, minPrice));
        }
    }
}
