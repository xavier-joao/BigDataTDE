package advanced.TDE.Q5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.util.Objects;


public class YearUnitCommodityCategory {

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
        Job j = new Job(c, "media");

        // registro das classes
        j.setJarByClass(YearUnitCommodityCategory.class);
        j.setMapperClass(MapForYearUnitCommodityCategory.class);
        j.setReducerClass(ReduceForYearUnitCommodityCategory.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(GroupByYearUnitCommodityCategory.class);
        j.setOutputValueClass(YearUnitCommodityCategoryWritable.class);
        j.setMapOutputKeyClass(GroupByYearUnitCommodityCategory.class);
        j.setMapOutputValueClass(YearUnitCommodityCategoryWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForYearUnitCommodityCategory extends Mapper<LongWritable, Text, GroupByYearUnitCommodityCategory, YearUnitCommodityCategoryWritable> {
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
            String commodityName = valores[2];
            String unitType = valores[7];
            String category = valores[8];
            String flow = valores[4];
            String country = valores[0];

            float sum = Float.parseFloat(valores[5]);

            if ((Objects.equals(country, "Brazil")) && (Objects.equals(flow, "Export"))){
                GroupByYearUnitCommodityCategory groupby = new GroupByYearUnitCommodityCategory(year, commodityName, unitType, category);
                // emitir <"media", (n=1, soma=valor)>
                YearUnitCommodityCategoryWritable val = new YearUnitCommodityCategoryWritable(1, sum);

                // emissao
                con.write(groupby, val);
            }
        }
    }

    public static class ReduceForYearUnitCommodityCategory extends Reducer<GroupByYearUnitCommodityCategory, YearUnitCommodityCategoryWritable, GroupByYearUnitCommodityCategory, FloatWritable> {
        public void reduce(GroupByYearUnitCommodityCategory groupby, Iterable<YearUnitCommodityCategoryWritable> values, Context con)
                throws IOException, InterruptedException {
            float sumVals = 0;
            int sumN = 0;
            for (YearUnitCommodityCategoryWritable val : values) {
                sumN += val.getN();
                sumVals += val.getPrice();
            }
            float avg = sumVals/sumN;
            // faz a saida no formato <valor pelo groupBy, media de precos>
            con.write(groupby, new FloatWritable(avg));
        }
    }

}

