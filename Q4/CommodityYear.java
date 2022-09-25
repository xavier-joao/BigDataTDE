package advanced.TDE.Q4;

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

public class CommodityYear {

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
        j.setJarByClass(CommodityYear.class);
        j.setMapperClass(MapForCommodityYear.class);
        j.setReducerClass(ReduceForCommodityYear.class);

        // definicao dos tipos de saida
        j.setOutputKeyClass(GroupByCommodityYear.class);
        j.setOutputValueClass(CommodityWritable.class);
        j.setMapOutputKeyClass(GroupByCommodityYear.class);
        j.setMapOutputValueClass(CommodityWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForCommodityYear extends Mapper<LongWritable, Text, GroupByCommodityYear, CommodityWritable> {
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
            float sum = Float.parseFloat(valores[5]);

            GroupByCommodityYear groupby = new GroupByCommodityYear(year, commodityName);
            // emitir <"media", (n=1, soma=valor)>
            CommodityWritable val = new CommodityWritable(1, sum);

            // emissao
            con.write(groupby, val);
        }
    }



    public static class ReduceForCommodityYear extends Reducer<GroupByCommodityYear, CommodityWritable, GroupByCommodityYear, FloatWritable> {
        public void reduce(GroupByCommodityYear groupby, Iterable<CommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            float sumVals = 0;
            int sumN = 0;
            for (CommodityWritable val : values) {
                sumN += val.getN();
                sumVals += val.getCommodityValue();
            }
            float avg = sumVals/sumN;
            // faz a saida no formato <palavra, somatorio de ocorrencias>
            con.write(groupby, new FloatWritable(avg));
        }
    }

}
