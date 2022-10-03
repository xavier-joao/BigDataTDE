package advanced.Q7;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class MostCommercializedCommodity {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        Path intermediate = new Path("./output/intermediate.tmp");

        // arquivo de saida
        Path output = new Path(files[1]);

        Job j1 = new Job(c, "JobCount");

        j1.setJarByClass(MostCommercializedCommodity.class);
        j1.setMapperClass(MostCommercializedCommodity.MapForCount.class);
        j1.setReducerClass(MostCommercializedCommodity.ReduceForCount.class);

        j1.setOutputKeyClass(GroupByMostCommercializedCommodity.class);
        j1.setOutputValueClass(IntWritable.class);

        j1.setMapOutputKeyClass(GroupByMostCommercializedCommodity.class);
        j1.setMapOutputValueClass(MostCommercializedCommodityWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, output);

        if (!j1.waitForCompletion(true)) {
            System.out.println("Job1 Error");
        }

    }

    public static class MapForCount extends Mapper<LongWritable, Text, GroupByMostCommercializedCommodity, MostCommercializedCommodityWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String texto = value.toString();
            if (texto.startsWith("country_or_area")) {
                return;
            }
            //Quebra a linha em palavras
            String[] linhas = texto.split("\t");
            String[] valores = linhas[0].split(";");

            String ano = valores[1];
            String commodityCode = valores[2];
            String flowType = valores[4];

            if (Objects.equals(ano, "2016")){
                GroupByMostCommercializedCommodity groupBy = new GroupByMostCommercializedCommodity(flowType,commodityCode);
                MostCommercializedCommodityWritable val = new MostCommercializedCommodityWritable(1);
                con.write(groupBy, val);
        }
    }
}

    public static class ReduceForCount extends Reducer<GroupByMostCommercializedCommodity, MostCommercializedCommodityWritable, GroupByMostCommercializedCommodity, IntWritable> {
        public void reduce(GroupByMostCommercializedCommodity key, Iterable<MostCommercializedCommodityWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for(MostCommercializedCommodityWritable v : values) {
                soma += v.getN();
            }
            con.write(key, new IntWritable(soma));


        }

    }


//    public static class MapEtapaB extends Mapper<LongWritable, Text, Text, BaseQtdWritable> {
//        public void map(LongWritable key, Text value, Context con)
//                throws IOException, InterruptedException {
//            String line = value.toString();
//
//            String [] palavra = line.split("\t");
//
//            String caracter = palavra[0];
//            int qntd = Integer.parseInt(palavra[1]);
//
//            BaseQtdWritable data = new BaseQtdWritable(caracter, qntd);
//
//            con.write(new Text("entropy"), data);
//
//        }
//    }
//
//    public static class ReduceEtapaB extends Reducer<Text, BaseQtdWritable, Text, DoubleWritable> {
//        public void reduce(Text key, Iterable<BaseQtdWritable> values, Context con)
//                throws IOException, InterruptedException {
//            for(BaseQtdWritable v : values) {
//
//            }
//        }
//    }
}
