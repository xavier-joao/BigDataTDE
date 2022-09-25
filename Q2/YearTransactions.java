package advanced.TDE.Q2;

import advanced.TDE.Q1.BrazilTransactions;
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

public class YearTransactions {
    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "/");
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "yearTransactions");

        //registro de classes
        j.setJarByClass(YearTransactions.class);
        j.setMapperClass(YearTransactions.MapForYearTransactions.class);
        j.setReducerClass(YearTransactions.ReduceForYearTransactions.class);

        //definicao dos tipos de saida
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //lanca o job e aguarda a sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForYearTransactions extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //Converte o bloco em string

            String texto = value.toString();
            if(texto.startsWith("country_or_area")){
                return;
            }
            //Quebra a linha em palavras
            String [] linhas = texto.split("\t");
            String [] valores = linhas[0].split(";");

            String ano = valores[1];
            IntWritable um = new IntWritable(1);

            con.write(new Text(ano),um);
        }
    }




    public static class ReduceForYearTransactions extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for(IntWritable v : values){
                soma += v.get();
            }
            con.write(key,new IntWritable(soma));
        }
    }
}
