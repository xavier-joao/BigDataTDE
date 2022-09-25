package advanced.TDE.Q3;

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

public class GroupByTransaction{
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
        Job j = new Job(c, "groupByTransaction");

        //registro de classes
        j.setJarByClass(GroupByTransaction.class);
        j.setMapperClass(GroupByTransaction.MapForGroupByTransactions.class);
        j.setReducerClass(GroupByTransaction.ReduceForGroupByTransactions.class);

        //definicao dos tipos de saida
        j.setMapOutputKeyClass(GroupByWritable.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(GroupByWritable.class);
        j.setOutputValueClass(IntWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //lanca o job e aguarda a sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForGroupByTransactions extends Mapper<LongWritable, Text, GroupByWritable, IntWritable> {

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

            GroupByWritable yearFlowType = new GroupByWritable(valores[1], valores[4]);

            IntWritable um = new IntWritable(1);

            // emissao
            con.write(yearFlowType, um);
        }
    }




    public static class ReduceForGroupByTransactions extends Reducer<GroupByWritable, IntWritable, GroupByWritable, IntWritable> {

        public void reduce(GroupByWritable yearFlowType, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {
            int soma = 0;
            for(IntWritable v : values){
                soma += v.get();
            }
            con.write(yearFlowType,new IntWritable(soma));
        }
    }
}
