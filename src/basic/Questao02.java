package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Questao02 {

    public static class YearMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final IntWritable yearKey = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            String[] fields = line.split(";", -1);
            if (fields.length != 10) return;

            try {
                int year = Integer.parseInt(fields[1].trim());
                yearKey.set(year);
                ctx.write(yearKey, ONE);
            } catch (NumberFormatException e) {
                // ignora anos inválidos
            }
        }
    }

    public static class SumReducer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<LongWritable> values, Context ctx)
                throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) sum += v.get();
            ctx.write(key, new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        // Defina os caminhos do arquivo de entrada e da pasta de saída
        Path input = new Path("in/operacoes_comerciais_inteira.csv"); // caminho do arquivo de entrada
        Path output = new Path("output/resultado_questao02"); // pasta de saída

        Job job = Job.getInstance(conf, "Questao02-TransactionsPerYear");
        job.setJarByClass(Questao02.class);

        job.setMapperClass(YearMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
