package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class Questao01 {

    public static class BrazilMapper extends Mapper<LongWritable, Text, NullWritable, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            String[] fields = line.split(";", -1);
            if (fields.length != 10) return;

            String country = fields[0].trim().toLowerCase();
            if (country.equals("brazil") || country.equals("brasil")) {
                ctx.write(NullWritable.get(), ONE);
            }
        }
    }

    public static class SumReducer extends Reducer<NullWritable, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(NullWritable key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v : values) sum += v.get();
            ctx.write(new Text("Brazil_transactions"), new LongWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/resultado_questao01");

        Job job = Job.getInstance(conf, "Questao01-BrazilTransactions");
        job.setJarByClass(Questao01.class);

        job.setMapperClass(BrazilMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
