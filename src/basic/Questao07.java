package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Writable;

/**
 * Questao07
 * Objetivo: Calcular o valor MÉDIO das transações por ano,
 * considerando apenas Exportações realizadas no Brasil.
 */
public class Questao07 {

    // Classe auxiliar: guarda soma e contagem para calcular média
    public static class SumCountWritable implements Writable {
        private double sum;
        private long count;

        public SumCountWritable() { this(0.0, 0L); }
        public SumCountWritable(double sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public void set(double sum, long count) {
            this.sum = sum;
            this.count = count;
        }

        public double getSum() { return sum; }
        public long getCount() { return count; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(sum);
            out.writeLong(count);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            sum = in.readDouble();
            count = in.readLong();
        }
    }

    // Mapper: filtra apenas Brasil + Export e envia <ano, (preço, 1)>
    public static class BrazilExportMapper extends Mapper<LongWritable, Text, IntWritable, SumCountWritable> {
        private final IntWritable outYear = new IntWritable();
        private final SumCountWritable outSC = new SumCountWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            String[] f = line.split(";", -1);
            if (f.length != 10) return;

            // ignora header
            if (f[0].equalsIgnoreCase("Country")) return;

            String country = f[0].trim().toLowerCase();
            String yearS = f[1].trim();
            String flow = f[4].trim().toLowerCase();
            String priceS = f[5].trim();

            if (!(country.equals("brazil") || country.equals("brasil"))) return;
            if (!flow.equals("export")) return;
            if (yearS.isEmpty() || priceS.isEmpty()) return;

            priceS = priceS.replace(',', '.');

            try {
                int year = Integer.parseInt(yearS);
                double price = Double.parseDouble(priceS);

                outYear.set(year);
                outSC.set(price, 1L);
                ctx.write(outYear, outSC);
            } catch (NumberFormatException ex) {
                // ignora erros de conversão
            }
        }
    }

    // Combiner: soma parcial antes de enviar ao reducer
    public static class SumCountCombiner extends Reducer<IntWritable, SumCountWritable, IntWritable, SumCountWritable> {
        private final SumCountWritable result = new SumCountWritable();

        @Override
        protected void reduce(IntWritable key, Iterable<SumCountWritable> values, Context ctx)
                throws IOException, InterruptedException {
            double s = 0.0;
            long c = 0L;
            for (SumCountWritable sc : values) {
                s += sc.getSum();
                c += sc.getCount();
            }
            result.set(s, c);
            ctx.write(key, result);
        }
    }

    // Reducer: calcula a média = soma / contagem
    public static class AvgReducer extends Reducer<IntWritable, SumCountWritable, IntWritable, Text> {
        private final Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<SumCountWritable> values, Context ctx)
                throws IOException, InterruptedException {
            double s = 0.0;
            long c = 0L;
            for (SumCountWritable sc : values) {
                s += sc.getSum();
                c += sc.getCount();
            }
            if (c == 0) return;
            double avg = s / c;
            outValue.set(String.format(Locale.US, "%.2f (count=%d)", avg, c));
            ctx.write(key, outValue);
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/resultado_questao07");

        Job job = Job.getInstance(conf, "Questao07-AverageExportBrazilPerYear");
        job.setJarByClass(Questao07.class);

        job.setMapperClass(BrazilExportMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AvgReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
