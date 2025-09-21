package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * Questao04
 * Objetivo: contar o número de transações por FLOW (ex.: Import, Export).
 * Entrada: CSV separado por ';' com 10 colunas.
 * Chave de saída (Reducer): Text (nome do flow)
 * Valor de saída: LongWritable (contagem)
 */
public class Questao04 {

    /**
     * Mapper: lê cada linha, extrai a coluna Flow (índice 4, 0-based) e emite <flow, 1>.
     * O cabeçalho é ignorado (quando a coluna contém o literal "flow").
     */
    public static class FlowMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private static final LongWritable ONE = new LongWritable(1);
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            // preserva vazios e reduz risco de perder colunas
            String[] fields = line.split(";", -1);
            if (fields.length != 10) return;

            // coluna 5 → índice 4 (Flow)
            String flow = fields[4].trim();

            // ignora header/linhas vazias
            if (flow.equalsIgnoreCase("flow") || flow.isEmpty()) return;

            outKey.set(flow);
            ctx.write(outKey, ONE);
        }
    }

    /**
     * Reducer: soma todos os valores 1 associados ao mesmo flow.
     * Entrada: <flow, [1,1,1,...]>
     * Saída:  <flow, total>
     */
    public static class SumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context ctx) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable v : values) sum += v.get();
            ctx.write(key, new LongWritable(sum));
        }
    }

    /**
     * Configuração padrão do Job (igual ao padrão das Q1 e Q2).
     */
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration conf = new Configuration();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/resultado_questao04");

        Job job = Job.getInstance(conf, "Questao04-TransactionsPerFlow");
        job.setJarByClass(Questao04.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(SumReducer.class);

        job.setMapOutputKeyClass(Text.class);
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