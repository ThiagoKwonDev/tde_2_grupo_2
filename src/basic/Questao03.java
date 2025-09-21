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
 * Questao03
 * Objetivo: contar o número de transações por CATEGORIA.
 * Entrada: CSV separado por ';' com 10 colunas.
 * Chave de saída (Reducer): Text (nome da categoria)
 * Valor de saída: LongWritable (contagem)
 */
public class Questao03 {

    /**
     * Mapper: lê cada linha, extrai a coluna Category (índice 9, 0-based) e emite <categoria, 1>.
     * O cabeçalho é ignorado (quando a coluna contém o literal "category").
     */
    public static class CategoryMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        // constante 1 para contagem
        private static final LongWritable ONE = new LongWritable(1);
        // objeto Text reaproveitado para evitar alocações a cada linha
        private final Text outKey = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            // split defensivo: mantém vazios e garante exatamente 10 campos quando válido
            String[] fields = line.split(";", -1);
            if (fields.length != 10) return;

            // coluna 10 → índice 9 (Category)
            String category = fields[9].trim();

            // pular o header se o arquivo de entrada ainda o contiver
            if (category.equalsIgnoreCase("category") || category.isEmpty()) return;

            outKey.set(category);
            // emite <categoria, 1> para o framework somar depois
            ctx.write(outKey, ONE);
        }
    }

    /**
     * Reducer: soma todos os valores 1 associados à mesma categoria.
     * Entrada: <categoria, [1,1,1,...]>
     * Saída:  <categoria, total>
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
     * Configuração do Job:
     * - Formatos de entrada/saída de texto
     * - Classes de Mapper/Reducer
     * - Tipos de chave/valor no mapa e na saída final
     * - Caminhos de input/output
     */
    public static void main(String[] args) throws Exception {
        // evita warnings do log4j no ambiente local
        BasicConfigurator.configure();

        Configuration conf = new Configuration();

        // ajuste os caminhos conforme sua estrutura
        Path input = new Path("in/operacoes_comerciais_inteira.csv");
        Path output = new Path("output/resultado_questao03");

        Job job = Job.getInstance(conf, "Questao03-TransactionsPerCategory");
        job.setJarByClass(Questao03.class);

        // pipeline MapReduce
        job.setMapperClass(CategoryMapper.class);
        job.setReducerClass(SumReducer.class);

        // tipos intermediários (saída do map)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // tipos finais (saída do reduce)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // formatos de arquivo
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // caminhos HDFS/local
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // dispara o job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}