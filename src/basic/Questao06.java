package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

import java.io.IOException;
import java.util.Locale;

/**
 * Questao06
 * Objetivo: Encontrar qual foi a transação MAIS CARA e a MAIS BARATA
 * realizadas no Brasil no ano de 2016.
 */
public class Questao06 {


    // Ele lê cada linha do arquivo de transações e seleciona apenas:
    // - transações do Brasil
    // - do ano de 2016
    // Depois manda a linha inteira para o REDUCER analisar.
    // A chave é sempre "NullWritable" (vazia), porque só queremos um único grupo
    // de dados no final (já que vamos buscar o menor e o maior globalmente).
    public static class Brazil2016Mapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private final Text out = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return; // pula linhas vazias

            String[] f = line.split(";", -1);
            if (f.length != 10) return; // se não tiver 10 colunas, pula

            // pula o cabeçalho (linha que tem "Country" escrito)
            String first = f[0].replace("\"", "").trim();
            if (first.equalsIgnoreCase("Country")) return;

            String country = f[0].trim().toLowerCase();
            String yearS = f[1].trim();
            String priceS = f[5].trim();

            // só segue se for Brasil e tiver ano e preço
            if (!(country.equals("brazil") || country.equals("brasil"))) return;
            if (yearS.isEmpty() || priceS.isEmpty()) return;

            try {
                int year = Integer.parseInt(yearS);
                if (year != 2016) return; // só interessa o ano de 2016

                // preço pode estar com vírgula, então trocamos por ponto
                priceS = priceS.replace(',', '.');
                double price = Double.parseDouble(priceS);

                // manda a linha inteira para o reducer, sem chave diferenciada
                // (chave = NullWritable, ou seja, "vazia")
                out.set(line);
                ctx.write(NullWritable.get(), out);
            } catch (NumberFormatException ex) {
                // ignora se não conseguir converter ano ou preço
            }
        }
    }

    // O Reducer vai procurar qual tem o menor preço e qual tem o maior.
    public static class MinMaxReducer extends Reducer<NullWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outVal = new Text();

        @Override
        protected void reduce(NullWritable key, Iterable<Text> values, Context ctx)
                throws IOException, InterruptedException {
            double minPrice = Double.POSITIVE_INFINITY;    // começa com infinito (bem grande)
            double maxPrice = Double.NEGATIVE_INFINITY;    // começa com -infinito (bem pequeno)
            String minLine = null;
            String maxLine = null;

            // percorre todas as linhas recebidas
            for (Text t : values) {
                String line = t.toString();
                String[] f = line.split(";", -1);
                if (f.length != 10) continue;

                String priceS = f[5].trim();
                if (priceS.isEmpty()) continue;
                priceS = priceS.replace(',', '.');

                try {
                    double price = Double.parseDouble(priceS);

                    // se encontrou um valor menor que o atual, atualiza
                    if (price < minPrice) {
                        minPrice = price;
                        minLine = line;
                    }

                    // se encontrou um valor maior que o atual, atualiza
                    if (price > maxPrice) {
                        maxPrice = price;
                        maxLine = line;
                    }
                } catch (NumberFormatException ex) {
                    // ignora valores inválidos
                }
            }

            // Escreve o resultado final no arquivo de saída
            if (minLine != null) {
                outKey.set("Transacao_mais_barata_BR_2016"); // etiqueta da saída
                outVal.set(String.format(Locale.US, "%s\tprice=%.2f", minLine, minPrice));
                ctx.write(outKey, outVal);
            }

            if (maxLine != null) {
                outKey.set("Transacao_mais_cara_BR_2016"); // etiqueta da saída
                outVal.set(String.format(Locale.US, "%s\tprice=%.2f", maxLine, maxPrice));
                ctx.write(outKey, outVal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // configura o log para não dar erro

        Configuration conf = new Configuration();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");   // arquivo de entrada
        Path output = new Path("output/resultado_questao06");           // pasta de saída

        Job job = Job.getInstance(conf, "Questao06-MinMax2016Brazil");
        job.setJarByClass(Questao06.class);

        // Definindo quem vai mapear e reduzir
        job.setMapperClass(Brazil2016Mapper.class);
        job.setReducerClass(MinMaxReducer.class);

        // Só queremos 1 reducer, para garantir que o menor e maior saiam juntos
        job.setNumReduceTasks(1);

        // Tipos de chave e valor no meio e no final
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Formato de entrada e saída (texto simples)
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Caminhos de entrada e saída
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Executa o job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
