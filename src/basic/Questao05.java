package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.Writable;

// Questão 05 - Descobrir o valor MÉDIO das transações por ano,
// considerando apenas as transações feitas no Brasil.

public class Questao05 {

    // Classe que guarda dois valores: a soma dos preços e a quantidade de registros.
    // Isso serve porque para calcular a média precisamos dessas duas coisas.
    public static class SumCountWritable implements Writable {
        private double sum;   // guarda a soma dos preços
        private long count;   // guarda quantas transações foram contadas

        public SumCountWritable() { this(0.0, 0L); }
        public SumCountWritable(double sum, long count) { this.sum = sum; this.count = count; }

        public void set(double sum, long count) { this.sum = sum; this.count = count; }

        public double getSum() { return sum; }
        public long getCount() { return count; }

        // Essas funções abaixo servem para o Hadoop conseguir "transportar"
        // o objeto entre as fases (Map, Shuffle, Reduce).
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

        @Override
        public String toString() {
            return sum + "\t" + count;
        }
    }

    // Mapper: responsável por ler linha a linha do arquivo e extrair só o que importa.
    // Aqui vamos pegar somente as linhas do BRASIL, pegar o ANO e o PREÇO da transação.
    // Ele vai mandar como chave o ano, e como valor um objeto com (preço, 1).
    public static class BrazilYearMapper extends Mapper<LongWritable, Text, IntWritable, SumCountWritable> {
        private final IntWritable outYear = new IntWritable();
        private final SumCountWritable outSC = new SumCountWritable();

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return; // pula linhas vazias

            // Quebramos a linha pelo separador ";"
            String[] f = line.split(";", -1);
            if (f.length != 10) return; // se não tiver 10 colunas, pula

            // Ignora a primeira linha (cabeçalho com os nomes das colunas)
            String first = f[0].replace("\"", "").trim();
            if (first.equalsIgnoreCase("Country")) return;

            // Verifica se o país é Brasil
            String country = f[0].trim().toLowerCase();
            if (!(country.equals("brazil") || country.equals("brasil"))) return;

            // Pega o ano e o preço
            String yearS = f[1].trim();
            String priceS = f[5].trim();

            if (yearS.isEmpty() || priceS.isEmpty()) return;

            // Troca vírgula por ponto (para funcionar no Double.parse)
            priceS = priceS.replace(',', '.');

            try {
                int year = Integer.parseInt(yearS);
                double price = Double.parseDouble(priceS);

                // Envia para o Hadoop: chave = ano, valor = (preço, 1)
                outYear.set(year);
                outSC.set(price, 1L);
                ctx.write(outYear, outSC);
            } catch (NumberFormatException ex) {
                // Se der erro na conversão, apenas ignora essa linha
            }
        }
    }

    // Combiner: roda depois do Mapper e antes do Reducer, já fazendo um "resumão".
    // Ele soma os preços e conta as transações no próprio nó antes de mandar pro Reducer.
    // Isso economiza dados trafegando na rede.
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

    // Reducer: recebe todos os valores de um mesmo ano (vindo de vários mappers),
    // soma tudo de novo e calcula a média: soma / quantidade.
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
            // Monta a saída com a média e também a quantidade de transações consideradas
            outValue.set(String.format(Locale.US, "%.2f\tcount=%d", avg, c));
            ctx.write(key, outValue);
        }
    }

    // Função principal: configura o job (quem é mapper, reducer, arquivos de entrada e saída, etc.)
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(); // evita erro de log no Hadoop

        Configuration conf = new Configuration();
        Path input = new Path("in/operacoes_comerciais_inteira.csv");   // arquivo de entrada
        Path output = new Path("output/resultado_questao05");           // pasta de saída

        Job job = Job.getInstance(conf, "Questao05-AveragePricePerYearBrazil");
        job.setJarByClass(Questao05.class);

        // Define quem é o Mapper, Combiner e Reducer
        job.setMapperClass(BrazilYearMapper.class);
        job.setCombinerClass(SumCountCombiner.class);
        job.setReducerClass(AvgReducer.class);

        // Define os tipos de chave e valor no meio e no final
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Define formato de leitura e escrita
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Entrada e saída do job
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        // Executa o job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
