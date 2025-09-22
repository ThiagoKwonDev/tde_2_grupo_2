package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
   Questao08
   Objetivo: Para cada (Ano, País), encontrar o menor e o maior valor da coluna Amount.

   Regras básicas:
   1. Será necessário retirar o cabeçalho. (linha que começa com "Country;")
   2. Será necessário tratar dados faltantes. (linhas com < 10 colunas ou campos vazios)
   3. Código fonte para a resolução do problema utilizando MapReduce em Java. ATENÇÃO: não
   serão consideradas como corretas, soluções que realizam a concatenação de strings para a
   formação de chaves ou valores compostos. (criar YearCountryKey e implementar um WritableComparable simples)
   4. O resultado da execução em um arquivo separado e no formato txt.

 */
public class Questao08 {

      // chave composta: (year, country)
      // minimalista: 2 campos + comparação
    public static class YearCountryKey implements WritableComparable<YearCountryKey> {
        private IntWritable year = new IntWritable();
        private Text country = new Text();

        public YearCountryKey() {
        }

        public YearCountryKey(int y, String c) {
            this.year.set(y);
            this.country.set(c);
        }

        public IntWritable getYear() {
            return year;
        }

        public Text getCountry() {
            return country;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            year.write(out);
            country.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            year.readFields(in);
            country.readFields(in);
        }

        @Override
        public int compareTo(YearCountryKey other) {
            int c = this.year.compareTo(other.year);
            if (c != 0) return c;
            return this.country.compareTo(other.country);
        }

        @Override
        public int hashCode() {
            // implementação simples e suficiente
            return 163 * year.hashCode() + country.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof YearCountryKey)) return false;
            YearCountryKey k = (YearCountryKey) o;
            return year.equals(k.year) && country.equals(k.country);
        }

        @Override
        public String toString() {
            // Apenas para exibir no resultado (humano)
            return year.get() + "," + country.toString();
        }
    }

      // MAPPER
      // entrada: (offset, linha CSV)
      // saída:   (YearCountryKey, amount)

    public static class Q8Mapper extends Mapper<Object, Text, YearCountryKey, DoubleWritable> {

        private final DoubleWritable outAmount = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null || line.isEmpty()) return;

            // Ignora cabeçalho
            if (line.startsWith("Country;")) return;

            // Split por ';' preservando vazios
            String[] f = line.split(";", -1);
            if (f.length < 10) return;

            // Campos que precisamos: 0 Country, 1 Year, 8 Amount
            String country = f[0] == null ? "" : f[0].trim();
            String yearStr = f[1] == null ? "" : f[1].trim();
            String amountStr = f[8] == null ? "" : f[8].trim();

            if (country.isEmpty() || yearStr.isEmpty() || amountStr.isEmpty()) return;

            // Normaliza ponto/vírgula decimal (se vier "12,34", vira "12.34")
            amountStr = amountStr.replace(",", ".");

            int year;
            double amount;
            try {
                year = Integer.parseInt(yearStr);
                amount = Double.parseDouble(amountStr);
            } catch (NumberFormatException e) {
                // linha inválida -> descarta
                return;
            }

            YearCountryKey outKey = new YearCountryKey(year, country);
            outAmount.set(amount);

            // Emite (Ano, País) -> amount
            ctx.write(outKey, outAmount);
        }
    }

      // REDUCER
      // Entrada:  (YearCountryKey, [amount1, amount2, ...])
      // Saída:    (YearCountryKey, "Min: x | Max: y")

    public static class Q8Reducer extends Reducer<YearCountryKey, DoubleWritable, YearCountryKey, Text> {

        private final Text outText = new Text();

        @Override
        protected void reduce(YearCountryKey key, Iterable<DoubleWritable> values, Context ctx)
                throws IOException, InterruptedException {

            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;

            for (DoubleWritable dw : values) {
                double v = dw.get();
                if (v < min) min = v;
                if (v > max) max = v;
            }

            outText.set("Min: " + min + " | Max: " + max);
            ctx.write(key, outText);
        }
    }
        // MAIN - configura e executa o job
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Uso: Questao08 <entrada.csv> <saida_dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Questao08 - Min/Max Amount por Ano e País");

        job.setJarByClass(Questao08.class);
        job.setMapperClass(Q8Mapper.class);
        job.setReducerClass(Q8Reducer.class);

        // Tipos intermediários (Mapper) e finais (Reducer)
        job.setMapOutputKeyClass(YearCountryKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(YearCountryKey.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));    // caminho do CSV
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // diretório novo (não pode existir)

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
