package basic;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PreprocessTransactions extends Configured implements Tool {

    public static class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        private static final int EXPECTED_COLS = 10;
        private final Text out = new Text();

        enum QCounters { SKIPPED_HEADER, PARSE_ERROR, BAD_YEAR, EMITTED }

        @Override
        protected void map(LongWritable key, Text value, Context ctx) throws IOException, InterruptedException {
            String line = value.toString();
            if (line == null) return;
            line = line.trim();
            if (line.isEmpty()) return;

            // Detecta e remove cabeçalho em qualquer arquivo/split
            if (isHeaderLine(line)) {
                ctx.getCounter(QCounters.SKIPPED_HEADER).increment(1);
                return;
            }

            String[] fields = parseCsvSemicolon(line);
            if (fields == null || fields.length != EXPECTED_COLS) {
                ctx.getCounter(QCounters.PARSE_ERROR).increment(1);
                return;
            }

            // Trim básico
            for (int i = 0; i < fields.length; i++) {
                fields[i] = fields[i] == null ? "" : fields[i].trim();
            }

            // 0 Country (string)
            fields[0] = normalizeString(fields[0]);

            // 1 Year (obrigatório e inteiro válido)
            Integer year = parseIntSafe(fields[1]);
            if (year == null || year < 1900 || year > 2100) {
                ctx.getCounter(QCounters.BAD_YEAR).increment(1);
                return; // sem ano, não serve pro restante do grupo
            }
            fields[1] = String.valueOf(year);

            // 2 Commodity code (string livre)
            fields[2] = normalizeString(fields[2]);

            // 3 Commodity (string livre)
            fields[3] = normalizeString(fields[3]);

            // 4 Flow (Export/Import ou outros; se vazio -> Unknown)
            fields[4] = normalizeString(fields[4]);

            // 5 Price (double; se inválido -> -1)
            fields[5] = normalizeNumeric(fields[5]);

            // 6 Weight (double; se inválido -> -1)
            fields[6] = normalizeNumeric(fields[6]);

            // 7 Unit (string)
            fields[7] = normalizeString(fields[7]);

            // 8 Amount (double; se inválido -> -1)
            fields[8] = normalizeNumeric(fields[8]);

            // 9 Category (string)
            fields[9] = normalizeString(fields[9]);

            // Reconstroi linha padronizada com ';'
            String cleaned = joinSemicolon(fields);
            out.set(cleaned);
            ctx.write(NullWritable.get(), out);
            ctx.getCounter(QCounters.EMITTED).increment(1);
        }

        private static boolean isHeaderLine(String line) {
            // robusto: verifica início do arquivo por primeira coluna "Country"
            // e também o cabeçalho completo se existir
            if (line.startsWith("Country;Year;") || line.startsWith("\"Country\";\"Year\";")) return true;
            // fallback: primeira coluna
            String first = line.split(";", 2)[0].replace("\"", "").trim();
            return first.equalsIgnoreCase("Country");
        }

        // Parser de CSV com ';' respeitando aspas
        private static String[] parseCsvSemicolon(String line) {
            String[] out = new String[10];
            int idx = 0;
            StringBuilder cur = new StringBuilder();
            boolean inQuotes = false;

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '\"') {
                    inQuotes = !inQuotes;
                } else if (c == ';' && !inQuotes) {
                    if (idx < out.length) out[idx++] = cur.toString();
                    cur.setLength(0);
                } else if ((c == '\n' || c == '\r')) {
                    // normaliza quebras internas
                    cur.append(' ');
                } else {
                    cur.append(c);
                }
            }
            if (idx < out.length) out[idx++] = cur.toString();

            // se veio com mais/menos colunas, falha
            if (idx != 10) return null;
            // remove aspas externas, se houver
            for (int i = 0; i < out.length; i++) {
                String s = out[i];
                if (s == null) s = "";
                s = s.trim();
                if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
                    s = s.substring(1, s.length() - 1);
                }
                out[i] = s;
            }
            return out;
        }

        private static String normalizeString(String s) {
            if (s == null) return "Unknown";
            s = s.trim();
            if (s.isEmpty()) return "Unknown";
            // protege contra ';' dentro do texto
            s = s.replace(';', ' ');
            return s;
        }

        private static String normalizeNumeric(String s) {
            if (s == null) return "-1";
            s = s.trim();
            if (s.isEmpty()) return "-1";
            // vírgula decimal → ponto
            s = s.replace(',', '.');
            try {
                double v = Double.parseDouble(s);
                if (Double.isNaN(v) || Double.isInfinite(v)) return "-1";
                return String.valueOf(v);
            } catch (NumberFormatException e) {
                return "-1";
            }
        }

        private static Integer parseIntSafe(String s) {
            if (s == null) return null;
            s = s.trim();
            if (s.isEmpty()) return null;
            // remove possíveis aspas
            if (s.startsWith("\"") && s.endsWith("\"") && s.length() >= 2) {
                s = s.substring(1, s.length() - 1);
            }
            try {
                return Integer.parseInt(s);
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private static String joinSemicolon(String[] fields) {
            // monta a linha final — 10 colunas separadas por ';'
            StringBuilder sb = new StringBuilder(128);
            for (int i = 0; i < fields.length; i++) {
                if (i > 0) sb.append(';');
                // não recolocamos aspas, pois já sanearmos ';' e quebras
                sb.append(fields[i] == null ? "" : fields[i]);
            }
            return sb.toString();
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Uso: PreprocessTransactions <input> <output>");
            return 1;
        }
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "TDE2-Preprocess-Transactions");
        job.setJarByClass(PreprocessTransactions.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CleanMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Map-only
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ec = ToolRunner.run(new Configuration(), new PreprocessTransactions(), args);
        System.exit(ec);
    }
}
