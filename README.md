TDE 2 — MapReduce (Pré-processamento + Execução Local)

Este repositório contém o job de pré-processamento responsável por:

Remover cabeçalho

Tratar faltantes (Unknown para textos, -1 para numéricos)

Garantir 10 colunas separadas por ;

Gerar out/clean_transactions.txt pronto para os demais jobs (Q1–Q8)

Pré-requisitos:

Java 8 (JDK)
Verifique:
java -version
javac -version

Hadoop 3.x (apenas para classpath/execução local)
Defina no seu ~/.bashrc:

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=$HOME/apps/hadoop-3.3.6
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

Recarregue com:
source ~/.bashrc

Observação: Não é necessário subir NameNode/DataNode. Rodamos o job em modo local, usando apenas o classpath do Hadoop.

Como rodar:

Coloque o CSV de entrada em in/ (ou informe o caminho).

Na raiz do projeto:
chmod +x run.sh
./run.sh in/operacoes_comerciais_inteira.csv

Saída:

Consolidado: out/clean_transactions.txt

Partições: out/clean_out/part-m-*

Validação rápida:

head -n 5 out/clean_transactions.txt | column -s ';' -t  
awk -F';' 'NF!=10{print "Linha inválida:", NR}' out/clean_transactions.txt  
wc -l out/clean_transactions.txt


Estrutura de pastas (sugestão):

tde_2_grupo_2/
├── src/
│ └── basic/
│ └── PreprocessTransactions.java
├── in/
│ └── operacoes_comerciais_inteira.csv (não comitar se for grande)
├── out/
│ ├── clean_out/ (part-m-*)
│ └── clean_transactions.txt (resultado final)
├── build/
├── run.sh
├── README.md
└── .gitignore

Por que não comitamos o .txt final?

O arquivo final tem centenas de MB/GB (GitHub tem limite de 100 MB por arquivo).

É arquivo gerado: melhor produzir sob demanda com run.sh.

Contato:

Qualquer dúvida, rodar ./run.sh ou chamar a Pessoa A do grupo.