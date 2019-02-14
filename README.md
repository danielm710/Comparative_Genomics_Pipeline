# FuncmE

A course project for BIOL469, Genomic. It's a comparative genomics pipeline based on Spotify Luigi pipeline framework. It takes Illumina raw sequence reads (either HiSeq or MiSeq platform), assembles genomes, predicts genes, annotates predicted genes, and performs basic comparisons.

## Getting Started

### Prerequisites

Pipeline uses several bioinformatics tools listed below.

#### Quality Control
```
Trimmomatic
FastQC
```

#### Genome Assembly
```
SPAdes
```

#### Gene Prediction
```
Prodigal
FragGeneScan
Glimmer
```

#### Alignment
```
DIAMOND
```

### Usage
```
[GlobalParams]
samples = {"ERX2618856": {"fastq1":"/home/group3/ERX2618856_ecoli_patho/ERX2618856_1.fastq", "fastq2":"/home/group3/ERX2618856_ecoli_patho/ERX2618856_2.fastq"}, "ERX008638": {"fastq1":"/home/group3/ERX008638_ecoli_k12/s_6_1.fastq", "fastq2":"/home/group3/ERX008638_ecoli_k12/s_6_2.fastq"}}

[assemble_spades]
cov_cutoff = 30

[FragGeneScan]
seq_type = 1
train_type = complete

[run_diamond_prodigal]
uniref_db = /home/group3/data/uniref/uniref50

[run_diamond_FragGeneScan]
uniref_db = /home/group3/data/uniref/uniref50

[assign_GO]
GO_db = /data/uniprot2go/uniprot-vs-go-db.sl3
```
