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

