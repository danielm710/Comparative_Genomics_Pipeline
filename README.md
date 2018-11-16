# FuncmE

A course project for BIOL469, Genomic. It's a pipeline for comparative genomics based on Spotify Luigi pipeline framework. It takes Illumina raw sequence reads (either HiSeq or MiSeq platform), assemble genome, predict genes, annotate predicted genes, and perform basic comparisons.

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

