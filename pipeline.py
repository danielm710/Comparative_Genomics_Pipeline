import os
import luigi
from subprocess import Popen, PIPE

def run_cmd(cmd):
    print(cmd)
    p = Popen(cmd, stdout=PIPE)
    output = p.communicate()[0]
    return output

class GlobalParams(luigi.Config):
    samples = luigi.DictParameter()

class assemble_spades(luigi.Task):
    samples = GlobalParams().samples
    cov_cutoff = luigi.Parameter(default='off')

    def output(self):
        output = {}
        tmp = {}
        for sample in self.samples:
            # path to assembly files
            contigs = os.path.join("assembly", sample, "contigs.fasta")
            scaffolds = os.path.join("assembly", sample, "scaffolds.fasta")

            tmp['contigs'] = luigi.LocalTarget(contigs)
            tmp['scaffolds'] = luigi.LocalTarget(scaffolds)
            output[sample] = tmp

        return output

    def run(self):
        ass_dir = 'assembly'

        for sample in self.samples:
            out_dir = os.path.join(ass_dir, sample)
            run_cmd(['spades',
                    '-o',
                    out_dir,
                    '-1',
                    self.samples[sample]['fastq1'],
                    '-2',
                    self.samples[sample]['fastq2'],
                    '--only-assembler',
                    '--careful',
                    '--cov-cutoff',
                    self.cov_cutoff
                    ])

class prodigal(luigi.Task):
    samples = GlobalParams().samples
    prodigal_dir = os.path.join("prediction", "prodigal")

    def requires(self):
        return assemble_spades()

    def output(self):
        output = {}
        tmp = {}
        for sample in self.samples:
            # path to gene prediction files
            proteins = os.path.join(self.prodigal_dir, sample, sample + "_" + "proteins.faa")
            genes = os.path.join(self.prodigal_dir, sample, sample + "_" + "genes")

            tmp['proteins'] = luigi.LocalTarget(proteins)
            tmp['genes'] = luigi.LocalTarget(genes)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            out_dir = os.path.join(self.prodigal_dir, sample)

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #input scaffold fasta file
            scaffolds = os.path.join("assembly", sample, "scaffolds.fasta")
            #output files
            proteins = os.path.join(out_dir, sample + "_" + "proteins.faa")
            genes = os.path.join(out_dir, sample + "_" + "genes")

            run_cmd(['prodigal',
                    '-i',
                    scaffolds,
                    '-o',
                    genes,
                    '-a',
                    proteins
                    ])

class FragGeneScan(luigi.Task):
    samples = GlobalParams().samples
    FragGeneScan_dir = os.path.join("prediction", "FragGeneScan")
    seq_type = luigi.Parameter(default='1')
    train_type = luigi.Parameter(default='complete')

    def requires(self):
        return assemble_spades()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            # path to gene prediction files
            proteins = os.path.join(self.FragGeneScan_dir, sample, sample + ".faa")
            genes = os.path.join(self.FragGeneScan_dir, sample, sample + ".ffn")

            tmp['proteins'] = luigi.LocalTarget(proteins)
            tmp['genes'] = luigi.LocalTarget(genes)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            #output directory
            out_dir = os.path.join(self.FragGeneScan_dir, sample)
            #output file prefix
            prefix = os.path.join(out_dir, sample)
            #input scaffold file
            scaffolds = os.path.join("assembly", sample, "scaffolds.fasta")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            run_cmd(['FragGeneScan',
                    '-s',
                    scaffolds,
                    '-o',
                    prefix,
                    '-w',
                    self.seq_type,
                    '-t',
                    self.train_type])

#dummy class to run all the tasks
class run_tasks(luigi.Task):
    def requires(self):
        task_list = [assemble_spades(),
                    prodigal(),
                    FragGeneScan()]

        return task_list

if __name__ == '__main__':
    luigi.run()
