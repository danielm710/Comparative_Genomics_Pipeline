import os
import luigi
from collections import Counter
from subprocess import Popen, PIPE

output_dir = "output"
assembly_dir = os.path.join(output_dir, "assembly")
prediction_dir = os.path.join(output_dir, "prediction")
compare_dir = os.path.join(output_dir, "compare")
annotation_dir = os.path.join(output_dir, "annotation")
script_dir = "scripts"

def run_cmd(cmd):
    p = Popen(cmd, stdout=PIPE)
    output = p.communicate()[0]
    return output

class GlobalParams(luigi.Config):
    samples = luigi.DictParameter()

class assemble_spades(luigi.Task):
    samples = GlobalParams().samples
    cov_cutoff = luigi.Parameter(default='off')
    assembly_dir = os.path.join("")

    def output(self):
        output = {}
        tmp = {}
        for sample in self.samples:
            # path to assembly files
            contigs = os.path.join(assembly_dir, sample, "contigs.fasta")
            scaffolds = os.path.join(assembly_dir, sample, "scaffolds.fasta")

            tmp['contigs'] = luigi.LocalTarget(contigs)
            tmp['scaffolds'] = luigi.LocalTarget(scaffolds)
            output[sample] = tmp

        return output

    def run(self):

        for sample in self.samples:
            out_dir = os.path.join(assembly_dir, sample)
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
    prodigal_dir = os.path.join(prediction_dir, "prodigal")

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
            scaffolds = os.path.join(assembly_dir, sample, "scaffolds.fasta")
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
    FragGeneScan_dir = os.path.join(prediction_dir, "FragGeneScan")
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
            scaffolds = os.path.join(assembly_dir, sample, "scaffolds.fasta")

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

class run_diamond_prodigal(luigi.Task):
    samples = GlobalParams().samples
    prodigal_dir = os.path.join(prediction_dir, "prodigal")
    diamond_dir = os.path.join(prediction_dir, "diamond")
    uniref_db = luigi.Parameter()

    def requires(self):
        return prodigal()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            diamond_prodigal = os.path.join(self.diamond_dir, sample,
                    sample + "_diamond_prodigal.dout")

            tmp['diamond'] = luigi.LocalTarget(diamond_prodigal)

            output[sample] = tmp

        return output

    def run(self):

        for sample in self.samples:
            #inputs
            prodigal_dir = os.path.join(self.prodigal_dir, sample)
            prodigal_faa = os.path.join(prodigal_dir, sample + "_" + "proteins.faa")

            #outputs
            out_dir = os.path.join(self.diamond_dir, sample)
            diamond_out = os.path.join(out_dir, sample + "_diamond_prodigal.dout")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #run diamond
            run_cmd(['diamond',
                    'blastp',
                    '-d',
                    self.uniref_db,
                    '-q',
                    prodigal_faa,
                    '-o',
                    diamond_out])

class run_diamond_FragGeneScan(luigi.Task):
    samples = GlobalParams().samples
    FragGeneScan_dir = os.path.join(prediction_dir, "FragGeneScan")
    diamond_dir = os.path.join(prediction_dir, "diamond")
    uniref_db = luigi.Parameter()

    def requires(self):
        return FragGeneScan()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            diamond_FragGeneScan = os.path.join(self.diamond_dir, sample,
                    sample + "_diamond_FragGeneScan.dout")
            tmp['diamond'] = luigi.LocalTarget(diamond_FragGeneScan)

            output[sample] = tmp

        return output

    def run(self):

        for sample in self.samples:
            #inputs
            FragGeneScan_dir = os.path.join(self.FragGeneScan_dir, sample)
            FragGeneScan_faa = os.path.join(FragGeneScan_dir, sample + ".faa")

            #output
            out_dir = os.path.join(self.diamond_dir, sample)
            diamond_out = os.path.join(out_dir, sample + "_diamond_FragGeneScan.dout")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #run diamond
            run_cmd(['diamond',
                    'blastp',
                    '-d',
                    self.uniref_db,
                    '-q',
                    FragGeneScan_faa,
                    '-o',
                    diamond_out])

class extract_top_hits_prodigal(luigi.Task):
    samples = GlobalParams().samples
    diamond_dir = os.path.join(prediction_dir, "diamond")

    def requires(self):
        return run_diamond_prodigal()

    def output(self):
        tmp = {}
        output= {}

        for sample in self.samples:
            tophits_prodigal = os.path.join(self.diamond_dir, sample,
                    sample + "_diamond_prodigal_tophits.dout")

            tmp['tophits'] = luigi.LocalTarget(tophits_prodigal)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            #inputs
            raw_diamond_dir = os.path.join(self.diamond_dir, sample)
            diamond_prodigal = os.path.join(raw_diamond_dir, sample +
                    "_diamond_prodigal.dout")
            #outputs
            tophits_prodigal = os.path.join(raw_diamond_dir, sample +
                    "_diamond_prodigal_tophits.dout")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    raw_diamond_dir])

            #extract top hits
            run_cmd(['python',
                    os.path.join(script_dir, "diamond_extract_tophits.py"),
                    '--i',
                    diamond_prodigal,
                    '--o',
                    tophits_prodigal])

class extract_top_hits_FragGeneScan(luigi.Task):
    samples = GlobalParams().samples
    diamond_dir = os.path.join(prediction_dir, "diamond")

    def requires(self):
        return run_diamond_FragGeneScan()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            tophits_FragGeneScan = os.path.join(self.diamond_dir, sample,
                    sample + "_diamond_FragGeneScan_tophits.dout")

            tmp['tophits'] = luigi.LocalTarget(tophits_FragGeneScan)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            #inputs
            raw_diamond_dir = os.path.join(self.diamond_dir, sample)
            diamond_FragGeneScan = os.path.join(raw_diamond_dir, sample +
                    "_diamond_FragGeneScan.dout")
            #outputs
            tophits_FragGeneScan = os.path.join(raw_diamond_dir, sample +
                    "_diamond_FragGeneScan_tophits.dout")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    raw_diamond_dir])

            #extract top hits
            run_cmd(['python',
                    os.path.join(script_dir, "diamond_extract_tophits.py"),
                    '--i',
                    diamond_FragGeneScan,
                    '--o',
                    tophits_FragGeneScan])

class get_protein_id_prodigal(luigi.Task):
    samples = GlobalParams().samples
    diamond_dir = os.path.join(prediction_dir, "diamond")
    protein_dir = os.path.join(prediction_dir, "protein")

    def requires(self):
        return extract_top_hits_prodigal()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            unique_ids = os.path.join(self.protein_dir, sample, sample +
                    "_prodigal_protein_unique.txt")
            dup_ids = os.path.join(self.protein_dir, sample, sample +
                    "_prodigal_protein_duplicate.txt")

            tmp['unique_ids'] = luigi.LocalTarget(unique_ids)
            tmp['dup_ids'] = luigi.LocalTarget(dup_ids)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            #inputs
            tophits_prodigal = os.path.join(self.diamond_dir, sample, sample +
                    "_diamond_prodigal_tophits.dout")

            #outputs
            out_dir = os.path.join(self.protein_dir, sample)
            unique_ids = os.path.join(out_dir, sample +
                    "_prodigal_protein_unique.txt")
            dup_ids = os.path.join(out_dir, sample +
                    "_prodigal_protein_duplicate.txt")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #get unique and duplicate protein ids
            run_cmd(['python',
                    os.path.join(script_dir, 'get_protein_id.py'),
                    '--i',
                    tophits_prodigal,
                    '--u',
                    unique_ids,
                    '--d',
                    dup_ids])

class get_protein_id_FragGeneScan(luigi.Task):
    samples = GlobalParams().samples
    diamond_dir = os.path.join(prediction_dir, "diamond")
    protein_dir = os.path.join(prediction_dir, "protein")

    def requires(self):
        return extract_top_hits_FragGeneScan()

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            unique_ids = os.path.join(self.protein_dir, sample, sample +
                    "_FragGeneScan_protein_unique.txt")
            dup_ids = os.path.join(self.protein_dir, sample, sample +
                    "_FragGeneScan_protein_duplicate.txt")

            tmp['unique_ids'] = luigi.LocalTarget(unique_ids)
            tmp['dup_ids'] = luigi.LocalTarget(dup_ids)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            #inputs
            tophits_FragGeneScan = os.path.join(self.diamond_dir, sample, sample +
                    "_diamond_FragGeneScan_tophits.dout")

            #outputs
            out_dir = os.path.join(self.protein_dir, sample)
            unique_ids = os.path.join(out_dir, sample +
                    "_FragGeneScan_protein_unique.txt")
            dup_ids = os.path.join(out_dir, sample +
                    "_FragGeneScan_protein_duplicate.txt")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #get unique and duplicate protein ids
            run_cmd(['python',
                    os.path.join(script_dir, 'get_protein_id.py'),
                    '--i',
                    tophits_FragGeneScan,
                    '--u',
                    unique_ids,
                    '--d',
                    dup_ids])

class extract_unique_protein_final(luigi.Task):
    samples = GlobalParams().samples
    protein_dir = os.path.join(prediction_dir, "protein")

    def requires(self):
        return [get_protein_id_prodigal(),
                get_protein_id_FragGeneScan()]

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            common_proteins = os.path.join(self.protein_dir, sample,
                    sample + "_common_unique_proteins.txt")
            prodigal_only = os.path.join(self.protein_dir, sample, sample +
                    "_prodigal_only_unique_proteins.txt")
            FragGeneScan_only = os.path.join(self.protein_dir, sample, sample +
                    "_FragGeneScan_only_unique_proteins.txt")

            tmp['common'] = luigi.LocalTarget(common_proteins)
            tmp['prodigal'] = luigi.LocalTarget(prodigal_only)
            tmp['FragGeneScan'] = luigi.LocalTarget(FragGeneScan_only)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            out_dir = os.path.join(self.protein_dir, sample)

            #inputs
            unique_prodigal_ids = os.path.join(out_dir, sample +
                    "_prodigal_protein_unique.txt")
            unique_FragGeneScan_ids = os.path.join(out_dir, sample +
                    "_FragGeneScan_protein_unique.txt")


            #outputs
            common_proteins = os.path.join(out_dir, sample +
                    "_common_unique_proteins.txt")
            prodigal_only = os.path.join(out_dir, sample +
                    "_prodigal_only_unique_proteins.txt")
            FragGeneScan_only = os.path.join(out_dir, sample +
                    "_FragGeneScan_only_unique_proteins.txt")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #extract 
            run_cmd(['bash',
                    os.path.join(script_dir, 'extract_common_proteins.sh'),
                    unique_prodigal_ids,
                    unique_FragGeneScan_ids,
                    common_proteins,
                    prodigal_only,
                    FragGeneScan_only])

class extract_duplicate_protein_final(luigi.Task):
    samples = GlobalParams().samples
    protein_dir = os.path.join(prediction_dir, "protein")

    def requires(self):
        return [get_protein_id_prodigal(),
                get_protein_id_FragGeneScan()]

    def output(self):
        tmp = {}
        output = {}

        for sample in self.samples:
            common_proteins = os.path.join(self.protein_dir, sample,
                    sample + "_common_duplicate_proteins.txt")
            prodigal_only = os.path.join(self.protein_dir, sample, sample +
                    "_prodigal_only_duplicate_proteins.txt")
            FragGeneScan_only = os.path.join(self.protein_dir, sample, sample +
                    "_FragGeneScan_only_duplicate_proteins.txt")

            tmp['common'] = luigi.LocalTarget(common_proteins)
            tmp['prodigal'] = luigi.LocalTarget(prodigal_only)
            tmp['FragGeneScan'] = luigi.LocalTarget(FragGeneScan_only)
            output[sample] = tmp

        return output

    def run(self):
        for sample in self.samples:
            out_dir = os.path.join(self.protein_dir, sample)

            #inputs
            unique_prodigal_ids = os.path.join(out_dir, sample +
                    "_prodigal_protein_duplicate.txt")
            unique_FragGeneScan_ids = os.path.join(out_dir, sample +
                    "_FragGeneScan_protein_duplicate.txt")


            #outputs
            common_proteins = os.path.join(out_dir, sample +
                    "_common_duplicate_proteins.txt")
            prodigal_only = os.path.join(out_dir, sample +
                    "_prodigal_only_duplicate_proteins.txt")
            FragGeneScan_only = os.path.join(out_dir, sample +
                    "_FragGeneScan_only_duplicate_proteins.txt")

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #extract 
            run_cmd(['bash',
                    os.path.join(script_dir, 'extract_common_proteins.sh'),
                    unique_prodigal_ids,
                    unique_FragGeneScan_ids,
                    common_proteins,
                    prodigal_only,
                    FragGeneScan_only])

class pan_genome(luigi.Task):
    samples = GlobalParams().samples
    protein_dir = os.path.join(prediction_dir, "protein")

    def requires(self):
        return extract_unique_protein_final()

    def output(self):
        output = {}

        for sample in self.samples:
            sample_only_proteins = os.path.join(compare_dir, sample +
                    "_variableGenome.txt")
            output[sample] = luigi.LocalTarget(sample_only_proteins)

        output['panGenome'] = luigi.LocalTarget(os.path.join(compare_dir,
            "panGenome.txt"))

        return output

    def run(self):
        sample_dict = {}
        set_list = []

        #create output directory
        run_cmd(['mkdir',
                '-p',
                compare_dir])

        for sample in self.samples:
            #temporary list to store protein IDs per file
            tmp = []
            #inputs
            sample_file = os.path.join(self.protein_dir, sample,
                    sample + "_common_unique_proteins.txt")

            with open(sample_file, 'r') as fh:
                for line in fh:
                    tmp.append(line.strip())

            sample_dict[sample] = set(tmp)
            set_list.append(set(tmp))

        #find pan genome
        common_proteins = set.intersection(*set_list)
        with self.output()['panGenome'].open('w') as fh:
            for _id in common_proteins:
                fh.write(_id + "\n")

        #find variable genome
        for sample in sample_dict:
            tmp = list(set_list)
            #in case there is only one sample
            if(len(sample_dict) > 1):
                tmp.remove(sample_dict[sample])
            rest_of_genomes = set.union(*tmp)

            variable_genome = sample_dict[sample].difference(rest_of_genomes)

            with self.output()[sample].open('w') as fh:
                for _id in variable_genome:
                    fh.write(_id + "\n")

class assign_GO(luigi.Task):
    samples = GlobalParams().samples
    GO_db = luigi.Parameter()

    def requires(self):
        return pan_genome()

    def output(self):
        output = {}

        for sample in self.samples:
            go_variable_genome = os.path.join(annotation_dir, sample, sample +
                    "_variableGenome_GO.txt")

            output[sample] = luigi.LocalTarget(go_variable_genome)

        go_pan_genome = os.path.join(annotation_dir, "panGenome_GO.txt")
        output['panGenome_GO'] = luigi.LocalTarget(go_pan_genome)

        return output

    def run(self):
        for sample in self.samples:
            #inputs
            variable_genome = os.path.join(compare_dir, sample +
                    "_variableGenome.txt")
            #outputs
            out_dir = os.path.join(annotation_dir, sample)

            #create output directory
            run_cmd(['mkdir',
                    '-p',
                    out_dir])

            #assign GO terms for variable genomes
            go_assigned = run_cmd(['python',
                                    os.path.join(script_dir, 'uniprot2go.py'),
                                    '-i',
                                    variable_genome,
                                    '-d',
                                    self.GO_db])

            with self.output()[sample].open('w') as fh:
                fh.write(go_assigned)

        #assign GO terms for pan genome
        pan_genome = os.path.join(compare_dir, "panGenome.txt")

        go_assigned = run_cmd(['python',
                                os.path.join(script_dir, 'uniprot2go.py'),
                                '-i',
                                pan_genome,
                                '-d',
                                self.GO_db])

        with self.output()['panGenome_GO'].open('w') as fh:
            fh.write(go_assigned)

class GO_statistics(luigi.Task):
    samples = GlobalParams().samples

    def requires(self):
        return assign_GO()

    def output(self):
        output = {}

        for sample in self.samples:
            go_frequency = os.path.join(annotation_dir, sample, sample +
                    "_GO_frequency.txt")
            output[sample] = luigi.LocalTarget(go_frequency)

        go_frequency = os.path.join(annotation_dir, "panGenome_GO_frequency.txt")
        output['GO_frequency'] = luigi.LocalTarget(go_frequency)

        return output

    def run(self):
        for sample in self.samples:
            #temporary list to hold GO terms
            tmp = []
            GO_freq_dict = {}

            #read input and process
            with self.input()[sample].open('r') as fh:
                for line in fh:
                    GO_terms = line.split('\t')[1].strip().split(',')
                    tmp.extend(GO_terms)

            tmp = list(filter(None, tmp))
            unique_GO_terms = set(tmp)
            for term in unique_GO_terms:
                GO_freq_dict[term] = tmp.count(term)

            #write output
            sorted_GO_freq = sorted(GO_freq_dict.items(), key=lambda kv: kv[1],reverse=True)
            with self.output()[sample].open('w') as fh:
                for item in sorted_GO_freq:
                    fh.write(item[0].strip() + "\t" + str(item[1]) + "\n")


        tmp = []
        GO_freq_dict = {}

        #read pan genome file and process
        with self.input()['panGenome_GO'].open('r') as fh:
            for line in fh:
                GO_terms = line.split('\t')[1].strip().split(',')
                tmp.extend(GO_terms)

        tmp = list(filter(None, tmp))
        unique_GO_terms = set(tmp)
        for term in unique_GO_terms:
            GO_freq_dict[term] = tmp.count(term)

        #write output
        sorted_GO_freq = sorted(GO_freq_dict.items(), key=lambda kv: kv[1],reverse=True)
        with self.output()['GO_frequency'].open('w') as fh:
            for item in sorted_GO_freq:
                fh.write(item[0].strip() + "\t" + str(item[1]) + "\n")

#dummy class to run all the tasks
class run_tasks(luigi.Task):
    def requires(self):
        task_list = [assemble_spades(),
                    prodigal(),
                    FragGeneScan(),
                    run_diamond_prodigal(),
                    run_diamond_FragGeneScan(),
                    extract_top_hits_prodigal(),
                    extract_top_hits_FragGeneScan(),
                    get_protein_id_prodigal(),
                    get_protein_id_FragGeneScan(),
                    extract_unique_protein_final(),
                    extract_duplicate_protein_final(),
                    pan_genome(),
                    assign_GO(),
                    GO_statistics()]

        return task_list

if __name__ == '__main__':
    luigi.run()
