from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--s', help='input scaffold file in fasta')
    parser.add_argument('--min-cov', type=int, help='coverage threshold')
    parser.add_argument('--o', help='path to output')

    return parser.parse_args()

def parse_scaffold(scaffold, threshold):
    pattern = '>'
    header = ''
    seq = ''
    #dictionary to store header as key and sequence as value
    seq_dict = {}

    with open(scaffold, 'r') as fh:
        for line in fh:
            if (pattern in line):
                #add header and corresponding sequence to dictionary
                if (header and seq):
                    seq_dict[header] = seq

                #delete previous header and sequence
                header = ''
                seq = ''

                coverage = float(line.split('_')[5])
                #store header if coverage exceeds threshold
                if (coverage >= threshold):
                    header = line
            else:
                seq = seq + line

    seq_dict[header] = seq

    return seq_dict

def write_new_scaffold(outfile, seq_dict):
    with open(outfile, 'w') as fh:
        for header in sorted(seq_dict, key=lambda x: int(x.split("_")[1])):
            fh.write(header  + seq_dict[header])

args = parse_args()
seq_dict = parse_scaffold(args.s, args.min_cov)
write_new_scaffold(args.o, seq_dict)
