"""
Extract top protein match from each cds region
"""
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--i', help="alignment result from DIAMOND")
    parser.add_argument('--o', help='path to output')

    return parser.parse_args()

def process_diamond(dout):
    tophits = []
    _id_prev = ''

    with open(dout, 'r') as fh:
        for line in fh:
            _id = line.split("\t")[0]
            if(_id != _id_prev):
                tophits.append(line)

            _id_prev = _id

    return tophits

def write(tophits, out):
    with open(out, 'w') as fh:
        for line in tophits:
            fh.write(line)


args = parse_args()
tophits = process_diamond(args.i)
write(tophits, args.o)
