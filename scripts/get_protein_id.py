"""
Get unique and duplicate protein IDs from diamond alignment result
against UniRef50 database
"""
from argparse import ArgumentParser

def parse_args():
    parser = ArgumentParser()
    parser.add_argument('--i', help='tophits from diamond alignment against' +
            ' UniRef database')
    parser.add_argument('--u', help='path to store unique protein ids')
    parser.add_argument('--d', help='path to store duplicate protein ids')

    return parser.parse_args()

def get_ids(tophits):
    unique_ids = set()
    dup_ids = []

    with open(tophits, 'r') as fh:
        for line in fh:
            _id = line.split("\t")[1].split("_")[1].strip()

            unique_ids.add(_id)
            dup_ids.append(_id)

    return unique_ids, dup_ids

def write(unique_ids, dup_ids, uniq_out, dup_out):
    with open(uniq_out, 'w') as fh:
        for _id in unique_ids:
            fh.write(_id + '\n')

    with open(dup_out, 'w') as fh:
        for _id in dup_ids:
            fh.write(_id + '\n')

args = parse_args()
unique_ids, dup_ids = get_ids(args.i)
write(unique_ids, dup_ids, args.u, args.d)
