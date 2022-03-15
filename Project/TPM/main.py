from TAR import *
from TAR.Model import Utils
import argparse
import os

parser = argparse.ArgumentParser(description='Temporal Association Rule')
parser.add_argument('-name', '--name', help='Name of experiment', default='Experiment')
parser.add_argument('-i', '--input', help='input path', required=True)
parser.add_argument('-o', '--output', help='output path', required=True)
parser.add_argument('-ms', '--minsup', help='min support', default=0.5)
parser.add_argument('-mc', '--minconf', help='confidence', default=0.5)
parser.add_argument('-e', '--epsilon', help='epsilon', default=0)
parser.add_argument('-mo', '--minoverlap', help='min overlap', default=1)
parser.add_argument('-mps', '--maxpatternsize', help='max pattern size', default=-1)
parser.add_argument('-nosave', '--nosavepattern', help='save pattern to file', default=False, action='store_true')
args = vars(parser.parse_args())


dataset = args['input']
minsup = float(args['minsup'])
minconf = float(args['minconf'])
epsilon = float(args['epsilon'])
minoverlap = float(args['minoverlap'])
name = args['name']
max_pattern_size = int(args['maxpatternsize'])
save_patterns = not bool(args['nosavepattern'])
output = os.path.join(args['output'], '{}_minsup{}_minconf_{}'.format(name, minsup, minconf))

Utils.checkArgs(minsup, minconf, epsilon, minoverlap, max_pattern_size)

config = TARConf(name, minsup, minconf, dataset, max_pattern_size, epsilon, minoverlap, output, save_patterns)

TAR(config)
