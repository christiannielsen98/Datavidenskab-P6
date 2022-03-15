# README for the Temporal Pattern Mining


## Prerequisites
- Python 3.7.3 (or later)
- numpy 1.18.2 (or later)
- pandas 1.0.3 (or later)

## To run 
python3 main.py -i path_input -o path_output -ms support_threshold -mc confidence_threshold -mps level

path_input: path of sequence database
path_output: path of folder contains output
support_threshold: the minimum support threshold
confidence_threshold: the minimum confidence threshold
level: the maximum level to mine

Example: python3 main.py -i Data/ISID.csv -o output -ms 0.1 -mc 0.1 -mps 3
