### fake driver

from pipeline import Pipeline
from components import C1, C2, C3
# TODO ^^ or should we use a registry ?

#http://en.wikipedia.org/wiki/Newick_format

# representing pipeline of format C1-C2-C4
#                                  |- C3

pipeline = 'C1(C2(C4),C3)'
leadgen_pipeline = Pipeline.from_newick(pipeline)
leadgen_pipeline.run()
