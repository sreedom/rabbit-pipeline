from pipeline import Pipeline
from phase import simpleAsyncPhase
from component import CountComponent, PassComponent

source = CountComponent(conf={'n':20})
dest = PassComponent()
ph1 = simpleAsyncPhase(source)
ph2 = simpleAsyncPhase(dest)
ph1.set_children([ph2])

pipeline = Pipeline(ph1)
pipeline.run()

import ipdb; ipdb.set_trace()  # XXX BREAKPOINT
source = CountComponent(conf={'n':30})
dest = PassComponent()
ph1 = simpleAsyncPhase(source)
ph2 = simpleAsyncPhase(dest)
ph1.set_children([ph2])

pipeline = Pipeline(ph1)
print pipeline._ioloop._stopped
import ipdb; ipdb.set_trace()  # XXX BREAKPOINT
pipeline.run()
