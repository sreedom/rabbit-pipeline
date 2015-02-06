import pika
from tornado import ioloop

# pipeline.py
# defines the pipeline class
# uses amqp to create message queues to connect the components
ENDOFLIST = 'pipeline.endoflist'

def is_control(data):
    '''return if the data object is of control type
    '''
    return data.get('type') == 'control'

def is_data(data):
    '''return if the data object is of data type
    '''
    return data.get('type') == 'data'

def extract_data(data):
    if is_data(data):
        return data['data']
    else:
        raise Exception("no data to be extracted")

def extract_control(data):
    if is_control(data):
        return data['control']

def datamessage(message):
    '''convert the message into a datamessage'''
    return  {'type' : 'data',
             'data' : message}

def controlmessage(message):
    '''convert the message into a datamessage'''
    return  {'type' : 'control',
             'control' : message}

class Pipeline(object):
    '''defines the structure of the pipeline
    and the setting up/interaction of queues
    the pipeline is instantiated per request
    '''

    def __init__(self, phaseDag, conf={}, amqp_url=None):
        self.host = amqp_url if amqp_url else 'localhost'
        self._conf = conf
        self._phaseDag = phaseDag
        #create an IOLoop
        self._ioloop = ioloop.IOLoop.instance()
        self.setup()

    def setup(self):
        for i,phase in enumerate(self._phaseDag):
            #add the adapters to the ioloop
            phase.pipeline = self
            self._ioloop.add_callback(phase.connect)

    def run(self):
        #start the source, and propagate the data through the dag
        self._ioloop.start()

    def stop(self):
        self._ioloop.clear_instance()

    def from_newick(self, newick_str):
        pass

class PhaseDag(object):
    def __init__(self, start):
        '''create leniar pipelines for now.
        '''
        self._phase = start
    def __iter__(self):
        #depth first search of the phase dag starting from self._phase
        return iter(self._phase)
