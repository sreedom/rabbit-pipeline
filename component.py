# component defenition
# author Sreeraj
#
'''Defines an abstract class for components
'''
import abc

class Component(object):

    ''' abstract Baseclass for all components
    '''
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs):
        ''' Optional init args:
            conf : dictionary of confs
            inputs : list/tuple of input params expected
            outputs : list/tuple of input params that will be added to obj
        '''
        self.conf = kwargs.get('conf',{})
        self.inputs = kwargs.get('inputs',())
        self.outputs = kwargs.get('outputs',())
        self.init()

    def init(self):
        ''' This method is called by __init__
        override here if you want your class to be initialized first
        '''
        pass

    def process_list(self, input_list):
        ''' This method is called by the f/w to process all the inputs
        override this if the inputs needs to be processed at once, else leave as is
        input_list should be a list of dict
        output should be a list of dict
        '''
        return (self.process(obj) for obj in input_list)

    def process_data(self, input):
        ''' Override this method to do transformation to the inputs (one by one)
        if you need the whole list to be processed, override process_all method
        return should be of type dict
        '''
        pass

class CountComponent(Component):
    def process_list(self, input_list):
        number = self.conf.get('n', 10)
        for i in range(number):
            yield {'number' : i + 1}

class PassComponent(Component):
    ''' dummy component that returns same as the input
    '''
    def process_data(self, input):
        print input
        return input
