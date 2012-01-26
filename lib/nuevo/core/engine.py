
import re
import pkg_resources

import logging
log = logging.getLogger(__name__)


def get_engines():
    engines = {}
    for ep in pkg_resources.iter_entry_points('nuevo.engines'):
        try:
            eng_class = ep.load()()
            engines[ep.name] = eng_class
            logging.debug("Loaded engine module %s", ep.name)
        except Exception as ex:
            logging.error("Could not import engine %s: %s", ep.name, ex)
            raise ex
    return engines

def create_engine(url):
    engines = get_engines()
    
    m = re.match( r"(?P<engine>[a-zA-Z0-9]+)(\+(?P<option>[a-zA-Z0-9]+))?:(?P<rest>.*)", url)
        
    engine, option, rest = m.group('engine'), m.group('option'), m.group('rest')
    
    eng_class = engines[engine]
    return eng_class(option, rest)

class Engine(object):
    
    name = None
    
    def query(self, language, q):
        try:
            query_fun = getattr(self, 'query_%s')
        except AttributeError:
            raise Exception("%s query language not supported by %s engine." % (language, self.name) )
        return query_fun(q)