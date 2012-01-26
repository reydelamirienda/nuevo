

import logging
log = logging.getLogger(__name__)

import requests, json
from nuevo.drivers.neo4j.commands import JSONCommandEncoder, Neo4jBatchedCommand
from nuevo.drivers.neo4j.content import Neo4jContent

from nuevo.core.exceptions import NuevoException

class RESTException(NuevoException):
    
    def __init__(self, message, status):
        self.status = status
        super(RESTException, self).__init__(message)

class Neo4jAtomicREST(object):
    
    def __init__(self, base_url):
        
        self.session = requests.session(
            headers = {
                'Accept':'application/json',
                'Content-Type':'application/json'
            }
        )
        
        self.base_url = base_url.rstrip('/')
    
    def execute(self, cmd, resp_cls=Neo4jContent):
        log.debug("EXEC: %s", cmd)
        response = self.send(cmd)
        if response is not None:
            return resp_cls(cid=None, response=response)
        else:
            return None
    
    def send(self, cmd):
        url  = self.base_url + cmd.resource
        data = json.dumps(cmd, cls=JSONCommandEncoder)
        log.debug("SEND: %s %s %s", cmd.method, url, data)
        
        try:
            resp = self.session.request(cmd.method, url, data=data)
            code = resp.status_code
            cont = resp.content            
            resp.raise_for_status()
            if cont:
                cont = json.loads(cont)
            else:
                cont = None
            log.debug("RECV: %s %s", code, cont)
            return cont
        except requests.exceptions.HTTPError as ex:
            raise RESTException(cont, code)

class Neo4jBatchedREST(object):
    
    def __init__(self, base_url):
        
        self.session = requests.session(
            headers = {
                'Accept':'application/json',
                'Content-Type':'application/json'
            }
        )
        
        self.base_url = base_url.rstrip('/')
        self._cid = 0
        
        self.batch   = []
        self.futures = []
    
    @property
    def next_cid(self):
        _cid = self._cid
        self._cid = self._cid + 1
        return _cid
    
    def execute(self, cmd, resp_cls=Neo4jContent):
        cid = self.next_cid
        
        cmd = Neo4jBatchedCommand(cmd, cid)
        fut = resp_cls(cid=cid, response=None)
        
        self.batch.append(cmd)
        self.futures.append(fut)
        
        return fut
    
    def flush(self):
        url  = "%s/batch" % self.base_url
        data = json.dumps(self.batch, cls=JSONCommandEncoder)
        log.debug("SEND: %s", data)

        resp = self.session.request("POST", url, data=data)
        try:
            resp.raise_for_status()
            responses = json.loads(resp.content)
            log.debug("RECV: %s", responses) 

            self.materialize(responses)
            
            self._cid = 0
            self.batch = []
            self.futures = []
        except requests.HTTPError as err:
            c = resp.content
            if 'message' in c:
                raise RESTException(c.message)
            else:
                raise RESTException(c.exception)
    
    def materialize(self, responses):
        for fut, response in zip(self.futures, responses):
            fut.__materialize__(response['body'])