from bottle import *
BaseRequest.MEMFILE_MAX = 110*1024
import json


@route('/primes/getworkunit/<clientid>')
def index(clientid):
    x = {
        "clientID": clientid
    }
    return json.dumps(x)
    #return "Hey " + str(clientid)

@route('/primes/submitworkunit/<clientid>', method='POST')
def SubmitWorkUnit(clientid):
     clientData  = request.forms.get('data')
     #clientDataDict= json.loads(clientData)
     for n in clientData:
        print(n)
     #print(clientDataDict["results"])

#BaseRequest.MEMFILE_MAX = 1024 * 1024
l = BaseRequest.MEMFILE_MAX
#print(l)
#

run(host='0.0.0.0', port=8080)