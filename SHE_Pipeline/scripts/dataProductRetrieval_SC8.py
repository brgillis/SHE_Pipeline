import ssl
import socket
import sys
import os
import argparse
import base64
import ast
import time
import datetime

import xml.etree.ElementTree as etree

try:
    from httplib import HTTPSConnection
    import httplib as httplib
    import urllib as urllib
except:
    from http.client import HTTPSConnection
    import http.client as httplib
    import urllib.request as urllib

try:
    sslcontext = ssl._create_unverified_context()
except:
    sslcontext = None


def check_content_length(rec):
    if 'Content-Length' in rec:
        return True
    elif 'content-length' in rec:
        return True
    elif 'Content-Length' in rec:
        return True
    return False


def get_content_length(rec):
    if 'Content-Length' in rec:
        return int(rec['Content-Length'])
    elif 'content-length' in rec:
        return int(rec['content-length'])
    elif 'Content-Length' in rec:
        return int(rec['Content-Length'])
    return -1



class HTTPSConnectionV3(HTTPSConnection):
    def __init__(self, *args, **kwargs):
        httplib.HTTPSConnection.__init__(self, *args, **kwargs)

    def connect(self):
        sock = socket.create_connection((self.host, self.port), self.timeout)
        if self._tunnel_host:
            self.sock = sock
            self._tunnel()
        ssl_version_list = [ssl.PROTOCOL_SSLv2,ssl.PROTOCOL_SSLv3,ssl.PROTOCOL_SSLv23,
                            ssl.PROTOCOL_TLSv1]

        for ssl_i in ssl_version_list:
            try:
                self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_SSLv3)
                break
            except ssl.SSLError as e:
                print("Failed:"+ssl._PROTOCOL_NAMES[ssl_i])
#            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, cert_reqs=ssl.CERT_NONE, ssl_version=ssl.PROTOCOL_SSLv23)
#            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_SSLv23)
#            self.sock = ssl.wrap_socket(sock, self.key_file, self.cert_file, ssl_version=ssl.PROTOCOL_SSLv2)


BASE_EAS_URL="https://eas-dps-cus-ops.esac.esa.int/NewEuclidXML?class_name="
#BASE_EAS_URL="https://eas-dps-rest-ops.esac.esa.int/NewEuclidXML?class_name="
#BASE_DSS_URL="https://dss-mdb.euclid.astro.rug.nl/"
BASE_DSS_URL="https://euclid-dss.roe.ac.uk/"
#BASE_DSS_HOST="dss-mdb.euclid.astro.rug.nl"
BASE_DSS_HOST="euclid-dss.roe.ac.uk"
BASE_DSS_PORT=443
buffer_size=16*1024

def geturl(inpstring):
    url = ''
    jobstatus = ''
    try:
       retdic=ast.literal_eval(inpstring)
#       print(retdic)
       if 'url' in retdic:
           url = retdic['url']
       if 'status' in retdic:
           jobstatus = retdic['status']
#       print(url,jobstatus)
    except:
       print("Can not decode string: %s" % inpstring)
       exit()
    return url, jobstatus

def checkasyjob(url):
   finished = False
   while True:
       time.sleep(1.0)
       easResponse = urllib.urlopen(url)
       jobresponse = easResponse.read().decode()
       url, jobstatus = geturl(jobresponse)
       if jobstatus=='FINISHED':
           finished = True
           break
       elif jobstatus=='ERROR':
           break
   return url, finished


def getMetadataXml(base_url, product_type, product_query, project):
  product_query = base_url + product_type + "&" + product_query + "&make_asy=True&PROJECT=" + project
#  print(product_query)
  print("Query submitted at %s" % datetime.datetime.now())
  easResponse = urllib.urlopen(product_query)
  jobresponse = easResponse.read().decode()
  url, jobstatus = geturl(jobresponse)
  url, finished = checkasyjob(url)
  print("Job finished on server side at %s" % datetime.datetime.now())
  easResponse = urllib.urlopen(url)
  productList = easResponse.read().decode()
  if not finished:
      print("Error in executing query")
      print(productList)
      return []
  print("Data products metadata retrieved at %s" % datetime.datetime.now())
#  print(productList)
  # Workaround for the EAS response, when a list of products is provided
  productList = productList.split("\n\n")
  cip = 0
  ret_p = []
  for ip in productList:
      if len(ip.strip())>0:
          if cip > 0:
              ip = '<?xml version="1.0" encoding="UTF-8"?>\n'+ip
          ret_p.append(ip)
          cip=cip+1
#  productList = productList.replace('<?xml version="1.0" encoding="UTF-8"?>', '<?xml version="1.0" encoding="UTF-8"?><dummyRoot>') + "</dummyRoot>"  
#  root_elem = etree.fromstring(productList)
  print("Found %d data products" % cip)
  return ret_p


def downloadDssFile(base_url, fname, username= None, password = None, datadir='data'):
  headers = {}
  if username and password:
      headers['Authorization'] = 'Basic %s' % (base64.b64encode(b"%s:%s" % (username.encode('utf-8'), password.encode('utf-8'))).decode('utf-8'))
  headers['pragma']='DSSGET'
  fileurl = base_url + fname
  if sslcontext:
      conn = HTTPSConnection(BASE_DSS_HOST, BASE_DSS_PORT, context=sslcontext)
  else:
      conn = HTTPSConnection(BASE_DSS_HOST, BASE_DSS_PORT)
  conn.request('GET', '/'+fname, headers=headers)
  response = conn.getresponse()
  recvheader = {}
  for k, v in dict(response.getheaders()).items():
      recvheader[k.lower()] = v
#  response = requests.get(fileurl, auth=(username, password))
  if not os.path.isdir(datadir):
      os.makedirs(datadir)
  if response.status == 200:
      if check_content_length(recvheader):
          try:
              total_length = get_content_length(recvheader)
              with open(os.path.join(datadir, fname), "wb") as f:
                  if total_length is None:
                      f.write(response.content)
                  else:
                      dl = 0.0
                      dlc = 0.0
                      data = response.read(buffer_size)
                      while data:
                          dlc = len(data)
                          dl = dl + dlc
                          f.write(data)
                          done = int(50.0*dl/total_length)
                          sys.stdout.write("\r[%s%s]" % ('=' * done, ' ' * (50-done)))
                          sys.stdout.flush()
#            print(total_length,dlc,dl)
                          data = response.read(buffer_size)
                      if dl < total_length:
                           sys.stdout.write("Wrong size for file %s - need %d, got %d\n" % (fname, total_length, dl))
                  sys.stdout.write("\n")
          except Exception as e:
              print("Can't write file %s - error %s" % (fname, str(e)))
  elif response.status == 403:
      sys.stdout.write("Wrong username or password supplied, exiting\n")
      conn.close()
      exit()
  elif response.status == 404:
      reason = ''
      if hasattr(response,'reason'):
          reason = response.reason
      out_message = 'File %s not found: %s\n' % (fname, reason)
      sys.stdout.write(out_message)
  else:
      reason = ''
      if hasattr(response,'reason'):
          reason = response.reason
      out_message = 'File %s can not be downloaded: %s\n' % (fname, reason)
      sys.stdout.write(out_message)
  conn.close()
  del conn


def saveMetaAndData(products, username=None, password=None, datadir='data'):
  for p in products:
    #findProductId = etree.XPath("//ProductId")
    #findFiles = etree.XPath("//FileName")

    root = etree.XML(p)
    ptype = root.find(".//ProductType").text
    pid = root.find(".//ProductId").text
    pfile = ptype[0].upper() + ptype[1:] + '__' + pid + ".xml"
    print("Saving " + pid)
    with open(pfile,'w') as f:
      f.write(p)

    files = [f.text for f in root.findall(".//FileName")]
    for f in files:
      if os.path.isfile(f):
        print("File %s already exists locally. Skipping its download" % (f))
      else:
        print("Start retrieving of " + f + " at " +str(datetime.datetime.now())+" :")
        downloadDssFile(BASE_DSS_URL, f, username, password, datadir=datadir)
        print("Finished retrieving of " + f + " at " +str(datetime.datetime.now()))


if __name__ == '__main__':

    FIELD_ID = ['52926','53401','53402','53403','53876','53877','53878','54348','54349']

    parser = argparse.ArgumentParser()
    parser.add_argument('--username', help='Cosmos or EAS username', required=True)
    parser.add_argument('--password', help='user password', required=True)
    parser.add_argument('--project', help='EAS project to query', default='EUCLID')
    parser.add_argument('--data_product', help='Data product type name, e.g. DpdMerFinalCatalog', required=True)
    parser.add_argument('--query', required=True, help='Product query string, e.g. \n'
                                    'Header.ProductId.ObjectId=like*EUC_MER_PPO-TILE*_SC3-PLAN-2-PPO-*-SDC-IT-RUN0-0-final_catalog-0')

    args = parser.parse_args()

    username = args.username
    password = args.password

    if password and os.path.isfile(password):
        with open(password) as f:
            password = f.read().replace("\n", "").strip()

    if username and not password:
        import getpass
        password = getpass.getpass('Type password for %s: ' % username)

    products = getMetadataXml(BASE_EAS_URL, args.data_product, args.query, args.project)

    saveMetaAndData(products, username, password)


