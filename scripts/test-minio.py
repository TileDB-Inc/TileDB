import http, http.client, ssl, os
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
script_dir = os.path.dirname(__file__)
cert_path = os.path.abspath(os.path.join(script_dir, "../test/inputs/test_certs/public.crt"))
ctx.load_verify_locations(cert_path)

c = http.client.HTTPSConnection("localhost", 9999, context=ctx)
  #, context=ssl._create_unverified_context())

c.request("GET", "/minio/health/live")
r = c.getresponse()
if (r.status != 200):
  raise Exception("minio is not available!")
r.read() # must read before making another request

c.request("GET", "/minio/health/ready")
r = c.getresponse()
if (r.status != 200):
  raise Exception("minio is not ready!")