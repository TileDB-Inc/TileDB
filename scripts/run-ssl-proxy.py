#!/usr/bin/env python3

import argparse as ap
import os
import queue
import select
import socket
import ssl
import sys
import threading

NUM_THREADS = os.cpu_count()

def log(*args):
  print(*args, flush=True)

class Session(object):
  def __init__(self, sock, addr, dst_port):
    self.src_sock = sock
    self.addr = addr
    self.dst_port = dst_port
    self.dst_sock = None

  def close(self):
    try:
      self.src_sock.close()
    except:
      pass
    try:
      self.dst_sock.close()
    except:
      pass

  def forward(self, from_sock, to_sock):
    data = from_sock.recv(1024)
    if not data:
      self.close()
      return False
    to_sock.sendall(data)
    return True

  def run(self):
    self.dst_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.dst_sock.connect(('127.0.0.1', self.dst_port))
    sockets = [self.src_sock, self.dst_sock]
    while True:
      (rlist, wlist, xlist) = select.select(sockets, [], sockets)
      if xlist:
        self.close()
        return
      for r in rlist:
        if r == self.src_sock:
          if not self.forward(self.src_sock, self.dst_sock):
            return
        elif r == self.dst_sock:
          if not self.forward(self.dst_sock, self.src_sock):
            return
        else:
          self.close()
          raise RuntimeError("Unknown socket: {}".format(r))

def handle_clients(conn_queue):
  while True:
    session = conn_queue.get()
    if session is None:
      return
    try:
      session.run()
    except Exception as e:
      log("Error handling client: {}".format(e))
    finally:
      log("Client disconnected: {}".format(session.addr))

def run_proxy(cfg):
  ssl_cert = cfg.public_certificate
  ssl_key = cfg.private_key
  if not os.path.isfile(ssl_cert):
    log("Missing public certificate: {}".format(ssl_cert))
    exit(3)
  if not os.path.isfile(ssl_key):
    log("Missing private key: {}".format(ssl_key))
    exit(3)
  ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
  ssl_ctx.load_cert_chain(ssl_cert, ssl_key)

  conn_queue = queue.Queue()

  threads = []
  for _ in range(NUM_THREADS):
    t = threading.Thread(target=handle_clients, args=(conn_queue,))
    t.daemon = True
    t.start()
    threads.append(t)

  addr = ("None", 0)
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0) as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(('127.0.0.1', cfg.source_port))
    listener.listen(1024)
    msg = "SSL Proxy Server listening at https://127.0.0.1:{}"
    log(msg.format(cfg.source_port))
    with ssl_ctx.wrap_socket(listener, server_side=True) as ssock:
      while True:
        try:
          conn, addr = ssock.accept()
          log("Client connected: {}".format(addr))
          conn_queue.put(Session(conn, addr, cfg.target_port))
        except Exception as e:
          log("Error creationg session for {} : {}".format(addr, e))

def parse_args():
  parser = ap.ArgumentParser(
      prog = "run-ssl-proxy.py",
      description = "A simple SSL Proxy - Not for Production Use"
  )
  parser.add_argument("-s", "--source-port", type=int,
      help = "Source port on which to accept connections")
  parser.add_argument("-d", "--target-port", type=int,
      help = "Target port to proxy connections to")
  parser.add_argument("-c", "--public-certificate",
      help = "The server public certificate to use")
  parser.add_argument("-k", "--private-key",
      help = "The server private key to use")
  args = parser.parse_args()

  if args.source_port is None:
    log("Missing source port")
    exit(1)
  if args.target_port is None:
    log("Missing target port")
    exit(1)
  if args.public_certificate is None:
    log("Missing public certificate")
    exit(1)
  if args.private_key is None:
    log("Missing private key")
    exit(1)

  return args

def main():
  log("SSL Proxy Initializing...")
  try:
    run_proxy(parse_args())
  except KeyboardInterrupt:
    pass
  finally:
    log("SSL Proxy Shutting Down")

if __name__ == "__main__":
  main()
