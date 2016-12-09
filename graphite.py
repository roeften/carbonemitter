import gevent, pickle, struct, time, logging, traceback
import gevent.queue
import gevent.event
import gevent.socket

class CarbonEmitter(object):

    def __init__(self, carbon_address):
        self.data = gevent.queue.Queue()
        self.exit_event = gevent.event.Event()
        self.carbon_address = carbon_address
      
    def start(self):
        self._senderlet = gevent.spawn(self.sender_loop)

    def stop(self):
        logging.debug("CarbonEmitter finishing up")
        self.data.put(SystemExit)

    # data is a list of tuples like [(path, (timestamp,value)),....]
    def add(self, data):
        for i in data:
            self.data.put(i)

    def add_metric(self,path,value,timestamp = time.time()):
        self.data.put((path,(timestamp,value)))

    def send(self):
        data = []
        exit = False
        try:
            while self.data.qsize():
                i = self.data.get_nowait()
                if i == SystemExit:
                    exit = True
                else:
                    data.append(i)

            if exit:
                self.data.put(SystemExit)

        except gevent.queue.Empty:
            pass
        finally:
            try:
                if data:
                    payload = pickle.dumps(data, protocol=2)
                    header  = struct.pack("!L", len(payload))
                    self.socket.sendall(header + payload)
            except:
                logging.debug("CarbonEmitter send suppressed: {}".format(traceback.format_exc()))
                self._reconnect()

    def _reconnect(self):
        try:
            self.socket.close()
            self._connect()
        except:
            logging.debug("CarbonEmitter reconnect suppressed: {}".format(traceback.format_exc()))

    def _connect(self):
        try:
            self.socket = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_STREAM)
            ip = gevent.socket.gethostbyname(self.carbon_address[0])
            self.socket.connect((ip,self.carbon_address[1]))
        except:
            logging.debug("CarbonEmitter connect suppressed: {}".format(traceback.format_exc()))

    def sender_loop(self):
        logging.debug("CarbonEmitter sender loop starting")
        self._connect()
        while True:
            d = self.data.peek()
            if d == SystemExit:
                break
            self.send()
        logging.debug("CarbonEmitter sender loop finishing up")
        self.socket.close()


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    c = CarbonEmitter(('century.homersinn.net', 2004))
    c.start()
    c.add_metric("enlight.controllers.pool_pumps","1")
    c.stop()
    gevent.wait()
