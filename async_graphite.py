import asyncio, pickle, struct, time, logging, traceback
import time



class CarbonEmitter(object):

    def __init__(self, carbon_address, loop = None, decimal_resolution = 6):
        """
            Instantiate asyncio Carbon emitter.

            Parameters
            ----------
            carbon_address: tuple(string,int)
                Tuple containg the address and port of the Carbon server.
            loop: asyncio.AbstractEventLoop
                If None asyncio.get_event_loop() will be used.
            decimal_resolution:
                Carbon discards metrics when the timestamp is produced by Python 3.
                Number of digits to round timestamp floats produced by time.time().
                Defaults to 6 (Python 2). Set to None to skip fixing timestamps.
        """

        if loop:
            self.loop = loop
        else:
            self.loop = asyncio.get_event_loop()

        self.fix_ts = decimal_resolution
        self.run = False
        self.carbon_address = carbon_address
      
    def start(self):
        self.run = True
        self.loop.run_until_complete(self._connect()) 

    def stop(self):
        logging.debug("CarbonEmitter finishing up")
        self.run = False
        self.writer.close()
        # could add wait_closed for python 3.7
 
    # data is a list of tuples like [(path, (timestamp,value)),....]
    def add(self, data):
        asyncio.ensure_future(self.send_multiple(data),loop=self.loop)

    # returns the future
    def add_metric(self,path,value,timestamp = None):
        # weird carbon does not seem to accept timestamp floats longer than 6 dd, maybe a pickle issue, same fix_imports is either.
        if not timestamp:
            timestamp = time.time()

        if self.fix_ts:
            timestamp = round(timestamp,self.fix_ts)

        return asyncio.ensure_future(self.send([(path,(timestamp,value))]),loop=self.loop)


    # does not fix the timestamps
    async def send_multiple(self,data):
            if self.fix_ts:
                d = []
                for i in data:
                    d.append((i[0],(round(i[1][0],self.fix_ts),i[1][1])))
             
                await self.send(d)
            else:
                await self.send(data)

    async def send(self,data):
        if not data:
            return
        while self.run:
            try:
                logging.debug("CarbonEmitter:send {0}".format(data))
                payload = pickle.dumps(data, protocol=2, fix_imports=True)
                header  = struct.pack("!L", len(payload))
                self.writer.write(header + payload)
                await self.writer.drain()
                break
            except:
                logging.debug("CarbonEmitter send suppressed: {}".format(traceback.format_exc()))
                await self._reconnect()
                await asyncio.sleep(10,loop=self.loop)
        logging.debug("CarbonEmitter:send finished")

    async def _reconnect(self):
        try:
            self.writer.close()
        except:
            logging.debug("CarbonEmitter reconnect suppressed: {}".format(traceback.format_exc()))
        finally:
            await self._connect()

    async def _connect(self):
        try:
            self.reader,self.writer = await asyncio.open_connection(self.carbon_address[0],self.carbon_address[1],loop=self.loop)
        except:
            logging.debug("CarbonEmitter connect suppressed: {}".format(traceback.format_exc()))


if __name__ == '__main__':

    logging.getLogger().setLevel(logging.DEBUG)
    c = CarbonEmitter(('carbon.example.com', 2004))
    c.start()
    c.loop.run_until_complete(c.add_metric("test.test","9"))
    c.stop()
    c.loop.stop()
    
