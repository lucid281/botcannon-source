from walrus.streams import TimeSeriesStream
from walrus.streams import datetime_to_id
from walrus.streams import id_to_datetime
from walrus.streams import Message
from walrus import Database

from redis.exceptions import ResponseError

from time import sleep
import json
import psutil
from multiprocessing import Process, current_process

import botcannon

from .torch import Torch


def run_plugin_with_cmd(item, namespace):
    """Take a list of messages and process them"""

    # _ = [f'Processing {len(item)} items with {current_process().name}:']
    # print(item)
    cannon = botcannon.MainCannon(namespace, Database(unix_socket_path='/run/redis/redis-server.sock'))
    cannon_data = cannon.render.dict(item['service_name'], hardfail=True)
    plugin, params = cannon.render.get_plugin(**cannon_data)
    _init_class = plugin(**params) if params else plugin()
    results = Torch(_init_class, '', item['input_cmd'])
    item['text'] = results
    runner, r_params = cannon.render.get_runner(**cannon_data)
    print(item)
    runner(**r_params).send(**item)
    print()



# def split_workload(_list, div_by):
#     length = len(_list)
#     if div_by >= length:
#         return [_list]
#     return [_list[i * length // div_by: (i + 1) * length // div_by]
#             for i in range(div_by)]


def read_and_process_cg(namespace, service_name, consumer_name, count, block):
    cannon = botcannon.MainCannon(namespace, Database(unix_socket_path='/run/redis/redis-server.sock'))
    ledger = cannon.ledger(service_name, consumer_name)
    proccess_name = current_process().name
    consumer = ledger.ts.consumer(consumer_name)

    while True:
        procs = []
        msgs = consumer.read(count=count, block=block)
        for message in msgs:
            print(message)
            cannon_data = cannon.render.dict(message.data['service_name'], hardfail=True)
            runner, params = cannon.render.get_runner(**cannon_data)
            m = runner.default_parse(message)
            if m:
                proc = Process(name=f'{proccess_name}.{len(procs) + 1}',
                           target=run_plugin_with_cmd,
                           args=(m, namespace))
                procs.append(proc)
                proc.start()
            ledger.cg_streams['inbox'].ack(message.message_id)
        # for proc in procs:
        #     proc.join()


def read_and_process_backlog(namespace, service_name, consumer_name):
    cannon = botcannon.MainCannon(namespace, Database(unix_socket_path='/run/redis/redis-server.sock'))
    ledger = CannonLedger(
        cannon,
        service_name,
        consumer_name, )

    def claim_pending(count=4):
        backlogged_cg = ledger.get_pending(count)
        for stream in backlogged_cg:
            cg = ledger._get_cg(stream)
            for i in backlogged_cg[stream]:
                ts, seq, group, age, delivered = ledger.parse_pending(i)
                msg = cg.claim(datetime_to_id(ts, seq))
                yield msg

    procs = []
    proccess_name = current_process().name
    for msg in claim_pending():
        print(msg)
        proc = Process(name=f'{proccess_name}.{len(procs) + 1}',
                       target=run_plugin_with_cmd,
                       args=(msg, namespace),
                       )
        proc.start()

    for proc in procs:
        proc.join()


class CannonLedger:
    def __init__(self, cannon, service_name, consumer_name):
        """
        Tools for working with streams based on a service name and schema, and a consumer name.

        A stream is time-series key:value data pairs.
        Consumer groups are a ledger of work against that stream, or many streams

        The consumer group allows us to record state of messages read in,
        an explict ack removes the message from the consumer group's pending list.

        :param cannon: instance of Cannon
        :param service_name: name of cannon service
        :param consumer_name: name of CG 'ledger'
        """
        self.cannon = cannon
        info = self.cannon.render.dict(service_name)
        True if info else exit(1)
        self.db = self.cannon.db
        self.consumer_name = consumer_name
        self.service_name = service_name

        # prepare streams and consumer groups
        self.channels = ['logging', 'data', 'inbox']
        self.stream_keys = [f'{self.cannon.name}|streams:{self.service_name}:{i}' for i in self.channels]
        self.ts = self.db.time_series(self.consumer_name, self.stream_keys)
        self.cg_streams = {}
        for cgkey in self.channels:
            self.cg_streams[cgkey] = self._get_cg(cgkey)
        self.init_cursor()

    def _get_cg(self, key: str):
        real_key = f'{self.cannon.name}|streams:{self.service_name}:{key}'
        attr_lookup = real_key\
            .replace(":", "_")\
            .replace("-", "_")\
            .replace("|", "_")\
            .lower()

        # TODO: there is a limit to the self.ts and lookup to to the replace, need to find a better way around this:
        # 'cannondb_streams_botcannon_demo_logging', manually create streams objects?

        if attr_lookup in self.ts.__dict__ and isinstance(self.ts.__dict__[attr_lookup], TimeSeriesStream):
            return self.ts.__dict__[attr_lookup]
        else:
            print(f'Lookup of channel failed, bad char in name?: {self.channels}')
            return None

    def init_cursor(self, force=False, cursor='$'):
        """
        set_id resets the your position and thus what is known as ack'd.
        This means anything 'reading' your CG with be updated with a new set of non-ackd messages
        You could also use this as a rewind feature.

        :param force: force set a new id.
        :param cursor: $ for 'from now on' or a a point in time historically.
        """

        # create something more digestible for our us
        groups = {}
        for key in self.stream_keys:
            try:
                q = self.db.xinfo_groups(key)
            except ResponseError:
                q = None
            groups[key] = {i['name'].decode(): i for i in q} if q else {}

        # check for existing consumer groups
        test = [groups[i] for i in groups if self.consumer_name in groups[i]]

        # create the streams and set our 'cursor' if test is empty
        if force or not test:
            self.ts.create()
            self.ts.set_id(cursor)
            return True  # new/change
        # otherwise the consumer groups is ready
        else:
            return False  # no change

    def write(self, channel: str, data: dict, status: str = 'OK', ts: str = ''):
        """
        :param channel: channel key ref to item in self.channels
        :param run_type: slack, rocket, etc (router to use)
        :param data: data to be jsonified
        :param status: for communicating failures
        :param ts: insert message at this time
        """
        if channel in self.cg_streams:
            cg = self.cg_streams[channel]
            p = {'service_name': self.service_name,
                 'data': json.dumps(data),
                 'status': status}
            cg.add(p, id=ts) if ts else cg.add(p)
        else:
            print('No consumer group to write to, exiting.')
            exit(1)


    def all_cgs(self):
        a = []
        for i in self.ts.__dict__:
            if isinstance(self.ts.__dict__[i], TimeSeriesStream):
                a.append(self.ts.__dict__[i])
        return a

    def watch(self):
        """Watch all streams"""
        # self.ts.set_id('$')  # Do not read the dummy items.
        while True:
            for m in self.ts.read(block=0, count=1):
                # Message type from Walrus
                # m.[data, message_id, sequence, stream, timestamp]
                d = m.data
                print(f'{m.stream} → {m.message_id} # {d["data"]}')

    def test_worker(self, worker, tasks):
        """Watch a stream in the context of the consumer group"""
        # if not key in self.cg_streams:
        #     return False
        # s = self.cg_streams[key]
        consumer_name = f'{self.consumer_name}.{worker}'
        consumer = self.ts.consumer(consumer_name)

        while True:
            r = consumer.read(block=100, count=tasks)
            for m in r:
                # Message type from Walrus
                # m.[data, message_id, sequence, stream, timestamp]
                d = m.data
                print(f'\n{m.stream} → {m.message_id} # {d["data"]}')
                self.ack(m.stream, m.message_id)
            sleep(2)
            self.db.set(f'{self.service_name}:ping:{consumer_name}', len(r), ex=5)
            print('.', end='')

    def worker(self, backlogged=False):
        import multiprocessing as mp

        mp.set_start_method('fork')
        procs = []

        if backlogged:
            proc = mp.Process(name=f'{self.consumer_name}.backlog',
                              target=read_and_process_backlog,
                              args=(self.cannon.name,
                                    self.service_name,
                                    self.consumer_name,
                                    ),
                              )
            procs.append(proc)
            proc.start()

        for worker in ['a', 'b', 'c']:
            proc = mp.Process(name=f'{self.consumer_name}.{worker}',
                              target=read_and_process_cg,
                              args=(self.cannon.name,
                                    self.service_name,
                                    self.consumer_name,
                                    3,
                                    0,
                                    )
                              )
            procs.append(proc)
            proc.start()

        try:
            for proc in procs:
                proc.join()

        except KeyboardInterrupt:
            for proc in procs:
                proc.terminate()

    def pending(self, churn=False):
        print('only printing up to 10 pending messages!')
        for s in self.cg_streams:
            stream = self.cg_streams[s]
            print(s)

            if not stream:
                print(f'  No stream yet!')
                continue

            pending = stream.pending(count=10)
            for i in pending:
                ts, seq, group, age, delivered = self.parse_pending(i)
                print(f'  {datetime_to_id(ts, seq)}', [group, age, delivered], end='')
                ack = self.ack(s, datetime_to_id(ts, seq)) if churn else None
                print(f' - {ack}') if ack else print('')

    def parse_pending(self, item):
        msgts_seq, group, age, delivered = item
        ts, seq = msgts_seq
        return ts, seq, group, age, delivered

    def get_pending(self, count):
        p = {}
        for s in self.cg_streams:
            stream = self.cg_streams[s]

            p[s] = {}
            if not stream:
                continue

            p[s] = [i for i in stream.pending(count=count)]

            # msgts_seq, group, age, delivered = i
            # ts, seq = msgts_seq
            # print(f'  {datetime_to_id(ts, seq)}', [group, age, delivered], end='')
            # ack = self.ack(s, datetime_to_id(ts, seq)) if churn else None
            # print(f' - {ack}') if ack else print('')
        return p

    def info(self):
        """Info about the consumer group and related streams"""
        for key in self.stream_keys:
            print(f'{key}')

            try:
                stream_info = self.db.xinfo_stream(key)
            except TypeError:
                stream_info = None
            print(f'  INFO:') if stream_info else print(f'  INFO:\n    No Data in Stream!')
            if stream_info:
                print('\n'.join([f'    {i}: {stream_info[i]}' for i in stream_info]))

            groups = self.db.xinfo_groups(key)
            print(f'  GROUPS:') if groups else print(f'GROUPS:\n    NONE')
            if groups:
                for group in groups:
                    print('\n'.join([f'    {i}: {group[i]}' for i in group]))
                    print('    ---')
            try:
                consumers = self.db.xinfo_consumers(key, self.consumer_name)
            except ResponseError:
                consumers = None

            print(f'  CONSUMERS:') if consumers else print(f'  CONSUMERS:\n    NONE')
            if consumers:
                for consumer in consumers:
                    print('\n'.join([f'    {i}: {consumer[i]}' for i in consumer]))
                    print('    ---')

            print('')

    def ack(self, stream, *args):
        stripped = stream.replace("-", "_").replace(":", "_").replace("|", "_").lower()
        # stripped = stripped
        if stripped in self.ts.__dict__ and isinstance(self.ts.__dict__[stripped], TimeSeriesStream):
            return [self.ts.__dict__[stripped].ack(i) for i in args]
        else:
            return False

    def claim(self, cg_stream, id):
        """Bring a message back for processing """
        if cg_stream in self.cg_streams:
            s = self.cg_streams[cg_stream]
            return s.claim(id)


def now():
    import datetime
    return datetime.datetime.now()
