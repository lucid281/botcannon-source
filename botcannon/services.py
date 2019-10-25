import importlib.util
from pathlib import Path
import inspect

from walrus.streams import TimeSeriesStream, datetime_to_id
from walrus import Database
from redis.exceptions import ResponseError

from .config import ConfigManager
from .stream_frame import BlindDataFrames


class BotCannonService:
    def __init__(self, service_name, namespace, paths_dict, socket_path, hardfail, consumer_name='CANNON', **kwargs):
        self.db = Database(unix_socket_path=str(socket_path.resolve()))
        self.conf = ConfigManager(namespace, self.db)
        self.ledger = CannonLedger(self, service_name, consumer_name)
        self.paths_dict = paths_dict

        service = self.conf.service.get(service_name)
        if not service:
            print(f'No service named "{service_name}"')
            exit(1) if hardfail else None

        key_test = [i for i in service._data if i in service._keys]
        if not key_test:
            print(f'Service named "{service_name}" missing {key_test}')
            exit(1) if hardfail else None

        # check for run context class
        if service.shell not in self.paths_dict:
            print(f'No python file named {service.shell}.py')
            exit(1) if hardfail else None
        elif service.collector not in self.paths_dict:
            print(f'No python file named {service.collector}.py')
            exit(1) if hardfail else None

        self.service = service
        self.dataframes = BlindDataFrames(self)

    def _get_entry(self, py_file_path: Path, class_in_spec, config_key, checks=True, hardfail=True):
        # hijack python's import system
        name = py_file_path.stem
        plugin = f'{py_file_path.parent}.{name}'
        py_spec = importlib.util.find_spec(plugin)
        module = importlib.util.module_from_spec(py_spec)
        py_spec.loader.exec_module(module)

        # look for our entry point
        if class_in_spec in module.__dict__:
            desired_class = module.__dict__[class_in_spec]
        else:
            print(f'Entrypoint, {class_in_spec}, not found in {py_spec}')
            return False

        params = {}
        kwargs_for_class = self.conf.params.get(config_key)
        if not kwargs_for_class:
            print(f'parameter entry  "{config_key}" not found.\n'
                  f'    Create with "conf params add {config_key}"')
            return exit(1) if hardfail else None

        if checks:
            # return self._check_class_parameters(desired_class, kwargs_for_class)
            service_sig = inspect.signature(desired_class.__init__).parameters

            failed = [i for i in service_sig if (i not in kwargs_for_class.data and i is not 'self')]
            defaults = [params.setdefault(i, service_sig.get(i).default) for i in failed if service_sig.get(i).default is not service_sig.get(i).empty]
            if failed and not defaults:
                print(f'Parameter object is missing key(s):\n'
                      f'    Create with "su params paste {kwargs_for_class.name} ', end='')
                print(' '.join(i for i in failed), end='"\n')
                exit(1)
            else:
                passed = [i for i in service_sig if i in kwargs_for_class.data]
                [params.setdefault(i, kwargs_for_class.data[i].decode()) for i in passed]
            return desired_class, params
        else:
            [params.setdefault(i, kwargs_for_class.data[i].decode()) for i in kwargs_for_class.data]
            return desired_class, params

    def get_shell(self):
        return self._get_entry(self.paths_dict[self.service.shell], self.service.s_entry, self.service.s_conf)

    def get_collector(self):
        return self._get_entry(self.paths_dict[self.service.collector], self.service.c_entry, self.service.c_conf)

    def get_tasks(self):
        t = self.service.tasks
        d = {}
        for i in t:
            d[i] = self._get_entry(self.paths_dict[t[i]["lib"]], t[i]["entry"], t[i]["conf"]["key"])
            # self.service.tasks[i]
        return d


class CannonLedger:
    def __init__(self, service: BotCannonService, service_name, consumer_name):
        self.s = service
        self.service_name = service_name
        self.consumer_name = consumer_name

        # prepare streams and consumer groups
        self.channel_keys = ['log', 'data', 'taskback']
        self.stream_keys = [f'{self.s.conf.name}|streams:{self.service_name}:{i}' for i in self.channel_keys]
        self.ts = self.s.db.time_series(self.consumer_name, self.stream_keys)
        self.channels = {}
        for cgkey in self.channel_keys:
            self.channels[cgkey] = self._get_cg(cgkey)
        self.init_cursor()

    def _get_cg(self, key: str):
        real_key = f'{self.s.conf.name}|streams:{self.service_name}:{key}'
        attr_lookup = real_key \
            .replace(":", "_") \
            .replace("-", "_") \
            .replace("|", "_") \
            .lower()

        # TODO: there is a limit to the self.ts and lookup to to the replace, need to find a better way around this:
        # 'cannondb_streams_botcannon_demo_logging', manually create streams objects?

        if attr_lookup in self.ts.__dict__ and isinstance(self.ts.__dict__[attr_lookup], TimeSeriesStream):
            return self.ts.__dict__[attr_lookup]
        else:
            print(f'Lookup of channel failed, bad char in name?: {self.channel_keys}')
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
                q = self.s.db.xinfo_groups(key)
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

    def write(self, channel, data: dict, ts: str = ''):
        """
        Write out dict to stream as k:v pairs.
        :param channel: channel key ref to item in self.channels
        :param run_type: slack, rocket, etc (router to use)
        :param data: data to be jsonified
        :param status: for communicating failures
        :param ts: insert message at this time
        """
        if channel in self.channels:
            self.channels[channel].add(data, id=ts) if ts else self.channels[channel].add(data)
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

    def pending(self, churn=False):
        print('only printing up to 10 pending messages!')
        for s in self.channels:
            stream = self.channels[s]
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
        for s in self.channels:
            stream = self.channels[s]

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
                stream_info = self.s.db.xinfo_stream(key)
            except TypeError:
                stream_info = None
            print(f'  INFO:') if stream_info else print(f'  INFO:\n    No Data in Stream!')
            if stream_info:
                print('\n'.join([f'    {i}: {stream_info[i]}' for i in stream_info]))

            groups = self.s.db.xinfo_groups(key)
            print(f'  GROUPS:') if groups else print(f'GROUPS:\n    NONE')
            if groups:
                for group in groups:
                    print('\n'.join([f'    {i}: {group[i]}' for i in group]))
                    print('    ---')
            try:
                consumers = self.s.db.xinfo_consumers(key, self.consumer_name)
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
        if cg_stream in self.channels:
            s = self.channels[cg_stream]
            return s.claim(id)
