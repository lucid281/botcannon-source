#!/usr/bin/env -S python3
import inspect
from pathlib import Path
from getpass import getpass
import datetime
from time import sleep
import multiprocessing as mp

from walrus import Database
from ruamel.yaml import YAML

from .config import ConfigManager
from .services import BotCannonService
from .models import Collector

class BotCannon:
    """"""
    def __init__(self, namespace='DEFAULT', lib_dir='./app'):
        self._system_path = Path('/run/redis/redis-server.sock')  # or system
        self._relative_path = Path('.') / 'sockets/redis-server.sock'
        socket_path = self._system_path if self._system_path.exists() else None
        if self._relative_path.exists():
            socket_path = self._relative_path
        if not socket_path:
            print(f'No redis socket in {self._system_path} OR {self._relative_path}.')
            exit(1)
        self._socket_path = socket_path
        self._namespace = namespace
        self._lib_dir = lib_dir
        self._service_kwargs = {}
        self._service = None

    def shell(self, service_name):
        """Call a ready-to-go shell as defined by your service."""
        self.init(service_name)
        shell, shell_params = self._service.get_shell()
        return shell(**shell_params) if shell_params else shell()

    def collector(self, service_name):
        """Call a ready-to-go collector as defined by your service."""
        self.init(service_name)
        collector, collector_params = self._service.get_collector()
        return collector(**collector_params) if collector_params else collector()

    def service(self, service_name: object) -> BotCannonService:
        """For working with a service more intimately."""
        self.init(service_name)
        return self._service

    def init(self, service_name):
        """To delay connections to redis"""
        from importlib import reload
        paths_dict = {}

        for py_file in Path(self._lib_dir).iterdir():
            if py_file.is_file() and py_file.suffix == '.py':
                paths_dict[py_file.stem] = py_file

        self._service_kwargs = {
            'namespace': self._namespace,
            'paths_dict': paths_dict,
            'socket_path': self._socket_path,
            'lib_dir': self._lib_dir,
            'hardfail': True,
        }

        self._service = BotCannonService(service_name, **self._service_kwargs)
        return self._service

    def up(self, service_name, lazy=False):
        from .multiprocess import ManageWorkers

        s = self.init(service_name)
        q = mp.Queue(maxsize=2)
        p = ManageWorkers(s.ledger.service_name, q, 60)

        print("Initializing collector process")
        collector, collector_params = s.get_collector()
        c = collector(**collector_params) if collector_params else collector()

        try:
            while True:
                t = True
                for data in c.read():
                    if data:
                        if not lazy:
                            q.put('hot')
                            t = False
                        s.ledger.write('data', data)

                for taskback in s.ledger.channels["taskback"].read(block=None):
                    c.taskback(taskback)
                    s.ledger.channels["taskback"].ack(taskback.message_id)
                    t = False

                if t:
                    q.put('loop_done')
                    sleep(0.005)

        except KeyboardInterrupt:
            print("Caught KeyboardInterrupt, terminating consumer")
            q.put('killjobs')

        finally:
            q.put(None)
            q.close()
            p.join()
            p.terminate()

    def yml(self, file_name, overwrite=True):

        y = YAML()
        compose = y.load(Path(file_name))

        if not 'services' in compose:
            print('No "services" key at top level of ! Exiting!')
            exit(1)

        conf = ConfigManager(
            namespace=self._namespace,
            db=Database(
                unix_socket_path=str(
                    self._socket_path.resolve())))

        root = compose['services']

        service_function_keys = ['collector', 'shell']
        service_config_keys = ['file', 'entry', 'conf']

        if overwrite:
            print(f'Overwriting existing configs.')
            # TODO: Clear configs

        for service_name in root:
            if not isinstance(service_name, str):
                print('service_name is not a string! exiting!')
                exit(1)

            check = conf.service.get(service_name)
            if not check or overwrite:
                print(f'{service_name}:')
                conf.service.add(service_name)

            yml_service = root[service_name]
            missing_service_keys = [i for i in service_function_keys if i not in yml_service]
            if missing_service_keys:
                print(f'  Missing the following keys:  {missing_service_keys}.')
                exit(1)

            missing_task_conf_keys = [
                [x for x in service_config_keys if x not in yml_service[i]]
                for i in service_function_keys
            ]
            if [i for i in missing_task_conf_keys if i]:
                print(f'  Missing the following keys:  {missing_task_conf_keys}.')
                exit(1)

            defined = {
                "service_name": service_name,
                "collector": yml_service["collector"]["file"],
                "c_entry": yml_service["collector"]["entry"],
                "c_conf": yml_service["collector"]["conf"]["key"],
                "shell": yml_service["shell"]["file"],
                "s_entry": yml_service["shell"]["entry"],
                "s_conf": yml_service["shell"]["conf"]["key"],
            }
            service_model = conf.service.define(**defined)

            def get_secret(key):
                # return input(f'  {service_name}->{key}: ')
                return getpass(f'  "{key}": ')

            module_kwargs = {
                "collector": yml_service["collector"]["conf"],
                "shell": yml_service["shell"]["conf"]
            }

            for module in module_kwargs:
                if "key" not in module_kwargs[module]:
                    print(f'Need a "key" entry in {service_name}:{module}:conf')
                    exit(1)
                if "kwargs" not in module_kwargs[module]:
                    if not conf.params.get(module_kwargs[module]["key"]):
                        print(f'No kwargs for {service_name}:{module}:conf found in Redis.\n'
                              f'Specify key+kwargs higher up in yml to reuse key')
                        exit(1)
                    else:
                        continue
                else:
                    print(f'  {module}:', [i for i in module_kwargs[module]["kwargs"]])
                    conf.params.add(module_kwargs[module]["key"])
                    secrets = {i: get_secret(i) for i in module_kwargs[module]["kwargs"]
                               if module_kwargs[module]["kwargs"][i] == "!!SECRET!!"}
                    from_yml = {i: module_kwargs[module]["kwargs"][i] for i in module_kwargs[module]["kwargs"]
                                if i not in secrets}
                    conf.params.kv(module_kwargs[module]["key"], **{**from_yml, **secrets})

            if 'tasks' in yml_service:
                missing_task_conf_keys = [
                    [{i: x} for x in service_config_keys if x not in yml_service["tasks"][i]]
                    for i in yml_service["tasks"]
                ]
                if [i for i in missing_task_conf_keys if i]:
                    print(f'    task is missing following:  {" ".join(str(i) for i in missing_task_conf_keys)}.')
                    exit(1)

                for task_name in yml_service["tasks"]:
                    task = yml_service["tasks"][task_name]
                    conf.params.add(task["conf"]["key"])
                    secrets = {i: get_secret(i) for i in task["conf"]["kwargs"]
                               if task["conf"]["kwargs"][i] == "!!SECRET!!"}
                    from_yml = {i: task["conf"]["kwargs"][i] for i in task["conf"]["kwargs"]
                                if i not in secrets}
                    conf.params.kv(task["conf"]["key"], **{**from_yml, **secrets})

                service_model.tasks = yml_service["tasks"]
            else:
                service_model.tasks = {}
            service_model.save()

    def ls(self):
        # _services = [i for i in self.su.service.all()]
        # if not _services:
        #     return f'No services defined. Run "services define".'
        #
        # b = [''.join(f'{i:<20}' for i in ['name', 'plugin', 'entry_config', 'runner', 'run_config', 'autostart'])]
        # [b.append(f'{s.name:<19} {s.plugin:<19} {s.entry_config:<19} {s.get_collector:<19}'
        #           f' {s.run_config:<19} {s.autostart}') for s in _services]
        #
        # return '\n'.join([str(i) for i in b])
        pass  # this command doesnt work anymore, needs tweaks

    def help(self):
        return inspect.getdoc(self)

