import importlib.util
from pathlib import Path
import inspect
import os

from walrus import Database

from .ledger import CannonLedger
from .torch import Torch


class MainCannon:
    def __init__(self, name, db: Database):
        from .models import Users
        from .models import Groups
        from .models import Services
        from .models import Params

        from .managers import UserManager
        from .managers import GroupManager
        from .managers import ServiceManager
        from .managers import ParameterManager

        self.name = name
        users = Users
        groups = Groups
        services = Services
        params = Params

        self.db = db
        for item in [users, groups, services, params]:
            item.__database__ = self.db
            item.__namespace__ = name

        self.users = UserManager(users)
        self.groups = GroupManager(groups)
        self.service = ServiceManager(services)
        self.params = ParameterManager(params)

        self.render = ServiceRenderer(self)

    def ledger(self, service, consumer_name='default'):
        return CannonLedger(self, service, consumer_name)


class ServiceRenderer:
    def __init__(self, cannon: MainCannon):
        self._db = cannon
        self.plugins = self.populate_py_in_dir(f'bots')
        self.runners = self.populate_py_in_dir(f'runners', in_module=True)

    @staticmethod
    def populate_py_in_dir(path, in_module=False):
        _plugins = {}
        where_am_i = inspect.getframeinfo(inspect.currentframe()).filename
        cannon_dir = os.path.dirname(os.path.abspath(where_am_i))
        paths = Path(cannon_dir) / path if in_module else Path(path)

        for py_file in paths.iterdir():
            if py_file.is_file():
                name = py_file.resolve().stem
                if in_module:
                    plugin = f'botcannon.{path}.{name}'
                else:
                    plugin = f'{path}.{name}'
                spec = importlib.util.find_spec(plugin)
                if spec:
                    _plugins[name] = spec
        return _plugins

    def dict(self, service_name, hardfail=False):
        # gotta start somewhere -- query the db
        service_definition = self._db.service.get(service_name)
        if not service_definition:
            print(f'No service named "{service_name}"')
            return exit(1) if hardfail else ()

        # test for missing keys in returned service config object
        keys_required = ['plugin', 'entrypoint', 'entry_config', 'runner', 'run_config']
        key_test = [i for i in service_definition._data if i in keys_required]
        if not key_test:
            print(f'Service named "{service_name}" missing {key_test}')
            return exit(1) if hardfail else ()

        # see if plugin exists in ./plugins/NAME.py
        # plugins = self.populate_py_in_dir(self.plugins_dir)
        if service_definition.plugin not in self.plugins:
            print(f'No plugin named {service_definition.plugin}.')
            return exit(1) if hardfail else ()
        plugin_spec = self.plugins[service_definition.plugin]

        # get config for entry point
        plugin_config = self._db.params.get(service_definition.entry_config)
        if not plugin_config:
            print(f'parameter entry named {service_definition.entry_config} not found.\n'
                  f'    Create with "su params add {service_definition.entry_config}"')
            return exit(1) if hardfail else ()

        # check for run context class
        # runners = self.populate_py_in_dir(self.runner_dir)
        if service_definition.runner not in self.runners:
            print(f'No runner plugin named {service_definition.runner}.')
            return exit(1) if hardfail else ()
        runner_spec = self.runners[service_definition.runner]

        # get config for run context entry point
        runner_config = self._db.params.get(service_definition.run_config)
        if not runner_config:
            print(f'Config for {service_definition.runner} named {service_definition.run_config} not found.\n'
                  f'    Create with "su params add {service_definition.run_config}"')
            return exit(1) if hardfail else ()

        return {'service_conf': service_definition,
                'plugin_spec': plugin_spec,
                'plugin_config': plugin_config,
                'runner_spec': runner_spec,
                'runner_config': runner_config,
                }

    def get_plugin(self, plugin_spec, plugin_config, **kwargs):
        return self.get_entry(plugin_spec, plugin_config)

    def get_runner(self, runner_spec, runner_config, **kwargs):
        return self.get_entry(runner_spec, runner_config)

    def get_entry(self, inital_py_code, config_hash, checks=True, **kwargs):
        # hijack python's import system
        service = importlib.util.module_from_spec(inital_py_code)
        inital_py_code.loader.exec_module(service)

        def check_class_parameters(cls, config):
            params = {}
            service_sig = inspect.signature(cls.__init__).parameters
            failed = [i for i in service_sig if (i not in config.data and i is not 'self')]
            passed = [i for i in service_sig if i in config.data]
            if failed:
                print(f'Parameter object is missing key(s):\n'
                      f'    Create with "su params paste {config.name} ', end='')
                print(' '.join(i for i in failed), end='"\n')
                exit(1)
            else:
                [params.setdefault(i, config.data[i].decode()) for i in passed]

            return cls, params

        # look for our entry point
        if '_entrypoint_' in service.__dict__:
            entry_name = service._entrypoint_
            service_class = service.__dict__[entry_name]
        else:
            print(f'Entrypoint not found in service')
            return False

        # make sure we can call plugin and finally return it
        if checks:
            return check_class_parameters(service_class, config_hash)
        else:
            return service_class, config_hash
