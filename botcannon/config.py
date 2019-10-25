from datetime import datetime

from walrus import Model
from walrus import Database
from walrus import TextField
from walrus import HashField
from walrus import PickledField


# from walrus import DateField
# from walrus import SetField
# from walrus import BooleanField


class ConfigManager:
    def __init__(self, namespace, db: Database):
        self.name = namespace
        services = Services
        params = Params

        for item in [services, params]:
            item.__database__ = db
            item.__namespace__ = namespace

        self.service = ServiceManager(services)
        self.params = ParameterManager(params)


class Services(Model):
    name = TextField(primary_key=True)
    _keys = ['shell', 's_entry', 's_conf', 'collector', 'c_entry', 'c_conf', 'tasks']

    collector = TextField()
    c_entry = TextField()
    c_conf = TextField()

    shell = TextField()
    s_entry = TextField()
    s_conf = TextField()

    tasks = PickledField()


class Params(Model):
    name = TextField(primary_key=True)
    data = HashField()  # {'key': 'value, 'api_key': KEY}


class BaseFunctions:
    def __init__(self, model: Model):
        self._model = model

    def all(self):
        return self._model.all()

    def get(self, name):
        try:
            return self._model.get(self._model.name == name)
        except ValueError:
            return None

    def add(self, name):
        return self._model.create(name=name)

    def rm(self, name):
        item = self.get(name)
        if item:
            self._model.delete(item)
            print('Item removed')
        else:
            print('Item doesnt exist')


class ServiceManager(BaseFunctions):

    def __init__(self, services: type(Services)):
        super().__init__(services)

    def define(self, service_name, shell, s_entry, s_conf, collector, c_entry, c_conf):
        service = self.get(service_name)
        if not service:
            service = self.add(service_name)

        service.shell, service.s_entry, service.s_conf = shell, s_entry, s_conf
        service.collector, service.c_entry, service.c_conf = collector, c_entry, c_conf
        service.save()
        return service


class ParameterManager(BaseFunctions):
    def __init__(self, configs: type(Params)):
        super().__init__(configs)

    def kv(self, name, **kwargs):
        conf = self.get(name)
        if conf:
            if not kwargs:
                return f'No args!'
            for k in kwargs:
                conf.data[k] = kwargs[k]
            return f'OK'
        else:
            return f'Key missing! Try: add {name}'

    def kv_rm(self, name, key):
        conf = self.get(name)
        if conf:
            del conf.data[key]
            return 'Deleted.'
        else:
            return 'Create this service first.'

    def keys(self, name):
        conf = self.get(name)
        if conf:
            return [i for i in conf.data]
        else:
            return 'Create this service first.'

    def paste(self, name, *args):
        import getpass

        conf = self.get(name)
        if conf:
            if not args:
                print(f'No args!')
            for k in args:
                pswd = getpass.getpass(f'{name}->{k} :')
                conf.data[k] = pswd
                print(f'OK\n')
        else:
            return f'Key missing! Try `add {name}`'
