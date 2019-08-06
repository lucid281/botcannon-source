from .models import *


class ModelWithName:
    def __init__(self, model: Model):
        self._model = model

    def all(self):
        return self._model.all()

    def get(self, name):
        try:
            return self._model.get(self._model.name == name)
        except ValueError:
            return None
        # q = [i for i in self._model.query(self._model.name == query)]
        # r = q.pop() if q else None
        # return r

    def add(self, name):
        return self._model.create(name=name)

    def rm(self, name):
        item = self.get(name)
        if item:
            self._model.delete(item)
            print('Item removed')
        else:
            print('Item doesnt exist')


class UserManager(ModelWithName):
    def __init__(self, users: type(Users)):
        super().__init__(users)

    def disable(self, name):
        a_user = self.get(name)
        if a_user:
            a_user.enabled = False
            print('Ok')
        else:
            print('User doesnt exist')

    def enable(self, name):
        a_user = self.get(name)
        if a_user:
            a_user.enabled = True
            print('Ok')
        else:
            print('User doesnt exist')

    def id_add(self, name, id, value):
        a_user = self.get(name)
        if a_user:
            a_user.ids.update(**{id: value})
            print('Ok')
        else:
            print('User doesnt exist')

    def id_rm(self, name, id):
        a_user = self.get(name)
        if a_user:
            ids = a_user.ids
            if id in ids:
                del ids[id]
                print('Ok')
            else:
                print('ID doesnt exist')
        elif not a_user:
            print('User doesnt exist')

    def id_all(self, name):
        a_user = self.get(name)
        if a_user:
            return a_user.ids.as_dict(True)
        else:
            print('User doesnt exist')

    def group_add(self, name, module):
        a_user = self.get(name)
        if a_user:
            a_user.groups.add(module)
            print('Ok')
        else:
            print('User doesnt exist')

    def group_rm(self, name, group):
        a_user = self.get(name)
        if a_user:
            groups = a_user.groups
            if group in groups:
                del groups[group]
                print('Ok')
            else:
                print('ID doesnt exist')
        elif not a_user:
            print('User doesnt exist')

    def group_all(self, name):
        a_user = self.get(name)
        if a_user:
            return a_user.groups.as_set(True)
        else:
            print('User doesnt exist')


class GroupManager(ModelWithName):
    def __init__(self, groups: type(Groups)):
        super().__init__(groups)

    def perm_add(self, name, module):
        a_user = self.get(name)
        if a_user:
            a_user.groups.add(module)
            print('Ok')
        else:
            print('User doesnt exist')

    def perm_rm(self, name, group):
        a_user = self.get(name)
        if a_user:
            groups = a_user.groups
            if group in groups:
                del groups[group]
                print('Ok')
            else:
                print('ID doesnt exist')
        elif not a_user:
            print('User doesnt exist')

    def perm_all(self, name):
        a_user = self.get(name)
        if a_user:
            return a_user.groups.as_set(True)
        else:
            print('User doesnt exist')


class ServiceManager(ModelWithName):
    def __init__(self, services: type(Services)):
        super().__init__(services)

    def define(self, service_name, plugin, plugin_config, runner, run_config):
        service = self.get(service_name)

        if not service:
            service = self.add(service_name)

        service.plugin = plugin
        # service.entrypoint = entrypoint
        service.entry_config = plugin_config
        service.runner = runner
        service.run_config = run_config
        return service.save()


class ParameterManager(ModelWithName):
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
                pswd = getpass.getpass(f'{k} :')
                conf.data[k] = pswd
                print(f'OK\n')
        else:
            return f'Key missing! Try `add {name}`'
