import inspect
import shlex
import six
import types
import pipes
import traceback


from fire.core import _OneLineResult
from fire.core import _DictAsString

from fire.core import _Fire
from fire.core import helputils


def Torch(component, name, command=None):
    # Get args as a list.
    if isinstance(command, six.string_types):
        args = shlex.split(command)
    elif isinstance(command, (list, tuple)):
        args = command
    else:
        raise ValueError('The command argument must be a string or a sequence of arguments.')

    caller = inspect.stack()[1]
    caller_frame = caller[0]
    caller_globals = caller_frame.f_globals
    caller_locals = caller_frame.f_locals
    context = {}
    context.update(caller_globals)
    context.update(caller_locals)

    buf = []

    try:
        component_trace = _Fire(component, args, context, name)
    except:
        return '\n'.join([i for i in traceback.format_exc().split('\n') if i])

    if component_trace.HasError():
        for help_flag in ['-h', '--help']:
            if help_flag in component_trace.elements[-1].args:
                command = '{cmd} -- --help'.format(cmd=component_trace.GetCommand())
                buf.append(('WARNING: The proper way to show help is {cmd}.\n'
                            'Showing help anyway.\n').format(cmd=pipes.quote(command)))
        buf.append(f'Fire trace:\n{component_trace}\n')
        result = component_trace.GetResult()
        buf.append(helputils.HelpString(result, component_trace, component_trace.verbose))

    elif component_trace.show_trace and component_trace.show_help:
        buf.append('Fire trace:\n{trace}\n'.format(trace=component_trace))
        result = component_trace.GetResult()
        buf.append(helputils.HelpString(result, component_trace, component_trace.verbose))

    elif component_trace.show_trace:
        buf.append('Fire trace:\n{trace}'.format(trace=component_trace))

    elif component_trace.show_help:
        result = component_trace.GetResult()
        buf.append(helputils.HelpString(result, component_trace, component_trace.verbose))

    else:
        result = component_trace.GetResult()
        if isinstance(result, (list, set, types.GeneratorType)):
            for i in result:
                buf.append(_OneLineResult(i))
        elif inspect.isgeneratorfunction(result):
            raise NotImplementedError
        elif isinstance(result, dict):
            buf.append(_DictAsString(result, component_trace.verbose))
        elif isinstance(result, tuple):
            buf.append(_OneLineResult(result))
        elif isinstance(result, (bool, six.string_types, six.integer_types, float, complex)):
            buf.append(str(result))
        elif result is not None:
            buf.append(helputils.HelpString(result, component_trace, component_trace.verbose))

    return ''.join(buf)
