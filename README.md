# botcannon-code
A framework for creating services backed by Redis streams. The original design idea was for chat bots, but it has evolved into a very flexible execution platform.

> this repo is not usable as-is for running botcannon, see main project template: 

version: 0.2.0

## Concepts
messages are collected and placed into a stream where a minimum of 1 worker is available at all times.  work for each message is executed in it's own process. 

normal operation creates a 'hot' worker process along with a new message to ensure either the 'idle' or 'hot' worker processes pick up the message quickly. an optional 'lazy' flag waits for the previous process/task to complete before creating a new 'idle' worker process. 

### vocab
#### service
a botcannon service contains a `collector`, a `shell` and `tasks` (optional). together these collectively form a chat bot, generic data collector, scheduler, listener, etc.

#### collector
message data has to originate from somewhere. collector class uses a `.read()` method that yields dicts. (chat clients or other data generation)

#### shell
your code to interact with collected data. can become a chatbot's root (see `torch`) for process exec or a service menu for a specific collector. 

#### tasks
the default action with no tasks places a message in the stream and moves on. tasks are methods for the worker processes to execute on the message.  
 
these optional modifiers/actions are initialized at startup for the workers (be mindful of connections).`TASKCLASS.task(message, results)` is called for each task in order as listed in your yml config. where `message` is the original message hash and `results` is dict of values that can be over written by the subsequent tasks.

**torch**
> is a special task that runs an input command against your `shell` using `fire`. see examples for more.


### taskback
similar to a callback, a taskback message allows a worker process to relay a message back to the original process. good for gaining access to the session/process of the collector. 


# setup

botcannon requires an `./app/` dir for the botcannon python files. botcannon loads files in this folder similarly to the python `import` builtin.

yml config is easy
```yaml
services:
  SERVICE_NAME:
    collector:
      COLLECTOR_SPEC
    shell:
      SHELL_SPEC
    tasks: 
      - TASK_SPECS1
      - TASK_SPECS2
```

with `SERVICE_NAME` being a name of your choosing and `TASK_SPECS` being a list of specs.
. you will refer to you `SERVICE_NAME` later when starting the service.

### class config spec
`COLLECTOR_SPEC`, `SHELL_SPEC` and individual `TASK_SPECS`:
```yaml
file: filename
entry: ClassNameInFile
conf:
    key: config_name
    kwargs:
      firstkwarg: value1
      secondkwarg: value2
```

`file` is the python filename minus `.py` extension. `entry` is the class to be used as an entrypoint into the script.

`conf:key` is the key name that will be used in redis for the `kwargs`. if you declared `config_name` at the top of your yml config, `config_name` could be used without specifying the kwargs.

botcannon takes your `entry`, `kwargs` and applies them like so:
  
 `ClassNameInFile(**kwargs)`
 
 or
 
 `ClassNameInFile(firstkwarg=value1, secondkwarg=value2`

now, depending on if the spec is a `collector` or `task`, there are specific implementations required for your `ClassNameInFile` in order for botcannon to use them.

### collector design

`collector.read()` is called in a loop repeatedly and should be a method that yields items in an efficient, timely manner. the collector is single threaded and the state of the collector is reflected in the workers when using `./botcannon up SERVICE`. if the collector crashes, workers will be terminated too (there can be no work with no data coming in).

items yielded must be a dict with only strings for values and are written to the service's stream.

the message schema from your collector is up to you, generally speaking try to mimic slack/rocketchat for new chat clients. if you need to, you can encode/decode some json to make bulk data available to workers.

if there are tasks for the service and those tasks have results, the worker will write to a taskback stream and the original collector process will pick up up that message, allowing actions to be called back to the original collector process, calling `collector.taskback(message)` after completing a collection loop, if a message is present.

## shell design
a `shell` has no schema required other than being a class.  this class can be some kind of tooling in the case of a chatbot, or it can be something like the `roombot` demo (see template) where it uses a different service's data to expand the capability of the overall botcannon environment

multiple services in your cannon-compose.yml files are encouraged, as long as they are supporting each other. new templates should be made for new eco systems (dev, prod, qa, ops, dev-ci, etc).

## task design
you should have a good grasp of what a `task` does from the `collector`. `tasks` determine if a `taskback` is called and is where any significant computation/io/wait should take place, as this will happen in a worker process.

`tasks` have full access to each message and the results of the previous task.