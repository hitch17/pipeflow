from collections import deque, OrderedDict, Iterable
import sys
import os
import os.path
import datetime
import csv
from optparse import OptionParser

class Param:
  def __init__(self, default=None, optional=False, desc=None):
    self.default = default
    self.optional = optional
    self.desc = desc

  def get(self, v, name=None):
    if v is None:
      if self.default:
        if callable(self.default):
          return self.coerce(self.default())
        else:
          return self.coerce(self.default)
      elif not self.optional:
        raise Exception("missing required parameter [%s]" % name)
    return self.coerce(v)

  def coerce(self, v):
    return v

class IntParam(Param):
  def coerce(self, v):
    return int(v)

class FloatParam(Param):
  def coerce(self, v):
    return float(v)

class DateParam(Param):
  def coerce(self, v):
    if isinstance(v, datetime.datetime):
      return v.date()
    elif isinstance(v, datetime.date):
      return v
    else:
      return datetime.datetime.strptime(v, "%Y-%m-%d").date()

class Target:
  def exists(self):
    return False
  def open(self, mode='r', makedirs=True):
    raise NotImplementedError

class FileTarget(Target):
  def __init__(self, filename):
    self.filename = filename
  def exists(self):
    return os.path.exists(self.filename)
  def makedirs(self, mode=0777):
    # borrowed from https://github.com/spotify/luigi/blob/master/luigi/local_target.py#L141
    normpath = os.path.normpath(self.filename)
    parentfolder = os.path.dirname(normpath)
    if parentfolder:
      try:
        os.makedirs(parentfolder, mode)
      except OSError:
        pass
  def open(self, mode='r', makedirs=True):
    if makedirs:
      self.makedirs()
    return file(self.filename, mode)
  def read(self):
    with self.open() as f:
      return f.read()
  def read_csv(self):
    with self.open() as f:
      return list(csv.DictReader(f))

class ConsoleNotifier:
  def notify(self, msg):
    print msg

class QuietNotifier:
  def notify(self, msg):
    pass

class Task:
  def __init__(self, **kvargs):
    self._args = OrderedDict()
    for name, param in self.__class__.task_parameters().items():
      value = param.get(kvargs.get(name), name)
      self._args[name] = value
      setattr(self, name, value)

  @classmethod
  def task_parameters(self):
    if getattr(self, '_params', None):
      return getattr(self, '_params')
    params = OrderedDict()
    for name in dir(self):
      param = getattr(self, name)
      if isinstance(param, Param):
        params[name] = param
    self._params = params
    return params

  def __repr__(self):
    return str(self)

  def __str__(self):
    return "%s(%s)" % (self.__class__.__name__, 
      ", ".join([ "%s=%s" % (name, self._args.get(name)) for name in self.task_parameters().keys() ]))

  def task_key(self):
    return tuple([self.__class__] + self._args.values())

  def __eq__(x, y):
    return type(x) == type(y) and x.task_key() == y.task_key()

  def __hash__(self):
    return hash(self.task_key())

  def requires(self):
    return None

  def complete(self):
    return self.output().exists()

  def run(self):
    pass

  def input(self):
    return map_requirements(self.requires(), lambda f: f.output())

  def output(self):
    return Target()

  def deps(self):
    if self.complete():
      return []
    graph = dict()
    queue = deque([self])
    while queue:
      t = queue.pop()
      rs = filter(lambda r: not r.complete(), enumerate_values(t.requires()))
      graph[t] = rs
      queue.extend([ r for r in rs if r not in graph ])
    return kahn_topsort(graph)

  def execute(self, notification=QuietNotifier()):
    tasks = self.deps()
    n_tasks = len(tasks)
    for i, t in enumerate(tasks):
      if not t.complete():
        notification.notify("Executing task [%s] %s of %s" % (t, (i+1), n_tasks))
        t.run()

  @classmethod
  def cli(self):
    parser = OptionParser()
    for name, param in self.task_parameters().items():
      parser.add_option("--" + name, help=param.desc)
    (options, args) = parser.parse_args()
    try:
      t = self(**vars(options))
      t.execute()
    except Exception as e:
      import traceback
      traceback.print_exc()
      parser.print_help()
      sys.exit(0)

def map_requirements(vs, fn):
  if vs is None:
    return None
  elif isinstance(vs, dict):
    return { k: fn(v) for k, v in vs.items() }
  elif isinstance(vs, list):
    return map(fn, vs)
  elif isinstance(vs, tuple):
    return tuple(map(fn, vs))
  else:
    return fn(vs)

def enumerate_values(vs):
  if vs is None:
    return
    yield
  elif isinstance(vs, Iterable):
    if isinstance(vs, dict):
      vs = vs.values()
    for v in vs:
      if isinstance(v, Iterable):
        for sub in enumerate_values(v):
          yield sub
      else:
        yield v
  else:
    yield vs

class WrapperTask(Task):
  # borrowed from https://github.com/spotify/luigi/blob/master/luigi/task.py#L795
  def complete(self):
    return all(r.complete() for r in self.requires())

class DependencyTree:
  def __init__(self):
    self.graph = {}
    self.in_degree = {}
    self.levels = {}

  def get_level(self, n):
    level_n = self.levels.get(n, set())
    self.levels[n] = level_n
    return level_n

  def set_level(self, n, value):
    self.remove_level(value)
    self.get_level(n).add(value)
    self.in_degree[value] = n

  def remove_level(self, value):
    degree = self.in_degree.get(value, 0)
    self.get_level(degree).discard(value)
    return degree

  def inc_level(self, value):
    degree = self.remove_level(value)
    self.get_level(degree + 1).add(value)
    self.in_degree[value] = degree + 1

  def dec_level(self, value):
    degree = self.remove_level(value)
    self.get_level(degree - 1).add(value)
    self.in_degree[value] = degree - 1

  def add(self, value, reqs):
    if value in self.graph:
      raise Exception("%s is already in the graph" % value)

    self.graph[value] = reqs

    if self.in_degree.get(value, 0) == 0:
      self.set_level(0, value)

    for r in reqs:
      self.inc_level(r)

  def next(self):
    if len(self.graph) == 0:
      return

    level_0 = self.get_level(0)
    if len(level_0) == 0:
      raise Exception("There is likely a cycle.")

    next = level_0.pop()
    next_rs = self.graph[next]
    self.graph.pop(next)
    for v in next_rs:
      self.dec_level(v)

    return next

  def order(self):
    v = self.next()
    while v:
      yield v
      v = self.next()


def kahn_topsort(graph):
  # https://en.wikipedia.org/wiki/Topological_sorting
  # borrowed from  https://algocoding.wordpress.com/2015/04/05/topological-sorting-python/
  in_degree = { u : 0 for u in graph } # determine in-degree 
  for u in graph:                      # of each node
    for v in graph[u]:
      in_degree[v] += 1
 
  Q = deque()                          # collect nodes with zero in-degree
  for u in in_degree:
    if in_degree[u] == 0:
      Q.append(u)
 
  L = deque()                               # list for order of nodes
   
  while Q:                
    u = Q.pop()                        # choose node of zero in-degree
    L.appendleft(u)                        # and 'remove' it from graph
    for v in graph[u]:
      in_degree[v] -= 1
      if in_degree[v] == 0:
        Q.appendleft(v)
 
  if len(L) == len(graph):
    return L
  else:                                # if there is a cycle,  
    raise Exception("cycle detected.")
