from collections import deque, OrderedDict, Iterable
import sys
import os
import os.path
import datetime
import csv
import copy
from optparse import OptionParser

class Param(object):
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

class Target(object):
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
      csv_in = csv.DictReader(f)
      for row in csv_in:
        yield row

class CsvFileTarget(FileTarget):
  def __init__(self, filename, columns):
    super(CsvFileTarget, self).__init__(filename)
    self.columns = OrderedDict()
    for c in columns:
      if type(c) == tuple and len(c) == 2:
        self.columns[c[0]] = c[1]
      elif isinstance(c, basestring):
        self.columns[c] = None
      else:
        raise Exception("column should either be a name or tuple of (name, type constructor)")
  def read_csv(self):
    for row in super(CsvFileTarget, self).read_csv():
      for k, func in self.columns.items():
        if func and k in row:
          row[k] = func(row[k])
      yield row
  def write_csv(self, rows):
    with self.open('w') as f:
      csv_out = csv.DictWriter(f,
        fieldnames=self.columns.keys(),
        extrasaction='ignore',
        lineterminator=os.linesep)
      csv_out.writeheader()
      csv_out.writerows(rows)


class ConsoleNotifier(object):
  def notify(self, msg):
    print msg

class QuietNotifier(object):
  def notify(self, msg):
    pass

class Task(object):
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
    return topsort(graph)

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

class DependencyTree(object):
  def __init__(self, graph=None):
    self.reverse = {}
    self.location = {}
    self.levels = {}
    if graph:
      self.expand(graph)

  def get_level(self, n):
    level_n = self.levels.get(n, OrderedDict())
    self.levels[n] = level_n
    return level_n

  def set_level(self, n, value):
    self.remove_level(value)
    self.get_level(n)[value] = True
    self.location[value] = n

  def remove_level(self, value):
    degree = self.location.pop(value, 0)
    level = self.get_level(degree)
    level.pop(value, None)
    if len(level) == 0:
      self.levels.pop(degree, None)
    return degree

  def inc_level(self, value, inc=1):
    degree = self.remove_level(value)
    self.get_level(degree + inc)[value] = True
    self.location[value] = degree + inc

  def dec_level(self, value, dec=1):
    degree = self.remove_level(value)
    self.get_level(degree - dec)[value] = True
    self.location[value] = degree - dec

  def expand(self, graph):
    for k, v in graph.items():
      self.add(k, v)

  def add_edge(self, start, end=None):
    self.reverse[start] = self.reverse.get(start, list())
    self.inc_level(start, 0)
    if end:
      self.add_edge(end)
      self.reverse[end].append(start)
      self.inc_level(start)

  def add(self, start, ends):
    if ends:
      for e in ends:
        self.add_edge(start, e)
    else:
      self.add_edge(start)

  def remove_node(self, node):
    for v in self.reverse.get(node, []):
      self.dec_level(v)
    self.remove_level(node)
    self.reverse.pop(node, None)

  def next(self):
    if len(self.reverse) == 0:
      raise StopIteration

    level_0 = self.get_level(0)
    if len(level_0) == 0:
      raise Exception("Cycle detected")

    next = level_0.popitem()[0]
    self.remove_node(next)
    return next

  def order(self):
    v = self.next()
    while v:
      yield v
      v = self.next()

  def peek_iter(self):
    d = DependencyTree()
    d.reverse = copy.deepcopy(self.reverse)
    d.location = copy.deepcopy(self.location)
    d.levels = copy.deepcopy(self.levels)
    return d.order()

def topsort(graph):
  return list(DependencyTree(graph).order())
