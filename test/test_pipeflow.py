import unittest
from pipeflow.pipeflow import *
import datetime
import tempfile
import os
import os.path
import shutil
from collections import OrderedDict

class TestParam(unittest.TestCase):
  def test_param(self):
    p = Param()
    self.assertEqual(p.get(1), 1)
    self.assertEqual(p.get('one'), 'one')
    with self.assertRaises(Exception):
      p.get(None)

  def test_param_default(self):
    p = Param(default='one')
    self.assertEqual(p.get(1), 1)
    self.assertEqual(p.get(None), 'one')

  def test_param_optional(self):
    p = Param(optional=True)
    self.assertEqual(p.get(1), 1)
    self.assertEqual(p.get(None), None)

class TestIntParam(unittest.TestCase):
  def test_param(self):
    p = IntParam()
    self.assertEqual(p.get(1), 1)
    self.assertEqual(p.get(1.1), 1)
    self.assertEqual(p.get('1'), 1)
    with self.assertRaises(ValueError):
      p.get('bad')
    with self.assertRaises(ValueError):
      p.get('1.1')

class TestFloatParam(unittest.TestCase):
  def test_param(self):
    p = FloatParam()
    self.assertEqual(p.get(1), 1)
    self.assertEqual(p.get(1.1), 1.1)
    self.assertEqual(p.get('1'), 1)
    self.assertEqual(p.get('1.1'), 1.1)
    with self.assertRaises(ValueError):
      p.get('bad')

class TestDateParam(unittest.TestCase):
  def test_param(self):
    p = DateParam()
    self.assertEqual(p.get('2017-05-01'), datetime.date(2017, 5, 1))
    self.assertEqual(p.get(datetime.date(2017, 6, 1)), datetime.date(2017, 6, 1))
    with self.assertRaises(ValueError):
      p.get('bad')

class TestFileTarget(unittest.TestCase):
  directory = os.path.join(tempfile.gettempdir(), 'pipeflow')
  filepath = os.path.join(directory, 'test.csv')
  target = FileTarget(filepath)
  csv_target = CsvFileTarget(filepath, columns=[
    'hello',
    ('world', int),
  ])

  def delete_temp_directory(self):
    if os.path.exists(self.directory):
      shutil.rmtree(self.directory)

  def setUp(self):
    self.delete_temp_directory()

  def tearDown(self):
    self.delete_temp_directory()

  def test_makedirs(self):
    self.assertFalse(os.path.exists(self.directory))
    self.target.makedirs()
    self.assertTrue(os.path.exists(self.directory))

  def test_exists_open_read(self):
    self.assertFalse(self.target.exists())
    self.csv_target.write_csv([{"hello":"1","world":2}])
    self.assertEqual(self.target.read(), "hello,world\n1,2\n")
    self.assertTrue(self.target.exists())

  def test_read_csv(self):
    self.csv_target.write_csv([{"hello":"1","world":2}])
    self.assertEqual([{"hello":"1","world":2}], list(self.csv_target.read_csv()))
    self.assertEqual([{"hello":"1","world":"2"}], list(self.target.read_csv()))

  def test_column_assignment(self):
    target = CsvFileTarget(None, columns=['hello', 'world'])
    self.assertEqual(OrderedDict([('hello', None),('world', None)]), target.columns)


class TestUtilityFunctions(unittest.TestCase):

  def test_map_requirements(self):
    self.assertEqual(None, map_requirements(None, int))

    self.assertEqual({}, map_requirements({}, int))
    self.assertEqual({"a":1,"b":2}, map_requirements({"a":"1","b":"2"}, int))

    self.assertEqual([], map_requirements([], int))
    self.assertEqual([1,2], map_requirements(["1","2"], int))

    self.assertEqual((), map_requirements((), int))
    self.assertEqual((1,2), map_requirements(("1","2"), int))

    self.assertEqual(1, map_requirements('1', int))

  def test_enumerate_values(self):
    self.assertEqual([], list(enumerate_values(None)))

    self.assertEqual([], list(enumerate_values({})))
    self.assertEqual([], list(enumerate_values([])))
    self.assertEqual([], list(enumerate_values(())))

    self.assertEqual([1], list(enumerate_values({"a":1})))
    self.assertEqual([1,2], list(enumerate_values(OrderedDict([("a",1),("b",2)]))))

    self.assertEqual([1], list(enumerate_values([1])))
    self.assertEqual([1,2], list(enumerate_values([1,2])))

    self.assertEqual([1], list(enumerate_values((1))))
    self.assertEqual([1,2], list(enumerate_values((1,2))))

    self.assertEqual([1], list(enumerate_values(1)))

    self.assertEqual([1, 2], list(enumerate_values( [[[1, 2]]] )))
    self.assertEqual([3, 4], list(enumerate_values( (((3, 4))) )))
    self.assertEqual([5], list(enumerate_values( {"a":{"b":5}} )))
    self.assertEqual([3], list(enumerate_values( {"a":{"b":{"c":3}}} )))

    self.assertEqual([1,2,3,4,5,6,7], list(enumerate_values([[1], 2, [3, 4], [5, [6, [7]]]])))
    self.assertEqual([1,2,3,4,5,6,7], list(enumerate_values(((1), 2, (3, 4), (5, (6, (7)))))))
    self.assertEqual([1,2,3,4,5,6,7], list(enumerate_values(OrderedDict(a=1, b=[2, OrderedDict(c=[[3]])], d=OrderedDict(e=4, f=(5, 6)), g=7))))

  def test_topsort(self):
    self.assertEqual(list(topsort({})), [])
    graph_tasks = { 
      "a" : ["b"],
      "b" : ["c", "d"],
      "c" : [],
      "d" : ["e"],
      "e" : [],
    }
    self.assertEqual(list(topsort(graph_tasks)), [
      "e",
      "d",
      "c",
      "b",
      "a",
    ])

class TaskA(Task):
  a = IntParam()
  b = FloatParam(default=2)
  c = 5

  def output(self):
    return FileTarget("aaaaa.txt")

class TaskB(Task):
  a = IntParam(default=5)

  def requires(self):
    return TaskA(a=self.a)

  def output(self):
    return FileTarget("bbbbb.txt")

class TestTask(unittest.TestCase):

  def test_task_parameters(self):
    params = TaskA.task_parameters()
    self.assertEqual(len(params), 2)
    self.assertTrue(isinstance(params['a'], IntParam))
    self.assertTrue(isinstance(params['b'], FloatParam))
    self.assertEqual(getattr(TaskA, '_params'), params)

  def test_task_init(self):
    with self.assertRaises(Exception):
      TaskA()
    task = TaskA(a=1)
    self.assertEqual(getattr(task, '_args'), {"a":1,"b":2.0})
    self.assertEqual(task.a, 1)
    self.assertEqual(task.b, 2)

  def test_str_repr(self):
    task = TaskA(a=1, b=2)
    self.assertEqual("TaskA(a=1, b=2.0)", str(task))
    self.assertEqual("TaskA(a=1, b=2.0)", repr(task))

  def test_key_eq_hash(self):
    task = TaskA(a=1, b=2)
    match = TaskA(a=1, b=2)
    notmatch = TaskA(a=0, b=0)
    self.assertEqual((TaskA, 1, 2.0), task.task_key())
    self.assertNotEqual((TaskA, 2, 1), task.task_key())
    self.assertNotEqual((TaskB, 1, 2.0), task.task_key())

    self.assertTrue(task == match)
    self.assertEqual(task, match)
    self.assertTrue(task != notmatch)
    self.assertNotEqual(task, notmatch)

    self.assertEqual(hash(task), hash(match))
    self.assertNotEqual(hash(task), hash(notmatch))

  def test_requires_complete(self):
    task = TaskB()
    self.assertFalse(task.complete())
    self.assertEqual(task.requires(), TaskA(a=5))

    self.assertEqual(task.output().filename, "bbbbb.txt")
    self.assertEqual(task.input().filename, "aaaaa.txt")


class TaskC(Task):
  def output(self):
    return FileTarget(os.path.join(tempfile.gettempdir(), 'pipeflow', 'testa.txt'))
  def run(self):
    with self.output().open('w') as f:
      f.write('hello')

class TaskD(Task):
  def requires(self):
    return TaskC()
  def output(self):
    return FileTarget(os.path.join(tempfile.gettempdir(), 'pipeflow', 'testb.txt'))
  def run(self):
    prefix = self.input().read()
    with self.output().open('w') as f:
      f.write(prefix)
      f.write(", world.")

class TestTaskRunning(unittest.TestCase):
  directory = os.path.join(tempfile.gettempdir(), 'pipeflow')

  def delete_temp_directory(self):
    if os.path.exists(self.directory):
      shutil.rmtree(self.directory)

  def setUp(self):
    self.delete_temp_directory()

  def tearDown(self):
    self.delete_temp_directory()

  def test_deps(self):
    self.assertEqual(list(TaskD().deps()), [TaskC()])
    self.assertEqual(list(TaskC().deps()), [])

  def test_run_full(self):
    TaskD().execute(QuietNotifier())
    self.assertEqual(TaskC().output().read(), "hello")
    self.assertEqual(TaskD().output().read(), "hello, world.")

  def test_run_partial(self):
    self.assertEqual(list(TaskD().deps()), [TaskC()])
    TaskC().execute(QuietNotifier())
    self.assertEqual(list(TaskD().deps()), [])
    TaskD().execute(QuietNotifier())
    self.assertEqual(list(TaskD().deps()), [])

class TestDependencyTree(unittest.TestCase):
  def test_task_order(self):
    d = DependencyTree(OrderedDict(
      a=["b"],
      b=["c", "d"],
      c=[],
      d=["e"],
      e=[],
    ))
    self.assertEqual(list(d.order()), ["e", "d", "c", "b", "a"])
  def test_find_identity_cycle(self):
    with self.assertRaises(Exception):
      DependencyTree(OrderedDict(a=["a"])).next()
  def test_find_simple_cycle(self):
    d = DependencyTree(OrderedDict(a=["b"]))
    self.assertEqual(d.reverse, {'a': [], 'b': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 1, 'b': 0})
    d.add("b", ["a"])
    self.assertEqual(d.reverse, {'a': ['b'], 'b': ['a']})
    self.assertEqual(d.location, {'a': 1, 'b': 1})
    self.assertEqual(d.levels, {1: OrderedDict([('a', True), ('b', True)])})
    with self.assertRaises(Exception):
      d.next()
  def test_a(self):
    d = DependencyTree(dict(a=[]))
    self.assertEqual(d.reverse, {'a': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 0})
    self.assertEqual(list(d.peek_iter()), ['a'])
  def test_a2b(self):
    d = DependencyTree(dict(a=["b"]))
    self.assertEqual(d.reverse, {'a': [], 'b': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 1, 'b': 0})
    self.assertEqual(list(d.peek_iter()), ['b','a'])
  def test_a2bc(self):
    d = DependencyTree(dict(a=["b", "c"]))
    self.assertEqual(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEqual(list(d.peek_iter()), ['c','b','a'])
  def test_a2b2c(self):
    d = DependencyTree(dict(a=["c"], c=["b"]))
    self.assertEqual(d.reverse, {'a': [], 'c': ['a'], 'b': ['c']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True), ('c', True)])})
    self.assertEqual(d.location, {'a': 1, 'c': 1, 'b': 0})
    self.assertEqual(list(d.peek_iter()), ['b','c','a'])
  def test_incremental(self):
    d = DependencyTree(dict(a=[]))
    self.assertEqual(d.reverse, {'a': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 0})
    self.assertEqual(list(d.peek_iter()), ['a'])

    d.add('b', ['c'])
    self.assertEqual(d.reverse, {'a': [], 'c': ['b'], 'b': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True), ('c', True)]), 1: OrderedDict([('b', True)])})
    self.assertEqual(d.location, {'a': 0, 'c': 0, 'b': 1})
    self.assertEqual(list(d.peek_iter()), ['c','b','a'])

    d.add('d', ['a'])
    self.assertEqual(d.reverse, {'a': ['d'], 'c': ['b'], 'b': [], 'd': []})
    self.assertEqual(d.levels, {0: OrderedDict([('c', True), ('a', True)]), 1: OrderedDict([('b', True), ('d', True)])})
    self.assertEqual(d.location, {'a': 0, 'c': 0, 'b': 1, 'd': 1})
    self.assertEqual(list(d.peek_iter()), ['a', 'd', 'c', 'b'])

    d.add('c', ['a', 'e'])
    self.assertEqual(d.reverse, {'a': ['d', 'c'], 'c': ['b'], 'b': [], 'e': ['c'], 'd': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True), ('e', True)]), 1: OrderedDict([('b', True), ('d', True)]), 2: OrderedDict([('c', True)])})
    self.assertEqual(d.location, {'a': 0, 'c': 2, 'b': 1, 'e': 0, 'd': 1})
    self.assertEqual(list(d.peek_iter()), ['e', 'a', 'c', 'b', 'd'])

    self.assertEqual(d.next(), 'e')
    self.assertEqual(d.reverse, {'a': ['d', 'c'], 'c': ['b'], 'b': [], 'd': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)]), 1: OrderedDict([('b', True), ('d', True), ('c', True)])})
    self.assertEqual(d.location, {'a': 0, 'c': 1, 'b': 1, 'd': 1})
    self.assertEqual(list(d.peek_iter()), ['a', 'c', 'b', 'd'])

    self.assertEqual(d.next(), 'a')
    self.assertEqual(d.reverse, {'b': [], 'c': ['b'], 'd': []})
    self.assertEqual(d.levels, {0: OrderedDict([('d', True), ('c', True)]), 1: OrderedDict([('b', True)])})
    self.assertEqual(d.location, {'c': 0, 'b': 1, 'd': 0})
    self.assertEqual(list(d.peek_iter()), ['c', 'b', 'd'])

    self.assertEqual(d.next(), 'c')
    self.assertEqual(d.reverse, {'b': [], 'd': []})
    self.assertEqual(d.levels, {0: OrderedDict([('d', True), ('b', True)])})
    self.assertEqual(d.location, {'b': 0, 'd': 0})
    self.assertEqual(list(d.peek_iter()), ['b', 'd'])

    d.add('f', ['d'])
    self.assertEqual(d.reverse, {'b': [], 'd': ['f'], 'f': []})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True), ('d', True)]), 1: OrderedDict([('f', True)])})
    self.assertEqual(d.location, {'b': 0, 'd': 0, 'f': 1})
    self.assertEqual(list(d.peek_iter()), ['d', 'f', 'b'])

    self.assertEqual(d.next(), 'd')
    self.assertEqual(d.reverse, {'b': [], 'f': []})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True), ('f', True)])})
    self.assertEqual(d.location, {'b': 0, 'f': 0})
    self.assertEqual(list(d.peek_iter()), ['f', 'b'])

    self.assertEqual(d.next(), 'f')
    self.assertEqual(d.reverse, {'b': []})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)])})
    self.assertEqual(d.location, {'b': 0})
    self.assertEqual(list(d.peek_iter()), ['b'])

    self.assertEqual(d.next(), 'b')
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {})
    self.assertEqual(d.location, {})
    self.assertEqual(list(d.peek_iter()), [])
  def test_levels(self):
    d = DependencyTree()
    d.set_level(1, 'a')
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {1: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 1})
    self.assertEqual(d.get_level(1), OrderedDict([('a', True)]))
    d.set_level(3, 'a')
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {3: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 3})
    self.assertEqual(d.get_level(3), OrderedDict([('a', True)]))
    d.inc_level('a', 2)
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {5: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 5})
    self.assertEqual(d.get_level(5), OrderedDict([('a', True)]))
    d.dec_level('a', 3)
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {2: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 2})
    self.assertEqual(d.get_level(2), OrderedDict([('a', True)]))
    d.remove_level('a')
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {})
    self.assertEqual(d.location, {})
  def test_add_edge_1(self):
    d = DependencyTree()
    d.add_edge('a')
    self.assertEqual(d.reverse, {'a': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 0})
    self.assertEqual(list(d.peek_iter()), ['a'])
  def test_add_edge_2(self):
    d = DependencyTree()
    d.add_edge('a', 'b')
    self.assertEqual(d.reverse, {'a': [], 'b': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 1, 'b': 0})
    self.assertEqual(list(d.peek_iter()), ['b','a'])
  def test_add_edge_3(self):
    d = DependencyTree()
    d.add_edge('a', 'b')
    d.add_edge('a', 'c')
    self.assertEqual(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEqual(list(d.peek_iter()), ['c','b','a'])
  def test_add_1(self):
    d = DependencyTree()
    d.add('a', [])
    self.assertEqual(d.reverse, {'a': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 0})
    self.assertEqual(list(d.peek_iter()), ['a'])
  def test_add_2(self):
    d = DependencyTree()
    d.add('a', ['b'])
    self.assertEqual(d.reverse, {'a': [], 'b': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 1, 'b': 0})
    self.assertEqual(list(d.peek_iter()), ['b','a'])
  def test_add_3(self):
    d = DependencyTree()
    d.add('a', ['b','c'])
    self.assertEqual(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEqual(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEqual(list(d.peek_iter()), ['c','b','a'])
  def test_remove_1(self):
    d = DependencyTree(dict(a=[]))
    d.remove_node('a')
    self.assertEqual(d.reverse, {})
    self.assertEqual(d.levels, {})
    self.assertEqual(d.location, {})
    self.assertEqual(list(d.peek_iter()), [])
  def test_remove_2b(self):
    d = DependencyTree(dict(a=['b']))
    d.remove_node('b')
    self.assertEqual(d.reverse, {'a': []})
    self.assertEqual(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEqual(d.location, {'a': 0})
    self.assertEqual(list(d.peek_iter()), ['a'])
  def test_remove_2a(self):
    d = DependencyTree(dict(a=['b']))
    d.remove_node('a')
    self.assertEqual(list(d.peek_iter()), ['b'])
  def test_remove_2a_cycle(self):
    d = DependencyTree(dict(a=['b'], b=['a']))
    d.remove_node('a')
    self.assertEqual(list(d.peek_iter()), ['b'])


if __name__ == '__main__':
    unittest.main()
