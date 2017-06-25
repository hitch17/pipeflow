import unittest
from pipeflow import *
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
    self.assertEquals(self.target.read(), "hello,world\n1,2\n")
    self.assertTrue(self.target.exists())

  def test_read_csv(self):
    self.csv_target.write_csv([{"hello":"1","world":2}])
    self.assertEquals([{"hello":"1","world":2}], list(self.csv_target.read_csv()))
    self.assertEquals([{"hello":"1","world":"2"}], list(self.target.read_csv()))

  def test_column_assignment(self):
    target = CsvFileTarget(None, columns=['hello', 'world'])
    self.assertEquals(OrderedDict([('hello', None),('world', None)]), target.columns)


class TestUtilityFunctions(unittest.TestCase):

  def test_map_requirements(self):
    self.assertEquals(None, map_requirements(None, int))

    self.assertEquals({}, map_requirements({}, int))
    self.assertEquals({"a":1,"b":2}, map_requirements({"a":"1","b":"2"}, int))

    self.assertEquals([], map_requirements([], int))
    self.assertEquals([1,2], map_requirements(["1","2"], int))

    self.assertEquals((), map_requirements((), int))
    self.assertEquals((1,2), map_requirements(("1","2"), int))

    self.assertEquals(1, map_requirements('1', int))

  def test_enumerate_values(self):
    self.assertEquals([], list(enumerate_values(None)))

    self.assertEquals([], list(enumerate_values({})))
    self.assertEquals([], list(enumerate_values([])))
    self.assertEquals([], list(enumerate_values(())))

    self.assertEquals([1], list(enumerate_values({"a":1})))
    self.assertEquals([1,2], list(enumerate_values(OrderedDict([("a",1),("b",2)]))))

    self.assertEquals([1], list(enumerate_values([1])))
    self.assertEquals([1,2], list(enumerate_values([1,2])))

    self.assertEquals([1], list(enumerate_values((1))))
    self.assertEquals([1,2], list(enumerate_values((1,2))))

    self.assertEquals([1], list(enumerate_values(1)))

    self.assertEquals([1, 2], list(enumerate_values( [[[1, 2]]] )))
    self.assertEquals([3, 4], list(enumerate_values( (((3, 4))) )))
    self.assertEquals([5], list(enumerate_values( {"a":{"b":5}} )))
    self.assertEquals([3], list(enumerate_values( {"a":{"b":{"c":3}}} )))

    self.assertEquals([1,2,3,4,5,6,7], list(enumerate_values([[1], 2, [3, 4], [5, [6, [7]]]])))
    self.assertEquals([1,2,3,4,5,6,7], list(enumerate_values(((1), 2, (3, 4), (5, (6, (7)))))))
    self.assertEquals([1,2,3,4,5,6,7], list(enumerate_values(OrderedDict(a=1, b=[2, OrderedDict(c=[[3]])], d=OrderedDict(e=4, f=(5, 6)), g=7))))

  def test_topsort(self):
    self.assertEquals(list(topsort({})), [])
    graph_tasks = { 
      "a" : ["b"],
      "b" : ["c", "d"],
      "c" : [],
      "d" : ["e"],
      "e" : [],
    }
    self.assertEquals(list(topsort(graph_tasks)), [
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
    self.assertEquals(len(params), 2)
    self.assertTrue(isinstance(params['a'], IntParam))
    self.assertTrue(isinstance(params['b'], FloatParam))
    self.assertEquals(getattr(TaskA, '_params'), params)

  def test_task_init(self):
    with self.assertRaises(Exception):
      TaskA()
    task = TaskA(a=1)
    self.assertEquals(getattr(task, '_args'), {"a":1,"b":2.0})
    self.assertEquals(task.a, 1)
    self.assertEquals(task.b, 2)

  def test_str_repr(self):
    task = TaskA(a=1, b=2)
    self.assertEquals("TaskA(a=1, b=2.0)", str(task))
    self.assertEquals("TaskA(a=1, b=2.0)", repr(task))

  def test_key_eq_hash(self):
    task = TaskA(a=1, b=2)
    match = TaskA(a=1, b=2)
    notmatch = TaskA(a=0, b=0)
    self.assertEquals((TaskA, 1, 2.0), task.task_key())
    self.assertNotEquals((TaskA, 2, 1), task.task_key())
    self.assertNotEquals((TaskB, 1, 2.0), task.task_key())

    self.assertTrue(task == match)
    self.assertEquals(task, match)
    self.assertTrue(task != notmatch)
    self.assertNotEquals(task, notmatch)

    self.assertEquals(hash(task), hash(match))
    self.assertNotEquals(hash(task), hash(notmatch))

  def test_requires_complete(self):
    task = TaskB()
    self.assertFalse(task.complete())
    self.assertEquals(task.requires(), TaskA(a=5))

    self.assertEquals(task.output().filename, "bbbbb.txt")
    self.assertEquals(task.input().filename, "aaaaa.txt")


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
    self.assertEquals(list(TaskD().deps()), [TaskC(), TaskD()])

  def test_run_full(self):
    TaskD().execute(QuietNotifier())
    self.assertEquals(TaskC().output().read(), "hello")
    self.assertEquals(TaskD().output().read(), "hello, world.")

  def test_run_partial(self):
    self.assertEquals(list(TaskD().deps()), [TaskC(), TaskD()])
    TaskC().execute(QuietNotifier())
    self.assertEquals(list(TaskD().deps()), [TaskD()])
    TaskD().execute(QuietNotifier())
    self.assertEquals(list(TaskD().deps()), [])

class TestDependencyTree(unittest.TestCase):
  def test_task_order(self):
    d = DependencyTree(OrderedDict(
      a=["b"],
      b=["c", "d"],
      c=[],
      d=["e"],
      e=[],
    ))
    self.assertEquals(list(d.order()), ["e", "d", "c", "b", "a"])
  def test_find_identity_cycle(self):
    with self.assertRaises(Exception):
      DependencyTree(OrderedDict(a=["a"])).next()
  def test_find_simple_cycle(self):
    d = DependencyTree(OrderedDict(a=["b"]))
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    d.add("b", ["a"])
    self.assertEquals(d.reverse, {'a': ['b'], 'b': ['a']})
    self.assertEquals(d.location, {'a': 1, 'b': 1})
    self.assertEquals(d.levels, {1: OrderedDict([('a', True), ('b', True)])})
    with self.assertRaises(Exception):
      d.next()
  def test_a(self):
    d = DependencyTree(dict(a=[]))
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_a2b(self):
    d = DependencyTree(dict(a=["b"]))
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_a2bc(self):
    d = DependencyTree(dict(a=["b", "c"]))
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_a2b2c(self):
    d = DependencyTree(dict(a=["c"], c=["b"]))
    self.assertEquals(d.reverse, {'a': [], 'c': ['a'], 'b': ['c']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True), ('c', True)])})
    self.assertEquals(d.location, {'a': 1, 'c': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','c','a'])
  def test_incremental(self):
    d = DependencyTree(dict(a=[]))
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])

    d.add('b', ['c'])
    self.assertEquals(d.reverse, {'a': [], 'c': ['b'], 'b': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True), ('c', True)]), 1: OrderedDict([('b', True)])})
    self.assertEquals(d.location, {'a': 0, 'c': 0, 'b': 1})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])

    d.add('d', ['a'])
    self.assertEquals(d.reverse, {'a': ['d'], 'c': ['b'], 'b': [], 'd': []})
    self.assertEquals(d.levels, {0: OrderedDict([('c', True), ('a', True)]), 1: OrderedDict([('b', True), ('d', True)])})
    self.assertEquals(d.location, {'a': 0, 'c': 0, 'b': 1, 'd': 1})
    self.assertEquals(list(d.peek_iter()), ['a', 'd', 'c', 'b'])

    d.add('c', ['a', 'e'])
    self.assertEquals(d.reverse, {'a': ['d', 'c'], 'c': ['b'], 'b': [], 'e': ['c'], 'd': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True), ('e', True)]), 1: OrderedDict([('b', True), ('d', True)]), 2: OrderedDict([('c', True)])})
    self.assertEquals(d.location, {'a': 0, 'c': 2, 'b': 1, 'e': 0, 'd': 1})
    self.assertEquals(list(d.peek_iter()), ['e', 'a', 'c', 'b', 'd'])

    self.assertEquals(d.next(), 'e')
    self.assertEquals(d.reverse, {'a': ['d', 'c'], 'c': ['b'], 'b': [], 'd': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)]), 1: OrderedDict([('b', True), ('d', True), ('c', True)])})
    self.assertEquals(d.location, {'a': 0, 'c': 1, 'b': 1, 'd': 1})
    self.assertEquals(list(d.peek_iter()), ['a', 'c', 'b', 'd'])

    self.assertEquals(d.next(), 'a')
    self.assertEquals(d.reverse, {'b': [], 'c': ['b'], 'd': []})
    self.assertEquals(d.levels, {0: OrderedDict([('d', True), ('c', True)]), 1: OrderedDict([('b', True)])})
    self.assertEquals(d.location, {'c': 0, 'b': 1, 'd': 0})
    self.assertEquals(list(d.peek_iter()), ['c', 'b', 'd'])

    self.assertEquals(d.next(), 'c')
    self.assertEquals(d.reverse, {'b': [], 'd': []})
    self.assertEquals(d.levels, {0: OrderedDict([('d', True), ('b', True)])})
    self.assertEquals(d.location, {'b': 0, 'd': 0})
    self.assertEquals(list(d.peek_iter()), ['b', 'd'])

    d.add('f', ['d'])
    self.assertEquals(d.reverse, {'b': [], 'd': ['f'], 'f': []})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('d', True)]), 1: OrderedDict([('f', True)])})
    self.assertEquals(d.location, {'b': 0, 'd': 0, 'f': 1})
    self.assertEquals(list(d.peek_iter()), ['d', 'f', 'b'])

    self.assertEquals(d.next(), 'd')
    self.assertEquals(d.reverse, {'b': [], 'f': []})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('f', True)])})
    self.assertEquals(d.location, {'b': 0, 'f': 0})
    self.assertEquals(list(d.peek_iter()), ['f', 'b'])

    self.assertEquals(d.next(), 'f')
    self.assertEquals(d.reverse, {'b': []})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)])})
    self.assertEquals(d.location, {'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b'])

    self.assertEquals(d.next(), 'b')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {})
    self.assertEquals(d.location, {})
    self.assertEquals(list(d.peek_iter()), [])
  def test_levels(self):
    d = DependencyTree()
    d.set_level(1, 'a')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1})
    self.assertEquals(d.get_level(1), OrderedDict([('a', True)]))
    d.set_level(3, 'a')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {3: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 3})
    self.assertEquals(d.get_level(3), OrderedDict([('a', True)]))
    d.inc_level('a', 2)
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {5: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 5})
    self.assertEquals(d.get_level(5), OrderedDict([('a', True)]))
    d.dec_level('a', 3)
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2})
    self.assertEquals(d.get_level(2), OrderedDict([('a', True)]))
    d.remove_level('a')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {})
    self.assertEquals(d.location, {})
  def test_add_edge_1(self):
    d = DependencyTree()
    d.add_edge('a')
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_add_edge_2(self):
    d = DependencyTree()
    d.add_edge('a', 'b')
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_add_edge_3(self):
    d = DependencyTree()
    d.add_edge('a', 'b')
    d.add_edge('a', 'c')
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_add_1(self):
    d = DependencyTree()
    d.add('a', [])
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_add_2(self):
    d = DependencyTree()
    d.add('a', ['b'])
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_add_3(self):
    d = DependencyTree()
    d.add('a', ['b','c'])
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_remove_1(self):
    d = DependencyTree(dict(a=[]))
    d.remove_node('a')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {})
    self.assertEquals(d.location, {})
    self.assertEquals(list(d.peek_iter()), [])
  def test_remove_2b(self):
    d = DependencyTree(dict(a=['b']))
    d.remove_node('b')
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_remove_2a(self):
    d = DependencyTree(dict(a=['b']))
    d.remove_node('a')
    self.assertEquals(list(d.peek_iter()), ['b'])
  def test_remove_2a_cycle(self):
    d = DependencyTree(dict(a=['b'], b=['a']))
    d.remove_node('a')
    self.assertEquals(list(d.peek_iter()), ['b'])


if __name__ == '__main__':
    unittest.main()
