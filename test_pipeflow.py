import unittest
from pipeflow import *
import datetime
import tempfile
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
    with self.target.open('w') as f:
      f.write("hello,world\n1,2")
    self.assertEquals(self.target.read(), "hello,world\n1,2")
    self.assertTrue(self.target.exists())

  def test_read_csv(self):
    with self.target.open('w') as f:
      f.write("hello,world\n1,2")
    self.assertEquals([{"hello":"1","world":"2"}], self.target.read_csv())

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
    self.assertEquals([], enumerate_values(None))

    self.assertEquals([], enumerate_values({}))
    self.assertEquals([], enumerate_values([]))
    self.assertEquals([], enumerate_values(()))

    self.assertEquals([1], enumerate_values({"a":1}))
    self.assertEquals([1,2], enumerate_values(OrderedDict([("a",1),("b",2)])))

    self.assertEquals([1], enumerate_values([1]))
    self.assertEquals([1,2], enumerate_values([1,2]))

    self.assertEquals([1], enumerate_values((1)))
    self.assertEquals([1,2], enumerate_values((1,2)))

    self.assertEquals([1], enumerate_values(1))

  def test_kahn_topsort(self):
    self.assertEquals(kahn_topsort({}), [])
    graph_tasks = { 
      "a" : ["b"],
      "b" : ["c", "d"],
      "c" : [],
      "d" : ["e"],
      "e" : [],
    }
    self.assertEquals(kahn_topsort(graph_tasks), [
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
    key = getattr(Task, '_key')
    self.assertEquals((TaskA, 1, 2.0), key(task))
    self.assertNotEquals((TaskA, 2, 1), key(task))
    self.assertNotEquals((TaskB, 1, 2.0), key(task))

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
    self.assertEquals(TaskD().deps(), [TaskC(), TaskD()])

  def test_run_full(self):
    TaskD().execute(QuietNotifier())
    self.assertEquals(TaskC().output().read(), "hello")
    self.assertEquals(TaskD().output().read(), "hello, world.")

  def test_run_partial(self):
    self.assertEquals(TaskD().deps(), [TaskC(), TaskD()])
    TaskC().execute(QuietNotifier())
    self.assertEquals(TaskD().deps(), [TaskD()])
    TaskD().execute(QuietNotifier())
    self.assertEquals(TaskD().deps(), [])


if __name__ == '__main__':
    unittest.main()