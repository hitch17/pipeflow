import unittest
from collections import deque, OrderedDict, Iterable
import copy

class Deps:
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
    d = Deps()
    d.reverse = copy.deepcopy(self.reverse)
    d.location = copy.deepcopy(self.location)
    d.levels = copy.deepcopy(self.levels)
    return d.order()

class TestDeps(unittest.TestCase):
  def test_task_order(self):
    d = Deps(OrderedDict(
      a=["b"],
      b=["c", "d"],
      c=[],
      d=["e"],
      e=[],
    ))
    self.assertEquals(list(d.order()), ["e", "d", "c", "b", "a"])
  def test_find_identity_cycle(self):
    with self.assertRaises(Exception):
      Deps(OrderedDict(a=["a"])).next()
  def test_find_simple_cycle(self):
    d = Deps(OrderedDict(a=["b"]))
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
    d = Deps(dict(a=[]))
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_a2b(self):
    d = Deps(dict(a=["b"]))
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_a2bc(self):
    d = Deps(dict(a=["b", "c"]))
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_a2b2c(self):
    d = Deps(dict(a=["c"], c=["b"]))
    self.assertEquals(d.reverse, {'a': [], 'c': ['a'], 'b': ['c']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True), ('c', True)])})
    self.assertEquals(d.location, {'a': 1, 'c': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','c','a'])
  def test_incremental(self):
    d = Deps(dict(a=[]))
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
    d = Deps()
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
    d = Deps()
    d.add_edge('a')
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_add_edge_2(self):
    d = Deps()
    d.add_edge('a', 'b')
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_add_edge_3(self):
    d = Deps()
    d.add_edge('a', 'b')
    d.add_edge('a', 'c')
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_add_1(self):
    d = Deps()
    d.add('a', [])
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_add_2(self):
    d = Deps()
    d.add('a', ['b'])
    self.assertEquals(d.reverse, {'a': [], 'b': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True)]), 1: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 1, 'b': 0})
    self.assertEquals(list(d.peek_iter()), ['b','a'])
  def test_add_3(self):
    d = Deps()
    d.add('a', ['b','c'])
    self.assertEquals(d.reverse, {'a': [], 'b': ['a'], 'c': ['a']})
    self.assertEquals(d.levels, {0: OrderedDict([('b', True), ('c', True)]), 2: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 2, 'b': 0, 'c': 0})
    self.assertEquals(list(d.peek_iter()), ['c','b','a'])
  def test_remove_1(self):
    d = Deps(dict(a=[]))
    d.remove_node('a')
    self.assertEquals(d.reverse, {})
    self.assertEquals(d.levels, {})
    self.assertEquals(d.location, {})
    self.assertEquals(list(d.peek_iter()), [])
  def test_remove_2b(self):
    d = Deps(dict(a=['b']))
    d.remove_node('b')
    self.assertEquals(d.reverse, {'a': []})
    self.assertEquals(d.levels, {0: OrderedDict([('a', True)])})
    self.assertEquals(d.location, {'a': 0})
    self.assertEquals(list(d.peek_iter()), ['a'])
  def test_remove_2a(self):
    d = Deps(dict(a=['b']))
    d.remove_node('a')
    # self.assertEquals(d.reverse, None)
    # self.assertEquals(d.levels, None)
    # self.assertEquals(d.location, None)
    self.assertEquals(list(d.peek_iter()), ['b'])
  def test_remove_2a_cycle(self):
    d = Deps(dict(a=['b'], b=['a']))
    d.remove_node('a')
    # self.assertEquals(d.reverse, None)
    # self.assertEquals(d.levels, None)
    # self.assertEquals(d.location, None)
    self.assertEquals(list(d.peek_iter()), ['b'])


if __name__ == '__main__':
    unittest.main()