import unittest
from collections import deque, OrderedDict, Iterable

class Deps:
  def __init__(self):
    self.graph = {}
    self.reverse = {}
    self.location = {}
    self.levels = {}

  def get_level(self, n):
    level_n = self.levels.get(n, set())
    self.levels[n] = level_n
    return level_n

  def set_level(self, n, value):
    self.remove_level(value)
    self.get_level(n).add(value)
    self.location[value] = n

  def remove_level(self, value):
    degree = self.location.get(value, 0)
    self.get_level(degree).discard(value)
    return degree

  def inc_level(self, value):
    degree = self.remove_level(value)
    self.get_level(degree + 1).add(value)
    self.location[value] = degree + 1

  def dec_level(self, value):
    degree = self.remove_level(value)
    self.get_level(degree - 1).add(value)
    self.location[value] = degree - 1

  def add(self, task, reqs):
    self.graph[task] = set(reqs)
    for r in reqs:
      rev = self.reverse.get(r, set())
      self.reverse[r] = rev
      rev.add(task)
    n_reqs = len(reqs)
    self.set_level(n_reqs, task)

  def next(self):
    if len(self.graph) == 0:
      return

    level_0 = self.get_level(0)
    if len(level_0) == 0:
      raise Exception("There is likely a cycle.")

    next = level_0.pop()
    print 'next', next
    for v in self.reverse.get(next, []):
      self.dec_level(v)
    self.graph.pop(next, None)
    self.reverse.pop(next, None)

    return next

  def order(self):
    v = self.next()
    while v:
      yield v
      v = self.next()

class TestDeps(unittest.TestCase):
  def test_run_partial(self):
    graph_tasks = OrderedDict(
      a=["b"],
      b=["c", "d"],
      c=[],
      d=["e"],
      e=[],
    )
    d = Deps()
    for task, reqs in graph_tasks.items():
      d.add(task, reqs)
    print 'graph', d.graph
    print 'reverse', d.reverse
    print 'location', d.location
    print 'levels', d.levels
    # print d.next()
    # print d.next()
    # print d.next()
    # print d.next()
    # print d.next()
    self.assertEquals(list(d.order()), [
      "e",
      "d",
      "c",
      "b",
      "a",
    ])


# if __name__ == '__main__':
#     unittest.main()