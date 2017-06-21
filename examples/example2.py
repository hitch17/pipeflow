#!/usr/bin/env python

import pipeflow

global results
results = {}

class Task1(pipeflow.Task):

  def requires(self):
    yield Task2()
    yield Task3()

  def complete(self):
    return results.get(self.task_key(), None) is not None

  def run(self):
    print "Running", self


class Task2(pipeflow.Task):

  # def requires(self):
  #   return Task3()

  def complete(self):
    return results.get(self.task_key(), None) is not None

  def run(self):
    print "Running", self
    results[self.task_key()] = True


class Task3(pipeflow.Task):

  def complete(self):
    return results.get(self.task_key(), None) is not None

  def run(self):
    print "Running", self
    results[self.task_key()] = True


# def xyz():
#   x = (yield 0)
#   print 'inside', x
#   x = x or 0
#   yield (x + 1)

# gen = xyz()
# for y in gen:
#   print y
#   print gen.send(y+1)
