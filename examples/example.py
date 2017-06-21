#!/usr/bin/env python

import pipeflow

class TaskA(pipeflow.Task):
  a = pipeflow.IntParam(desc="a number to put into the file")

  def requires(self):
    return TaskB(b=self.a+1)

  def output(self):
    return pipeflow.FileTarget("temp/world.txt")

  def run(self):
    print "Running TaskA"
    with self.output().open('w') as f:
      f.write(self.input().read())
      f.write(", world.")


class TaskB(pipeflow.Task):
  b = pipeflow.IntParam()

  def output(self):
    return pipeflow.FileTarget("temp/hello.txt")

  def run(self):
    print "Running TaskB"
    with self.output().open('w') as f:
      f.write(str(self.b))
      f.write(" hello")


class TaskCatalog(pipeflow.Task):
  n = pipeflow.IntParam()

  def output(self):
    return pipeflow.FileTarget("temp/catalog.txt")

  def run(self):
    print "Running TaskCatalog"
    with self.output().open('w') as f:
      f.write(",".join(map(str, xrange(0, self.n))))

class TaskPrereq(pipeflow.Task):
  n = pipeflow.IntParam()

  def output(self):
    return pipeflow.FileTarget("temp/%s.txt" % self.n)

  def run(self):
    print 'TaskPrereq', self.n
    with self.output().open('w') as f:
      f.write(str(self.n))

class TaskMain(pipeflow.Task):
  n = pipeflow.IntParam()

  def requires(self):
    catalog = yield TaskCatalog(n=self.n)
    # catalog.run()
    # items = catalog.output().read().split(',')
    # return { x: TaskPrereq(n=x) for x in items }

  def run(self):
    print 'TaskMain', self.n

TaskMain.cli()



