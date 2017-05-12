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

TaskA.cli()
