#!/usr/bin/env python

import pipeflow

class TaskA(pipeflow.Task):
  a = pipeflow.IntParam()

  def requires(self):
    return TaskB()

  def output(self):
    return pipeflow.FileTarget("temp/world.txt")

  def run(self):
    print "Running TaskA"
    with self.output().open('w') as f:
      f.write(self.input().read())
      f.write(", world.")


class TaskB(pipeflow.Task):
  def output(self):
    return pipeflow.FileTarget("temp/hello.txt")

  def run(self):
    print "Running TaskB"
    with self.output().open('w') as f:
      f.write("hello")

task = TaskA(a=5)
task.execute()
