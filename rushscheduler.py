#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

# NAME:         pythonscheduler.py ( Python )

# OS
import os,sys,re,time,threading

# HOUDINI
import pdg
from pdg.scheduler import PyScheduler
from pdg.job.callbackserver import CallbackServerMixin

class RushScheduler(CallbackServerMixin, PyScheduler):
    """
    Python scheduler implementation
    """
    lock_      = threading.Semaphore()      # create semaphore lock object
    jobid_     = None                       # rush jobid (if any)
    workitems_ = []                         # cached workitems

    def __init__(self, scheduler, name):
        """
        __init__(self, pdg.Scheduler) -> NoneType

        Initializes the Scheduler with a C++ scheduler reference and name
        """
        PyScheduler.__init__(self, scheduler, name)
        CallbackServerMixin.__init__(self, True)

    @classmethod
    def templateName(cls):
        return "python_scheduler"

    def onTransferFile(self, file_path):
        # Custom transferFile logic. Returns True on success, else False.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # file_path     -  Path to file that should be moved

        print("--- onTransferFile")
        return self.transferFile(file_path)

    def submitAsJob(self, graph_file, node_path):
        # Custom submitAsJob logic. Returns the status URI for the submitted job.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # graph_file    -  Path to a .hip file containing the TOP Network, relative to $PDGDIR.
        # node_path     -  Op path to the TOP Network

        print("--- submitAsJob")
        return ""

    def onSchedule(self, work_item):
        # ERCO: This runs to start new work items (Rush frames).
        #
        # Custom onSchedule logic. Returns pdg.ScheduleResult.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # work_item     -  The pdg.WorkItem to schedule

        print("--- onSchedule")
        import subprocess
        import os
        import sys

        # Ensure directories exist and serialize the work item
        self.createJobDirsAndSerializeWorkItems(work_item)

        # expand the special __PDG_* tokens in the work item command
        item_command = self.expandCommandTokens(work_item.command, work_item)

        # add special PDG_* variables to the job's environment
        temp_dir = str(self.tempDir(False))

        job_env = os.environ.copy()
        job_env['PDG_RESULT_SERVER'] = str(self.workItemResultServerAddr())
        job_env['PDG_ITEM_NAME'] = str(work_item.name)
        job_env['PDG_ITEM_ID'] = str(work_item.id)
        job_env['PDG_DIR'] = str(self.workingDir(False))
        job_env['PDG_TEMP'] = temp_dir
        job_env['PDG_SCRIPTDIR'] = str(self.scriptDir(False))

        # accumulate req
        print("--- RUSH onSchedule:\n" +
                "        PDG_ITEM_NAME: %s\n" % job_env['PDG_ITEM_NAME'] +
                "    PDG_RESULT_SERVER: %s\n" % job_env['PDG_RESULT_SERVER'] +
                "        PDG_ITEM_NAME: %s\n" % job_env['PDG_ITEM_NAME'] +
                "          PDG_ITEM_ID: %s\n" % job_env['PDG_ITEM_ID'] +
                "              PDG_DIR: %s\n" % job_env['PDG_DIR'] + 
                "             PDG_TEMP: %s\n" % job_env['PDG_TEMP'] + 
                "        PDG_SCRIPTDIR: %s\n" % job_env['PDG_SCRIPTDIR'] +
                "\n" +
                "     work_item id: %s\n" % work_item.id +
                "   work_item name: %s\n" % work_item.name +
                "  work_item label: %s\n" % work_item.label +
                "\n" +
                "    EXECUTING: %s" % item_command)


        # run the given command in a shell
        # returncode = subprocess.call(item_command, shell=True, env=job_env)
        returncode = 0

        # if the return code is non-zero, report it as failed
        if returncode == 0:
            return pdg.scheduleResult.CookSucceeded
        return pdg.scheduleResult.CookFailed

    def onScheduleStatic(self, dependencies, dependents, ready_items):
        # Custom onScheduleStatic logic.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # dependencies  -  pdg.WorkItem map of dependencies
        # dependents    -  pdg.WorkItem map of dependents
        # ready_items   -  pdg.WorkItem array of work items

        print("--- onScheduleStatic:\n" +
              "   dependents: %s\n" % repr(dependents) +
              "  ready_items: %s" % repr(ready_items))

        return

    def onStart(self):
        # Custom onStartCook logic. Returns True if started.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # static        -  True if static cook
        # cook_set      -  Set of nodes to cook

        print("--- onStart")
        self.setWorkingDir(".", ".")  # (local,remote)
        return True

    def onStop(self):
        # Custom onStop logic. Returns True if stopped.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance

        print("--- onStop")
        self.stopCallbackServer()

        return True

    def onStartCook(self, static, cook_set):
        # Custom onStartCook logic. Returns True if started.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # static        -  True if static cook
        # cook_set      -  Set of nodes to cook

        # repr(cook_set) says it's a list, first item seems to be of type "Processor", so see:
        # https://www.sidefx.com/docs/houdini/tops/pdg/Processor.html

        print("--- onStartCook")
        processor = cook_set[0]
        rush_sched_name = processor.scheduler.name

        wd = self["pdg_workingdir"].evaluateString()
        self.setWorkingDir(wd, wd)

        if not self.isCallbackServerRunning():
            self.startCallbackServer()

        return True

    def onStopCook(self, cancel):
        # Custom onStopCook logic. Returns True if stopped.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # cancel        -  True if cook was cancelled
        print("--- onStopCook")

        return True

    def onTick(self):
        # Custom onTick logic. Returns a pdg.tickResult.
        # Called periodically when the graph is cooking.  Can be used to check the state of
        # running work items.  Returns a result to PDG to affect subsequent calls to `onSchedule`.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        from pdg import tickResult

        print("--- onTick")
        return tickResult.SchedulerReady

    def getLogURI(self, work_item):
        # Custom getLogURI logic. Returns the farm's log URI for the given task.
        # Should return a valid URI or empty string.
        # E.g.: 'file:///myfarm/tasklogs/jobid20.log'
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # work_item     -  pdg.WorkItem

        print("--- getLogURI")
        return "file:///var/tmp/foo.log"    # DEBUGGING

    def getStatusURI(self, work_item):
        # Custom getStatusURI logic. Returns the farm's status URI for the given task.
        # Should return a valid URI or empty string.
        # E.g.: 'http://myfarm/status/jobid20'
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # work_item     -  pdg.WorkItem
        print("--- getStatusURI")
        return ""

    def endSharedServer(self, sharedserver_name):
        # Custom endSharedServer logic. Returns True on success, else False.
        #
        # The following variables are available:
        # self               -  A reference to the current pdg.Scheduler instance
        # sharedserver_name  -  shared server name

        return True

    def applicationBin(self, name, work_item):
        if name == 'python':
            return self._pythonBin()
        elif name == 'hython':
            return self._hythonBin()

def registerTypes(type_registry):
    type_registry.registerScheduler(RushScheduler)

print("\033[1m--- rushscheduler.py loaded ---\033[0m")
