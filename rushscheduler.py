#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

# NAME:         pythonscheduler.py ( Python )

# OS
import os,sys,re,time,json,threading

# HOUDINI
import pdg
from pdg.scheduler import PyScheduler
from pdg.job.callbackserver import CallbackServerMixin

def SaveJSON(filename, data):
    '''
    Save json data to 'filename'
    May raise IOError exceptions on file errors.
    '''
    fd = open(filename, "w")
    fd.write(json.dumps(data,sort_keys=True, indent=4))
    fd.close()

def LoadJSON(filename):
    '''
    Load json data from 'filename', returns data on success.
    May raise IOError exceptions on file errors.
    Returns data loaded from json file.
    '''
    fd   = open(filename, "r")
    data = json.load(fd)
    fd.close()
    return data

class RushScheduler(CallbackServerMixin, PyScheduler):
    """
    Rush scheduler implementation
    """
    jobs = {}                     # job data instance (indexed by rushsched name)
    sched_name = None

    def __init__(self, scheduler, name):
        """
        __init__(self, pdg.Scheduler) -> NoneType

        Initializes the Scheduler with a C++ scheduler reference and name
        """
        print("-- INIT:")
        PyScheduler.__init__(self, scheduler, name)
        CallbackServerMixin.__init__(self, True)
        self.sched_name = name            # save our instance name for later

    @classmethod
    def templateName(cls):
        return "python_scheduler"

    def onStartCook(self, static, cook_set):
        # Custom onStartCook logic. Returns True if started.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # static        -  True if static cook
        # cook_set      -  Set of nodes to cook

        # repr(cook_set) is a list, first item is a "Processor", so see:
        # https://www.sidefx.com/docs/houdini/tops/pdg/Processor.html

        print("--- onStartCook [%s]" % self.sched_name)

        # New scheduler?
        name = self.sched_name
        if name not in self.jobs:
            self.jobs[name] = {}     # new dict for this scheduler

        # Reset this scheduler's dict
        self.jobs[name]["lock"]       = threading.Semaphore()
        self.jobs[name]["child_id"]   = None
        self.jobs[name]["jobid"]      = None
        self.jobs[name]["work_items"] = []

        wd = self["pdg_workingdir"].evaluateString()
        self.setWorkingDir(wd, wd)

        if not self.isCallbackServerRunning():
            self.startCallbackServer()

        return True

    def onSchedule(self, work_item):
        # ERCO: This runs to start new work items (Rush frames).
        #
        # Custom onSchedule logic. Returns pdg.ScheduleResult.
        #
        # The following variables are available:
        # self          -  A reference to the current pdg.Scheduler instance
        # work_item     -  The pdg.WorkItem to schedule

        print("--- onSchedule [%s]" % self.sched_name)
        print("    workitem.id: %d" % work_item.id)
        print("     rush frame: %04d" % work_item.id)

        # Ensure directories exist and serialize the work item
        self.createJobDirsAndSerializeWorkItems(work_item)

        # expand the special __PDG_* tokens in the work item command
        item_command = self.expandCommandTokens(work_item.command, work_item)

        # add special PDG_* variables to the job's environment
        temp_dir = str(self.tempDir(False))

        # Put all workitem data into a dict we can save as a json object
        workitem_data = { "job_env":
                            {
                                "PDG_RESULT_SERVER": str(self.workItemResultServerAddr()),
                                "PDG_ITEM_NAME":     str(work_item.name),
                                "PDG_ITEM_ID":       str(work_item.id),
                                "PDG_DIR":           str(self.workingDir(False)),
                                "PDG_TEMP":          temp_dir,
                                "PDG_SCRIPTDIR":     str(self.scriptDir(False))
                            },
                          "command": item_command,
                          "rush_frame": "%04d" % work_item.id,
                          "sched_name": self.sched_name
                        }

        json_filename = "%s/rush-%04d.json" % (temp_dir, work_item.id)
        try:
            SaveJSON(json_filename, workitem_data)
        except IOError as e:
            print("ERROR: SaveJSON() could not create '%s': %s" % (json_filename, e.strerror))
            return pdg.scheduleResult.CookFailed

        print("    Wrote json file: %s" % json_filename)

        # TODO: Queue work item for child thread
        return pdg.scheduleResult.CookSucceeded

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
