#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

# NAME:         pythonscheduler.py ( Python )

# OS
import os,sys,re,time,json,threading

# HOUDINI
import pdg
from pdg.scheduler import PyScheduler
from pdg.job.callbackserver import CallbackServerMixin

class RushException(Exception):
    pass

class RushScheduler(CallbackServerMixin, PyScheduler):
    """
    Rush scheduler implementation
    """
    job = {}                     # job data
    sched_name = None
    frame_fmt  = "%05d"          # frame padding format char TODO: Load from rush.conf!

    def __init__(self, scheduler, name):
        """
        __init__(self, pdg.Scheduler) -> NoneType

        Initializes the Scheduler with a C++ scheduler reference and name
        """
        print("-- INIT: [%s]" % name)
        PyScheduler.__init__(self, scheduler, name)
        CallbackServerMixin.__init__(self, True)
        self.sched_name = name            # save our instance name for later

    @classmethod
    def templateName(cls):
        return "python_scheduler"

    def SaveJSON(self, filename, data):
        '''
        Save json data to 'filename'
        May raise IOError exceptions on file errors.
        '''
        fd = open(filename, "w")
        fd.write(json.dumps(data,sort_keys=True, indent=4))
        fd.flush()      # (nfs) flush write
        fd.close()
        os.sync()       # (nfs) ensure flush dirty buffers to nodes

    def LoadJSON(self, filename):
        '''
        Load json data from 'filename', returns data on success.
        May raise IOError exceptions on file errors.
        Returns data loaded from json file.
        '''
        fd   = open(filename, "r")
        data = json.load(fd)
        fd.close()
        return data

    def SaveRenderScript(self, filename, python_cmd, temp_dir):
        '''
        Create the rush render script that loads the json file and runs
        the houdini work item.
        '''
        fd = open(filename, "w")
        # NOTE: Beware escaped chars (e.g. \n) are expanded even inside triple quotes!
        fd.write('#!' + python_cmd + '''
import os,sys,json,subprocess

# Rush render script for rushscheduler generated work items

def LoadJSON(filename):
    'Load a JSON file, return the resulting data'
    fd = open(filename, "r")
    data = json.load(fd)
    fd.close()
    return data

temp_dir  = "''' + temp_dir       + '''"     # Houdini tempdir for this job
frame_fmt = "''' + self.frame_fmt + '''"     # Rush frame format, e.g. "%04d"

# If env var not set and no frame specified on cmd line? fail
if "RUSH_FRAME" not in os.environ and len(sys.argv) <= 1:
    print("ERROR: RUSH_FRAME env var is unset and no frame parameter specified")
    sys.exit(1)

# Rush frame#
if len(sys.argv) > 1: framepad = frame_fmt % int(sys.argv[1])
else:                 framepad = frame_fmt % int(os.environ["RUSH_FRAME"])

# Load JSON file for this frame / workitem
jsonfile = temp_dir + "/rush-%s.json" % framepad
print("--- Loading json frame %s: %s" % (framepad, jsonfile))
workitem_data = LoadJSON(jsonfile)
print("    workitem name: %s\\n" % workitem_data["workitem_name"])

# Merge current environment with vars from workitem
job_env = os.environ.copy()
job_env.update(workitem_data["job_env"])	# merge

# Execute the houdini workitem command
print("Executing: %s" % workitem_data["command"])
sys.stdout.flush()
sys.stderr.flush()
exitcode = subprocess.call(workitem_data["command"], shell=True, env=job_env)

# Check for success
if exitcode != 0:
    print("FAILED (Exit code %d)" % exitcode)
    sys.exit(1)		# tell rush we failed

print("SUCCEEDS")
sys.exit(0)		# tell rush we succeeded
''')
        fd.flush()
        fd.close()
        os.sync()
        os.chmod(filename, 0o775)    # all read/exec, user/grp write

    def SubmitJob(self, submitinfo):
        '''
        Submit rush job.
        'submitinfo' is a multiline string containing 'rush -submit' commands.  At minimum:

            title foo
            cpus +any=1
            command some_command

        It's OK to leave 'frames' unspecified; they can be added later. See docs for info:
        https://www.seriss.com/rush.103.00/rush/rush-submit-cmds.html#Submit%20Command%20Reference

        Returns jobid, or throws an exception on error.
        '''
        print("DEBUG: SubmitJob(): submitinfo:\n---\n%s---\n" % submitinfo)
        #DEBUG raise RushException("could not submit job: TESTING")

    def QueueWorkItem(self, workitem_data):
        # TODO:
        #    1) thread lock
        #    2) add work item to self.job
        #    3) unlock
        #
        # Thread will take care of adding the frame every 5 secs or some such.
        #
        # If rush job and child thread isn't started, start them!
        # Or if parent should do this, check if it did and fail if not.
        #
        return ""

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

        # Reset this scheduler's dict
        self.job["lock"]       = threading.Semaphore()  # child thread semaphore lock
        self.job["child_id"]   = None                   # child thread id
        self.job["jobid"]      = None                   # current rush jobid for workitems
        self.job["rush_frames"] = []                    # cache of rush frames to be added to job by child thread

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
        print("      workitem.id: %d" % work_item.id)
        print("       rush frame: %04d" % work_item.id)
        print("    workitem.name: %s" % work_item.name)

        # Ensure directories exist and serialize the work item
        self.createJobDirsAndSerializeWorkItems(work_item)

        # expand the special __PDG_* tokens in the work item command
        item_command = self.expandCommandTokens(work_item.command, work_item)

        # add special PDG_* variables to the job's environment
        temp_dir   = str(self.tempDir(False))
        python_cmd = self.expandCommandTokens("__PDG_PYTHON__", work_item)
        renderscript_filename = temp_dir + "/rush-render.py"

        # No job submitted yet? submit one
        if self.job["jobid"] == None:
            # Write the render script
            self.SaveRenderScript(renderscript_filename, python_cmd, temp_dir)
            print("    Wrote render script: %s" % renderscript_filename)
            # TODO: Build job submitinfo
            job_title   = "testing"       # TODO
            job_cpus    = "linuxbox99=1"  # TODO
            submitinfo = ("title   %s\n" % job_title +
                          "cpus    %s\n" % job_cpus  +
                          "command %s %s\n" % (python_cmd, renderscript_filename))
            # SUBMIT RUSH JOB
            #
            # TODO: Not sure if we should trap exceptions and pop a dialog,
            #       or simply let houdini handle it as an error in the gui.
            #       The latter for now..
            #
            self.job["jobid"] = self.SubmitJob(submitinfo)      # let houdini trap exceptions

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
                          "command":       item_command,
                          "rush_frame":    "%05d" % work_item.id,
                          "sched_name":    self.sched_name,
                          "workitem_name": work_item.name,
                        }

        frame_str = self.frame_fmt % int(work_item.id)    # e.g. 1 -> "00001"
        json_filename = "%s/rush-%s.json" % (temp_dir, frame_str)
        try:
            self.SaveJSON(json_filename, workitem_data)
            print("    Wrote json file: %s" % json_filename)
        except IOError as e:
            print("ERROR: SaveJSON() could not create '%s': %s" % (json_filename, e.strerror))
            return pdg.scheduleResult.CookFailed

        self.QueueWorkItem(workitem_data)

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
