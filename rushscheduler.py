#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

#
# rushscheduler.py - handle scheduling houdini (20.x) PDG workitems through rush jobs
#

# OS
import os,sys,re,time,json,threading,re

# HOUDINI
import pdg
from pdg.scheduler import PyScheduler
from pdg.job.callbackserver import CallbackServerMixin

class RushError(Exception):
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

# TODO: Are we supposed to define this?
#   def workItemResultServerAddr(self):
#       return None

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

    def LoadText(self, filename):
        '''
        Load text from file and return the file's contents.
        May raise IOError exceptions on file errors.
        '''
        fd  = open(filename, "r")
        out = fd.read()
        fd.close()
        return out

    def SubmitJob(self, submitinfo_str, job_temp_dir):
        '''
        Submit rush job.
        'submitinfo_str' is a multiline string containing 'rush -submit' commands.  At minimum:

            title   foo
            cpus    +any=1
            command some_command

        It's OK if 'frames' unspecified; they can be added later. Docs for info:
        https://www.seriss.com/rush.103.00/rush/rush-submit-cmds.html#Submit%20Command%20Reference

        Returns (jobid,output), where:
            On success:
                -- 'jobid' has submitted jobid
                -- 'output' has all output from 'rush -submit'
            On failure:
                -- 'jobid' is ""
                -- 'output' has error messages from rush
        '''
        print("DEBUG: SubmitJob(): submitinfo:\n---\n%s---" % submitinfo_str)

        # Save submitinfo to a file
        submitinfo_file = job_temp_dir + "/submit"
        fd = open(submitinfo_file, "w")
        fd.write(submitinfo_str)
        fd.close()
        # Submit job
        out = job_temp_dir + "/submit.out"
        err = job_temp_dir + "/submit.err"
        cmd = ("rush -submit "
              + " < "  + submitinfo_file
              + " 2> " + err
              + " > "  + out
              )
        os.system(cmd)      # actual submit
        msg = self.LoadText(out) + self.LoadText(err) # Read back submit results
        # Check for errors
        r = re.search(r"RUSH_JOBID.(\S+)", msg) 
        if r is None: return("", msg)       # Failed?
        jobid = r.groups()[0]               # Success?
        return (jobid, msg)

    def StartWorkItemJob(self, workitem):
        '''
        Start job to manage workitems
        '''
        # Create a new tempdir for this workitem
        self.job["jobdir"] = self.tempDir(False) + "/" + workitem.name
        os.mkdir(self.job["jobdir"])
        # Expand python command based on workitem's PDG_PYTHON variable
        python_cmd = self.expandCommandTokens("__PDG_PYTHON__", workitem)
        # Create render script
        renderscript_filename = self.job["jobdir"] + "/rush-render.py"
        print("    Writing render script: %s" % renderscript_filename)
        self.SaveRenderScript(renderscript_filename, python_cmd, self.job["jobdir"])
        # Create logdir
        self.job["logdir"] = self.job["jobdir"] + "/logs"
        print("    Creating logdir: %s" % self.job["logdir"])
        os.mkdir(self.job["logdir"], 0o777)
        # Create submitinfo
        # TODO: Build job submitinfo
        job_title  = "HOUDINI:" + workitem.name + "/" + self.sched_name
        job_cpus   = "iron=1"                 # TODO
        submitinfo = ("title   %s\n"    % job_title
                     +"cpus    %s\n"    % job_cpus
                     +"command %s %s\n" % (python_cmd, renderscript_filename)
                     +"logdir  %s\n"    % self.job["logdir"]
                     )
        # SUBMIT RUSH JOB
        #
        # TODO: Should we trap exceptions and pop dialog ourself,
        #       or let houdini handle raw exception itself? The latter for now..
        #
        print("--- Starting rush job: %s" % job_title)
        return self.SubmitJob(submitinfo, self.job["jobdir"])

    def QueueWorkItem(self, workitem):
        '''
        Queue the work item for rendering by the existing rush job.
        Save json file for the rush work item, add a frame to the rush job
        to manage it.

        Returns:
            On Success -- returns pdg.scheduleResult.CookSucceeded
            On Failure -- returns pdg.scheduleResult.CookFailed
        '''
        # TODO:
        #    1) thread lock
        #    2) add work item to self.job
        #    3) unlock
        #
        # Thread will take care of adding the frame every 5 secs or some such.
        # onCookStart() should start the child thread -- we should check if it hasn't.
        #

        rushframepad = self.frame_fmt % int(workitem.id)    # e.g. 1 -> "00001"

        # Put all workitem data into a dict we can save as a json object
        workitem_data = { "job_env":
                            {
                                # "PDG_RESULT_SERVER": str(self.workItemResultServerAddr()),  # TODO: need to configure houdini?
                                "PDG_ITEM_NAME":     str(workitem.name),
                                "PDG_ITEM_ID":       str(workitem.id),
                                "PDG_DIR":           str(self.workingDir(False)),
                                "PDG_TEMP":          str(self.tempDir(False)),
                                "PDG_SCRIPTDIR":     str(self.scriptDir(False))
                            },
                          "command":       str(self.expandCommandTokens(workitem.command, workitem)),
                          "rush_frame":    "%05d" % workitem.id,
                          "sched_name":    self.sched_name,
                          "workitem_name": workitem.name,
                        }

        # Save rush workitem json file
        json_filename = "%s/rush-%s.json" % (self.job["jobdir"], rushframepad)
        print("    Writing json file: %s" % json_filename)
        try: self.SaveJSON(json_filename, workitem_data)
        except IOError as e:
            print("ERROR: SaveJSON() could not create '%s': %s" % (json_filename, e.strerror))
            return pdg.scheduleResult.CookFailed

        # Add rush frame (workitem.id) to render the work item
        ret = os.system("rush -af %d %s" % (workitem.id, self.job["jobid"]))
        if ret != 0:
            # Failed to add rush frame?
            print("ERROR: Could not add rush frame %d to jobid %s" % (workitem.id, self.job["jobid"]))
            return pdg.scheduleResult.CookFailed

        # Return success
        return pdg.scheduleResult.CookSucceeded

    def onStartCook(self, static, cook_set):
        '''
        Custom onStartCook logic. Returns True if started.
        The following variables are available:

            self          -  A reference to the current pdg.Scheduler instance
            static        -  True if static cook
            cook_set      -  Set of nodes to cook. First item in list is a "Processor" instance; see:
                             https://www.sidefx.com/docs/houdini/tops/pdg/Processor.html
        '''
        print("--- onStartCook [%s]" % self.sched_name)

        # New scheduler?
        # TODO:
        #     -- Should maybe dump last rush job (if any)
        #     -- Start new child thread if none already running
        #     -- Lock before clearing these instance variables
        #

        # Reset this scheduler's dict
        self.job["lock"]       = threading.Semaphore()  # child thread semaphore lock
        self.job["child_id"]   = None                   # child thread id
        self.job["jobid"]      = None                   # current rush jobid for workitems
        self.job["rush_frames"] = []                    # cache of rush frames to be added to job by child thread

        # Houdini advises these lines..
        wd = self["pdg_workingdir"].evaluateString()
        self.setWorkingDir(wd, wd)
        if not self.isCallbackServerRunning():
            self.startCallbackServer()

        return True

    def onSchedule(self, workitem):
        # ERCO: This runs to start new work items (Rush frames).
        #
        # Custom onSchedule logic. Returns pdg.ScheduleResult.
        #
        # The following variables are available:
        # self         -  A reference to the current pdg.Scheduler instance
        # workitem     -  The pdg.WorkItem to schedule

        print("--- onSchedule [%s]" % self.sched_name)

        rushframepad = self.frame_fmt % int(workitem.id)    # e.g. 1 -> "00001"

        print("         workitem.id: %d" % workitem.id)
        print("          rush frame: %s" % rushframepad)
        print("       workitem.name: %s" % workitem.name)
        print("    workitem.tempdir: %s" % str(self.tempDir(False)))

        # Ensure directories exist and serialize the work item
        self.createJobDirsAndSerializeWorkItems(workitem)

        # No job submitted yet? submit one
        if self.job["jobid"] == None:
            # Start rush job to manage work items
            (jobid, msg) = self.StartWorkItemJob(workitem)
            # Show output of 'rush -submit', regardless of success|failure
            print(msg)
            if jobid == "":
                # Submit failed?
                print("ERROR: 'rush -submit' failed:\n%s" % msg)
                return pdg.scheduleResult.CookFailed
            else:
                # Submit succeeded? save jobid..
                self.job["jobid"] = jobid

        # Queue the work item
        #     Saves work item as a json file, adds a rush frame to the job
        #     to schedule rendering the item.
        #
        self.QueueWorkItem(workitem)


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

        print("--- onScheduleStatic:\n"
             +"   dependents: %s\n" % repr(dependents)
             +"  ready_items: %s" % repr(ready_items)
             )

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

    def getLogURI(self, workitem):
        # Custom getLogURI logic. Returns the farm's log URI for the given task.
        # Should return a valid URI or empty string.
        # E.g.: 'file:///myfarm/tasklogs/jobid20.log'
        #
        # The following variables are available:
        # self         -  A reference to the current pdg.Scheduler instance
        # workitem     -  pdg.WorkItem

        print("--- getLogURI")
        return "file:///var/tmp/foo.log"    # DEBUGGING

    def getStatusURI(self, workitem):
        # Custom getStatusURI logic. Returns the farm's status URI for the given task.
        # Should return a valid URI or empty string.
        # E.g.: 'http://myfarm/status/jobid20'
        #
        # The following variables are available:
        # self         -  A reference to the current pdg.Scheduler instance
        # workitem     -  pdg.WorkItem
        print("--- getStatusURI")
        return ""

    def endSharedServer(self, sharedserver_name):
        # Custom endSharedServer logic. Returns True on success, else False.
        #
        # The following variables are available:
        # self               -  A reference to the current pdg.Scheduler instance
        # sharedserver_name  -  shared server name

        return True

    def SaveRenderScript(self, filename, python_cmd, job_temp_dir):
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

job_temp_dir       = "''' + job_temp_dir      + '''"   # Rush job temp dir
frame_fmt          = "''' + self.frame_fmt    + '''"   # Rush frame format, e.g. "%04d"

# If env var not set and no frame specified on cmd line? fail
if "RUSH_FRAME" not in os.environ and len(sys.argv) <= 1:
    print("ERROR: RUSH_FRAME env var is unset and no frame parameter specified")
    sys.exit(1)

# Rush frame#
if len(sys.argv) > 1: framepad = frame_fmt % int(sys.argv[1])
else:                 framepad = frame_fmt % int(os.environ["RUSH_FRAME"])

# Load JSON file for this frame / workitem
jsonfile = job_temp_dir + "/rush-%s.json" % framepad
print("--- rush-render: Loading json frame %s: %s" % (framepad, jsonfile))
workitem_data = LoadJSON(jsonfile)
print("--- rush-render: workitem name: %s" % workitem_data["workitem_name"])

# Merge current environment with vars from workitem
job_env = os.environ.copy()
job_env.update(workitem_data["job_env"])	# merge

# Execute the houdini workitem command
print("--- rush-render: Executing: %s" % workitem_data["command"])
sys.stdout.flush()
sys.stderr.flush()
exitcode = subprocess.call(workitem_data["command"], shell=True, env=job_env)

# Check for success
if exitcode != 0:
    print("--- rush-render: FAILED (Exit code %d)" % exitcode)
    sys.exit(1)		# tell rush we failed

print("--- rush-render: SUCCEEDS")
sys.exit(0)		# tell rush we succeeded
''')
        fd.flush()
        fd.close()
        os.sync()
        os.chmod(filename, 0o775)    # all read/exec, user/grp write

    def applicationBin(self, name, workitem):
        if name == 'python':
            return self._pythonBin()
        elif name == 'hython':
            return self._hythonBin()

def registerTypes(type_registry):
    type_registry.registerScheduler(RushScheduler)

print("\033[1m--- rushscheduler.py loaded ---\033[0m")
