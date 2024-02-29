#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

'''
  rushscheduler.py - handle scheduling houdini (20.x) PDG work_items through rush jobs

      VERS AUTHOR          DATE         DESCRIPTION
      1.00 erco@seriss.com Feb 04 2024  Initial implementation

  Workitem Files:
      temp_dir/<work_item_name>/json/<rush_frame#>.json  - rush frame json files (contains env settings and work_item command)
      temp_dir/<work_item_name>/status/<rush_frame#>.txt - rush frame status files ("Que", "Run", "Done", "Fail")
      temp_dir/<work_item_name>/logs/<rush_frame#>       - rush frame log files - stdout/stderr from renderer
'''

# OS
import os,sys,re,json

# HOUDINI
import pdg
from pdg.scheduler import PyScheduler, convertEnvMapToUTF8
from pdg.job.callbackserver import CallbackServerMixin

#UNUSED class RushError(Exception):
#UNUSED     '''Rush exception class'''
#UNUSED     pass

class RushScheduler(CallbackServerMixin, PyScheduler):
    '''
    Rush scheduler implementation
    '''
    def __init__(self, scheduler, name):
        '''__init__(self, pdg.Scheduler) -> NoneType
           Initializes the Scheduler with a C++ scheduler reference and name

           GUI Parameters:
             x Job Title   -- rush_title      (default: PDG-$HIPNAME:$OS)
             x RAM         -- rush_ram        (default: 16000)
             x Priority    -- rush_priority   (default: 10)
             x Host CPUS   -- rush_cpus       (default: +H20=30)
             x Nevercpus   -- rush_nevercpus  (default: "")
             x Max Cpus    -- rush_maxcpus    (default: 300)
             x Autodump    -- rush_autodump   (default: off)  off,done,donefail,fail
             x Verbose     -- rush_verbose    (default: off)  off,on
             x Tick Period -- rush_tickperiod (default: 2)    1sec ... 10sec (integer)
        '''
        print("-- INIT: [%s]" % name)
        PyScheduler.__init__(self, scheduler, name)
        CallbackServerMixin.__init__(self, True)

        # job data
        self.jobdir          = None      # used by JobDirectory()
        self.jobid           = None      # used by RushJobid()
        self.sched_name      = name      # save our instance name for later
        self.frame_fmt       = "%05d"    # frame padding format char TODO: Load from rush.conf on init!
        self.rushframe_cache = []
        self.work_item_ids   = []        # list of scheduled work_item id's
        self.parmprefix      = "rush"    # access UI elements
        self.autodump        = False     # if true, we automatically dump jobs TODO: Change this to use Jon's UI params
        # XXX: Apparently UI elements aren't accessable within __init__()

    @classmethod
    def templateName(cls):
        '''(TBD: Needs description)'''
        return "rushscheduler"

    @classmethod
    def templateBody(cls):
        return json.dumps({
            "name": "rushscheduler",
            "parameters": [
                {
                    "name" : "rush_inheritlocalenv",
                    "type" : "Integer",
                    "size" : 1,
                    "tag" : ["pdg::scheduler"]
                },
                {
                    "name" : "rush_envunset",
                    "type" : "String",
                    "tag" : ["pdg::scheduler"]
                },
                # multi of [hqueue_envname#, hqueue_envvalue#]
                {
                    "name" : "rush_envmulti",
                    "label" : "Environment",
                    "type" : "Integer",
                    "tag" : ["pdg::scheduler"]
                }
            ]
        })
        
    @staticmethod
    def SaveJSON(filename, data):
        '''Save json data to 'filename'
           May raise IOError exceptions on file errors.
        '''
        fd = open(filename, "w")
        fd.write(json.dumps(data, sort_keys=True, indent=4))
        fd.flush()      # (nfs) flush write
        fd.close()
        os.sync()       # (nfs) ensure flush dirty buffers to nodes

    @staticmethod
    def LoadJSON(filename):
        '''Load json data from 'filename', returns data on success.
           May raise IOError exceptions on file errors.
           Returns data loaded from json file.
        '''
        fd   = open(filename, "r")
        data = json.load(fd)
        fd.close()
        return data

    @staticmethod
    def LoadText(filename):
        '''Load text from file and return the file's contents.
           May raise IOError exceptions on file errors.
        '''
        fd  = open(filename, "r")
        out = fd.read()
        fd.close()
        return out

    def submitAsJob(self, graph_file, node_name):
        """
        Called by pressing the 'Submit as Job' button on the scheduler node UI.
        Creates a job which cooks that TOP graph using hython.
        """
        print("----------------------------------")
        print("------------- Hello --------------")
        print("----------------------------------")
        return None,None

    def JobDirectory(self):
        '''Returns the job's working directory.
           This is the houdini instance's "temp_dir" + subdirectory named after
           the scheduler's instance name to ensure different rushschedule instances
           don't overlap rush jobs.

        Example: /some/where/pdgtemp/31768/pdg_rushscheduler1
                 ------------------- ----- ------------------
                   |                  |    |__ scheduler instance name
                   |                  |__ houdini's PID
                   |__ somewhere on your network drive
        '''
        return self.jobdir

    def LogDirectory(self):
        '''Returns the job's rush log directory for this job.
           This is where rush redirects the frame logs, which contains the
           render's stdout/stderr messages during rendering.

        Example: /some/where/pdgtemp/31768/pdg_rushscheduler1/logs
                 -----------         ----- ------------------ ----
                   |                  |    |__ scheduler instance name
                   |                  |__ houdini's PID
                   |__ somewhere on your network drive
        '''
        return self.JobDirectory() + "/logs"

    def LogFilename(self, frame):
        '''Returns the filename of a rush frame's frame log.'''
        return self.LogDirectory() + "/" + (self.frame_fmt % int(frame))

    def StatusDirectory(self):
        '''Returns the directory name of the rush frame status directory.
           This directory contains status files our rush render script
           updates for onTick() to get progress during rendering.
        '''
        return self.JobDirectory() + "/status"

    def StatusFilename(self, frame):
        '''Returns the filename of a rush frame's status file.
           Status files are updated while frame is rendering:
             "Run"   -- frame is running
             "Done"  -- frame succeeded
             "Fail"  -- frame failed

        Example: /some/where/pdgtemp/31768/pdg_rushscheduler1/status/0005.txt
                 --------------------------------------------------- --------
                 Status directory                                     status filename.
                                                                      One per rush-frame.
        '''
        return self.StatusDirectory() + "/" + (self.frame_fmt % frame) + ".txt"

    def GetStatus(self, frame):
        '''Returns the status of a rush frame.'''
        statusfile = self.StatusFilename(frame)
        if not os.path.exists(statusfile): return "Que"
        fd = open(statusfile, "r")
        status = fd.read()
        fd.close()
        return status

    def JSONDirectory(self):
        '''Returns the job's json directory.'''
        return self.JobDirectory() + "/json"

    def JSONFilename(self, frame):
        '''Returns the filename of a rush frame's json file.

        When houdini sends a work item to the farm for "cooking",
        work_item is assigned a rush frame number (the work item's ID).
        The work_item's info needed to run the command on the farm is
        saved in a json file.

        These json files are written when houdini schedules the work_item,
        and read by the rush render script during rendering, containing:

            > The environment variables to set
            > The command to run
            > Any other info needed during rendering.

        Example: /some/where/pdgtemp/31768/json/0005.json
                 ------------------------- ---- ---------
                 houdini temp dir          |    json file
                                    json directory
        '''
        return self.JSONDirectory() + "/" + (self.frame_fmt % frame) + ".json"

    def CreateJobDirectories(self):
        '''Creates the job directories needed by a rush job:
              > jobdir,     e.g. /some/where/<houdini-PID>/
              > json dir,   e.g. /some/where/<houdini-PID>/json/
              > log dir,    e.g. /some/where/<houdini-PID>/logs/
              > status dir, e.g. /some/where/<houdini-PID>/status/
        '''
        dirs = [ [ "Creating job directory",    self.JobDirectory()    ],
                 [ "Creating json directory",   self.JSONDirectory()   ],
                 [ "Creating log directory",    self.LogDirectory()    ],
                 [ "Creating status directory", self.StatusDirectory() ] ]
        for (msg, dirpath) in dirs:
            print("%28s: %s" % (msg, dirpath))
            if not os.path.isdir(dirpath):
                os.mkdir(dirpath, 0o777)

    def SetRushJobid(self, jobid):
        '''Set the current rush jobid. Use None to clear.'''
        self.jobid = jobid

    def RushJobid(self):
        '''Return the current rush jobid, or None if no job is running.'''
        return self.jobid

    def DumpRushJob(self):
        '''Dump the current rush job (if any), and sets RushJobid() to None.'''
        if self.RushJobid() != None:
            sys.stdout.write("\033[1m")     # highlight output
            sys.stdout.flush()
            os.system("rush -dump " + self.RushJobid())
            sys.stdout.write("\033[0m")
            sys.stdout.flush()
            self.SetRushJobid(None)

    def StopRushJob(self):
        '''Pause the rush job, and change all Run frames to Que.
           Does not dump, so user can inspect the job.
        '''
        if self.RushJobid() != None:
            os.system("rush -pause "   + self.RushJobid())
            os.system("rush -que Run " + self.RushJobid())

    def SubmitJob(self, submitinfo_str, job_temp_dir):
        '''Submit rush job.
           'submitinfo_str' is a multiline string containing 'rush -submit' commands.  At minimum:

                title   foo
                cpus    +any=1
                command some_command

           It's OK if 'frames' unspecified; they can be added later. Docs for info:
           https://www.seriss.com/rush.103.00/rush/rush-submit-cmds.html#Submit%20Command%20Reference

           Returns (jobid,output), where:
               On success:
                   -- 'jobid' is submitted jobid
                   -- 'output' has stdout/err from 'rush -submit'
               On failure:
                   -- 'jobid' is None
                   -- 'output' has stdout/err from 'rush -submit'
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
        if r is None: return(None, msg)     # Failed?
        jobid = r.groups()[0]               # Success?
        return (jobid, msg)

    def StartWorkItemJob(self, work_item):
        '''Start job to manage work_items
           Returns (jobid, output), where:
               On success:
                   -- 'jobid' is submitted jobid
                   -- 'output' has stdout/err from 'rush -submit'
               On failure:
                   -- 'jobid' is None
                   -- 'output' has stdout/err from 'rush -submit'
        '''
        # Create job dirs (json, logs, status)
        self.CreateJobDirectories()

        # Create render script
        python_cmd = self.expandCommandTokens("__PDG_PYTHON__", work_item)

        renderscript_filename      = self.JobDirectory() + "/rush_render.py"
        print("%26s: %s" % ("Creating render script", renderscript_filename))
        self.SaveRenderScript(renderscript_filename, python_cmd, self.JobDirectory())

        renderscript_pre_filename  = self.JobDirectory() + "/rush_render_pre.py"
        print("%26s: %s" % ("Creating render script pre", renderscript_pre_filename))
        self.SaveRenderScriptCustom(renderscript_pre_filename,  self["rush_render_pre"].evaluateString())

        renderscript_post_filename = self.JobDirectory() + "/rush_render_post.py"
        print("%26s: %s" % ("Creating render script post", renderscript_post_filename))
        self.SaveRenderScriptCustom(renderscript_post_filename, self["rush_render_post"].evaluateString())

        # Create submitinfo
        job_title  = self["rush_title"].evaluateString()
        maxcpus    = self["rush_maxcpus"].evaluateString()
        nevercpus  = self["rush_nevercpus"].evaluateString()
        ram        = self["rush_ram"].evaluateString()
        submitinfo = ("title      %s\n"    % job_title
                     +"priority   %s\n"    % self["rush_priority"].evaluateString()
                     +"cpus       %s\n"    % self["rush_cpus"].evaluateString()
                     +"autodump   %s\n"    % self["rush_autodump"].evaluateString()
                     +"command    %s %s\n" % (self["rush_pythonexe"].evaluateString(), renderscript_filename)
                     +"logdir     %s\n"    % self.LogDirectory()
                     +"frameflags keepnotes\n"
                     )
        # Don't specify maxcpus to rush if the field is blank
        if maxcpus   != "": submitinfo += "maxcpus   %s\n" % maxcpus
        if nevercpus != "": submitinfo += "nevercpus %s\n" % nevercpus
        if ram       != "": submitinfo += "ram       %s\n" % ram

        # SUBMIT RUSH JOB
        #
        # TODO: Should we trap exceptions and pop dialog ourself,
        #       or let houdini handle raw exception itself? The latter for now..
        #
        print("---       Starting rush job: %s" % job_title)
        return self.SubmitJob(submitinfo, self.JobDirectory())


    def FlushFrames(self):
        '''Called at intervals to flush the rush frame buffer cache out to rush.
           Compress frame list into a single 'rush -af' command frame range, if possible.
        '''
        # Nothing to do? early exit..
        if len(self.rushframe_cache) == 0: return
        ranges   = ""
        item_ids = []
        rushcmds = []
        for i in range(0, len(self.rushframe_cache)):
            frame         = self.rushframe_cache[i]
            id_val        = frame["id"]
            frm_sortorder = frame["frm_sortorder"]
            frm_notes     = frame["frm_notes"].replace(" ", "_")
            item_ids.append(id_val)
            # Build the 'rush -af' commands
            if ranges != "": ranges += " "
            ranges += "%d@%s:%s" % (id_val, frm_sortorder, frm_notes)
            # Enforce a line length limit for "rush -lf" command
            if len(ranges) > 2048:
                rushcmds.append("rush -af %s %s" % (ranges, self.RushJobid()))
                ranges = ""

        # Handle any remaining ranges for 'rush -lf' command
        if ranges != "":
            rushcmds.append("rush -af %s %s" % (ranges, self.RushJobid()))
            ranges = ""

        # Add the frame ranges to rush
        for i in range(0, len(rushcmds)):
            rushcmd = rushcmds[i]
            print("   Executing: %s" % rushcmd)
            if os.system(rushcmd) != 0:
                # Failed to add rush frame?
                emsg = "WARNING: Could not add frames to rush jobid %s: %s" % (self.RushJobid(), ranges)
                sys.stderr.write(emsg + "\n")
                return                          # Leave frames in cache; maybe command will work later

        # Tell onTick() to keep an eye on these rush frames (work item ids)
        self.work_item_ids += item_ids

        # empty cache
        self.rushframe_cache = []

    def PushRushFrame(self, rushframe):
        '''Add rush frame to buffer cache for later adding to rush job.
           This avoids running one 'rush -af' command per frame.
           Instead we collect the frames in a cache, then at regular intervals (onTick)
           run a single 'rush -af' command to add the cache of frames.
        '''
        self.rushframe_cache.append(rushframe)

    def QueueWorkItem(self, work_item):
        '''Queues work item for rendering in rush:
               > Saves a json file for the work_item
               > Adds a frame to the rush job to handle the work item
            Returns:
                On Success -- returns pdg.scheduleResult.CookSucceeded
                On Failure -- returns pdg.scheduleResult.CookFailed
        '''
        # Retreive the Houdini environment if the scheduler parms
        # tell us we want to pass it on the workers.
        job_env = self.resolveBaseEnvironment(self.parmprefix, None, work_item)

        # Add common job env vars
        job_env.update(
            {
                # For this list of names, see: "expandCommandTokens()" in
                # https://www.sidefx.com/docs/houdini/tops/pdg/Scheduler.html
                #
                "PDG_ITEM_NAME":     str(work_item.name),
                "PDG_ITEM_ID":       str(work_item.id),
                "PDG_DIR":           str(self.workingDir(False)),
                "PDG_TEMP":          str(self.tempDir(True)),
                "PDG_SCRIPTDIR":     str(self.scriptDir(False)),
                "PDG_HFS":           str(self.expandCommandTokens("__PDG_HFS__", work_item)), # ffmpegencodevideo1 node needs this
                # commented out to avoid RPC errors at end of renders
                # "PDG_RESULT_SERVER": str(self.workItemResultServerAddr()),
            }
        )

        # Work items can have custom environment variables, so ensure they are included
        if work_item:
            # check the exported attributes
            env_map = work_item.environment
            setmap = {}
            for var, val in list(env_map.items()):
                var = var.strip()#.encode('ascii', 'ignore')
                setmap[var] = str(val)#.strip().encode('ascii', 'ignore')
            job_env.update(setmap)

        # TODO: resolve path mapping
        # Un-comment this when we know the Rush scheduler handles path maps properly
        # self.resolvePathMapping(job_env)

        # rush env is supplied as multiparm of key:key
        job_env_dict, removekeys = self.resolveEnvParams(
            self.parmprefix, work_item, True)

        # process any removals
        for k in removekeys:
            if k in job_env:
                del job_env[k]

        job_env.update(job_env_dict)

        # ensure there is no unicode in the environment
        job_env = convertEnvMapToUTF8(job_env)

        # Put all work_item data into a dict we can save as a json object
        work_item_data = {
            "job_env": job_env,
            "command":        str(self.expandCommandTokens(work_item.command, work_item)),
            "rush_frame":     "%05d" % work_item.id,
            "sched_name":     self.sched_name,
            "work_item_name": work_item.name,
        }

        # Write json file for this work_item
        json_filename = self.JSONFilename(work_item.id)
        if self.verbose: print("    Writing json file: %s" % json_filename)
        try: self.SaveJSON(json_filename, work_item_data)
        except IOError as e:
            print("ERROR: SaveJSON() could not create '%s': %s" % (json_filename, e.strerror))
            return pdg.scheduleResult.CookFailed

        # Buffer the 'rush -af' operations - build a list of rush frames
        rushframe = { "id":            work_item.id,
                      "frm_sortorder": work_item.priority,   # save houdini frame priority as rush frame sort order
                      "frm_notes":     work_item.name }      # save node name as rush frame notes
        
        self.PushRushFrame(rushframe)

        # Return success
        return pdg.scheduleResult.CookSucceeded

    def onStartCook(self, static, cook_set):            # HOUDINI CALLBACK
        '''Custom onStartCook logic. Returns True if started.
           The following variables are available:

               self          -  A reference to the current pdg.Scheduler instance
               static        -  True if static cook
               cook_set      -  Set of nodes to cook. First item in list is a "Processor" instance; see:
                                https://www.sidefx.com/docs/houdini/tops/pdg/Processor.html
        '''
        _ = (static, cook_set)      # unused: use these later (pylint)
        print("--- onStartCook [%s]" % self.sched_name)

        # New scheduler?
        # TODO:
        #     -- Should maybe dump last rush job (if any)
        #     -- Start new child thread if none already running
        #     -- Lock before clearing these instance variables
        #

        # XXX: Can't do this in __init__() - bad port
        if self["rush_verbose"].evaluateString() == "on": self.verbose = True
        else:                                             self.verbose = False

        # Reset this scheduler's dict
        #TBD self.lock       = threading.Semaphore()  # child thread semaphore lock
        #TBD self.child_id   = None                   # child thread id
        if self.RushJobid() != None:        # TODO: Make controllable, but not with self.autodump
            self.DumpRushJob()

        self.SetRushJobid(None)           # clear jobid
        self.work_item_ids   = []         # clear schedule of work_item ids
        self.rushframe_cache = []         # clear frame cache

        # Set onTick() period
        self["pdg_tickperiod"] = self["rush_tickperiod"].evaluateFloat()
        self["pdg_maxitems"]   = 50

        # Houdini advises these lines..
        wd = self["pdg_workingdir"].evaluateString()
        self.setWorkingDir(wd, wd)
        if not self.isCallbackServerRunning():
            self.startCallbackServer()
        return True

    def onSchedule(self, work_item):            # HOUDINI CALLBACK
        '''Schedules work items (rush frames) to run on the render farm.
               self       --  A reference to the current pdg.Scheduler instance
               work_item  --  The pdg.WorkItem to schedule
           Returns a pdg.ScheduleResult.
        '''
        print("--- onSchedule [%s]" % self.sched_name)

        # Set the jobdir early in onSchedule()
        self.jobdir = self.tempDir(True) + "/" + self.sched_name

        rushframepad = self.frame_fmt % int(work_item.id)    # e.g. 1 -> "00001"
        if self.verbose:
            print("                work_item.id: %d\n" % work_item.id
                 +"          work_item.priority: %d\n" % work_item.priority
                 +"                  rush frame: %s\n" % rushframepad
                 +"              work_item.name: %s\n" % work_item.name
                 +"     work_item.tempdir[TRUE]: %s\n" % str(self.tempDir(True))
                 +"    work_item.tempdir[FALSE]: %s\n" % str(self.tempDir(False))
                 +"                 rush jobdir: %s\n" % self.JobDirectory()
                 )
        # Ensure directories exist and serialize the work item
        #    This creates the houdini tempdir and {scripts,logs,data} subdirs
        #
        self.createJobDirsAndSerializeWorkItems(work_item)

        # No rush job submitted yet? submit one
        if self.RushJobid() == None:
            # Start rush job (if not already):
            #    > creates rush job dir and {json,status,logs} subdirs
            #    > creates rush_render.py script
            #    > creates submitinfo file
            #    > submits rush job with no frames
            #
            (jobid, msg) = self.StartWorkItemJob(work_item)
            # Show output of starting rush job, regardless of success|failure
            print("\033[1m" + msg + "\033[0m")
            if jobid == None:
                # Submit failed?
                print("ERROR: 'rush -submit' failed:\n%s" % msg)
                return pdg.scheduleResult.CookFailed
            else:
                # Submit succeeded? save jobid..
                self.SetRushJobid(jobid)

        # Queue the work item
        #     Saves work item as a json file, adds a rush frame to the job
        #     to schedule the work_item for rendering..
        #
        self.QueueWorkItem(work_item)       # XXX: For some reason we can't "return" here.
                                            #      If we do, onTick() never gets called?!
                                            #      If we don't, pylint pissed about return values
                                            #      So for now, let pylint complain. But _why_ does:
                                            #      return self.QueueWorkItem(work_item)
                                            #      ..cause onTick to stop working? No errors..
        # return None                             # XXX: returning this works?!
        # return pdg.scheduleResult.CookSucceeded # XXX: this doesn't work?!
        return None

    def onScheduleStatic(self, dependencies, dependents, ready_items):          # HOUDINI CALLBACK
        '''Needs docs'''
        _ = (self, dependencies, dependents, ready_items)      # unused: use these later (pylint)
        print("--- onScheduleStatic: unused")

    def onStart(self):                              # HOUDINI CALLBACK
        '''Scheduler start callback'''
        _ = self    # unused (pylint)
        print("--- onStart: unused")

    def onStop(self):                               # HOUDINI CALLBACK
        '''Scheduler stop callback'''
        _ = self    # unused (pylint)
        print("--- onStop: unused")
        return True

    def onStopCook(self, cancel):                   # HOUDINI CALLBACK
        '''Custom onStopCook logic. Returns True if stopped.
           ERCO: Called on regular completion, or if Task "Cancel" button is hit.
               self    - A reference to the current pdg.Scheduler instance
               cancel  - True if cook was cancelled
        '''
        if cancel: print("--- onStopCook [CANCELLED]")
        else:      print("--- onStopCook [completed normally]")

        # User cancelled cooking? Dump the rush job
        # TODO: Change this to use Jon's UI params
        if cancel:
            if self.autodump: self.DumpRushJob()        # cancel + autodump==true? dump job
            else:             self.StopRushJob()        # cancel + autodump==false? stop job (leave queued)

        return True

    def onTick(self):                               # HOUDINI CALLBACK
        '''Called periodically when the graph is cooking.
           Can be used to check the state of running work items.
           Returns a pdg.tickResult.
               self  -  A reference to the current pdg.Scheduler instance
        '''
        from pdg import tickResult
        # print("--- onTick")

        # Walk the running work_items, check for frame status files
        #
        #    Change the state accordingly when we detect a change
        #        workItemStartCook(id) - Reports that the specified work item has started cooking.
        #        workItemFailed(id)    - Reports that the specified work has failed.
        #        workItemSuccess(id)   - Reports that the specified work item has succeeded.
        #
        sys.stdout.write("--- onTick(%s): " % self.sched_name)

        # Flush rush frame cache
        #    Handles running 'rush -af' on a block of accumulated rush frames between ticks
        #
        self.FlushFrames()

        # Loop thru work_item ids
        for i in range(0, len(self.work_item_ids)):
            work_id = self.work_item_ids[i]
            rushframe = work_id
            if rushframe < 0: rushframe = -rushframe    # negative means already logged as finished
            status    = self.GetStatus(rushframe)
            # print("%d) %s" % (work_id, status))
            c = '?'
            if   status == "Que":  c = "."
            elif status == "Run":  c = "\N{Black Large Circle}" # Unicode filled circle icon
            elif status == "Done": c = "\033[32m" + "\N{Heavy Check Mark}" + "\033[0m"   # Unicode check mark icon
            elif status == "Fail": c = "\033[31m" + "X" + "\033[0m"
            sys.stdout.write(c)

            if work_id > 0:          # Not unscheduled?
                # TODO: should probably remove from work_item_ids if Done or Fail
                if status == "Que":
                    pass
                elif status == "Run":
                    self.workItemStartCook(work_id, index=-1)                   # TODO: index=-1?
                elif status == "Done":
                    self.workItemSucceeded(work_id, index=-1, cook_duration=0)  # TODO: index=-1?, cook_duration?
                    self.work_item_ids[i] = -work_id     # switch to negative on completion
                elif status == "Fail":
                    self.workItemFailed(work_id,    index=-1)                   # TODO: index=-1?
                    self.work_item_ids[i] = -work_id     # switch to negative on completion
        print("")
        return tickResult.SchedulerReady

    def getLogURI(self, work_item):                               # HOUDINI CALLBACK
        '''Return the farm's log filename for the work_item, or an empty string if none.'''
        print("--- getLogURI")
        rushframe = work_item.id
        return "file://" + self.LogFilename(rushframe)

    def getStatusURI(self, work_item):                            # HOUDINI CALLBACK
        '''Return the farm's status URI, or an empty string if none.'''
        _ = (self, work_item)   # unused (pylint)
        print("--- getStatusURI")
        return ""

    def endSharedServer(self, sharedserver_name):                 # HOUDINI CALLBACK
        '''Called by job or on cook end to terminate the sharedserver.'''
        _ = (self, sharedserver_name)   # unused (pylint)
        print("--- endSharedServer: unused")
        return True

    def SaveRenderScriptCustom(self, filename, python_code):
        '''Create the user's custom pre/post renderscripts'''
        fd = open(filename, "w")
        fd.write(python_code)
        fd.close()

    def SaveRenderScript(self, filename, python_cmd, job_temp_dir):
        '''Create the rush render script that loads json file and runs houdini work item.'''
        fd = open(filename, "w")
        # NOTE: Beware escaped chars (e.g. \n) are expanded even inside triple quotes!
        fd.write('#!' + python_cmd + '''
import os,sys,json,time,subprocess

# Rush render script for rushscheduler generated work items

def LoadJSON(filename):
    """
    Load a JSON file, return as data.
    """
    retries = 5
    delay   = 5
    for retry in range(0, retries):
        try:
            fd = open(filename, "r")
            break
        except OSError as e:
            sys.stderr.write("WARNING: %s: %s (%d sec retries, %d/%d)\\n" % (filename, e.strerror, delay, retry, retries))
            if retry == (retries-1):
                Fail("Too many failures trying to load %s" % filename)
            time.sleep(delay)        # RETRY INCASE OF "NFS Stale File Handle"
            continue

    data = json.load(fd)
    fd.close()
    return data

def UpdateStatus(filename, status):
    """
    Write out rush status to a file that onTick() can get with self.GetStatus(frame).
    Status: "Run", "Done", "Fail"
    """
    tmpfilename = filename + ".tmp"
    fd = open(tmpfilename, "w")
    fd.write(status)
    fd.close()
    os.rename(tmpfilename, filename)
    os.sync()

def Message(msg):
    sys.stdout.write("--- render-script: %s\\n" % msg)
    sys.stdout.flush()

def Fail(msg):
    """Fail the render"""
    Message(msg)
    UpdateStatus(status_file, "Fail")       # tell onTick() we failed
    sys.exit(1)                             # tell rush we failed

job_temp_dir = "''' + job_temp_dir      + '''"   # Rush job temp dir
frame_fmt    = "''' + self.frame_fmt    + '''"   # Rush frame format, e.g. "%04d"

# If env var not set and no frame specified on cmd line? fail
if "RUSH_FRAME" not in os.environ and len(sys.argv) <= 1:
    print("ERROR: RUSH_FRAME env var is unset and no frame parameter specified")
    sys.exit(1)

# Rush frame#
if len(sys.argv) > 1: framepad = frame_fmt % int(sys.argv[1])
else:                 framepad = frame_fmt % int(os.environ["RUSH_FRAME"])

# Status file
status_dir  = job_temp_dir + "/status"
status_file = status_dir + "/" + framepad + ".txt"
while not os.path.isdir(status_dir):
    Message("Waiting for status dir to exist (3sec retries)")
    time.sleep(3)
Message(" Creating status file: %s" % status_file)
UpdateStatus(status_file, "Run")            # tell onTick() we're running

# Load JSON file for this frame / work_item
jsonfile = job_temp_dir + "/json/%s.json" % framepad
Message("    Loading json file: %s" % jsonfile)
while not os.path.exists(jsonfile):
    Message("Waiting for json file to exist (3sec retries)")
    time.sleep(3)
work_item_data = LoadJSON(jsonfile)

Message("       work_item name: %s" % work_item_data["work_item_name"])

# Merge current environment with vars from work_item
job_env = os.environ.copy()
job_env.update(work_item_data["job_env"])   # merge

# Import user's custom pre script code
import rush_render_pre

# Execute the houdini work_item command
print("")
Message("Executing: %s" % work_item_data["command"])
sys.stdout.flush()
sys.stderr.flush()
exitcode = subprocess.call(work_item_data["command"], shell=True, env=job_env)

# Import user's custom post script code
import rush_render_post

# Check for success
if exitcode != 0:
    Fail("FAILED (Exit code %d)" % exitcode)

Message("SUCCEEDS")
UpdateStatus(status_file, "Done")           # tell onTick() we succeeded
sys.exit(0)                                 # tell rush we succeeded
''')
        fd.flush()
        fd.close()
        os.sync()
        os.chmod(filename, 0o777)    # all read/exec, user/grp write

    def applicationBin(self, name, work_item):
        '''When node creates a command that uses an app that can be parameterized by the scheduler.
           e.g. a ui to control which 'python' app should be used for python jobs.
           At minimum hython and python should be supported.
        '''
        _ = work_item   # unused (pylint)
        if   name == 'python': return self._pythonBin()
        elif name == 'hython': return self._hythonBin()
        return None     # shouldn't happen

def registerTypes(type_registry):
    '''Needs docs'''
    type_registry.registerScheduler(RushScheduler)

print("\033[1m--- rushscheduler.py loaded ---\033[0m")  # DEBUGGING
