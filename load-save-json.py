#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import os,sys,re,time,json,threading

def SaveJson(filename, data):
    try:
        fd = open(filename, "w")
    except IOError as e:
        return "SaveJson: could not create '%s': %s" % (filename, e.strerror)
    fd.write(json.dumps(workdata,sort_keys=True, indent=4))
    fd.close()
    return None

def LoadJson(filename):
    '''Load json data from 'filename', returns data on success, or error msg on failure.
       Returns:
           On success: (None, data)
           On failure: (emsg, None)
    '''
    try:
        fd = open(filename, "r")
    except IOError as e:
        emsg = "LoadJson: could not open '%s': %s" % (filename, e.strerror)
        return (emsg, None)
    data = json.load(fd)
    fd.close()
    return (None, data)

# MAIN
workdata = { "job_env":
                {
                "PDG_RESULT_SERVER": "aa",
                "PDG_ITEM_NAME":     "bb",
                "PDG_ITEM_ID":       "cc",
                "PDG_DIR":           "dd",
                "PDG_TEMP":          "ee",
                "PDG_SCRIPTDIR":     "ff",
                }
	       }

# TEST SAVE
filename = "./test.json"
emsg = SaveJson(filename, workdata)
if emsg != None:
    print("ERROR: %s" % emsg)
else:
    print("OK: Created %s" % filename)

# TEST LOAD
workdata = {}
(emsg,workdata) = LoadJson(filename)
if emsg != None:
    print("ERROR: %s" % emsg)
else:
    print("workdata: %s" % workdata)
    print("job_env: %s" % workdata["job_env"])
    for i in workdata["job_env"]:
        print("    %20s=%s" % (i, workdata["job_env"][i]))

