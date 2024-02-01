#!/usr/bin/python
# vim: autoindent tabstop=4 shiftwidth=4 expandtab softtabstop=4 filetype=python

import os,sys,re,time,json,threading

def SaveJson(filename, data):
    '''
    Save json data to 'filename'
    May raise IOError exceptions on file errors.
    '''
    fd = open(filename, "w")
    fd.write(json.dumps(workdata,sort_keys=True, indent=4))
    fd.close()

def LoadJson(filename):
    '''
    Load json data from 'filename', returns data on success.
    May raise IOError exceptions on file errors.
    Returns data loaded from json file.
    '''
    fd   = open(filename, "r")
    data = json.load(fd)
    fd.close()
    return data

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
try: SaveJson(filename, workdata)
except IOError as e:
    print("ERROR: SaveJson() could not create '%s': %s" % (filename, e.strerror))
    sys.exit(1)
print("OK: Created %s" % filename)

# TEST LOAD
workdata = {}
try: workdata = LoadJson(filename)
except IOError as e:
    print("ERROR: LoadJson: could not open '%s': %s" % (filename, e.strerror))
    sys.exit(1)

# SHOW WHAT WAS LOADED
print("workdata: %s" % workdata)
print("job_env: %s" % workdata["job_env"])
for i in workdata["job_env"]:
    print("    %20s=%s" % (i, workdata["job_env"][i]))

