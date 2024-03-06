from . import rushscheduler

#  See also:
#  /opt/hfs20.0.590/houdini/pdg/types/schedulers/*

def registerTypes(type_registry):
    type_registry.registerScheduler(rushscheduler.RushScheduler,
                                    label="Rush Scheduler")

