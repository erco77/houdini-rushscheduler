import itertools

# Example of how to compress a possibly unordered list of frames into rush frame ranges
#
#     1.00 06/11/2020 "luca" - Stack Overflow, "to_ranges)"
#     1.10 02/06/2024 erco@seriss.com - Initial implementation, see ref
#
# ref: https://stackoverflow.com/questions/4628333/converting-a-list-of-integers-into-range-in-python
#
def FramesAsRangeGroups(iterable):
    '''Return possibly unsorted frames[] into a list of of (sfrm,efrm) groups'''
    iterable = sorted(set(iterable))
    for key, group in itertools.groupby(enumerate(iterable), lambda t: t[1] - t[0]):
        group = list(group)
        yield group[0][1], group[-1][1]

def FramesAsRanges(frames):
    '''Return possibly unsorted frames[] into a string of rush frame ranges'''
    ranges = ""
    for (sfrm,efrm) in FramesAsRangeGroups(frames):
        if ranges != "": ranges += " "
        if sfrm == efrm: ranges += ("%d" % sfrm)
        else:            ranges += ("%d-%d" % (sfrm,efrm))
    return ranges

frames = [0, 1, 2, 3, 4, 7, 8, 9, 11]
print("FramesAsRanges[1]: BEFORE: %s" % frames)
print("                    AFTER: %s" % FramesAsRanges(frames))
print("")

frames = [44, 45, 2, 56, 23, 11, 3, 4, 7, 9, 1, 2, 2, 11, 12, 13, 45]
print("FramesAsRanges[2]: BEFORE: %s" % frames)
print("                    AFTER: %s" % FramesAsRanges(frames))

