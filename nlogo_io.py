#!python

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Python script for breaking up Netlogo .nlogo models containing behavioral space
experiments with value sets and lists leading to multiple runs into XML files
containing a single value combination. Sometimes useful when setting up
experiments to run com computer clusters. 
"""

__author__ = "Lukas Ahrenberg <lukas@ahrenberg.se>"

__license__ = "GPL3"

__version__ = "0.3"


import os.path

from string import Formatter




def expandValueSets( value_tuples):
    """
    Recursive generator giving the different combinations of variable values.
    
    Parameters
    ----------
    
    value_tuples : list
       List of tuples, each tuple is on the form 
       (variable_name, [value_0, value_1, ... , value_N])
       where the value list is the possible values for that variable.
       
    Yields
    ------
       : Each yield results in a list of unique variable_name and value 
       combination for all variables listed in the original value_tuples.
    
    """
    if len(value_tuples) == 1:
        for val in value_tuples[0][1]:
            yield [(value_tuples[0][0], val)]
    else:            
        for val in value_tuples[0][1]:
            for vlist in expandValueSets(value_tuples[1:]):
                yield [(value_tuples[0][0], val)] + vlist

def steppedValueSet(first, step, last):
    """
    Tries to mimic the functionality of BehaviorSpace SteppedValueSet class.
    
    Parameters
    ----------
    
    first : float
       Start of value set.
       
    step : float
       Step length of value set.

    last : float
       Last value of the set. Inclusive in most cases, but may be exclusive 
       due to floating point rounding errors. This is as BehavioirSpace 
       implements it.


    Returns
    -------

    values : list
       The values between first and last taken with step length step.

    """
    # May look backward, but this will have the same rounding behavior
    # as the BehaviorSpace code as far as I can tell.
    n = 0
    val = first
    values = []
    while val <= last:
        values.append(val)
        n+=1
        val = first + n * step

    return values


def saveExperimentToXMLFile(experiment, xmlfile):
    """
    Given an experiment XML node saves it to a file wrapped in an experiments tag.
    The file is also furnished with DOCTYPE tag recognized by netlogo.
    File name will be the experiment name followed by the experiment number (zero padded), optionally prefixed.

    Parameters
    ----------
    
    experiment : xml node
       An experiment tag node and its children.

    xmlfile : file pointer
       File opened for writing.
    """

    xmlfile.write("""<?xml version="1.0" encoding="us-ascii"?>\n""")
    xmlfile.write("""<!DOCTYPE experiments SYSTEM "behaviorspace.dtd">\n""")
    xmlfile.write("""<experiments>\n""")
    experiment.writexml(xmlfile)
    xmlfile.write("""</experiments>\n""")
    

def createScriptFile(script_fp,
                     xmlfile, 
                     nlogofile,
                     experiment,
                     combination_nr,
                     script_template,
                     csv_output_dir = "./"
                     ):
    """
    Create a script file from a template string.

    Parameters
    ----------

    script_fp : file pointer
       File opened for writing.    

    xmlfile : string
       File name and path of the xml experiment file.
       This string will be accessible through the key {setup}
       in the script_template string.

    nlogofile : string
       File name and path of the ,nlogo model file.
       This string will be accessible through the key {model}
       in the script_template string.

    experiment : string
       Name of the experiment.
       This string will be accessible through the key {experiment}
       in the script_template string.

    combination_nr : int
       The experiment combination number.
       This value will be accessible through the key {combination}
       in the script_template string.

    script_template : str
       The script template string. This string will be cloned for each script
       but the following keys can be used and will have individual values.
       {job} - Name of the job. Will be the name of the xml-file (minus extension).
       {combination} - The value of the parameter combination_nr.
       {experiment} - The value of the parameter experiment.
       {csv} - File name, including full path, of a experiment-unique csv-file.
       {setup} - The value of the parameter csvfile.
       {model} - The value of the parameter nlogofile.
       {csvfname} - Only the file name part of the {csv} key.
       {csvfpath} - Only the path part of the {csv} key.
       
    csv_output_dir : str, optional
       Path to the directory used when constructing the {csv} and {csvfpath} 
       keys.


    Returns
    -------

    file_name : str
       Name of the file name used for the script.

    """
    jobname = os.path.splitext(os.path.basename(xmlfile))[0]


    fname = jobname + ".csv"
    csvfile = os.path.join(csv_output_dir, fname)

    strformatter = Formatter()
    formatmap = {
        "job" : jobname, 
        "combination" : combination_nr, 
        "experiment" : experiment,
        "csv" : csvfile,
        "setup" : xmlfile,
        "model" : nlogofile,
        "csvfname" : fname,
        "csvfpath" : csv_output_dir
        }
    # Use string formatter to go through the script template and
    # look for unknown keys. Do not replace them, but print warning.
    for lt, fn, fs, co in strformatter.parse(script_template):
        if fn != None and fn not in formatmap.keys():
            print("Warning: Unsupported key '{{{0}}}' in script template. Ignoring."\
                      .format(fn))
            formatmap[fn] = "{" + fn + "}"
            
    script_fp.write(script_template.format(**formatmap))

