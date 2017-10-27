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
Python script for executing Netlogo .nlogo models across MPI clusters.  The
models must contain behavioral space experiments with value sets and lists 
which are used to generate all possible combinations in the behavior spoace
and divide the execution up among the MPI processes. This work extends
on the split_nlogo_experiment code (Lukas Ahrenberg <lukas@ahrenberg.se>) which
is utilized for the parsing of .nlogo files, generation of xml inputs, and the 
generation of behavior space combinations.  However this approach should scale 
better to larger runs as it does not create multiple files for each parameter 
combination and bog the file system down.
"""

__author__ = "Aaron Fisher <funktektronic@gmail.com>"

__license__ = "GPL3"

__version__ = "0.3"

from mpi4py import MPI
import string
import sys
import os
import argparse
from xml.dom import minidom
import csv
from nlogo_io import *
                
def main():    
    comm = MPI.COMM_WORLD
    mpi_size = comm.Get_size()
    mpi_rank = comm.Get_rank()

    experiments_to_expand = []
    
    aparser = argparse.ArgumentParser(description = "MPI run nlogo behavioral space experiments.")
    aparser.add_argument("nlogo_file", help = "Netlogo .nlogo file with the original experiment")
    aparser.add_argument("experiment", nargs = "*", help = "Name of one or more experiments in the nlogo file to expand. If none are given, --all_experiments must be set.")
    aparser.add_argument("--all_experiments", action="store_true", help = "If set all experiments in the .nlogo file will be expanded.")
    aparser.add_argument("--repetitions_per_run", type=int, nargs = 1, help="Number of repetitions per generated experiment run. If the nlogo file is set to repeat an experiment N times, these will be split into N/n individual experiment runs (each repeating n times), where n is the argument given to this switch. Note that if n does not divide N this operation will result in a lower number of total repetitions.")
    aparser.add_argument("--output_dir", default="./", help = "Path to output directory if not current directory.")
    aparser.add_argument("--output_prefix", default="", help = "Generated files are named after the experiment, if set, the value given for this option will be prefixed to that name.")
    # Scripting options.
    aparser.add_argument("--create_script", dest = "script_template_file", help = "Tell the program to generate script files (for instance PBS files) alongside the xml setup files. A template file must be provided. See the external documentation for more details.")
    aparser.add_argument("--script_output_dir", help = "Path to output directory for script files. If not specified, the same directory as for the xml setup files is used.")
    aparser.add_argument("--csv_output_dir", help = "Path to output directory where the table data from the simulations will be saved. Use with script files to set output directory for executed scripts. If not specified, the same directory as for the xml setup files is used.")
    aparser.add_argument("--create_run_table", action="store_true", help = "Create a csv file containing a table of run numbers and corresponding parameter values. Will be named as the experiment but postfixed with '_run_table.csv'.")
    aparser.add_argument("--no_path_translation", action="store_true", help = "Turn off automatic path translation when generating scripts. Advanced use. By default all file and directory paths given are translated into absolute paths, and the existence of directories are tested. (This is because netlogo-headless.sh always run in the netlogo directory, which create problems with relative paths.) However automatic path translation may cause problems for users who, for instance, want to give paths that do yet exist, or split experiments on a different file system from where the simulations will run. In such cases enabling this option preserves the paths given to the program as they are and it is up to the user to make sure these will work.")
    aparser.add_argument("-v", "--version", action = "version", version = "split_nlogo_experiment version {0}".format(__version__))

    aparser.add_argument("--java", help="Command to launch java.", default="java")
    aparser.add_argument("--nlogo_path", help="Path to the nlogo java file.")
    argument_ns = aparser.parse_args()

    # Check so that there's either experiments listed, or the all_experiments switch is set.
    if len(argument_ns.experiment) < 1 and argument_ns.all_experiments == False:
        print("Warning. You must either list one or more experiments to expand, or use the --all_experiments switch.")
        exit(0)

    experiments_xml = ""
    try:
        with open(argument_ns.nlogo_file) as nlogof:
            # An .nlogo file contain a lot of non-xml data
            # this is a hack to ignore those lines and
            # read the experiments data into an xml string
            # that can be parsed.
            nlogo_text = nlogof.read()
            alist = nlogo_text.split("<experiments>")
            for elem in alist[1:]:
                blist = elem.split("</experiments>")
                experiments_xml += "<experiments>{0}</experiments>\n".format(blist[0])
    except IOError as ioe:
        sys.stderr.write(ioe.strerror + " '{0}'\n".format(ioe.filename))
        exit(ioe.errno)


    # Absolute paths.
    # We create absolute paths for some files and paths in case given relative.
    if argument_ns.no_path_translation == False:
        argument_ns.output_dir = os.path.abspath(argument_ns.output_dir)
    if argument_ns.script_output_dir == None:
        argument_ns.script_output_dir = argument_ns.output_dir
    elif argument_ns.no_path_translation == False:
        argument_ns.script_output_dir = os.path.abspath(argument_ns.script_output_dir)
    if argument_ns.csv_output_dir == None:
        argument_ns.csv_output_dir = argument_ns.output_dir
    elif argument_ns.no_path_translation == False:
        argument_ns.csv_output_dir = os.path.abspath(argument_ns.csv_output_dir)

    # This is the absolute path name of the nlogo model file.
    if argument_ns.no_path_translation == False:
        nlogo_file_abs = os.path.abspath(argument_ns.nlogo_file)
    else:
        nlogo_file_abs = argument_ns.nlogo_file

    # Check if scripts should be generated and read the template file.
    if argument_ns.script_template_file != None:
        script_extension = os.path.splitext(argument_ns.script_template_file)[1]
        try:
            with open(argument_ns.script_template_file) as pbst:
                script_template_string = pbst.read()
        except IOError as ioe:
            sys.stderr.write(ioe.strerror + " '{0}'\n".format(ioe.filename))
            exit(ioe.errno)
            sys.stdout.write("tst {0}: ".format(argument_ns.repetitions_per_run))
        
    # Start processing.
    original_dom = minidom.parseString(experiments_xml)
    # Need a document to create nodes.
    # Create a new experiments document to use as container.
    experimentDoc = minidom.getDOMImplementation().createDocument(None, "experiments", None)    

    # Get all of the filenames for the outputs both temporary and permenent
    xml_filename = os.path.join(argument_ns.output_dir, "proc" + str(mpi_rank).zfill(4) + ".xml")
    dat_filename = os.path.join(argument_ns.output_dir, "proc" + str(mpi_rank).zfill(4) + ".dat")
    hdr_filename = os.path.join(argument_ns.output_dir, "headers.dat")
    csv_filename = os.path.join(argument_ns.output_dir, "proc" + str(mpi_rank).zfill(4) + ".csv")

    experiment_names = get_experiment_names(original_dom, argument_ns)
    experiment_lengths = get_experiment_lengths(original_dom, argument_ns)
    (start_exp_name, start_i) = get_start_run(experiment_names, experiment_lengths, csv_filename, mpi_rank, mpi_size) 
    started = False

    # Remember which experiments were processed.
    processed_experiments = []
    
    for orig_experiment in original_dom.getElementsByTagName("experiment"):
        if argument_ns.all_experiments == True \
                or orig_experiment.getAttribute("name") \
                in argument_ns.experiment:

            processed_experiments.append(orig_experiment.getAttribute("name"))

            experiment = orig_experiment.cloneNode(deep = True)
            experiment_name = experiment.getAttribute("name").replace(' ', '_').replace('/', '-').replace('\\','-')
            
            # Store tuples of varying variables and their possible values.
            value_tuples = []
            num_individual_runs = 1

            # Number of repetieitons.
            # In the experiment.
            # Read original value first. Default is to have all internal.
            reps_in_experiment = int(experiment.getAttribute("repetitions"));
            # Repeats of the created experiment.
            reps_of_experiment = 1;
            # Check if we should split experiments. An unset switch or value <= 0 means no splitting.
            if argument_ns.repetitions_per_run != None \
                    and argument_ns.repetitions_per_run[0] > 0:
                original_reps = int(experiment.getAttribute("repetitions"))
                if original_reps >= argument_ns.repetitions_per_run[0]:
                    reps_in_experiment = int(argument_ns.repetitions_per_run[0])
                    reps_of_experiment = int(original_reps / reps_in_experiment)
                    if(original_reps % reps_in_experiment != 0):
                        sys.stderr.write("Warning: Number of repetitions per experiment does not divide the number of repetitions in the nlogo file. New number of repetitions is {0} ({1} per experiment in {2} unique script(s)). Original number of repetitions per experiment: {3}.\n"\
                                             .format((reps_in_experiment*reps_of_experiment), 
                                                     reps_in_experiment, 
                                                     reps_of_experiment,
                                                     original_reps))

            # Handle enumeratedValueSets
            for evs in experiment.getElementsByTagName("enumeratedValueSet"):
                values = evs.getElementsByTagName("value")
                # If an enumeratedValueSet has more than a single value, it should
                # be included in the value expansion tuples.
                if len(values) > 1:
                    # A tuple is the name of the variable and
                    # A list of all the values.
                    value_tuples.append((evs.getAttribute("variable"), 
                                         [val.getAttribute("value") \
                                              for val in values]
                                         )
                                        )
                    num_individual_runs *= len(value_tuples[-1][1])
                    # Remove the node.
                    experiment.removeChild(evs)
                    

            # Handle steppedValueSet
            for svs in experiment.getElementsByTagName("steppedValueSet"):
                first = float(svs.getAttribute("first"))
                last = float(svs.getAttribute("last"))
                step = float(svs.getAttribute("step"))
                # Add values to the tuple list.
                value_tuples.append((svs.getAttribute("variable"),
                                     steppedValueSet(first, step, last)
                                     )
                                    )
                num_individual_runs *= len(value_tuples[-1][1])
                # Remove node.
                experiment.removeChild(svs)

            
            # Now create the different individual runs.
            enum = 0
            # Keep track of the parameter values in a run table.
            run_table = []
            ENR_STR = "Experiment number"
            exp_all = []
            if num_individual_runs > 1:
                vsgen = expandValueSets(value_tuples)
                for exp in vsgen:
                    exp_all.append(exp)
            else:
                # If there were no experiments to expand create a dummy-
                # expansion just to make sure the single experiment is still
                # created.
                vsgen = [[]]
          
            #Handle the restart if we doing that now 
            if started:
                exp_start_i = mpi_rank
            else:
                if start_exp_name == experiment_name:
                    exp_start_i = start_i
                    started = True
                else:
                    exp_start_i = num_individual_runs + 1   #Skip this experiment
 
            for exp_i in range(exp_start_i, num_individual_runs, mpi_size):
                exp = exp_all[exp_i]
                for exp_clone in range(reps_of_experiment):
                    # Add header in case we are on the first row.
                    if enum < 1:
                        run_table.append([ENR_STR])
                    run_table.append([enum])

                    experiment_instance = experiment.cloneNode(deep = True)
                    experiment_instance.setAttribute("repetitions",str(reps_in_experiment))
                    for evs_name, evs_value in exp:
                        evs = experimentDoc.createElement("enumeratedValueSet")
                        evs.setAttribute("variable", evs_name)
                        vnode = experimentDoc.createElement("value")
                        vnode.setAttribute("value", str(evs_value))
                        evs.appendChild(vnode)
                        experiment_instance.appendChild(evs)

                    write_instance_xml(xml_filename, experiment_instance)
                    run_nlogo(argument_ns.java, argument_ns.nlogo_path, argument_ns.nlogo_file, xml_filename, dat_filename)
                    append_data_to_cvs(dat_filename, experiment_name, exp_i, csv_filename)
                    if exp_i == 0:
                        append_header(dat_filename, experiment_name, hdr_filename)
                    
                    #Finally remove the temp files
                    os.remove(xml_filename)
                    os.remove(dat_filename)

    # Warn if some experiments could not be found in the file.
    for ename in argument_ns.experiment:
        if ename not in processed_experiments:
            print("Warning - Experiment named '{0}' not found in model file '{1}'".format(ename, argument_ns.nlogo_file))

def get_last_run(cvs_filename):
    last_exp = ""
    last_i = -1
    if os.path.isfile(cvs_filename):
        with open(csv_filename, "r") as fout:
            for line in fout:
                last = line
            else:
                last = ""                
            part = string.split(last,",")
            if len(part) >= 2:
                last_exp = part[0]
                last_i = int(part[1])
    return (last_exp, last_i)

def get_experiment_names(original_dom, argument_ns):
    experiment_names = []
    for orig_experiment in original_dom.getElementsByTagName("experiment"):
        if argument_ns.all_experiments == True \
                or orig_experiment.getAttribute("name") \
                in argument_ns.experiment:
            experiment = orig_experiment.cloneNode(deep = True)
            experiment_name = experiment.getAttribute("name").replace(' ', '_').replace('/', '-').replace('\\','-')
            experiment_names.append(experiment_name)
    return experiment_names

def get_experiment_lengths(original_dom, argument_ns):
    experiment_lengths = []
    for orig_experiment in original_dom.getElementsByTagName("experiment"):
        if argument_ns.all_experiments == True \
                or orig_experiment.getAttribute("name") \
                in argument_ns.experiment:
            experiment = orig_experiment.cloneNode(deep = True)

            num_individual_runs = 1
            # Handle enumeratedValueSets
            for evs in experiment.getElementsByTagName("enumeratedValueSet"):
                values = evs.getElementsByTagName("value")
                # If an enumeratedValueSet has more than a single value, it should
                # be included in the value expansion tuples.
                if len(values) > 1:
                    # A tuple is the name of the variable and
                    # A list of all the values.
                    temp = [val.getAttribute("value") for val in values]
                    num_individual_runs *= len(temp)

            # Handle steppedValueSet
            for svs in experiment.getElementsByTagName("steppedValueSet"):
                first = float(svs.getAttribute("first"))
                last = float(svs.getAttribute("last"))
                step = float(svs.getAttribute("step"))
                # Add values to the tuple list.
                temp = steppedValueSet(first, step, last)
                num_individual_runs *= len(temp)

            experiment_lengths.append(num_individual_runs)
    return experiment_lengths

def get_start_run(experiment_names, experiment_lengths, csv_filename, mpi_rank, mpi_size):
    (last_exp_name, last_i) = get_last_run(csv_filename)
    num_exp = len(experiment_names)
    if last_i > -1:
        for exp_i in range(num_exp):
            if last_exp_name == experiment_names[exp_i]:
                if last_i + mpi_size < experiment_lengths[exp_i]:
                    start_exp = experiment_names[exp_i]
                    start_i = last_i + mpi_size
                    break
                else:
                    if exp_i + 1 < len(experiment_names):
                        start_exp = experiment_names[exp_i+1]
                        start_i = mpi_rank
                        break
                    else:
                        start_exp = experiment_names[exp_i]
                        start_i = last_i + mpi_size
                        break
    else:
        start_exp = experiment_names[0]
        start_i = mpi_rank
    return (start_exp, start_i)

def write_instance_xml(xml_filename, experiment_instance):
    try:
        with open(xml_filename, 'w') as xmlfile:
            saveExperimentToXMLFile(experiment_instance, xmlfile)
    except IOError as ioe:
        sys.stderr.write(ioe.strerror + " '{0}'\n".format(ioe.filename))
        exit(ioe.errno)

def append_header(dat_filename, experiment_name, hdr_filename):
    try:
        with open(dat_filename, 'r') as datfile:
            lines = datfile.readlines()
            out_line = experiment_name + ",run_number,"
            if len(lines) > 6:
                out_line += lines[6]
            else:
                print "Cannot find header in NLOGO output .dat file!"
            with open(hdr_filename, "a") as fout:
                fout.write(out_line)
    except IOError as ioe:
        print "Cannot find NLOGO output .dat file!"
        exit(ioe.errno)

def append_data_to_cvs(dat_filename, experiment_name, exp_i, csv_filename):
    try:
        with open(dat_filename, 'r') as datfile:
            lines = datfile.readlines()
            out_line = experiment_name + "," + str(exp_i).zfill(6) + ","
            if len(lines) > 7:
                out_line += string.replace(lines[7],'"', '')
            else:
                out_line += '\n'
            with open(csv_filename, "a") as fout:
                fout.write(out_line)
    except IOError as ioe:
        sys.stderr.write(ioe.strerror + " '{0}'\n".format(ioe.filename))
        exit(ioe.errno)

def run_nlogo(java, nlogo_path, model_file, setup_file, output_file):
    runstr =  java + " -Xmx2048m -Dfile.encoding=UTF-8"
    runstr += " -classpath " + nlogo_path + " org.nlogo.headless.Main"
    runstr += " --model " + model_file
    runstr += " --setup-file " + setup_file
    runstr += " --table " + output_file
    os.system(runstr)

if __name__ == "__main__":
    main()
