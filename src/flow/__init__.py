'''
A Swiss-army knife for scientific workflows using linear process model

Flow is a *Format*-neutral, *Language*-agnostic, *Open*-source *Workflow* tool
-- a veritible Swiss-army knife for scientific workflow integration,
automation, execution, and reporting. Flow cares not about which data formats
or software tools you use. Flow cares only about running workflows with
minimal integration effort. Flow uses a database of workflow *styles* and a
simple linear process model to automate execution for a myrid of different
scientific workflow styles. For example, a 'simple' style uses the following
process model:

* Setup
* Import
* Ingest
* Model 
* Digest
* Export
* Finish

Flow implements an N-ary pass method for each workflow step. That is,
download1.sh, download2.sh, etc., are run in numeric order. This includes the
language flow, so download1.sh, download1.py, download2.sh, etc.

=====
Usage
=====

    Usage: flow [options]

    Options:
      --version        show program's version number and exit
      -h, --help       show this help message and exit
      -q, --quiet      suppress status messages (default: No)
      -r               flow recursively (default: No)
      -n, --dryrun     do not actually execute any scripties (default: No)
      -k, --keep-going Keep going when scripties exit with error (default: Yes)
      -K, --not-keep-going
                       Turns off -k (default: No).
      -N, --nonumbers  suppress running numbered files (default: No)
      -j               flow with up to 16 concurrent jobs (default: 1)
      -i               flow with interactive confirmations (default: No)
      -d dir           directory in which to run (default: .)
      --style=STYLE    flow with style (default: 'standard')
      --task=TASK      flow with task (default: None)
      --exclude=NAME   exclude from running within folder named NAME
      --flows          print list of flow styles and languages supported

=============
Example Usage
=============

-------------------
Running flows
-------------------

Running flow for the current directory::

    % flow

Running flow for the current directory and all subdirectories::

    % flow -r

Same but allowing concurrent jobs::

    % flow -rj

Running flow with interactive prompts before running scripties::

    % flow -i
    
------------------
Monitoring flow
------------------

See which scripties flow will run::

    % flow -n

Print only filenames of scripties flow will run to stdout::

    % flow -nq
    % vi `flow -nq`

-------------------
Working with flows
-------------------

Show built-in flow styles::

    % flow --flows
    
Override the default style::

    % env FLOW_STYLE=simple flow

Override the languages::

    % env FLOW_LANGUAGES="sh,sql,py" flow --flows

Override the style::

    % env FLOW_STYLE_SIMPLE="setup,finish" flow -s simple
    
Use a single task flow::

    % flow -t doit

'''
__svnid__ = '$Id: __init__.py 5895 2013-01-16 01:14:33Z hardy $'
__author__ = "Darren Hardy <hardy@nceas.ucsb.edu>"
__version__ = '0.9.3 (r%d)' % (int(__svnid__.split()[2])) 
__credits__ = "New BSD Licence"

from flow import Flow

__all__ = [ 'Flow' ]
