'''
Flow: A Swiss-army knife for scientific workflows using linear model
'''

import __init__ as flow
import optparse
import sys
import os
import json
import signal

try:
    from commands import getoutput
    _ncpu = int(getoutput("grep -c -e '^processor' /proc/cpuinfo"))
except Exception, e:
    _ncpu = 4

try:
    _maxdepth = int(os.pathconf('/', 'PC_PATH_MAX'))/2
except Exception, e:
    _maxdepth = 128

signal.signal(signal.SIGINT, lambda signal, frame: sys.exit(-1))
signal.signal(signal.SIGTERM, lambda signal, frame: sys.exit(-1))

# setup static functions
    
def main(commandargs = sys.argv):
    parser = optparse.OptionParser(prog = "flow", formatter=optparse.IndentedHelpFormatter(width=os.getenv('COLUMNS', 132)), version=flow.__version__)
    parser.add_option("-q", "--quiet",
                      action="store_true", dest="quiet", default=False,
                      help="suppress status messages (default: No)")
    parser.add_option("-v", "--verbose",
                      action="store_false", dest="quiet",
                      help=optparse.SUPPRESS_HELP)
    parser.add_option("-r",
                      action="store_true", dest="recursive", default=False,
                      help="flow recursively (default: No)")
    parser.add_option("-d",
                      action="store", dest="srcdir", default='.',
                      help="directory in which to run (default: .)")
    parser.add_option("-n", "--dryrun",
                      action="store_true", dest="dryrun", default=False,
                      help="do not actually execute any scripties (default: No)")
    parser.add_option("-k", "--keep-going",
                      action="store_true", dest="keep_going", default=True,
                      help="Keep going when scripties exit with error (default: Yes)")
    parser.add_option("-K", "--not-keep-going",
                      action="store_false", dest="keep_going", default=True,
                      help="Turns off -k (default: No).")

    parser.add_option("-N", "--ignore-numbers",
                      action="store_false", dest="numbered", default=True,
                      help="suppress running numbered files (default: No)")
    parser.add_option("-j",
                      action="store_const", dest="jobs", const=_ncpu, default=1,
                      help="flow with up to %s concurrent jobs (default: 1)" % (_ncpu))
    parser.add_option("-i",
                      action="store_true", dest="interactive", default=False,
                      help="flow with interactive confirmations (default: No)")
    parser.add_option("-s", "--style", metavar="STYLE",
                      action="store", dest="style", default=None,
                      help="flow with style (styles are %s)" % ', '.join(sorted(flow.Flow()._styles.keys())))
    parser.add_option("-t", "--task", metavar="TASK",
                      action="store", dest="task", default=None,
                      help="flow with single task")
    parser.add_option("--exclude", metavar="NAME",
                      action="append", dest="excluded_dirs", default=['data', 'orig', 'tmp'],
                      help="exclude files/folders named NAME (default: data, orig, tmp)")
    parser.add_option("--exclude-prefix", metavar="PREFIX",
                      action="append", dest="excluded_prefix", default=['.', '_'],
                      help="exclude files/folders starting with PREFIX (default: .*, _*)")
    parser.add_option("--flows",
                      action="store_true", dest="listflows", default=False,
                      help="print list of flow styles and languages supported")
    
    # parse command-line options
    (options, args) = parser.parse_args(commandargs)

    # verify command-line options
    if options.style is not None and options.style.startswith('-'):
        parser.error('''ERROR: --style requires argument''')
    if options.task is not None and options.task.startswith('-'):
        parser.error('''ERROR: --task requires argument''')
    if options.task is not None and options.style is not None:
        parser.error('''ERROR: Cannot use --style and --task concurrently.''')

    # process command-line options
    if options.task is not None:
        options.style = 'default'

    if options.style is None:
        options.style = os.getenv('FLOW_STYLE', 'default')

    assert options.style is not None

    if options.style not in flow.Flow()._styles:
        parser.error('''ERROR: Style "%s" is not registered.''' % (options.style))

    # check for special behaviors
    if options.listflows:
        print json.dumps({ 
            'flow-styles': flow.Flow()._styles, 
            'flow-languages': flow.Flow()._languages}, 
            indent=2, sort_keys=True)
        sys.exit(0)

    f = flow.Flow(os.getcwd() if options.srcdir == '.' else options.srcdir, 
             dryrun=options.dryrun, 
             interactive=options.interactive,
             quiet=options.quiet,
             numbered=options.numbered,
             style=options.style,
             keep_going=options.keep_going,
             excluded_dirs=options.excluded_dirs,
             excluded_prefix=options.excluded_prefix)

    if options.task is not None:
        f.styles('task', [options.task])
        f.style('task')

    if options.jobs > 1 and options.interactive:
        parser.error('ERROR: Flow cannot run with both -i and -j flags on.')

    f.run(depth=_maxdepth if options.recursive else 0, 
          nproc=options.jobs)

if __name__ == '__main__':
    main()
