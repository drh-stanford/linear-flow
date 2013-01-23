'''
A Swiss-army knife for scientific workflows using linear process model
'''
from collections import deque
from glob import glob
import multiprocessing as mp
import os
import os.path
import re
import subprocess
import sys

def _pool_run_one(flow): 
    '''this must be a static, pickle-able function for mp.Pool to work correctly'''
    flow.run(0, 1)

def _ingest_from_env(var, default, delim=','):
    return [s.strip() for s in os.getenv(var, default).split(delim)]

def _match_prefixes(s, prefixes):
    for p in prefixes:
        if p.endswith('*') and not p.endswith('\*'):
            p = p[0:len(p)-1]
        if s.startswith(p):
            return True
    return False

# setup classes

class FlowLanguages(list):
    '''data type for iterating languages'''
    def __init__(self, langs = _ingest_from_env('FLOW_LANGUAGES', 'sh,py,pl,rb,R,r,sql,tex')):
        super(FlowLanguages, self).__init__(langs)

class FlowStyle(list):
    '''data type for iterating style execution'''
    def __init__(self):
        super(FlowStyle, self).__init__()
        
    def from_env(self, k, v):
        [self.append(i) for i in _ingest_from_env(k, v)]
        return self

class FlowStyles(dict):
    def __init__(self):
        super(FlowStyles, self).__init__({
            'default':  FlowStyle().from_env('FLOW_STYLE_DEFAULT',  
                                             'setup,download,import,ingest,model,run,digest,export,report,upload,finish'),
            'trivial':  FlowStyle().from_env('FLOW_STYLE_TRIVIAL',  
                                             'run'),
            'simple':   FlowStyle().from_env('FLOW_STYLE_SIMPLE',   
                                             'setup,import,run,export'),
            'db':       FlowStyle().from_env('FLOW_STYLE_DB',       
                                             'create,load,index,query'),
            'doc':      FlowStyle().from_env('FLOW_STYLE_DOC',      
                                             'setup,doc'),
            'upload':   FlowStyle().from_env('FLOW_STYLE_UPLOAD',     
                                             'export,upload'),
            'test':     FlowStyle().from_env('FLOW_STYLE_TEST',     
                                             'setup,test,report')
        })

class FlowExecution(object):
    """docstring for FlowExecution"""
    def __init__(self, dryrun = False, quiet = False, interactive = False, keep_going = True):
        super(FlowExecution, self).__init__()
        self._dryrun = dryrun
        self._quiet = quiet
        self._interactive = interactive
        self._keep_going = keep_going
        self._m = {
            'pl':   'run_perl',
            'rb':   'run_ruby',
            'py':   'run_python'
        }
        
    def method(self, ext):
        '''Resolve an extension or key into method name for the run method'''
        return self._m[ext] if ext in self._m else 'run_%s' % (ext)

    def run(self, fn, ext = None):
        '''Main execution interface. 
        Runs the script in `fn` using the `ext` to determine how to execute the script. 
        If `ext` is None, then uses `os.path.splitext` to determine extensions. 
        Note that all extensions are case-insensitive.
        
        @param fn script filename
        @param ext how to execute the file
        '''

        if ext is None:
            ext = os.path.splitext(fn)[1][1:]
        
        k = self.method(ext.lower())
        if hasattr(self, k):
            if self._interactive:
                i = raw_input("run scriptie \"%s\" now? (Y/n) " % (fn))
                ok = True if i.strip().lower() in ('y','yes','') else False
            else:
                ok = True
            
            if ok:
                return getattr(self, k)(fn)
        else:
            raise NotImplemented('Unknown script extention: %s' % (ext))

    def run_perl(self, fn):
        '''Runs a perl script. Supports PERL environment variable (default is 'perl'). @param fn script filename'''
        return self._execcmd([os.getenv("PERL", 'perl'), fn])

    def run_ruby(self, fn):
        '''Runs a ruby script. Supports RUBY environment variable (default is 'ruby'). @param fn script filename'''
        return self._execcmd([os.getenv("RUBY", 'ruby'), fn])

    def run_sh(self, fn):
        '''Runs a shell script. Supports the SHELL environment variable to choose a shell (default is 'sh'). @param fn script filename'''
        return self._execcmd([os.getenv("SHELL", 'sh'), fn])

    def run_python(self, fn):
        '''Runs a python script. Supports PYTHON environment variable (default is 'python'). @param fn script filename'''
        return self._execcmd([os.getenv('PYTHON', 'python'), fn])

    def run_r(self, fn):
        '''Runs an R script in non-interactive batch mode, or if R_FLAGS is set it uses regular mode and inserts the flags provided. @param fn script filename'''
        flags = os.getenv("R_FLAGS", None)
        if (flags is None):
            return self._execcmd(['R', 'CMD', 'BATCH', '--no-save', '--no-restore', fn]) # side-effect of fn.Rout as log
        
        l = ['R']
        for flag in flags.split(' '):
            l.append(flag)
        l.append('-f')
        l.append(fn)
        return self._execcmd(l)

    def run_sql(self, fn):
        '''Runs an SQL script. Supports the SQL_SHELL environment variable to choose an SQL interpreter (default is 'psql'), and the SQL_FLAGS environment variable to pass extra flags (default is '-w'). @param fn script filename'''
        l = [os.getenv("SQL_SHELL", 'psql')]
        for flag in os.getenv("SQL_FLAGS", "-w").split(' '):
            l.append(flag)
        if self._quiet:
            l.append('--quiet')
        l.append('-f')
        l.append(fn)
        return self._execcmd(l)

    def run_tex(self, fn):
        '''Runs an LaTeX script. Default flags are '-silent'. Supports LATEX_SHELL which defaults to latexmk. @param fn script filename'''
        return self._execcmd([os.getenv("LATEX_SHELL", 'latexmk'), '-silent', fn])             

    def _execcmd(self, cmdarray, inputfn = '/dev/null', outputfn = '-', logfn = '-'):
        self._logger('Running %s script %s' % (os.path.basename(cmdarray[0]), cmdarray[-1]))

        if self._dryrun:
            if self._quiet: # print out scriptie name only (in quiet mode)
                print >>sys.stdout, cmdarray[-1] # scriptie name assumed to be last
            return

        if inputfn is None or inputfn == '-':
            inf = sys.stdin
        else:
            inf = open(inputfn, 'rb')

        if outputfn is None or outputfn == '-':
            outf = sys.stdout
        else:
            outf = open(outputfn, 'wb')

        if logfn is None or logfn == '-':
            logf = sys.stderr
        else:
            logf = open(logfn, 'ab')

        try:
            return subprocess.check_call(cmdarray, stdin=inf, stdout=outf, stderr=logf)
        except subprocess.CalledProcessError, e:
            self._logger('ERROR: Running %s script %s exited with non-zero status code %d' % (os.path.basename(cmdarray[0]), cmdarray[-1], e.returncode))
            if not self._keep_going:
                raise e

        
    def _logger(self, s):
        if not self._quiet:
            print >>sys.stderr, s


class Flow(object):
    '''Flow is the main base class for executing flows'''

    def __init__(self, 
                 rootdir = '.', 
                 dryrun = False, 
                 interactive = False, 
                 quiet = False, 
                 numbered = False, 
                 style = 'default',
                 styles = None,
                 languages = None,
                 runner = None,
                 keep_going = True,
                 excluded_dirs = [],
                 excluded_prefix = []):
        '''@param numbered supports numbered scripties (e.g., export01.sh, export02.sh, etc.)'''
        super(Flow, self).__init__()
        self._rootdir = rootdir
        self._dryrun = dryrun
        self._numbered = numbered
        self._quiet = quiet
        self._interactive = interactive
        self._style = style
        self._languages = languages if languages is not None else FlowLanguages()
        self._styles = styles if styles is not None else FlowStyles()        
        self._runner = runner if runner is not None else FlowExecution(dryrun, quiet, interactive, keep_going)
        self._excluded_dirs = excluded_dirs
        self._excluded_prefix = excluded_prefix
        # print self._excluded_dirs, self._excluded_prefix

    def style(self, style = None):
        if style is not None:
            self._style = style
        return self._style
        
    def styles(self, style = None, value = None):
        if style is not None and value is not None:
            self._styles[style] = value
        return self._styles
        
    def rootdir(self, rootdir = None):
        if rootdir is not None:
            self._rootdir = rootdir
        return self._rootdir

    def run(self, depth = 0, nproc = 1, style = None):
        '''Run from the current directory. 
        @param depth is an integer for how many subdirectories to execute in level-by-level order (default: 0 for no recursion).
        @param nproc is an integer for the number of processors to execute concurrently.
        @param style is the style in which to run.
        '''
        
        if style is None:
            style = self._style
        self._logger('Running flow [rootdir=%s] depth=%d nproc=%d' % (self._rootdir, depth, nproc))
        if depth > 0:
            n = 0
            pool = None
            if nproc > 1:
                pool = mp.Pool(nproc)
            for level in self._bylevel_iter(self._rootdir):
                n = level[0]
                self._logger('Running Level %d' % (n))
                flows = [self.spawn(p) for p in sorted(level[1:])]
                if pool is None:
                    map(lambda f: f.run(0,1), flows)
                else:
                    pool.map(_pool_run_one, flows)
        
        self._run_package(style)

    def __deepcopy__(self):
        return Flow(rootdir = self._rootdir, 
                    dryrun = self._dryrun, 
                    interactive = self._interactive, 
                    quiet = self._quiet, 
                    numbered = self._numbered, 
                    style = self._style,
                    styles = self._styles,
                    languages = self._languages,
                    runner = self._runner,
                    excluded_dirs = self._excluded_dirs,
                    excluded_prefix = self._excluded_prefix)

    def spawn(self, rootdir):
        f = self.__deepcopy__()
        f.rootdir(rootdir)
        return f

    # Private methods --------------------------------------------

    def _find_scripties(self, prefix):
        l = list()
        
        # first to the simple thing
        for ext in self._languages:
            fn = '%s.%s' % (prefix, ext)
            if os.path.isfile(fn):
                l.append(fn)
        
        
        if self._numbered:
            # look for files like "model1.sh", "model10.sh", and sort by number
            bynum = dict()
            fns = glob('%s[0-9]*' % (prefix))
            for fn in fns:
                (name, ext) = os.path.splitext(os.path.basename(fn))
                try:
                    n = int(name[len(prefix):len(name)])
                    if n >= 0:
                        if n not in bynum:
                            bynum[n] = []
                        bynum[n].append(fn)
                except ValueError, e:
                    pass
            
            for i in sorted(bynum):
                for ext in self._languages:
                    for fn in bynum[i]:
                        if fn.endswith(ext) and os.path.isfile(fn):
                            l.append(fn)

        return l
        
    def _run_scriptie(self, prefix):
        for fn in self._find_scripties(prefix):
            self._runner.run(fn)

    def _run_package(self, style = 'default'):
        '''Changes to the rootdir and then runs all commands for the package'''
        prev_dir = os.path.abspath(os.getcwd())
        self._logger('Running package with %s style [%s]' % (style, self._rootdir))
        os.chdir(self._rootdir)
        try:
            for scriptie in self._styles[style]:
                self._run_scriptie(scriptie)
        finally:
            os.chdir(prev_dir)


    def _iter_dirs(self, root):
        results = deque()
        q = deque()
        q.append(root)
        while len(q) > 0:
            dirp = q.popleft()
            for dirent in os.listdir(dirp):
                p = os.path.join(dirp, dirent)
                if os.path.isdir(p) \
                   and dirent not in self._excluded_dirs \
                   and not _match_prefixes(dirent, self._excluded_prefix):
                    q.append(p)
                    results.append(p[len(root)+1:]) # strip root prefix
        return results

    def _bylevel_iter(self, root):
        '''returns an inverse level-ordered list of pathnames to subdirectories relative to root'''
        # fetch all subdirs minus exclusions
        alldirs = self._iter_dirs(root)

        # find max depth
        maxdepth = 0
        for p in alldirs:
            depth = len(p.split('/'))
            maxdepth = max(depth, maxdepth)

        # create level-based list
        results = [[i+1] for i in xrange(0, maxdepth)]

        # insert all paths into level-based list
        for p in alldirs:
            depth = len(p.split('/'))
            results[depth-1].append(p)

        # invert the list
        results.reverse()
        return results

    def _logger(self, s):
        if not self._quiet: 
            print >>sys.stderr, s
