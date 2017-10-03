from glob import glob
from os import path

import yt
from yt.funcs import mylog  # TODO: don't depend on yt

from functools import lru_cache
import re
import subprocess


INFO_RE = re.compile('info_(\d{5})\.txt')
CONFIG = dict(
    exe='/home/cadiou/codes/HaloFinder/f90/HaloFinder',
    exe_BR='/home/cadiou/codes/HaloFinder/f90/HaloFinder_BR'
)

def dict2conf(d):
    res = []
    for k, v in d.items():
        lhs = '{:<20s}'.format(k)
        if type(v) == bool:
            rhs = '.true.' if v else '.false.'
        else:
            rhs = '{}'.format(v)

        res += ['{} = {}'.format(lhs, rhs)]

    return '\n'.join(res)

class HaloFinder:
    def __init__(self, folder, prefix='.', ifirst=None, ilast=None):
        self._outputs = None
        self._datasets = None

        self.folder = folder
        self.ifirst = ifirst
        self.ilast = ilast

        self.prefix = prefix
        self.ppn = 16

    @property
    def outputs(self):
        if self._outputs is not None: return self._outputs

        folder = self.folder
        ifirst = self.ifirst
        ilast = self.ilast

        ## Detect the output files
        if not path.isabs(folder):
            before = folder
            folder = path.abspath(folder)
            mylog.warning('Changed folder from relative %s to absolute %s' % (before, folder))

        outputs = sorted(glob(path.join(folder, 'output_?????')))

        # Filter out outputs not in range
        keys = [int(out.split('_')[-1]) for out in outputs]

        if ifirst is None:
            ifirst = min(keys)
        if ilast is None:
            ilast = max(keys)

        outputs = [path.join(out, 'info_{:0>5d}.txt'.format(i)) for i, out in zip(keys, outputs)
                   if (ifirst <= i <= ilast)]

        # Print some information
        mylog.debug('Detected %s outputs' % len(outputs))
        self._outputs = outputs
        return self._outputs

    @property
    def datasets(self):
        if self._datasets is not None: return self._datasets

        self._datasets = yt.load(self.outputs)
        return self._datasets

    def write_inputfiles(self):
        outputs = self.outputs

        self.input_filelist = path.join(self.prefix, 'inputfiles_HaloMaker.dat')
        s = []
        for out in outputs:
            iout = int(INFO_RE.findall(out)[0])
            d, _ = path.split(out)
            s += ["'%s/'  Ra3  1  %05d" % (d, iout)]

        s = '\n'.join(s)
        with open(self.input_filelist, 'w') as f:
            mylog.info('Writing inputfiles_HaloMaker.dat')
            mylog.debug(s)
            f.write(s)

    def write_input(self):
        outputs = self.outputs
        ds = self.datasets[-1]

        # Compute last expansion factor
        config = dict(
            af = 1/(ds.current_redshift+1),
            lbox = ds.domain_width.in_units('Mpccm').value[0],
            H_f = ds.hubble_constant,
            omega_f = ds.omega_matter,
            lambda_f = ds.omega_lambda,
            npart = 100,
            method = 'MSM',
            cdm = False,
            b = 0.2,
            nvoisins = 20,
            nhop = 20,
            rhot = 80.,
            fudge          = 4.,      # parameter for adaptahop (usually 4.)
            fudgepsilon    = 0.001,   # parameter for adaptahop (usually 0.05)
            alphap         = 1.0,     # parameter for adaptahop (usually 1.)
            verbose        = False,   # verbose parameter for both halo finder
            megaverbose    = False,   # parameter for adaptahop
            nsteps         =  len(outputs),     # Number of time steps to analyse
            FlagPeriod     =   1,     # Periodic boundaries (1:True)
            DPMMC          = False,   # Activate the densest point in the most massive cell
            SC             = True,    # Activate the com in concentric spheres
            dcell_min      = 0.005781 # smallest possible cell size in Mpc
        )

        res = dict2conf(config)
        self.input_file = path.join(self.prefix, 'input_HaloMaker.dat')
        with open(self.input_file, 'w') as f:
            mylog.info('Writing input_HaloMaker.dat')
            mylog.debug(res)
            f.write(res)

    def write_qsub(self):
        src = """
#!/bin/sh
#PBS -S /bin/sh
#PBS -N HaloFinder
#PBS -j oe
#PBS -l nodes=1:ppn={ppn},walltime=4:00:00

module () {{
  eval $(/usr/bin/modulecmd bash $*)
}}

DIR={run_dir}
touch .started
export OMP_NUM_THREADS={ppn}
module purge
module load intel/15.0-python-3.5.1

mkdir -p $DIR
cd $DIR

{HaloFinder_BR}
touch .done
rm .started
"""
        src = src.format(
            run_dir=path.abspath(self.prefix),
            HaloFinder_BR=CONFIG['exe_BR'],
            ppn=self.ppn
        )

        self.qsub_file = path.join(self.prefix, 'job.sh')
        with open(self.qsub_file, 'w') as f:
            f.write(src)

    def submit_qsub(self):
        if not hasattr(self, 'input_filelist'):
            self.write_inputfiles()

        if not hasattr(self, 'input_file'):
            self.write_input()

        if not hasattr(self, 'qsub_file'):
            self.write_qsub()

        mylog.info('Submitting job to qsub')
        subprocess.call(['qsub', self.qsub_file])

