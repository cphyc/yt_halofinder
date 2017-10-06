from glob import glob
import os
from os import path, mkdir

import yt
from yt.funcs import mylog  # TODO: don't depend on yt

from functools import lru_cache
import re
import subprocess
import shutil


INFO_RE = re.compile('info_(\d{5})\.txt')
BRICK_RE = re.compile('tree_bricks(\d{3})')

CONFIG = dict(
    halofinder   ='/home/cadiou/codes/HaloFinder/f90/HaloFinder',
    halofinder_BR='/home/cadiou/codes/HaloFinder/f90/HaloFinder_BR',
    treemaker   ='/home/cadiou/codes/TreeMaker/f90/TreeMaker',
    treemaker_BR='/home/cadiou/codes/TreeMaker/f90/TreeMaker_BR'
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
        mylog.info('Writing input files to %s' % self.input_filelist)
        with open(self.input_filelist, 'w') as f:
            mylog.debug(s)
            f.write(s)

    def write_input(self, **kwargs):
        outputs = self.outputs
        ds = self.datasets[-1]

        # Compute last expansion factor
        config = dict(
            af = 1/(ds.current_redshift+1),
            lbox = ds.domain_width.in_units('Mpccm').value[0], # in Mpc
            H_f = ds.hubble_constant*100,                      # in km/s/Mpc
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
            dcell_min      = 3.05e-3  # smallest possible cell size in Mpc (levelmax=15)
        )

        config.update(kwargs)

        res = dict2conf(config)
        self.input_file = path.join(self.prefix, 'input_HaloMaker.dat')
        mylog.info('Writing input parameters to %s' % self.input_file)
        with open(self.input_file, 'w') as f:
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

DIR={wdir}
export OMP_NUM_THREADS={ppn}
module purge
module load intel/15.0-python-3.5.1

cd $DIR
touch .started
{HaloFinder_BR} && rm .started && touch .done
"""

        wdir = path.abspath(self.prefix)
        if not path.isdir(wdir):
            subprocess.call(['mkdir', wdir, '-p'])

        src = src.format(
            wdir=wdir,
            HaloFinder_BR=CONFIG['halofinder_BR'],
            ppn=self.ppn
        )

        mylog.info('Copying binary file into %s' % self.prefix)
        subprocess.call(['cp', CONFIG['halofinder_BR'], self.prefix])

        self.qsub_file = path.join(self.prefix, 'job_halomaker.sh')
        mylog.info('Writing qsub file to %s' % self.qsub_file)
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


class TreeMaker:
    def __init__(self, folder, prefix='.'):
        self.folder = folder
        self.prefix = path.abspath(prefix)

        self.ppn = 16

        self._bricks = None

    @property
    def bricks(self):
        if self._bricks is not None: return self._bricks

        self._bricks = sorted(glob(path.join(self.prefix, 'tree_bricks???')))
        mylog.info('Found %s tree bricks' % len(self._bricks))
        return self._bricks


    def write_inputfiles(self):
        bricks = ("'%s'" % b for b in self.bricks)

        self.input_filelist = path.join(self.prefix, 'input_TreeMaker.dat')
        mylog.info('Writing list of bricks in %s' % self.input_filelist)

        header_line = '{nbrick_in} {ntree_out}\n'.format(
            nbrick_in = len(self.bricks),
            ntree_out = 1
        )
        with open(self.input_filelist, 'w') as f:
            f.write(header_line)
            f.write('\n'.join(bricks))

    def write_qsub(self):
        src = """
#!/bin/sh
#PBS -S /bin/sh
#PBS -N TreeMaker
#PBS -j oe
#PBS -l nodes=1:ppn={ppn},walltime=4:00:00

module () {{
  eval $(/usr/bin/modulecmd bash $*)
}}

DIR={wdir}
export OMP_NUM_THREADS={ppn}
module purge
module load intel/15.0-python-3.5.1

cd $DIR
touch .started
{TreeMaker_BR} && rm .started && touch .done
"""

        wdir = path.abspath(self.prefix)
        if not path.isdir(wdir):
            subprocess.call(['mkdir', wdir, '-p'])

        src = src.format(
            wdir=wdir,
            TreeMaker_BR=CONFIG['treemaker_BR'],
            ppn=self.ppn
        )

        mylog.info('Copying binary file into %s' % self.prefix)
        subprocess.call(['cp', CONFIG['treemaker_BR'], self.prefix])

        self.qsub_file = path.join(self.prefix, 'job_treemaker.sh')
        mylog.info('Writing qsub file to %s' % self.qsub_file)
        with open(self.qsub_file, 'w') as f:
            f.write(src)

    def submit_qsub(self):
        if not hasattr(self, 'input_filelist'):
            self.write_inputfiles()

        if not hasattr(self, 'qsub_file'):
            self.write_qsub()

        mylog.info('Submitting job to qsub')
        subprocess.call(['qsub', self.qsub_file])


def setup_for_pyrat(prefix, dest='.'):
    # Get absolute paths
    prefix = path.abspath(prefix)
    dest = path.abspath(dest)

    # Find all brick files
    bricks = sorted(glob(path.join(prefix, 'tree_bricks???')))

    # Helper to create a folder if not existing
    def mkdir_p(d):
        if not path.isdir(d):
            mkdir(d)

    mkdir_p(dest)

    ## Create Halo directory
    mylog.info('Creating halo directories')

    dest_halos = path.join(dest, 'Halos')
    mkdir_p(dest_halos)

    for b in bricks:
        ibrick = int(BRICK_RE.findall(b)[0])
        _, brick_name = path.split(b)
        dest_halo_i = path.join(dest_halos, str(ibrick))
        mkdir_p(dest_halo_i)

        bdest = path.join(dest_halo_i, brick_name)
        mylog.debug('Linking %s -> %s' % (b, bdest))

        if path.exists(bdest): os.remove(bdest)

        # Create symlink
        os.symlink(b, bdest)

    ## Create Tree directory
    mylog.info('Creating tree directories')
    dest_tree = path.join(dest, 'Trees')
    mkdir_p(dest_tree)

    for b in bricks:
        ibrick = int(BRICK_RE.findall(b)[0])
        _, brick_name = path.split(b)

        bdest = path.join(dest_tree, brick_name)
        mylog.debug('Linking %s -> %s' % (b, bdest))

        if path.exists(bdest): os.remove(bdest)

        os.symlink(b, bdest)

    ## Create links of all result files
    def link_files(mask):
        for file in glob(path.join(prefix, mask)):
            fname = path.split(file)[1]
            dest = path.join(dest_tree, fname)

            mylog.debug('Linking %s -> %s' % (file, dest))
            if path.exists(dest): os.remove(dest)

            os.symlink(file, dest)

    link_files('tstep_file_???.001')
    link_files('tree_file_???.001')
    link_files('props_???.001')
    link_files('halos_results.???')
