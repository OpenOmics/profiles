#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# ~~~~~~~~~~~~~~~
#   module loading functions for the Dmux software package
# ~~~~~~~~~~~~~~~
from Dmux.config import get_current_server
from shutil import which
from os import system, environ


host = get_current_server()


def init_mods():
    if host == 'biowulf':
        try:
            snk_exists = 'snakemake' in environ["PATH"]
            if not snk_exists:
                # account for virtual environment snakemake
                if "__LMOD_REF_COUNT_PATH" in environ:
                    environ["__LMOD_REF_COUNT_PATH"] = "/usr/local/current/singularity/3.10.5/bin:1;/usr/local/apps/snakemake/7.32.3/bin:1;" + environ['__LMOD_REF_COUNT_PATH']
                else:
                    environ["__LMOD_REF_COUNT_PATH"] = "/usr/local/current/singularity/3.10.5/bin:1;/usr/local/apps/snakemake/7.32.3/bin:1;"

                if "_LMFILES_" in environ:
                    environ["_LMFILES_"] = "/usr/local/lmod/modulefiles/snakemake/7.32.3.lua:" +  environ["_LMFILES_"]
                else:
                    environ["_LMFILES_"] = "/usr/local/lmod/modulefiles/snakemake/7.32.3.lua:"

                environ["PATH"] = "/usr/local/apps/snakemake/7.32.3/bin:" + environ["PATH"]

            if "_LMFILES_" in environ:
                environ["_LMFILES_"] = "/usr/local/lmod/modulefiles/singularity/3.10.5.lua::" +  environ["_LMFILES_"]
            else:
                environ["_LMFILES_"] = "/usr/local/lmod/modulefiles/singularity/3.10.5.lua:"
            
            if "__LMOD_REF_COUNT_MANPATH" in environ:
                environ["__LMOD_REF_COUNT_MANPATH"] = "/usr/local/current/singularity/3.10.5/share/man:1;" + environ['__LMOD_REF_COUNT_MANPATH']
            else:
                environ["__LMOD_REF_COUNT_MANPATH"] = "/usr/local/current/singularity/3.10.5/share/man:1;"

            if "MANPATH" in environ:
                environ["MANPATH"] = "/usr/local/current/singularity/3.10.5/share/man:" + environ["MANPATH"]
            else:
                environ["MANPATH"] = "/usr/local/current/singularity/3.10.5/share/man:"

            environ["PATH"] = "/usr/local/current/singularity/3.10.5/bin:" + environ['PATH']
            update_vals = dict(
                LMOD_FAMILY_SINGULARITY = "singularity",
                biowulf_FAMILY_SINGULARITY = "singularity",
                LOADEDMODULES = "singularity/3.10.5" if not snk_exists else "snakemake/7.32.3:singularity/3.10.5"
            )
            environ.update(update_vals)
        finally:
            proc = 0
    else:
        proc = system(get_mods())
    return proc == 0


def get_mods():
    mods_needed = ['snakemake', 'singularity']
    mod_cmd = []

    if host == 'bigsky':
        # singularity is installed to system
        mod_cmd.append('source /gs1/RTS/OpenOmics/bin/dependencies.sh')
    elif host == 'biowulf':
        mod_cmd.append('module purge')
        mod_cmd.append(f"module load {' '.join(mods_needed)}")
    else:
        raise NotImplemented('Unknown host profile: do not know how to initialize modules')

    return '; '.join(mod_cmd)


def close_mods():
    if host == 'biowulf':
        p = system('module purge')
    elif host == 'bigsky':
        p = system('spack unload -a')
    return int(p) == 0
    