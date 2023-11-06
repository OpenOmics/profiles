#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# ~~~~~~~~~~~~~~~
#   file system helper functions for the Dmux software package
# ~~~~~~~~~~~~~~~
from pathlib import Path
from os import access as check_access, R_OK
from functools import partial
from .sample_sheet import SampleSheet
from .labkey import LabKeyServer
from .config import LABKEY_CONFIGS, DIRECTORY_CONFIGS


def get_all_seq_dirs(top_dir, server):
    """
        Gather and return all sequencing directories from the `top_dir`. 
        This is tightly coupled at the moment to the directory that is on RML-BigSky.
        In the future will need to the take a look at how to do this more generally
    """
    if isinstance(top_dir, str): top_dir = Path(top_dir)
    _dirs = []
    for _file in top_dir.glob('*'):
        if _file.is_dir():
            for _file2 in _file.glob('*'):
                if _file2.is_dir() and check_access(_file2, R_OK):
                    _dirs.append(_file2.resolve())
    # check if directory is processed or not
    return _dirs


def get_all_staged_dirs(top_dir, server):
    return list(filter(partial(is_dir_staged, server), get_all_seq_dirs(top_dir, server)))


def runid2samplesheet(runid, top_dir=DIRECTORY_CONFIGS['bigsky']['seq']):
    """
        Given a valid run id return the path to the sample sheet
    """
    ss_path = Path(top_dir, runid)
    if not ss_path.exists():
        raise FileNotFoundError(f"Run directory does not exist: {ss_path}")
    if Path(ss_path, f"SampleSheet_{runid}.txt").exists():
        ss_path = Path(ss_path, f"SampleSheet_{runid}.txt")
    elif Path(ss_path, f"SampleSheet_{runid}.csv").exists():
        ss_path = Path(ss_path, f"SampleSheet_{runid}.csv")
    else:
        raise FileNotFoundError("Run sample sheet does not exist: " + str(ss_path) + f"/SampleSheet_{runid}.[txt, csv]")
    return ss_path


def sniff_samplesheet(ss):
    """
        Given a sample sheet file return the appropriate function to parse the
        sheet.
    """
    # TODO: 
    #   catalogoue and check for multiple types of sample sheets, so far just
    #   the NextSeq, MinSeq, CellRanger are the only supported formats
    return SampleSheet


def parse_samplesheet(ss):
    """
        Parse the sample sheet into data structure
    """
    parser = sniff_samplesheet(ss)
    return parser(ss)


def is_dir_staged(server, run_dir):
    """
        filter check for wheter or not a directory has the appropriate breadcrumbs or not

        RTAComplete.txt - file transfer from instrument breadcrumb, CSV file with values:
            Run Date, Run time, Instrument ID        
    """
    global LABKEY_CONFIGS
    this_labkey_project = LABKEY_CONFIGS[server]['container_path']
    TRANSFER_BREADCRUMB = 'RTAComplete.txt'
    SS_SHEET_EXISTS = LabKeyServer.runid2samplesheeturl(server, this_labkey_project, run_dir.name)

    analyzed_checks = [
        Path(run_dir, TRANSFER_BREADCRUMB).exists(),
        SS_SHEET_EXISTS is not None
    ]
    return all(analyzed_checks)
