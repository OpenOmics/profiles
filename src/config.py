import re
from pathlib import Path
from os import access as check_access, R_OK
from socket import gethostname
from collections import defaultdict


# ~~~ hosts configurations ~~~ 
def get_current_server():
    hn = gethostname()
    # bigsky hostnames
    re_bigsky = (r"ai-rml.*\.niaid\.nih\.gov", "bigsky")

    # biowulf hostnames
    re_biowulf_head = (r"biowulf\.nih\.gov", "biowulf")
    re_biowulf_compute = (r"cn\d{4}", "biowulf")
    
    # locus hostnames
    re_locus_head = (r"ai\-submit\d{1}", "locus")
    re_locus_compute = (r"ai\-hpcn\d{3}", "locus")

    host_profiles = [re_bigsky, re_biowulf_compute, re_biowulf_head, re_locus_compute, re_locus_head]

    host = None
    for pat, this_host in host_profiles:
        if re.match(pat, hn):
            host = this_host
            break
    if host is None:
        raise ValueError(f"Unknown host profile")
    return host


BIGSKY_QA, BIGSKY_DEV, BIGSKY_PROD = "rtblims-qa.niaid.nih.gov", "rtblims-dev.niaid.nih.gov", "rtblims.niaid.nih.gov"
BIGSKY_PATH = "Prod_Dup_20230509"


FRCE_PROD = "hgrepo.niaid.nih.gov",
FRCE_PATH = "COVID-19_Consortium"


# ~~~ labkey configurations ~~~ 
CONTEXT_PATH = "labkey"
LABKEY_CONFIGS = {
    "bigsky": {"domain": BIGSKY_DEV, "container_path": BIGSKY_PATH, "context_path": CONTEXT_PATH, "use_ssl": True},
    "frce": {"domain": FRCE_PROD, "container_path": FRCE_PATH, "context_path": CONTEXT_PATH, "use_ssl": True}
}


# ~~~ snakemake configurations ~~~ 
SNAKEFILE = {
    "Illumnia": {
        "demux": Path(Path(__file__).parent, "workflow", "illumina_demux", "Snakefile").resolve(),
        "ngs_qc": Path(Path(__file__).parent, "workflow", "ngs_qaqc", "Snakefile").resolve(),
    }
}

# ~~~ directory configurations ~~~ 
def get_biowulf_seq_dirs():
    top_dir = Path("/data/RTB_GRS/SequencerRuns/")
    transfer_breadcrumb = "RTAComplete.txt"
    if not top_dir.exists():
        return None
    return [xx for x in top_dir.iterdir() if x.is_dir() for xx in x.iterdir() if xx.is_dir() and Path(xx, transfer_breadcrumb).exists()]


def get_bigsky_seq_dirs():
    top_dir = Path("/gs1/RTS/NextGen/SequencerRuns/")
    transfer_breadcrumb = "RTAComplete.txt"
    if not top_dir.exists():
        return None
    seq_dirs = []
    for this_dir in top_dir.iterdir():
        if not this_dir.is_dir(): continue
        for this_child_elem in this_dir.iterdir():
            try:
                elem_checks = [
                    this_child_elem.is_dir(), 
                    Path(this_child_elem, transfer_breadcrumb).exists(),
                    check_access(this_child_elem, R_OK)
                ]
            except (PermissionError, FileNotFoundError) as error:
                continue
            if all(elem_checks):
                seq_dirs.append(this_child_elem.absolute())
    return seq_dirs


def get_locus_seq_dirs():
    # TODO: locus
    top_dir = Path("/hpcdata/rtb_grs/SequencerRuns/")
    return None


DIRECTORY_CONFIGS = {
    'bigsky': {
        'seq': get_bigsky_seq_dirs(),
        'profile': Path(Path(__file__).parent, 'profiles', 'slurm').resolve(),
        'sif': '/gs1/RTS/OpenOmics/SIFs/',
        'kaiju': {
            ''
        }
    },
    'biowulf': {
        'seq': get_biowulf_seq_dirs(),
        'profile': Path(Path(__file__).parent, 'profiles', 'biowulf').resolve(),
        'sif': '/data/OpenOmics/SIFs/',
        'kaiju': {
            'nodes': "/data/OpenOmics/references/Dmux/kaiju/kaiju_db_nr_euk_2023-05-10/nodes.dmp",
            'database': "/data/OpenOmics/references/Dmux/kaiju/kaiju_db_nr_euk_2023-05-10/kaiju_db_nr_euk.fmi",
        }
    }
}