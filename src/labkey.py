from .config import BIGSKY_QA
import pandas as pd
from requests import head
from labkey.api_wrapper import APIWrapper


class LabKeyServer:
    api = None

    def __init__(self, domain, container_path, context_path, use_ssl) -> None:
        self.api = APIWrapper(domain, container_path, context_path, use_ssl=use_ssl)

    @staticmethod
    def runid2samplesheeturl(server, project, run_id):
        LABKEY_SAMPLE_SHEET_LOCATION = {
            'bigsky': f"https://{BIGSKY_QA}/labkey/_webdav/" + project + r"/%40files/ss_transformation/SampleSheets/" + f"SampleSheet_{run_id}.csv"
        }
        ss = LABKEY_SAMPLE_SHEET_LOCATION[server]
        ss_check = head(ss)
        print(ss)
        if ss_check.status_code != 200:
            return None
        return ss

