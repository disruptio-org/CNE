
import pandas as pd
from ..models.schemas import CandidateRecord
from .normalizer import dataframe_from_records
def export_csv(path: str, records: list) -> str:
    df = dataframe_from_records(records)
    df = df.fillna('')
    if 'NUM_ORDEM' in df.columns:
        df['NUM_ORDEM'] = pd.to_numeric(df['NUM_ORDEM'], errors='coerce').fillna(0).astype(int)
    for col in ['DTMNFR','SIGLA','SIMBOLO','NOME_LISTA','NOME_CANDIDATO','PARTIDO_PROPONENTE','ORGAO','TIPO']:
        if col in df.columns:
            df[col] = df[col].astype(str).replace({'None':'','nan':'','NaN':'','NoneType':''})
    df.to_csv(path, index=False, sep=';')
    return path
