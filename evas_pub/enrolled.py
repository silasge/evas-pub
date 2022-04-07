import os
from glob import glob
import pandas as pd
from evas_pub.utils import clean_strings, clean_cpf_rg


def _enrolled_types(names_or_dtype: str):
    col_names_wtypes = {
        "NU_ETAPA": "float",
        "CO_IES": "float",
        "NO_IES": "string",
        "SG_IES": "string",
        "SG_UF_IES": "string",
        "NO_CAMPUS": "string",
        "CO_IES_CURSO": "float",
        "NO_CURSO": "string",
        "DS_TURNO": "string",
        "DS_FORMACAO": "string",
        "QT_VAGAS": "float",
        "CO_INSCRICAO_ENEM": "float",
        "NO_INSCRITO": "string",
        "NU_CPF": "string",
        "DT_NASCIMENTO": "string",
        "TP_SEXO": "string",
        "NU_RG": "string",
        "NO_MAE": "string",
        "DS_LOGRADOURO": "string",
        "NU_ENDERECO": "float",
        "DS_COMPLEMENTO": "string",
        "SG_UF_INSCRITO": "string",
        "NO_MUNICIPIO": "string",
        "NO_BAIRRO": "string",
        "NU_CEP": "string",
        "NU_FONE1": "string",
        "NU_FONE2": "string",
        "DS_EMAIL": "string",
        "NU_NOTA_L": "float",
        "NU_NOTA_CH": "float",
        "NU_NOTA_CN": "float",
        "NU_NOTA_M": "float",
        "NU_NOTA_R": "float",
        "CO_CURSO_INSCRICAO": "float",
        "DT_CURSO_INSCRICAO": "string",
        "HR_CURSO_INSCRICAO": "string",
        "DT_MES_DIA_INSCRICAO": "string",
        "ST_OPCAO": "string",
        "NU_NOTA_CURSO_L": "float",
        "NU_NOTA_CURSO_CH": "float",
        "NU_NOTA_CURSO_CN": "float",
        "NU_NOTA_CURSO_M": "float",
        "NU_NOTA_CURSO_R": "float",
        "ST_ADESAO_ACAO_AFIRMATIVA_CURS": "string",
        "NO_MODALIDADE_CONCORRENCIA": "string",
        "ST_BONUS_PERC": "string",
        "QT_BONUS_PERC": "float",
        "NO_ACAO_AFIRMATIVA_BONUS": "string",
        "NU_NOTA_INSCRITO": "float",
        "NU_NOTACORTE_CONCORRIDA": "float",
        "NU_CLASSIFICACAO": "float",
        "ST_APROVADO": "string",
        "ST_MATRICULA": "string",
        "DT_MATRICULA_EFETIVADA": "string",
        "DT_MES_DIA_MATRICULA": "string",
        "ST_MATRICULA_CANCELADA": "string",
        "DT_MATRICULA_CANCELADA": "string",
        "NO_MOD_CONCORRENCIA_ORIG": "string",
        "ST_LEI_OPTANTE": "string",
        "ST_LEI_RENDA": "string",
        "ST_LEI_ETNIA_P": "string",
        "ST_LEI_ETNIA_I": "string",
    }
    if names_or_dtype == "string":
        return {col_name:col_type for col_name, col_type in col_names_wtypes.items() if col_type != "float"}
    elif names_or_dtype == "numeric":
        return [col_name for col_name, col_type in col_names_wtypes.items() if col_type == "float"]
    elif names_or_dtype == "names":
        return col_names_wtypes.keys()
    elif names_or_dtype == "dtypes":
        return col_names_wtypes
    


def read_enrolled(path_to_enrolled: str, encoding="ISO-8859-1") -> pd.DataFrame:
    if not os.path.exists(path=path_to_enrolled):
        raise FileNotFoundError(f"The path {path_to_enrolled} doesn't exist")
    str_dtypes = _enrolled_types("string")
    ufba_enrolled = pd.read_csv(
        path_to_enrolled,
        sep=";",
        encoding=encoding,
        dtype=str_dtypes)
    return ufba_enrolled


def _process_numeric_cols(ufba_enrolled):
    numeric_cols = _enrolled_types("float")
    ufba_enrolled[numeric_cols] = ufba_enrolled[numeric_cols].apply(pd.to_numeric, errors="coerce", downcast="float")
    return ufba_enrolled


def _process_string_and_docs_cols(ufba_enrolled):
    string_cols = _enrolled_types("string")
    ufba_enrolled[list(string_cols.keys())] = ufba_enrolled[list(string_cols.keys())].applymap(clean_strings, na_action="ignore")
    ufba_enrolled[["NU_CPF", "NU_RG"]] = ufba_enrolled[["NU_CPF", "NU_RG"]].applymap(clean_cpf_rg, na_action="ignore") 
    return ufba_enrolled


def process_enrolled(enrolled_df: pd.DataFrame, cols: list) -> pd.DataFrame:
    if not isinstance(enrolled_df, pd.DataFrame):
        raise TypeError("'enrolled_df' must be a Pandas DataFrame")
    process_enrolled_df = _process_string_and_docs_cols(enrolled_df)
    #process_enrolled_df = _process_numeric_cols(process_enrolled_df)
    process_enrolled_df = (process_enrolled_df
        .loc[:, cols]
        .rename(columns={
            "NU_NOTA_L": "NOTA_LC",
            "NU_NOTA_CH": "NOTA_CH",
            "NU_NOTA_CN": "NOTA_CN",
            "NU_NOTA_M": "NOTA_MT",
            "NU_NOTA_R": "NU_NOTA_REDACAO",
            "NU_CPF": "cpf"}))
    return process_enrolled_df
    
    
def main():
    cols_interesse = [
        "NO_CAMPUS",
        "NO_CURSO",
        "DS_TURNO",
        "DS_FORMACAO",
        "QT_VAGAS",
        "NO_INSCRITO",
        "NU_CPF",
        "DT_NASCIMENTO",
        "TP_SEXO",
        "SG_UF_INSCRITO",
        "NO_MUNICIPIO",
        "NO_BAIRRO",
        "NU_NOTA_L",
        "NU_NOTA_CH",
        "NU_NOTA_CN",
        "NU_NOTA_M",
        "NU_NOTA_R",
        "NU_NOTA_INSCRITO",
        "NU_NOTACORTE_CONCORRIDA",
        "ST_APROVADO",
        "ST_LEI_OPTANTE", 
        "ST_LEI_RENDA", 
        "ST_LEI_ETNIA_P", 
        "ST_LEI_ETNIA_I"
    ]
    inscritos = glob("./data/raw/sisu/*_inscritos.csv")
    for inscrito in inscritos:
        ufba_inscritos = read_enrolled(inscrito)
        ufba_inscritos = process_enrolled(ufba_inscritos, cols_interesse)
        ufba_inscritos.to_csv(f"./data/interim/sisu/{os.path.basename(inscrito)}_processed.csv", index=False)
        
        
if __name__ == "__main__":
    main()
    