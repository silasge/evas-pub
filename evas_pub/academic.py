import os
from evas_pub.utils import clean_strings
import dask.dataframe as dd
import pandas as pd


def _academic_names_and_types(names_or_dtype):
    col_names_wtypes = {
        "cpf": "string",
        "mtr": "string",
        "inscrica": "string",
        "nome": "string",
        "per_ingr": "float64",
        "cd_forma_ingr": "float64",
        "descr_forma_ingr": "string",
        "per_saida": "float64",
        "cd_forma_saida": "float64",
        "descr_forma_saida": "string",
        "cr": "float64",
        "escore": "float64",
        "class_geral": "float64",
        "categoria_class": "string",
        "cod_curso": "float64",
        "per_crs_ini": "float64",
        "nome_curso": "string",
        "colegiado": "string",
        "col_nm_colegiado": "string",
        "per_let_disc": "float64",
        "disc": "string",
        "ch_disc": "float64",
        "nat_disc": "string",
        "tur": "string",
        "nota": "float64",
        "resultado": "string",
        "doc_nu_matricula_docente": "float64",
        "doc_nm_docente": "string",
        "doc_vinculo": "string",
        "doc_titulacao": "string",
        "doc_nivel": "string",
        "doc_regime_trab": "string",
        "nascimento": "string",
        "aln_cd_estado_civil": "string",
        "ecv_ds_estado_civil": "string",
        "sexo": "string",
        "dtnasc": "string",
        "aln_sg_estado_nascimento": "string",
        "aln_nm_pai": "string",
        "aln_nm_mae": "string",
        "aln_cd_cor": "string",
        "cor_nm_cor": "string",
        "aln_nm_cidade_nascimento": "string",
        "eda_nm_email": "string"
    }
    if names_or_dtype == "string":
        return {col_name:col_type for col_name, col_type in col_names_wtypes.items() if col_type != "float64"}
    elif names_or_dtype == "float":
        return [col_name for col_name, col_type in col_names_wtypes.items() if col_type == "float64"]
    elif names_or_dtype == "names":
        return col_names_wtypes.keys()
    elif names_or_dtype == "dtypes":
        return col_names_wtypes


def read_academic(path_to_academic, chunksize=100000):
    string_cols = _academic_names_and_types("string")
    col_names = _academic_names_and_types("names")
    academic_chunk = pd.read_csv(path_to_academic, names=col_names, sep=";", dtype=string_cols, chunksize=chunksize)
    return academic_chunk


def _process_numeric_cols(academic_chunk):
    numeric_cols = _academic_names_and_types("float")
    academic_chunk[numeric_cols] = academic_chunk[numeric_cols].apply(pd.to_numeric, errors="coerce", downcast="float")
    return academic_chunk


def _process_string_cols(academic_chunk):
    string_cols = _academic_names_and_types("string")
    academic_chunk[list(string_cols.keys())] = academic_chunk[list(string_cols.keys())].applymap(clean_strings, na_action="ignore")
    return academic_chunk


def process_academic(academic_chunk, dir_path):
    chunk_n = 1
    processed_academic = map(_process_numeric_cols, academic_chunk)
    processed_academic = map(_process_string_cols, processed_academic)
    for chunk in processed_academic:
        chunk.to_csv(os.path.join(dir_path, f"iteracao_{chunk_n}.csv"))
        print(f"chunk {chunk_n} processado")
        chunk_n += 1


def create_unique_academic_dataset(acad_0304, acad_0519, acad_1921):
    dtypes = _academic_names_and_types("dtypes")
    acad_0304 = dd.read_csv(acad_0304, dtype=dtypes)
    # fix 2019.2: melhor opção foi remover 2019.2 da base 0519
    acad_0519 = dd.read_csv(acad_0519, dtype=dtypes).query("per_let_disc != 20192")
    acad_1921 = dd.read_csv(acad_1921, dtype=dtypes)
    acad_unico = dd.concat([acad_0304, acad_0519, acad_1921])
    acad_unico.compute().to_csv("./data/interim/academica/ufba_academica_0321.csv")
            
         

if __name__ == "__main__":
    create_unique_academic_dataset(
        "data/interim/academica/academica_0304.csv", 
        "data/interim/academica/academica_0519/iteracao*.csv", 
        "data/interim/academica/academica_192021.csv"
    )