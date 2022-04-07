import os
import pandas as pd 


def _enem_names_and_dtypes(enem_dict_path, names_or_dtype):
    enem_dict = (pd.read_excel(enem_dict_path, skiprows=2)
                 .rename(columns={"NOME DA VARIÁVEL": "nome", "Tipo": "tipo"})
                 .iloc[2:, [0, 5]]
                 .dropna())
    enem_dict["tipo"] = enem_dict["tipo"].map(lambda x: "float" if x == "Numérica" else "string")
    enem_dict = {names:types for names, types in zip(enem_dict["nome"], enem_dict["tipo"])}
    if names_or_dtype == "string":
        return {col_name:col_type for col_name, col_type in enem_dict.items() if col_type != "float"}
    elif names_or_dtype == "numeric":
        return [col_name for col_name, col_type in enem_dict.items() if col_type == "float"]
    elif names_or_dtype == "names":
        return enem_dict.keys()
    elif names_or_dtype == "dtypes":
        return enem_dict

def read_enem(path_enem: str, chunksize: int = 100000):
    enem_chunk = pd.read_csv(path_enem, chunksize=chunksize, sep=";", encoding="ISO-8859-1")
    return enem_chunk
    
    
def process_enem(enem_chunk, path_to_save):
    n_chunk = 1
    for chunk in enem_chunk:
        chunk = chunk.query(
            "TP_PRESENCA_CN == 1 & TP_PRESENCA_CH == 1 & TP_PRESENCA_LC == 1 & TP_PRESENCA_MT == 1 & TP_STATUS_REDACAO == 1")
        chunk.to_csv(os.path.join(path_to_save, f"enem_{n_chunk}.csv"), index=False)
        print(f"chunk {n_chunk} concluido")
        n_chunk += 1
    
    
def main():
    enem = read_enem("./data/raw/enem/microdados_enem_2016.csv")
    enem = process_enem(enem, "./data/interim/enem/2016")
    

if __name__ == "__main__":
    main()