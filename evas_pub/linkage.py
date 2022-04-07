import os
from glob import glob
import dask.dataframe as dd
import pandas as pd
from evas_pub.enem import _enem_names_and_dtypes
from evas_pub.enrolled import _enrolled_types

def linkage_enem_enrolled(enrolled_path, enem_path):
    enrolled_dtypes = _enrolled_types("string")
    enem_dtypes = _enem_names_and_dtypes("docs/dicionarios/Dicionário_Microdados_Enem_2017.xlsx", "string")
    enrolled = pd.read_csv(enrolled_path, dtype=enrolled_dtypes)
    enrolled["TP_SEXO"] = enrolled["TP_SEXO"].str.upper()
    print(len(enrolled))
    enem_files = glob(enem_path)
    enem = pd.read_csv(enem_files[0], dtype=enem_dtypes)
    linkage = enrolled.merge(enem, how="inner")
    linkage.to_csv(os.path.join("./data/processed/enem", "teste.csv"), index=False)
    print(len(linkage))


def main():
    linkage_enem_enrolled("./data/interim/sisu/20181_inscritos.csv_processed.csv", "./data/interim/enem/2017/enem_*.csv")
    #linkage.to_csv("./data/processed/linkage_enem_enrolled.csv", index=False)
    
    
if __name__ == "__main__":
    main()