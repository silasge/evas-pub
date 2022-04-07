import re 
from unidecode import unidecode


def clean_strings(string: str) -> str:
    if not isinstance(string, str):
        raise TypeError("'string' must be a str object")
    patt = re.compile(r"\s+")
    strip_lower_str = string.strip().lower()
    squish_str = re.sub(patt, " ", strip_lower_str)
    remove_accents_str = unidecode(squish_str)
    return remove_accents_str


def clean_cpf_rg(string: str) -> str:
    if not isinstance(string, str):
        raise TypeError("'string' must be a str object")
    patt = re.compile(r"\.|-")
    cleaned_cpf_rg = re.sub(patt, "", string)
    return cleaned_cpf_rg
    
    