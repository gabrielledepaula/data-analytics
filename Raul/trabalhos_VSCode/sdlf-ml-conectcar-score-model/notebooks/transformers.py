import pandas as pd
import numpy as np


#vehicletype
def fun_clean_vehicletype(col, replace_method = 'most_common'):
    '''This function imputes missing values as the majority class leve
    col : Pandas series
    replace_method : most_common or missing
    '''
    col_clean = None
    if replace_method == 'most_common':
        return col.replace([None], 'Leve').str.lower().to_frame()
    elif replace_method == 'missing':
        return col.replace([None], 'Missing').str.lower().to_frame()
    
#customerage
def fun_clean_customerage(col):
    '''This function clips customer age upper limit using the life expectancy in Brazil
    col: Pandas Series
    Reference: https://data.worldbank.org/indicator/SP.DYN.LE00.IN?locations=BR
    '''
    return col.clip(lower=18, upper=76).to_frame()

#sexoid
def fun_clean_sexoid(col, replace_method = 'most_common'):
    '''This function imputes relabels categories and missing values as the majority class or as missing
    col : Pandas series
    replace_method : most_common or missing
    '''
    clean_sex = col.replace(2.0, 'female').replace(1.0, 'male')
    
    if replace_method == 'most_common':
        return clean_sex.replace([np.nan], 'male').str.lower().to_frame()
    elif replace_method == 'missing':
        return clean_sex.replace([np.nan], 'missing').str.lower().to_frame()

#estadocivilid
def fun_clean_estadocivilid(col):
    '''This function relabels categories
    col : Pandas series
    replace_method : most_common or missing
    '''
    return col.replace([1], 'single').replace([2], 'married').replace([3, 4, 5, np.nan], 'other').to_frame()

#estado
def fun_clean_estado(col, regroup = False):
    '''This function cleans estado and gives the chance to aggregate categories.
        col : Pandas series
        regroup: False if no regrouping and True if regrouping.
    '''
    
    top_estados = ['sao_paulo', 'rio_de_janeiro', 'minas_gerais', 'parana', 'rio_grande_do_sul', 'santa_catarina']
    
    clean_estado = (
        col.str.normalize('NFKD').str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.lower()
            .str.replace(" ", "_")
    )
    
    if regroup == False:  
        return clean_estado.where(clean_estado.isin(top_estados), 'other').to_frame()
    else:
        return clean_estado.to_frame()
    
# nomeplano
def fun_clean_nomeplano(col):
    '''This function aggregate the main planos.
    col : Pandas series
    '''
    top_planos =  ['Completo', 'Básico', 'Abastece Aí']
    return (col
            .where(col.isin(top_planos), 'other')
            .str.normalize('NFKD')
            .str.encode('ascii', errors='ignore')
            .str.decode('utf-8')
            .str.lower()
            .str.replace(" ", "_")
            .to_frame())

# doesstillhasdebit
def fun_clean_doesstillhasdebit(col):
    '''Relabel categories
    col : Pandas series
    '''
    return (col
            .replace('HAD NO DEBITS ON HIS LAST TRANSACTION', 'last_transaction_no_loan')
            .replace('NO', 'last_transaction_loan_not_repaid')
            .replace('YES', 'last_transaction_loan_repaid')
            .to_frame())

# do nothing
def fun_do_noting(col):
    return col

# defaulted_on_average
def fun_clean_defaulted_on_average(col):
    '''Converts boolean to numeric'''
    return col.astype('float64')
