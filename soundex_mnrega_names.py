# -*- coding: utf-8 -*-
# This file transliterates names in the MNREGA dataset from Roman script to Hindi and then creates
# a pronunciation code (in Hindi) for each name (via the soundex code).

# Requirements: This file requires the soundex and charmap scripts to be in the same folder to run.

import os
import pandas as pd
import soundex as sdx
from indic_transliteration import sanscript as sans
from indic_transliteration.sanscript import transliterate as trans

# Directory in the university's computer cluster:
dir_path = '/home/ecb4357/caste_pol_ineq/code/reviewed/bash_output'
os.chdir(dir_path)
data_dir = '../../../data/output'
in_file = '/unique_names_mnrega.csv'
out_file = '/mnrega_names_sndx.csv'

# Load file of unique names in MNREGA data:
names =  pd.read_csv(data_dir + in_file)

# Save step to outlog file so progress is accessible from the computer cluster's server:
print('Data loaded\n')

# Create a Hindi transliteration of names that are in Latin script, leave Hindi names as they are:
names['name_'] = names.name_.apply(lambda x: str(x))
names['name_lang'] = names.name_.apply(lambda x: sdx.language(x[0].lower()))
names.loc[names['name_lang'] == "en_US", 'name_hin'] = names.loc[names['name_lang'] == "en_US", 'name_'].apply(lambda x: trans(x, sans.ITRANS, sans.DEVANAGARI))
names.loc[names['name_lang'] == "hi_IN", 'name_hin'] = names.loc[names['name_lang'] == "hi_IN", 'name_'].apply(lambda x: x)

# Save step to outlog file:
print('Names transliterated\n')
print(names.head())

# Generate the soundex code from the Hindi version of all names
names['name_hin_sndx'] = names.name_hin.apply(lambda x: sdx.soundex(str(x), len=12))
names = names.drop(columns = 'name_lang')

# Save step to outlog file:
print('Soundex code generated, sample of dataset below\n')
print(names.head())

# Save dataset:
names.to_csv(data_dir + out_file, index = False)