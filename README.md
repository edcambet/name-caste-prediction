# name-caste-prediction
Code to transliterate names, convert them to a Hindi pronunciation code, and then predict caste using the names.

## Table of contents
* [General information] (#general-information)
* [Requirements] (#requirements)
* [Name conversion] (#name-conversion)
* [Caste prediction results] (#caste-prediction-results)

## General information


## Requirements
The soundex_mnrega_names.py script requires the charmap.py and soundex/py scripts and the following modules, listed on the requirements.txt file:
* os
* pandas
* indic-transliteration

## Name conversion
The basis of predicting caste is using names. In order to make meaningful matches of names across datasets, I follow the procedure laid out by Raphael Susewind to predict religion. I first transliterate names from Roman script to Hindi script and then create a pronunciation code for each name using the soundex.py and charmap.py scripts developed by Santhosh Thottingal.


## Caste prediction results
