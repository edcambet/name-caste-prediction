# name-caste-prediction
Code to transliterate names, convert them to a Hindi pronunciation code, and then predict caste using the names.

## Table of contents
* [General information](#general-information)
* [Requirements](#requirements)
* [Name conversion](#name-conversion)
* [Caste prediction results](#caste-prediction-results)

## General information
This repository consists of two scripts that I use in my job market paper. The first is a Python script that transliterates a list of names and generates pronunciation codes. The second is a Stata script that evaluates the predicted caste given a person's names within a dataset that contains both names and caste information for a nationaly representative sample of India.

## Requirements
The soundex_mnrega_names.py script requires the charmap.py and soundex/py scripts and the following modules, listed on the requirements.txt file:
* os
* pandas
* indic-transliteration

## Name conversion
The basis of predicting caste is using names. In order to make meaningful matches of names across datasets, I follow the procedure laid out by Raphael Susewind to predict religion. I first transliterate names from Roman script to Hindi script and then create a pronunciation code for each name using the soundex.py and charmap.py scripts developed by Santhosh Thottingal.

## Caste prediction results
This Stata script takes the Hindi pronounciation names of individuals in the REDS data and evaluates the predicted castes using a cross-validation exercise. This exercise consists of repeatedly and randomly splitting the data into a training and testing subsets. The training subset is used to create estimates of P(caste|name) for each name (defined by its Hindi pronunciation) and for each of the ten largest lower castes in Uttar Pradesh. This script outputs a dataset with two measures of how the caste prediction algortithm performs:
* The average probability predicted for the correct caste of the individual. (1 - Type II error rate)
* The average probability predicted to each of the incorrect castes of an individual. (Type I error rate)

