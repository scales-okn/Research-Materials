# code instructions

package dependencies and language models are contained within `pip_installs.sh`. on the command line
this script can be executed with bash.

`$ bash pip_installs.sh`

running the transformation from raw data to `ifp_cases.csv` is accomplished with:

`$ python build_judge_ifp_data.py`

running the transformation from the csv file to the figure is accomplished with:

`$ do figure1_script.do`

# overview

## raw data collection and availability

we queried the pacer cm/ecf system for all cases filed between jan. 1st to december 31st in 2016. we
then downloaded the dockets for all cases with a “cv” or “cr” designation that were the main case
filing directly from pacer cm/ecf that were not already contained in the free law project’s recap
database.

as of june 1st, 2020 the raw data is currently being prepared for public usage and distribution
through the scales open knowledge network. that preparation entails ensuring protections for
individual privacy and sensitive personal information that is contained in the raw data itself. updates on
availability will be given both here and on the [scales okn](http://www.scales-okn.org) website. 

## Exploitation of case assignment as a RCT

Case assignment to a judge is effectively randomized after a case is filed. There are a few situations where this is 
not the case, but these situations are largely rare and predominantly occur for two technical
reasons.  The two main reasons are (1) cases that are exceptionally procedurally
complex are sometimes assigned to a judge with expertise in handling such cases; and (2) judges
who take senior status (a kind of semi-retirement) have some control over their docket (though they
see much fewer cases than non-senior-status judges). Fortunately, the number of cases that fall
under one of thees two reasons are both small and unlikely to involve a petition for *in forma
pauperis*. 


## Identification of *in forma pauperis* and outcomes
 
We filter cases using the phrase `forma pauperis` – if this phrase occurs in any docket entry in a
case then that case is processed to identify its potential as a case with an in forma pauperis
request. Identification of the outcome of an *in forma pauperis request* is accomplished with regular
expressions.

There are four categories of regular expressions: grant, deny, non-instantiation, and dismissal.

* grant – regular expressions that have been verified as the granting of an in forma pauperis
request
* deny – regular expressions verified as the denial of an in forma pauperis request
* non-instantiation – regular expressions verified as the mention of in forma pauperis by the
judge him/herself 
* dismissal – regular expressions verified as the dismissal of a case.
 
All regular expressions from every category are applied to each docket entry. We then evaluate the categories for all
possible outcomes in order of granting, denial, non-instantiation, and dismissal. If none of the
categorical outcomes are detected the case is marked as having an indeterminate outcome.

## Attribution of petition decision to an individual judge
 
Attribution of docket entries to an individual judge is made by dividing a docket sheet into
passages of docket entries. The beginning/end of a passage of docket entries is marked by the direct matching 
of the term ‘Executive Committee Order: It appearing that cases’ in a docket entry, which transfers
the case from one judge to another.
 
Natural Language Processing (python package `spacy`, language model `en_core_web_sm`) is used to
identify all PERSON entities in an entry passage. The name with the maximum number of utterances in
the passage is assigned as the presiding judge over the passage of docket entries. If no PERSON
entity is identified, then the presiding judge is left as blank.
 
In the docket text itself or via the entity detection, only the last name of a judge may be the most
commonly used name. We attempt all possible matches of name `i` being a substring of name `j`, where
name `j` is drawn from the list of all judge names. If there is only one match between a name as a
substring and a larger substring, we match the short name to the long name. If there are no or
multiple possible substring matches, then we do not make a match return `None` as the name.

### Run options for `build_judge_ifp_data.py`

By default `build_judge_ifp_data.py` will follow the methods outlined above, which will replicate
the figure and analysis in the perspective. There are two options that will change attribution
method as described.

* `python build_judge_ifp_data.py --default_attr` -- will recognize that there are transfers but if
  `spacy` cannot detect a PERSON entity in the docket passage then it will default to the judge
  listed in the case metadata (i.e. the last recorded judge presiding over case). 
* `python build_judge_ifp_data.py --no_transfer` -- ignores the fact that there are transfers and
  defaults to attributing the case the judge listed in the case metadata.

Figures for both approaches are in the repository with the corresponding option in the filename.
Both options results in 37.63% of the judges differing from their district cohort, as opposed to ~40%
when we attempt to identify the judge in a case with transfers. These numbers are derived from following 
the same analysis and regression as specified in the perspective (`figure1_script.do`). 

## `ifp_cases.csv` file coding

The resultant data file after rules have been applied to identify motions and outcomes is
`ifp_cases.csv`. Each identified petition is a row in the csv and the columns are as follows:

* `jurisdiction` (str) - the jurisdiction that the case was downloaded from. Uses the PACER court
  abbreviation (i.e. "nyed" is the "Eastern District of New York").
* `case_id` (str) - the case identifier assigned to the case in the corresponding PACER portal.
* `hash_name` (str) - The hashed judge identifed from docket entries that presided over the in forma
  pauperis motion. 
* `entry_date` (str) - Date of the docket entry determined to be the petition outcome
* `resolution` (int) - Detected outcome of the in forma pauperis petition.

### Resolution outcomes 

There are five potential resolution outcomes: `1`, `0`, `-1`, `-10`, `-999`. Their meanings are as
follows:

* `1` - the petition was filed and granted
* `0` - the petition was filed, but no resolution is detected.
* `-1` - the petition was filed and denied. 
* `-10` - the petition was filed, but the case is detected as being dismissed without a decision on
  the petition itself. 
* `-999` - detected as the judge or administrative body referencing in forma pauperis without a
  petition being filed from the plaintiff.
