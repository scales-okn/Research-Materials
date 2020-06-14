# Instructions


# Data file coding

The resultant data file after rules have been applied to identify motions and outcomes is
`ifp_cases.csv`. Each identified petition is a row in the csv and the columns are as follows:

* `jurisdiction` (str) - the jurisdiction that the case was downloaded from. Uses the PACER court
  abbreviation (i.e. "nyed" is the "Eastern District of New York").
* `case_id` (str) - the case identifier assigned to the case in the corresponding PACER portal.
* `judge_name` (str) - The hashed judge identifed from docket entries that presided over the in forma
  pauperis motion. 
* `entry_date` (str) - Date of the docket entry determined to be the petition outcome
* `resolution` (int) - Detected outcome of the in forma pauperis petition.

## Resolution outcomes 

There are five potential resolution outcomes: `1`, `0`, `-1`, `-10`, `-999`. Their meanings are as follows:

* `1` - the petition was filed and granted
* `0` - the petition was filed, but no resolution is detected.
* `-1` - the petition was filed and denied. 
* `-10` - the petition was filed, but the case is detected as being dismissed without a decision on
  the petition itself. 
* `-999` - detected as the judge or administrative body referencing in forma pauperis without a
  petition being filed from the plaintiff.
