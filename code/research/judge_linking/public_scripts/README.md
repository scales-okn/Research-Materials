# SCALES-OKN Judicial Entity Disambiguation (JED)

This repository is a collection of scripts that constitute the **Judicial Entity Disambiguation (JED) Pipeline.** In federal court records, every docket contains multiple mentions of judges, singular and plural. Our task was to identify, extract, and disambiguate these judges.

The Federal Judicial Center publishes a [biographical dictionary](https://www.fjc.gov/history/judges/biographical-directory-article-iii-federal-judges-export) of Article III judges annually with appointments, commissions, and presiding courts. Notably, this list does not contain Article I judges (such as magistrates) nor district judges from Guam, Northern Mariana Islands, and the Virgin Islands. Our task was intended to identify and label the existing Article III judges appropriately as well as identify and label these other judicial entities.

This repository represents a pipeline that receives pre-extracted entity data as an input. The entity data is assumed to have been confidently extracted in that there will not be a large portion of entities that are *not* judges. The disambiguation pipeline will detect clerks, attorneys, and arbitrators as meaningfully different than judges given their contexts and will attempt to not tag these as judges. However, the more noise this pipeline receives, the higher the chance an incorrectly extracted entity may be mistaken for a judge.

### What is not in this repository:
- Named Entity Recognition (NER) models trained on federal court records to identify and extract judicial entities
- Scripts that will extract entities or apply a user specified model to federal court dockets
- Specific, large scale entity-tagged datasets

### What is in this repository:
- Sample inputs
- Various scripts and sub-modules used in disambiguation
- [Data Models](./data_models.md) for the various input and output datasets

### What do I need?
The following python packages are needed to run these scripts: 
- pandas
- fuzzywuzzy
- [flashtext](https://github.com/vi3k6i5/flashtext)
- tqdm

Additionally, it is assumed a user has familiarity with or is already using other SCALES-OKN tools such as our docket-tools, filehandle-tools and has familiarity with our json structure.

## **Baseline Disambiguation:**
The baseline disambiguation task is meant to be run when a user has a alrge pool of court records that branch across multiple cases and courts. Minimally a user has over 5,000 unique cases and ideally over 10,000 cases to perform disambiguation.

Main: 
- locate `disambiguation_scripts/JED_Main_public.py`
- edit `config.cfg`
- run `python JED_Main_public.py`

Inputs: 
- `inputs/input_extractions/` contains 95 files representing micro-samples of extracted entities from dockets in 94 district courts + 1 file containing all case header judge allocations 
- `inputs/input_header_meta/` contains 1 file representing parties listed in case headers and 1 file representing those parties corresponding counsels. If a robust entity extraction model existed, these additional entities could be excluded from disambiguation. They are used to invalidate false positive entity extractions that are parties involved in a case.
- `disambiguation_scripts/config.cfg` is the main configuration file for disambiguation, pre-populated with this repository's samples
    - **name_of_proj**: generic name for your project
    - **base_directory**: filepath to write outputs, relative to this config and main script
    - **data_outputs**: name to give the outputs folder to be nested within the base_directory
    - **fjc_file**: relative location of the FJC Article III judges csv exported from the FJC data website.
    - **commission_start** and **commission_end**: YYYY-MM-DD bounds specified for what range of commissioned judges are to be included in the output list of unique judges 
    - **NER_Extractions**: directory input of extracted entity data
    - **parties_csv** and **counsels_csv**: relative file locations of case metadata from headers

Outputs:
- By default, the project creates an output directory located in parallel with the inputs folder `sample_public_jed/` containing 2 subdirectories:
    - `/outputs_main/` (or whatever **data_outputs** location is specified) which contains:
        - `JEL_HH_MM_SS.jsonl`
        - `SEL_HH_MM_SS.jsonl`
        - `SEL_DIR/` containing case-level tagged entity data jsonls
    - `/Logs/` which contains logfiles tracking dismabiguation decisions made throughout the pipeline during various runs
        - `JED_log_HH_MM_SS.log`

## **Ad-Hoc Case Tagging:**
Ad-Hoc Case Tagging is performed on a small sample of new cases when a user already has an existing list of tagged and disambiguated unique judicial entities. This script is meant to tag cases with known judges but *does not* identify new judges nor re-disambiguate existing docket data.

Main: 
- locate `disambiguation_scripts/JED_Tag_New_Cases.py`
- edit `config_tag_new.cfg`
- run `python JED_Tag_New_Cases.py`

Inputs: 
- `inputs/input_new_tags/` contains 4 files to be ingested as inputs
    - `new_counsels.csv` and `new_parties.csv`: csv files containing the case metadata from the headers of the new dockets to be tagged with known judges
    - `new_entries.csv`: pre-extracted entities for the new dockets to be tagged with known judges
    - `new_headers.csv`: pre-extracted entities from the header asssignments of the new dockets to be tagged with known judges
- `disambiguation_scripts/config_tag_new.cfg` is the configuration file for tagging new cases, pre-populated with this repository's samples
    - **name_of_proj**: generic name for your project (existing from the baseline run)
    - **base_directory**: filepath to write outputs, relative to this config and main script (existing from the baseline run)
    - **data_outputs**: name to give the outputs folder to be nested within the base_directory (existing from the baseline run)
    - **jel_location**: relative filepath to the jel jsonl file which contains the original baseline disambiguation uniquie judge information
    - **fjc_file**: relative location of the FJC Article III judges csv exported from the FJC data website.
    - **commission_start** and **commission_end**: YYYY-MM-DD bounds specified for what range of commissioned judges are to be included in the output list of unique judges 
    - **active_period_min**: argument used to speed up case tagging, if cases are newer, this minimum date cutoff will throw out all retired or terminated judges before the minimum date to speed up tagging.
    - **new_xxxxxxx.csv**: relative paths to input files specified above

Outputs:
- By default, tagging adds updated case jsonls into the existing directory structure. The bulk jsonl is placed in parallel to the original `SEL_HH_MM_SS.jsonl`
    - `/outputs_main/` (or whatever **data_outputs** location is specified) which contains:
        - `JEL_HH_MM_SS.jsonl`
        - `SEL_HH_MM_SS.jsonl`
        - `SEL_DIR/` containing case-level tagged entity data jsonls
        - [Tagging Script Creates] `SEL_Update_HH_MM_SS.jsonl`
    - `/Logs/` which contains logfiles tracking disambiguation decisions made throughout the pipeline during various runs
        - `JED_tagging_log_HH_MM_SS.log`


## **Updating Disambiguation:**
The disambiguation update is meant to be run infrequently, when a new pool of cases is receieved (upwards fo 10,000 new cases) that would prompt a re-evaluation of the "unique judge list". The JEL (unique judge list) is a one-way entity list in that entities can only be appended or altered, but never removed. Alterations occur as entity label predictions (i.e. once a Magistrate Judge --> now a District Judge).

Main: 
- `disambiguation_scripts/JED_Update_Disambiguation_public.py`
- edit `config_disambiguation_update.cfg`
- run `python JED_Update_Disambiguation_public.py`

Inputs: 
- `inputs/input_new_disambiguation/extracted_entities/` contains 95 files representing micro-samples of extracted entities from dockets in 94 district courts + 1 file containing all case header judge allocations. These 95 files contain extracted entities from prior disambiguations *as well as* the updated case entities.
- `inputs/input_new_disambiguation/meta_headers/` contains 1 file representing parties listed in case headers and 1 file representing those parties corresponding counsels. Again these files contain the entity information for all cases, both prior already disambiguated and new cases.
- `disambiguation_scripts/config_disambiguation_update.cfg` is the main configuration file for disambiguation, pre-populated with this repository's samples
    - **name_of_proj**: generic name for your project (existing from the baseline run)
    - **base_directory**: filepath to write outputs, relative to this config and main script (existing from the baseline run)
    - **data_outputs**: name to give the outputs folder to be nested within the base_directory (existing from the baseline run)
    - **jel_location**: relative filepath to the jel jsonl file which contains the original baseline disambiguation uniquie judge information
    - **fjc_file**: relative location of the FJC Article III judges csv exported from the FJC data website.
    - **commission_start** and **commission_end**: YYYY-MM-DD bounds specified for what range of commissioned judges are to be included in the new run of disambiguation. *NOTE: this should be a mutually exclusive timeframe with the baseline disambiguation (and will likely represent the time elapsed since prior disambiguation run)* 
    - **NER_Extractions**: directory input of extracted entity data
    - **parties_csv** and **counsels_csv**: relative file locations of case metadata from headers

Outputs:
- By default, the project creates an output directory located in parallel with the inputs folder `sample_public_jed/` containing 2 subdirectories:
    - `/outputs_main/` (or whatever **data_outputs** location is specified) which contains:
        - `JEL_HH_MM_SS.jsonl`
        - `SEL_HH_MM_SS.jsonl`
        - `SEL_DIR/` containing case-level tagged entity data jsonls for the old and new cases now
        - [Update Disambiguation Script Creates] `Updated_JEL_HH_MM_SS.jsonl` which is the up to date unique entity list
    - `/Logs/` which contains logfiles tracking dismabiguation decisions made throughout the pipeline during various runs
        - `JED_Update_log_HH_MM_SS.log`