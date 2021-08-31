import sys
sys.path.append('../../../../')
import support.data_tools as dtools

#nonstandard imports
import pandas as pd

import JED_Utilities_public as JU
import JED_Cleaning_Functions_public as JCF
import JED_Pipelines_public as JP
import JED_Algorithms_public as JA

##########
## MAIN ##
##########
if __name__ == "__main__":

    cfg = JU.ingest_disambiguation_update_config('config_disambiguation_update.cfg')


    # Gather in the new extracted entity data
    raw_df, heads_df = JCF.ingest_raw_entities(cfg['DATA_FILES'])  

    # fjc demographics
    fjc_active = JCF.ingest_the_fjc(
        cfg['FJC']['fjc_file'],
        low = cfg['FJC']['Low'],
        high = cfg['FJC']['High'],
        is_update=True)  

    # bring in docket data for year information
    # dockets table for year/filing dates
    dockets = dtools.load_unique_files_df()
    all_dockets = dtools.load_unique_files_df()
    # parties and counsels
    parties = JCF.ingest_header_entities(cfg['DATA_PARTIES'])
    counsels = JCF.ingest_header_entities(cfg['DATA_COUNSELS'])

    # load in the prior iteration of the JEL that we will be updating
    oldJEL = pd.read_json(cfg['JEL'], lines=True)
    # identify the previoulsy known NIDs of FJC judges
    oldNIDS = [str(i).split('.')[0] for i in oldJEL.NID.values if not pd.isna(i)]

    # identify the updated codebook judges that we did not know about before
    # -- it is possible that a new FJC judge was a previoulsy identified magistrate in the JEL
    # -- this gets addressed in disambiguation code, don't you worry
    newest_fjc = fjc_active[~fjc_active.nid.isin(oldNIDS)]

    # for the new dockets, run them through the standard cleaning and preparation pipeline
    FDF = JP.PIPELINE_Disambiguation_Prep(raw_df, heads_df, dockets)

    # # for the new dockets again, run through UCID only matching and then court only matching
    # # we do this for new ones only because we do not label existing entities as "court"-specific as they may move around.
    # Because there are no court specifics, if there are duplicate entities they will naturally be merged together during the free match
    Post_UCID = JP.UCID_MATCH_PIPELINE(FDF, parties, counsels)
    Post_Court = JP.COURT_MATCH_PIPELINE(Post_UCID)

    # using the old SEL files, old JEL, and new dockets, run a disambiguation routine that keeps all old JEL entities, edits them, or appends to them, and introduces new entities if applicable
    paths = cfg['OUT_PATHS']
    readySEL, RPDF, updated_JEL = JP.NEW_DISAMBIGUATION_FREEMATCH_PIPELINE(paths, all_dockets, newest_fjc, fjc_active, oldJEL, Post_Court)
    # run a final crosscheck through the old and new ucid dockets
    # if the old ones get changed, we will edit their jsonl files, new ones get instantiated
    Old_docks, New_docks = JP.PIPE_UPDATE_FINAL_CROSSCHECK(readySEL, RPDF)

    # write it all to file
    JU.UPDATED_DISAMBIGUATION_TO_JSONL(paths, updated_JEL, Old_docks, New_docks)
