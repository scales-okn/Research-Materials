import sys
sys.path.append('../../../../')
import support.data_tools as dtools

import pandas as pd
import JED_Utilities_public as JU
import JED_Cleaning_Functions_public as JCF
import JED_Pipelines_public as JP
import JED_Algorithms_public as JA

if __name__ == "__main__":
    """ main call
    it does the stuff
    """

    cfg = JU.ingest_new_tagger_config('config_tag_new.cfg')

    # bring in the raw extraction data for tagging
    entries_df = pd.read_csv(cfg['DATA_FILES']['entries'])
    heads_df =  pd.read_csv(cfg['DATA_FILES']['headers'])

    # bring in docket data for parties and counsels
    parties = pd.read_csv(cfg['DATA_FILES']['parties'])
    counsels = pd.read_csv(cfg['DATA_FILES']['counsels'])

    # bring in docket information we generally know about
    # normally this would be new cases filtered (testing was 2020+ were new)
    # we use our docket lookup to generate our list of "ucids to update" -- those filed in 2020 and later
    dockets = dtools.load_unique_files_df()
    dockets = dockets[dockets.year>=2020]

    # read in FJC data, we will use it to shrink the pool of JEL nodes we compare against in tagging
    # basically, we probably won't be tagging judges from the 1800s and early 1900s so remove them for efficiency
    fjc_active = JCF.ingest_the_fjc(
        cfg['FJC']['fjc_file'],
        low = cfg['FJC']['Low'],
        high = cfg['FJC']['High']) 
    FJC_Terms = fjc_active.groupby(['FullName','nid'], as_index=False)['Termination Date'].max()
    FJC_Terms.columns = FJC_Terms.columns.droplevel(1) # we hate pandas multi-indexes :(

    # read in the JEL
    JEL = pd.read_json(cfg['JEL'], lines=True)
    
    # cut the JEL FJC judges that we know were terminated before 2014 (new cases for now are anticipated to be 2021+)
    Qualified_JEL = JEL.merge(FJC_Terms[['nid','Termination Date']], how='left', left_on='NID', right_on='nid')
    
    Compy_JEL = Qualified_JEL[
        (Qualified_JEL['Termination Date'].isna())|
        (Qualified_JEL['Termination Date'] >= pd.to_datetime(cfg['FJC']['Active_Period']).date())]

    # prep the incoming new data for disambiguation
    UDF = JP.PIPELINE_Disambiguation_Prep(entries_df, heads_df, dockets)
    # do ucid level disambiguation on the incoming data before we try mapping it to known JEL entities
    Post_UCID = JP.UCID_MATCH_PIPELINE(UDF, parties, counsels)

    # # run the updater function using the new data and existing JEL nodes for comparisons
    PDF = JA.assess_new_cases(Post_UCID, Compy_JEL)

    # # write out the updated JSONLs for the new cases
    paths = cfg['OUT_PATHS']
    JU.UPDATE_TO_JSONL(PDF,paths)