import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[4]))
import support.data_tools as dtools

#nonstandard imports
import JED_Utilities_public as JU
import JED_Cleaning_Functions_public as JCF
import JED_Pipelines_public as JP
import JED_Algorithms_public as JA



if __name__=="__main__":

    cfg = JU.ingest_config('config.cfg')
       
    ############################
    ### >> DATA INGESTION << ###
    ############################
    
    raw_df, heads_df = JCF.ingest_raw_entities(cfg['DATA_FILES'])

    # raw_df = raw_df[raw_df.year==2016].copy()
    # heads_df =  heads_df[heads_df.year==2016].copy()

    # fjc_active = JCF.ingest_the_fjc(settings.JUDGEFILE)
    ba_mag = JCF.ingest_ba_mag(
        cfg['BA_MAG']['judges'], 
        cfg['BA_MAG']['positions']
        )

    fjc_active = JCF.ingest_the_fjc(
        cfg['FJC']['fjc_file'])    

    # bring in docket data for year and filing date informtion
    # dockets table for year/filing dates
    dockets = dtools.load_unique_files_df()
        
    # parties and counsels
    parties = JCF.ingest_header_entities(cfg['DATA_PARTIES'])
    counsels = JCF.ingest_header_entities(cfg['DATA_COUNSELS'])

    ###########################
    ### >> DATA CLEANING << ###
    ###########################
    FDF = JP.PIPELINE_Disambiguation_Prep(raw_df, heads_df)

    ############################
    ### >> DISAMBIGUATION << ###
    ############################
    # within UCID disambiguation
    Post_UCID = JP.UCID_MATCH_PIPELINE(FDF, parties, counsels)
    # within court disambiguation
    ID_Mappings, ALL_NODE_IDs = JP.COURT_MATCH_PIPELINE(Post_UCID, fjc_active, ba_mag)
    # free-for-all disambiguation
    JEL, PCID_Mappings = JP.FREE_MATCH_PIPELINE(ID_Mappings, ALL_NODE_IDs, fjc_active)
    
    # Final crosschecking unmatched entities with "valid" entities on their UCIDs
    PRE_SEL = JA.FINAL_CLEANUP(PCID_Mappings)

    ###########################
    ### >> WRITING FUNC. << ###
    ###########################
    paths = cfg['OUT_PATHS']
    JEL, SEL = JU.TO_JSONL(PRE_SEL, JEL, paths)