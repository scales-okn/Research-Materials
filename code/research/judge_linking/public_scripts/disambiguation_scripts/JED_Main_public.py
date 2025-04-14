import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[4]))
import support.data_tools as dtools
import support.settings as settings

# nonstandard imports
sys.path.append(str(Path(__file__).resolve().parents[0]))
import JED_Utilities_public as JU
import JED_Cleaning_Functions_public as JCF
import JED_Pipelines_public as JP
import JED_Algorithms_public as JA



def run_jed(use_config_file=True, indir=None, outdir=None):

    if use_config_file:
        cfg = JU.ingest_config('config.cfg')
    else:
        indir = Path(indir).resolve()
        outdir = Path(outdir).resolve()
       
    ############################
    ### >> DATA INGESTION << ###
    ############################
    
    raw_df, heads_df = JCF.ingest_raw_entities(cfg['DATA_FILES'] if use_config_file else indir)
    ba_mag = JCF.ingest_ba_mag(settings.BAMAG_JUDGES, settings.BAMAG_POSITIONS)
    fjc_active = JCF.ingest_the_fjc(settings.JUDGEFILE)    
        
    # parties and counsels
    parties = JCF.ingest_header_entities(cfg['DATA_PARTIES'] if use_config_file else indir/'parties.csv')
    counsels = JCF.ingest_header_entities(cfg['DATA_COUNSELS'] if use_config_file else indir/'counsels.csv')

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

    paths = cfg['OUT_PATHS'] if use_config_file else {'JEL':outdir/'JEL.jsonl', 'SEL':outdir/'SEL.jsonl', 'SEL_DIR':outdir/'SEL_DIR'}
    JEL, SEL = JU.TO_JSONL(PRE_SEL, JEL, paths)



if __name__=="__main__":
    run_jed()