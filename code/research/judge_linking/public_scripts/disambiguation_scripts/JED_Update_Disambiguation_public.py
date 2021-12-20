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
    paths = cfg['OUT_PATHS']

    # Gather in the new extracted entity data
    raw_df, heads_df = JCF.ingest_raw_entities(cfg['DATA_FILES'])  

    raw_df = raw_df[raw_df.year==2017].copy()
    heads_df =  heads_df[heads_df.year==2017].copy()

    # load in the prior iteration of the JEL that we will be updating
    oldJEL = pd.read_json(cfg['JEL'], lines=True)

    # fjc demographics
    fjc_active = JCF.ingest_the_fjc(
        cfg['FJC']['fjc_file'])  

    ba_mag = JCF.ingest_ba_mag(
        cfg['BA_MAG']['judges'], 
        cfg['BA_MAG']['positions']
        )

    new_ucids = list(set(list(raw_df.ucid.unique())+list(heads_df.ucid.unique())))
    yrs_old = range(int(cfg['SEL_UPDATE_YRS']['low']),int(cfg['SEL_UPDATE_YRS']['high'])+1)
    
    # parties and counsels
    parties = JCF.ingest_header_entities(cfg['DATA_PARTIES'])
    counsels = JCF.ingest_header_entities(cfg['DATA_COUNSELS'])
    
    dockets = dtools.load_unique_files_df()
    dockets_old = dockets[
        ~dockets.index.isin(new_ucids) &
        dockets.year.isin(yrs_old)
    ].reset_index()

    old_SJID_Data, old_Inconclusive_Data = JU.multi_process_SEL_loader(dockets_old, paths)

    old_entries, old_heads = JCF.Transform_SEL_to_Disambiguation_Data_Model(old_Inconclusive_Data)

    RAWS = pd.concat([raw_df, old_entries])
    HEADS = pd.concat([heads_df, old_heads])

    RAWS.index = range(len(RAWS))
    HEADS.index = range(len(HEADS))

    # for the inconclusive and new rows, run them through the standard cleaning and preparation pipeline
    PreDF = JP.PIPELINE_Disambiguation_Prep(RAWS, HEADS, oldJEL)

    
    Post_UCID = JP.UCID_MATCH_PIPELINE(PreDF, parties, counsels)
       
    ID_Mappings, ALL_NODE_IDs = JP.COURT_MATCH_PIPELINE(Post_UCID, fjc_active, ba_mag)

    FPCID, newJEL = JP.PIPE_UPDATE_FREE_MATCH(ID_Mappings, ALL_NODE_IDs, oldJEL, old_SJID_Data, fjc_active)

    PRE_SEL = JA.FINAL_CLEANUP(FPCID)

    JU.UPDATE_WRITER(PRE_SEL, newJEL, dockets_old, paths)    