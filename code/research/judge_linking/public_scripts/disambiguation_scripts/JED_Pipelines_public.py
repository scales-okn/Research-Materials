import pandas as pd
import tqdm

from collections import defaultdict
import JED_Classes_public as JED_Classes
import JED_Cleaning_Functions_public as JCF
import JED_Algorithms_public as JA
import JED_Utilities_public as JU
import JED_Globals_public as JG

from fuzzywuzzy import fuzz

def PIPELINE_Disambiguation_Prep(entry_frame: pd.DataFrame, header_frame: pd.DataFrame, JEL=[]):
    """Preliminary cleaning and respanning to prep the data for disambiguation

    Args:
        entry_frame (pandas.DataFrame): extracted entities using spacy model on docket entries
        header_frame (pandas.DataFrame): extracted entities from header metadata
        JEL (pandas.DataFrame or empty list): a formerly created JEL table or empty list

    Returns:
        pandas.DataFrame: combined dataframe of all rows to be used in disambiguation
    """
    
    # merge the unique files table to get the filing date/year -- this might no longer be needed
    # edf = JCF.merge_to_uni(entry_frame, dockets)
    # hdf = merge_to_uni(header_frame, dockets)
    edf = entry_frame
    hdf = header_frame

    # run the cleaning and respanning processes on each dataframe independently
    edf = single_df_pipeline(edf, is_header=False, JEL=JEL)
    hdf = single_df_pipeline(hdf, is_header=True)
    
    # clean all the entities in preparation for disambiguation
    edf, hdf = pipe_junction(edf, hdf)
    
    # create quick counts of number of ucids in a court that an entity appears
    edf, hdf = pipe_UCID_Counts(edf, hdf)
    
    # drop bad/low-occurrence entities and fuse the header and entry data into one SEL-like dataframe
    fdf = pipe_concatenation(edf, hdf)

    return fdf


def single_df_pipeline(DF: pd.DataFrame, is_header: bool, JEL=[]):
    """Cleaning and respanning functions for the raw spacy extracted entities

    Args:
        DF (pandas.DataFrame): raw extracted entities either from spacy extractions or header metadata
        is_header (bool): is this the header dataframe
        JEL (pandas.DataFrame or empty list): a formerly created JEL table or empty list

    Returns:
        pandas.DataFrame: dataframe with cleaned, respanned entities, with pretext classified
    """
    
    # verbose
    PIPE = "Entries" if not is_header else "Header"
    print(f"\nPipeline Running for: {PIPE}")

    # if it's the entry df, then spacy was used and we need to do a specialty reshuffling
    # to account for the language bias the model has
    if not is_header:
        print(">> Corrective Actions")
        DF = JCF.reshuffle_exception_entities(DF, JEL)

    print(">> Initial Eligibility Check")
    DF = JCF.eligibility(DF, "extracted_entity")
    
    print(">> Entity Cleaning Pipe")
    DF = cleaning_pipe(DF)
    
    print(">> Secondary Eligibility Check")
    DF = JCF.eligibility(DF, "Respanned_Entity")
    
    print(">> Respanning Cleaned Entities")
    NORMS = JCF.apply_respanning(DF, is_header)

    print(">> Handling Exception Cases")
    FIXES = JCF.handle_exception_entities(DF)
    if len(FIXES)>0:
        print(">>>> Exceptions were found (cleaning them)")
        FDF = cleaning_pipe(FIXES)
        # now that they have been found and cleaned, cast the exceptions to
        # not-exceptions so they can now be considered for disambiguation
        FDF['is_exception'] = False
        # make empty entities or null entities ineligible for disambiguation
        FDF.loc[(FDF.Respanned_Entity.isna())|(FDF.Respanned_Entity==''), 'eligible'] = False

        print(">>>> Exceptions were found (respanning them)")
        FDF = JCF.apply_respanning(FDF, is_header)
        
        # merge the dfs back into one
        MID = pd.concat([NORMS, FDF])
    else:
        # if there were no exceptions no merge necessary
        MID = NORMS

    print(">> Categorizing Pretoken Text")
    FIN = JCF.prefix_categorization(MID, is_header)

    return FIN

def cleaning_pipe(df: pd.DataFrame):
    """take a dataframe of entities and clean them, then identify their true spans in the original entries

    Args:
        df (pandas.DataFrame): rows of entities, pretext, original spans, etc.

    Returns:
        pandas.DataFrame: same length as input DF, now with cleaned and respanned entities
    """
    
    # unique list of judge entities in the dataframe
    JUDGES = sorted(
        list(
            df[(df.eligible) &(~df.is_exception)].extracted_entity.unique()
        ),
        key = lambda x: len(x), reverse=True
    )
    
    pipe_map = {}
    pipe_index = {}
    pipe_length = {}
    exceptions = []

    # iterate through each entity
    for ori in tqdm.tqdm(JUDGES):
        each = ori
        # clean the entity and identify if it is an exception
        each, exception = JCF.string_cleaning_hierarchy(each)
        exceptions+=exception

        # now create the map from old entity to new one
        pipe_map[ori] = each    
        if each:
            # if there is a new entity, we need a new index and new length
            # index is with respect to the original entity (an offset)
            pipe_index[ori] = ori.index(each)
            pipe_length[ori] = len(each)
        else:
            # if the entity was made into nothing, match the index offset and length as None now
            pipe_index[ori] = None
            pipe_length[ori] = None
        
    # map the new respanned entities using the old ones
    df['Respanned_Entity'] = df.extracted_entity.map(pipe_map)
    df['Respanned_Start'] = df.extracted_entity.map(pipe_index)
    df['Respanned_Length'] = df.extracted_entity.map(pipe_length)
    # any of the flagged exceptions will now be notated as such
    df.loc[df.extracted_entity.isin(exceptions), "is_exception"] = True

    # all of the exceptions "respanned entities" should just be themselves
    df.loc[(df.eligible)&(df.is_exception), "Respanned_Entity"]= df.extracted_entity
    df.loc[(df.eligible)&(df.is_exception), "Respanned_Start"]= 0 # their offset is zero
    df.loc[(df.eligible)&(df.is_exception), "Respanned_Length"]= df.extracted_entity.apply(lambda x: len(str(x)))
         
    return df

def pipe_junction(df_entries: pd.DataFrame, df_headers: pd.DataFrame):
    """take both the header and entry dataframe and combine their overall unique entities for cleaning

    Args:
        df_entries (pandas.DataFrame): entries entities df
        df_headers (pandas.DataFrame): header entities df

    Returns:
        pandas.DataFrame, pandas.DataFrame: entry and header dataframes with the cleaned entity column added
    """
    
    print("First Junction Pipe Running")
    # get all unique judge-like entities pre-disambiguation
    JUDGES = set(
        list(df_entries[df_entries.eligible].New_Entity.unique()) +
        list(df_headers[df_headers.eligible].New_Entity.unique())
    )
    
    print(">> Mapping Cleaned Entities")
    # run through the pre-disambiguation cleaning on them
    # (i.e. remove hyphens, 's, periods, etc.)
    JMAP = {original:JCF.stacked_cleaning(original) for original in JUDGES}
    
    # remap the cleaned entities onto the originals
    for each in [df_entries, df_headers]:
        each['Cleaned_Entity'] = each.New_Entity.map(JMAP)
        # if any cleaned entity is now balnk, mark as ineligible for disambiguation
        each.loc[(each.Cleaned_Entity.isna())|(each.Cleaned_Entity==''), 'eligible'] = False
    
    return df_entries, df_headers


def pipe_UCID_Counts(df_entries: pd.DataFrame, df_headers: pd.DataFrame):
    """ Take the entity and header dataframes and determine for each entity,
    how many cases does it appear on in its respective court

    Args:
        df_entries (pandas.DataFrame): entries entities df
        df_headers (pandas.DataFrame): header entities df

    Returns:
        pandas.DataFrame, pandas.DataFrame: entry and header dataframes with the ucid count column added
    """
    
    print("Primary UCID Count pipe running")
    print(">> Aggregating counts by Court")
    # create triples of entity, ucid, court
    ee = zip(df_entries.Cleaned_Entity, df_entries.ucid, df_entries.court)
    hh = zip(df_headers.Cleaned_Entity, df_headers.ucid, df_headers.court)

    # we will make a general ucid count dict and one for specifically header metadata references only
    N_ucids = {}
    N_ucids_heads_only = {}
    # pool the ucid and entityies by court, starting with header data
    for ent, ucid, court in tqdm.tqdm(hh, total=len(df_headers)):
        if court not in N_ucids:
            N_ucids[court] = {ent: [ucid]}
            N_ucids_heads_only[court] = {ent: [ucid]}
        else:
            if ent not in N_ucids[court]:
                N_ucids[court][ent] = [ucid]
                N_ucids_heads_only[court][ent] = [ucid]
            else:
                N_ucids[court][ent].append(ucid)
                N_ucids_heads_only[court][ent].append(ucid)
    # now add entry ucids                
    for ent, ucid, court in tqdm.tqdm(ee, total=len(df_entries)):
        if court not in N_ucids:
            N_ucids[court] = {ent: [ucid]}
        else:
            if ent not in N_ucids[court]:
                N_ucids[court][ent] = [ucid]

            else:
                N_ucids[court][ent].append(ucid)

    # retabulate as unique ucid counts using the length of the set
    tabulated_all = []
    tabulated_heads = []
    for court, ents in N_ucids.items():
        for ent, ucids in ents.items():
            tabulated_all.append({'court':court,
                            'Cleaned_Entity':ent,
                            'N_ucids':len(set(ucids))})
    
    # do the same for header metadata counts
    for court, ents in N_ucids_heads_only.items():
        for ent, ucids in ents.items():
            tabulated_heads.append({'court':court,
                            'Cleaned_Entity':ent,
                            'N_ucids_heads_only':len(set(ucids))})
    
    # make DFs
    TALL = pd.DataFrame(tabulated_all)
    THEAD = pd.DataFrame(tabulated_heads)

    # Join DFs
    UCID_Counts = TALL.merge(THEAD, how = 'outer', on = ['court','Cleaned_Entity'])
    UCID_Counts.fillna(0, inplace=True)

    dfe = df_entries.merge(UCID_Counts, how='left', on = ['court','Cleaned_Entity'])
    dfh = df_headers.merge(UCID_Counts, how='left', on = ['court','Cleaned_Entity'])
        
    return dfe, dfh

def pipe_concatenation(df_entries: pd.DataFrame, df_headers: pd.DataFrame):
    """Join header and entry dataframes into one large dataset pre-disambiguation

    Args:
        df_entries (pandas.DataFrame): cleaned and respanned entry entities
        df_headers (pandas.DataFrame): cleaned and respanned header entities

    Returns:
        pandas.DataFrame: Pre-SEL dataframe with row per entity to be used in disambiguation
    """
    
    print("Last Junction Pipe Running - Combining Header and Entry Data")
    
    # any entity that 
    # appeared on only 1 ucid
    # and was not on the header 
    # AND had no judgey keywords in the pretext is ruled ineligible
    df_entries.loc[
        (df_entries.N_ucids_heads_only==0) &
        (df_entries.N_ucids<=1) & 
        (df_entries.Prefix_Categories == "No_Keywords"), 'eligible'] = False
    
    # if the entity is one character or less --> ineligible
    df_entries.loc[
        (df_entries.Cleaned_Entity.apply(lambda x: len(str(x))<=1)), 'eligible'] = False

    print(">> Grabbing eligible rows")
    fin_e = df_entries[df_entries.eligible].copy()
    fin_h = df_headers[df_headers.eligible].copy()
    
    # instantiate these columns as null for the entry dataframe (these are keys in the header frame)
    for each in ['judge_enum', 'party_enum', 'pacer_id']:
        fin_e[each] = None

    # Columns I want to keep from both dataframes, and will use to concat the 2 together
    SEL_COLS =['Entity_Extraction_Method', 'docket_source', 'judge_enum', 'party_enum', 'pacer_id', 'docket_index',
           'ucid','cid','court','year','entry_or_filing_date',
           'original_text','extracted_entity','New_Pre_Tokens','New_Entity','New_Post_Tokens','Cleaned_Entity',
           'Prefix_Categories','Transferred_Flag','full_span_start','full_span_end','New_Span_Start','New_Span_End',
           'eligible','N_ucids']
    
    print(">> Concatenating Pre-SEL DataFrame")
    fin_e.rename({'entry_date':'entry_or_filing_date'}, axis=1, inplace=True)
    fin_h.rename({'filing_date':'entry_or_filing_date'}, axis=1, inplace=True)

    PRE_SEL_E = fin_e[SEL_COLS]
    PRE_SEL_H = fin_h[SEL_COLS]
    
    # merge into 1 df
    RET = pd.concat([PRE_SEL_E, PRE_SEL_H])
    
    return RET


def Single_UCID_Pipeline(entity_list: list, ucid: str):
    """List of IntraMatch objects that are going to be reduced within a ucid if possible

    Args:
        entity_list (list): list of IntraMatch objects belonging to the ucid
        ucid (str): the SCALES ucid corresponding to the case

    Returns:
        list: same list of objects that entered the function, with updated pointers if any mapped to each other
    """

    updated_list = JA.PIPE_Fuzzy_Matching(entity_list, ucid) # generic fuzzy match

    updated_list = JA.PIPE_Tokens_in_Tokens(updated_list, ucid, False, False, 'Plain') # token in token checks
    updated_list = JA.PIPE_Tokens_in_Tokens(updated_list, ucid, False, True, 'Plain') # TiT abbreviated middle initial
    updated_list = JA.PIPE_Tokens_in_Tokens(updated_list, ucid, True, True, 'Plain') # TiT abbreviated first and middle initial
    updated_list = JA.PIPE_Tokens_in_Tokens(updated_list, ucid, False, False, 'Unified') # universal name spellings
    updated_list = JA.PIPE_Tokens_in_Tokens(updated_list, ucid, False, False, 'Nicknames') # using nicknames

    updated_list = JA.PIPE_Anchor_Reduction_UCID(updated_list, ucid) # reduce using surnames
    updated_list = JA.PIPE_Anchor_Reduction_II_UCID(updated_list, ucid) # secondary surname reduction
    updated_list = JA.PIPE_Anchor_Reduction_III_UCID(updated_list, ucid) # third surname reduction

    return updated_list

def PIPE_Prepare_for_UCID_Layer(FDF: pd.DataFrame, parties: pd.DataFrame, counsels: pd.DataFrame):
    
    ucid_map = UCID_PIPE_Object_Builder(FDF) # build the objects for disambiguation
    ucid_map, toss_map = JA.UCID_PIPE_Drop_Parties(ucid_map, parties, counsels) # drop parties and counsels that were misattributed as judges

    return ucid_map, toss_map

def UCID_MATCH_PIPELINE(FDF: pd.DataFrame, parties: pd.DataFrame, counsels: pd.DataFrame):
    """Disambiguation Pipeline for Intra-UCID entity matching

    Args:
        FDF (pandas.DataFrame): [description]
        parties (pandas.DataFrame): parties on the cases we are disambiguating
        counsels (pandas.DataFrame): counsels on the cases we are disambiguating

    Returns:
        pandas.DataFrame: Updated Df with entities and pointers to their parent entities after intra-ucid matching
    """

    ucid_map, toss_map = PIPE_Prepare_for_UCID_Layer(FDF, parties, counsels)
   
    # NEED NEW_MAP IN THE END
    end_map = {}
    for ucid, entity_list in ucid_map.items():
        end_map[ucid] = Single_UCID_Pipeline(entity_list, ucid)
    
    UCID_df = UCID_PIPE_Build_Remapped_Lookup(end_map, toss_map, FDF) # remap into a Dataframe
    # in frame shape will not match outframe shape if any parties or counsels were detected and dropped

    return UCID_df

def Single_Court_Pipeline(entity_list_long: list, entity_list_short: list, court: str):
    """a cycling function that takes a courts list of entities and cycles through various disambiguation algorithms with them

    Args:
        entity_list_long (list): list of IntraMatch objects in a ucid/court we are trying to reduce that are multi-tokened
        entity_list_short (list): list of IntraMatch objects in a ucid/court we are trying to reduce that are single-tokened
        court (str): used for logging to indicate where in the pipeline the matching is happening

    Returns:
        list: list of the combined single and multi-token objects that entered the function, now pointing and mapped to each other
    """

    updated_map_long = JA.PIPE_Fuzzy_Matching(entity_list_long, court)
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, False, False, 'Plain')
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, False, True, 'Plain')
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, True, True, 'Plain')
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, False, False, 'Unified')
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, False, False, 'Nicknames')
    updated_map_long = JA.PIPE_Tokens_in_Tokens(updated_map_long, court, False, True, 'Nicknames')
    updated_map_long = JA.PIPE_UCID_COURT_INITIALISMS(updated_map_long, court)
    updated_map_single = JA.PIPE_COURT_Anchors_Self_Reduction(entity_list_short, court)

    updated_map = JA.PIPE_Anchor_Reduction_Court(updated_map_long, updated_map_single, court)
    return updated_map


def COURT_MATCH_PIPELINE(Post_UCID: pd.DataFrame, fjc_active: pd.DataFrame, ba_mag: pd.DataFrame):
    """Disambiguation Pipeline for Intra-Court entity matching

    Args:
        Post_UCID (pandas.DataFrame): post-ucid matching df
        fjc_active (pandas.DataFrame): loaded and transformed ground truth entity file from the FJC
        ba_mag (pandas.DataFrame): loaded and transformed ground truth entity file from the UVA BA/MAG dataset

    Returns:
        pandas.DataFrame: again disambiguated in court, now with a column mapping to newest parent entities
    """
    # build the objects and split into single or multi-token names
    court_map_long, court_map_single, serial_init, GDF = COURT_PIPE_Object_Builder(Post_UCID)
    FJC_Nodes, serial_init = FJC_Node_Creator(fjc_active, serial_init)
    BA_MAG_Nodes, serial_init = BA_MAG_Node_Creator(ba_mag, serial_init)

    # slice the ground truth objects into their respective courts
    unallocated_remainder = []
    for court, objs in FJC_Nodes.items():
        if court in court_map_long:
            court_map_long[court]+=objs
        else:
            unallocated_remainder+=objs
    for court, objs in BA_MAG_Nodes.items():
        if court in court_map_long:
            court_map_long[court]+=objs
        else:
            unallocated_remainder+=objs

    # disambiguate within each court
    court_map = {}
    for court in set(court_map_long.keys()).union(court_map_single.keys()):
        court_map[court] = Single_Court_Pipeline(court_map_long[court], court_map_single[court], court)

    # after disambiguation, rebuild the entity dataframe
    ID_Mappings, ALL_NODE_IDs = COURT_PIPE_Build_Remapped_Lookup(court_map, unallocated_remainder, Post_UCID, GDF)

    return ID_Mappings, ALL_NODE_IDs


def FREE_MATCH_CYCLER(ACTIVE_NODES: list):
    """Cycler function that takes a large list of entities and considers them for disambiguation in a court-agnostic context

    Args:
        ACTIVE_NODES (list): list of the entity objects to be disambiguated in free matching

    Returns:
        list: list of the entity objects that entered the function, now pointing and mapped to each other
    """
    
    ACTIVE_NODES = JA.PIPE_Free_Exact_Beginning(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Fuzzy_Pool_Based(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens_Pool_Based(ACTIVE_NODES, False, False, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens_Pool_Based(ACTIVE_NODES, False, True, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens_Pool_Based(ACTIVE_NODES, False, False, 'Unified')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens_Pool_Based(ACTIVE_NODES, False, False, 'Nicknames')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens_Pool_Based(ACTIVE_NODES, False, True, 'Nicknames')
    ACTIVE_NODES = JA.PIPE_Free_Vacuum_Pool_Based(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Token_Sort_Pool_Based(ACTIVE_NODES)      
    ACTIVE_NODES = JA.PIPE_Free_Van_Sweeps_Pool_Based(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Initialisms_Pool_Based(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Single_Letter_Names_Pool_Based(ACTIVE_NODES)

    return ACTIVE_NODES

def FREE_MATCH_PIPELINE(ID_Mappings: pd.DataFrame, ALL_NODE_IDs: list, fjc_active: pd.DataFrame):
    """Final round of disambiguation with no locks on court or ucid

    Args:
        ID_Mappings (pandas.DataFrame): df after court level disambiguation
        ALL_NODE_IDs (pandas.DataFrame): df of node ids with their core information up to this point
        fjc_active (pandas.DataFrame): df of FJC judges we wish to include in the JEL

    Returns:
        pandas.DataFrame: final pre-SEL df
    """

    # build our pool of entities to be involved in court-agnostic free-matching. This will now incorporate judges from the FJC biographical dictionary
    NODES, ALL_NODE_IDs = PIPE_NODE_BUILDER(ID_Mappings, ALL_NODE_IDs)
    print("Total Nodes: ",len(NODES))
    # when an entity is a ground truth entity it may be duplicated from court matching, reduce them here
    NODES = reduce_ground_truths(NODES)

    # split out the "dormant" entities that are likely retired and wont appear in our data
    ACTIVE_NODES, DORMANT_NODES = PIPE_FIND_DORMANT_FJC(NODES, fjc_active)
    
    ACTIVE_NODES = FREE_MATCH_CYCLER(ACTIVE_NODES)
    
    ALL_NODES = ACTIVE_NODES+DORMANT_NODES

    # after disambiguation, recreate the entity dataframe
    PCID_Mappings, FINAL_NODE_LOOKUP, IREM = PIPE_REPOINT_Free_Match(ALL_NODES, ID_Mappings, ALL_NODE_IDs)
    
    # run through the labeller to build SJID labels for the newly disambiguated entities
    JEL, PCID_Mappings = PIPE_LABEL_AND_BUILD(PCID_Mappings, FINAL_NODE_LOOKUP, IREM)

    return JEL, PCID_Mappings

def PIPE_Assess_New_Cases(Post_UCID: pd.DataFrame, Compy_JEL: pd.DataFrame):
    """for the new case tagger, run through a series of cleaning and tagging of entities. This process
    does not update the JEL and identify new distinct entities, instead it tags extractions with known
    entities only

    Args:
        Post_UCID (pd.DataFrame): df after ucid level disambiguation
        Compy_JEL (pd.DataFrame): the old JEL to be used to tag these new cases

    Returns:
        pandas.DataFrame: a dataframe ready to be written out with post-disambiguation data
    """
    # IntraMatch dataframe is built using entity, what it points to after ucid matching, the court, and ucids
    IM = Post_UCID[['Cleaned_Entity','Points_To','court','ucid']]

    # group the df by the updated entity (points to) and determine how many unique ucids each of those entities is on
    GDF = IM.groupby(["Points_To","court"], as_index=False)['ucid'].nunique()

    # createa  serial id for each row in the dataframe
    GDF['_Serial_ID'] = range(len(GDF))

    # determine if any of the extracted entities are just 3 consonants (likely initials which we don't try to disambiguate)
    TCM = build_triple_consonants_map(GDF.Points_To.unique())
    GDF['Triple_Consonant'] = GDF.Points_To.map(TCM)
    
    # determine entities that qualify for disambiguation
    # cannot identify triple consonants (intiails) or entities where the maximum token length is 1 character
    Match_Nodes = GDF[
        (~GDF.Triple_Consonant)&
        (GDF.Points_To.apply(lambda x: max( [len(tok) for tok in x.split()])>1))
    ]
    
    # create the network objects for tagging
    Update_Nodes = []
    for point_starter, court, n_ucids, serial_id, triple_consonant in tqdm.tqdm([tuple(x) for x in Match_Nodes.to_numpy()]):
        Update_Nodes.append(
            JED_Classes.FreeMatch(name= point_starter,
                                additional_reprs = [],
                                n_ucids = n_ucids,
                                courts= [court],
                                FJC_NID = None, BA_MAG_ID = None,
                                serial_id= serial_id)
        )
        
    # we treat JEL rows as ground truth objects in this tagging
    JEL_objs = []
    for index, row in Compy_JEL.iterrows():

        # if a name has additional variants (marital name, obscure nicknames, etc.)
        alternates = None
        if not pd.isnull(row['NID']):
            alternates = JG.FJC_ADDITIONAL_REPRESENTATIONS.get(row['NID'])

        JEL_objs.append(
            JED_Classes.FreeMatch(
            row['name'], alternates, 0, courts=[],
            FJC_NID= row['NID'], BA_MAG_ID=row['BA_MAG_ID'], serial_id=0, SJID = row['SJID']
            )
        )
    
    # split the taggable entities into single tokened names or multi-tokened
    solos = [N for N in Update_Nodes if N.token_length==1 or len(N.tokens_wo_suff)==1]
    multis = [N for N in Update_Nodes if N not in solos]
    
    # reduce the uni-token entities amongst themselves
    SOLO_OUT = JA.PIPE_Anchor_Reduction_Court(JEL_objs, solos, "New Case Tags")
    
    # combine all the multi-token names and groundtruth into one list
    ACTIVE_NODES = multis+JEL_objs
    
    # perform free matching disambiguation on the multi-token name list
    ACTIVE_NODES = FREE_MATCH_CYCLER(ACTIVE_NODES)
    
    input_lookup = []
    # create a lookup of all the nodes and their IDs
    inputs= multis+SOLO_OUT
    for node in inputs:
        input_lookup.append({
            'node_name': node.name, # this was the input points to which became the name
            'original_SID': node.serial_id,
            'is_eligible': node.eligible,
            'is_ambiguous': node.is_ambiguous,
            'Possible_Pointers': [o.SJID for o in node.Possible_Pointers],
            'SJID':node.SJID

        })
    IL_NODES = pd.DataFrame(input_lookup)
    
    # merge the grouped data with the node lookup
    RES = GDF.merge(IL_NODES,
         how='left',
         left_on = ['_Serial_ID',"Points_To"],
         right_on= ['original_SID','node_name'])
    
    # remap the ambiguous entities if applicable
    RES['Ambiguous_SJIDS'] = None
    RES.loc[RES.is_ambiguous, "Ambiguous_SJIDS"] = RES.Possible_Pointers
    RES.loc[RES.is_ambiguous, "SJID"] = "Ambiguous"
    
    # now rebuild the input dataframe, with the pointers updated to the final identified parent entities
    Disam = Post_UCID.merge(RES[['court','Points_To','SJID','Ambiguous_SJIDS']],
                   how='left',
                   left_on = ['court','Points_To'],
                   right_on = ['court','Points_To'])

    # mark remaining inconclusive entities as such
    Disam["SJID"].fillna("Inconclusive", inplace=True)

    # change the header prefixes to be nondescript
    Disam.loc[
        Disam.Prefix_Categories.isin(
            ['assigned_judge', 'referred_judges']
        ), 'Prefix_Categories'] = 'Nondescript_Judge'
    
    # do one final crosscheck for remaining inconclusive entities
    out = JA.FINAL_CLEANUP(Disam)
    
    return out


# def NEW_DISAMBIGUATION_FREEMATCH_PIPELINE(paths, all_dockets, newest_fjc, fjc_active, oldJEL, Post_Court):
#     """Freematch pipeline that swaps for the original freematch pipeline when a new, updated JEL is being generated. This pipeline accounts for previously identified entities and previously tagged dockets.

#     Args:
#         paths (dict): dictionary of various filepaths for SEL files, JEL files, and FJC files
#         all_dockets (pandas.DataFrame): unique files table default loaded from settings file
#         newest_fjc (pandas.DataFrame): DF of new FJC Article III judges only
#         fjc_active (pandas.DataFrame): DF of full FJC codebook as we know it at time of script running (1700s-present)
#         oldJEL (pandas.DataFrame): DF of the old JEL loaded in from JSONL file
#         Post_Court (pandas.DataFrame): DF of the new dockets post ucid and court disambiguation. These rows should be ready for free matching disambiguation

#     Returns:
#         pandas.DataFrame, pandas.DataFrame, pandas.DataFrame: 3 different DFs after disambiguation. readySEL is any updated old SEL file rows, RPDF is the repointed DF of the new dockets only (pointed to mapped disambiguated entities), and updated_JEL is the newest iteration of the JEL.
#     """
#     # prep for freematching by loading in the prior SEL dockets we want to pool into disambiguation
#     readySEL = PIPE_PREPARE_UPDATE_FREEMATCH(all_dockets, paths, ['2016', '2019'])
    
#     # build the object nodes we will use in disambiguation
#     Free_Nodes = PIPE_UPDATE_NODE_BUILDER_FREEMATCH(Post_Court, readySEL, newest_fjc, fjc_active, oldJEL)

#     # cull the pool before beginning to remove FJC judges that are no longer active, or not likely to show up in our data (i.e. retired by 1995 if we're using 2015-2020 dockets)
#     active, dormant  = PIPE_FIND_DORMANT_FJC(Free_Nodes)
    
#     # full disambiguation routine
#     ACTIVE_NODES = JA.PIPE_Free_Fuzzy(active)
#     ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Plain')
#     ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Plain')
#     ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Unified')
#     ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Nicknames')
#     ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Nicknames')
#     ACTIVE_NODES = JA.PIPE_Free_Vacuum(ACTIVE_NODES) # vacuum out the middle names and compare entities
#     ACTIVE_NODES = JA.PIPE_Free_Token_Sort(ACTIVE_NODES) # compare tokens in flipped ortders
#     ACTIVE_NODES = JA.PIPE_Free_Van_Sweep(ACTIVE_NODES) # special patch for germanic names
#     ACTIVE_NODES = JA.PIPE_Free_Initialisms(ACTIVE_NODES) # special patch for initial heavy judges
#     ACTIVE_NODES = JA.PIPE_Free_Single_Letters(ACTIVE_NODES) # special patch for judges that go by a single letter of one of their names
#     # ACTIVE_NODES = JA.Abrams_Patch(ACTIVE_NODES) # special patch for marital name changes

#     # when wrapping up, make sure we include all of the dormant nodes again as we still want them included when rebuilding and expanding the JEL
#     FIN_NODES = ACTIVE_NODES + dormant
    
#     # for the new dockets, repoint any child entities to the correct parent entities
#     RPDF = PIPE_Update_Final_Repoint(Post_Court, FIN_NODES)
    
#     # tag the old ucids with new entity labels if applicable, build a new JEL
#     readySEL, updated_JEL = PIPE_UPDATE_NEW_LABELS_AND_BUILD(readySEL, RPDF, oldJEL, FIN_NODES)
    
#     return readySEL, RPDF, updated_JEL

def PIPE_FIND_DORMANT_FJC(nodes, fjc_active, time_bound = '1995-01-01'):
    """Helper function that filters out FJC entities from a disambiguation pool based on their latest termination date as an Article III judge

    Args:
        nodes (list): list of IntraMatch child objects that will be used in a disambiguation pool
        time_bound (str, optional): Latest time-bound to cutoff and rule an Article III judge as dormant with respect to the years of the dockets in disambiguation. Defaults to '1995-01-01'.

    Returns:
        list, list: returns 2 lists of IntraMatch child objects whose lengths and items sum to the length and items of the original nodes input list. The active list are entities to be used in disambiguation, dormant are those believed to be no longer judges at the time of the dockets filing in the disambiguation pool.
    """

    TERM_MAP = {nid:tdate for nid, tdate in fjc_active[[('nid',''),('Termination Date','max')]].to_numpy()}
    dormant_nids = {k:v for k,v in TERM_MAP.items() if v <= pd.to_datetime(time_bound).date()}

    dormant_nodes = [N for N in nodes if N.NID in dormant_nids]
    active_nodes = [N for N in nodes if N.NID not in dormant_nids]

    return active_nodes, dormant_nodes


def UCID_PIPE_Object_Builder(concatenated_df):
    """build dictionary of objects per UCID for first round of disambiguation

    Args:
        concatenated_df (pandas.DataFrame): Pre-SEL df

    Returns:
        dict: key = ucid, values = list of objects (UCIDMatch objs)
    """
    
    print("Pipe: UCID Object Builder")

    # only care about unique entities (the N_Ucid column will keep track of frequency for us)
    temp_group = concatenated_df[["Cleaned_Entity","ucid", "N_ucids", "docket_source"]].drop_duplicates()

    # grab header specific ones
    Heads_Only = temp_group[temp_group.docket_source!="line_entry"].copy()
    Heads_Only['Was_Header'] = True

    # again drop duplicates, these are all possible objects
    subset = concatenated_df[["Cleaned_Entity","ucid", "N_ucids"]].copy()
    subset.drop_duplicates(inplace=True)

    # we will now be able to discern if an entity object is derived from the header or JUST from entries
    subset = subset.merge(
        Heads_Only[["Cleaned_Entity","ucid", "N_ucids","Was_Header"]],
        how='left', on = ["Cleaned_Entity","ucid", "N_ucids"])
    subset['Was_Header'].fillna(False, inplace=True)

    print(">> Building Objects")

    # create a map that has a list of every entity as an object grouped together by ucid. There is no overlap in objects from ucid to ucid
    # judge smith on ucid 1 is a different object from judge smith on ucid 2
    ucid_map = defaultdict(list)
    for cleaned_ent, ucid, N_ucids, was_header in tqdm.tqdm([tuple(x) for x in subset.to_numpy()], total=len(subset)):
        ucid_map[ucid].append(JED_Classes.UCIDMatch(cleaned_ent, ucid, N_ucids, was_header))

    # only return ucids where more than one entity was detected
    # single entity ucids have no intra-ucid disambiguation to be done
    ucid_map = {k:v for k,v in ucid_map.items() if len(v)>1}
    print(">> Objects Built")
    
    return ucid_map


def COURT_PIPE_Object_Builder(Post_UCID, serial_init = 0):
    """build dict of objects grouped by court for court matching and disambiguation

    Args:
        Post_UCID (panda.DataFrame): DF that has already been through intra-ucid matching and disambiguation

    Returns:
        dict, dict: dictionaries of multi-token entity objects vs. single token entity objects, grouped (keyed) by court
    """
    
    # IntraMatch dataframe is built using entity, what it points to after ucid matching, the court, and ucids
    IM = Post_UCID[['Cleaned_Entity','Points_To','court','ucid']]

    # group the df by the updated entity (points to) and determine how many unique ucids each of those entities is on
    GDF = IM.groupby(["Points_To","court"], as_index=False)['ucid'].nunique()
    GDF['_Serial_ID'] = range(len(GDF))

    print(">> Building Objects")
    # with the grouped data, build the courtmatch objects
    court_map = defaultdict(list)
    for point_starter, court, n_ucids, serial_id in tqdm.tqdm([tuple(x) for x in GDF.to_numpy()]):
        court_map[court].append(
            JED_Classes.FreeMatch(name= point_starter,
                                additional_reprs = [],
                                n_ucids = n_ucids,
                                courts= [court],
                                FJC_NID = None, BA_MAG_ID = None,
                                serial_id= serial_id)
        )
    

    # split the objects into single token names vs. multitoken names per court
    court_map_single = {}
    court_map_long = {}
    for court, objects in court_map.items():
        court_map_single[court] = [o for o in objects if o.token_length==1 or len(o.tokens_wo_suff)==1]
        court_map_long[court] = [o for o in objects if o not in court_map_single[court]]

    print(">> Objects Built")
    
    serial_init = GDF._Serial_ID.max()
    serial_init+=1
    GDF.columns = ['Points_To','court','n_ucids','_Serial_ID']
    return court_map_long, court_map_single, serial_init, GDF


def UCID_PIPE_Build_Remapped_Lookup(new_map, toss_map, concatenated_df):
    """After intra_ucid matching, we need to rejoin and remap the entities to point to the correct "parents" on the original dataframe

    Args:
        new_map (dict): keys = ucid, values = lists of objects on the ucid, mapped to each other if they were disambiguated
        toss_map (dict): keys = ucid, values = lists of any objects that were thrown out because they were parties or counsels the spacy model inadvertently extracted
        concatenated_df (pandas.DataFrame): SEL-like rows

    Returns:
        pandas.DataFrame: Updated dataframe of rows for entity lookup that now have a "points to" column filled if an entity points to another one
    """

    print("Pipe: Building Remapped DataFrame")
    print(">> Applying remappings")
    # loop through the objects by ucid grouping
    remapped = []
    # key is a ucid, objs are the entities on the case
    for key, objs in new_map.items():
        # on a ucid, if the entity was mapped to ineligible, that means it now points to a new one
        # (it is no longer eligible to be mapped to)
        swapped = [o for o in objs if not o.eligible]
        # for each of those entities, we will create a df row that maps its old entity (name) to its new entity parent (POINTS TO)
        # note we clarify it is not a party
        for each in swapped:
            remapped.append(
                    {'ucid':key,
                    'Cleaned_Entity':each.name,
                    'Points_To': each.POINTS_TO,
                    'is_party':False}
                )

    print(">> Tossing junk entities")
    # same concept applies here, except we map the tossed entities into a null category so they are ruled out for all
    # further disambiguation -- these were parties/counsels
    tossed = []
    for key, objs in toss_map.items():
        swapped = [o for o in objs if not o.eligible]
        for each in objs:
            tossed.append(
                    {'ucid':key,
                    'Cleaned_Entity':each.name,
                    'Points_To': None,
                    'is_party':True}
                )

    # build the DF of remapped and tossed entities
    REM = pd.DataFrame(remapped+tossed)

    # merge the remapped data onto the original dataframe by the original cleaned entity name
    print(">> Merging into Core Data")
    Post_UCID = concatenated_df.merge(REM, how='left', on = ['ucid','Cleaned_Entity']) if len(REM) else concatenated_df.assign(Points_To=None, is_party=None)

    # if the old frame's "point to" was null, that means it remained an eligible entity, fill its point to with itself
    Post_UCID["Points_To"].fillna(Post_UCID.Cleaned_Entity, inplace=True)
    # any entity not tagged as a party, fill as false
    Post_UCID['is_party'].fillna(False,inplace=True)
    # drop the parties out for future disambiguation
    Post_UCID = Post_UCID[~Post_UCID.is_party]

    # Bool flag if the row's pretext before the entity had any judgey words
    Post_UCID['Has_Pref'] = Post_UCID.Prefix_Categories != "No_Keywords"
    # group by entity-court, how many occurences had prefixed judge terms
    # note points_to is functionally our "ground truth" entity representation now for each row of data as it accounts for the first round of disambiguation now
    Blanks = Post_UCID.groupby(['Points_To','court'], as_index = False)['Has_Pref'].sum()

    # count distinct ucids by entity-court
    Unique_Ucid_Counts = Post_UCID[['ucid','Cleaned_Entity','Points_To','court']].groupby(
    ['Points_To','court'], as_index=False)['ucid'].nunique()

    # merge the ucid counts to blank counts
    Court_DF = Unique_Ucid_Counts.merge(Blanks, how='left', on = ['Points_To','court'])
    # default to not ignore any of them
    Court_DF['Ignore'] = False

    # if it appears on under 3 ucids in a court, is a single token, and did not have any prefix judgey terms
    # then ignore it for court-level disambiguation
    Court_DF.loc[(Court_DF.ucid<=3)&
            (Court_DF.Points_To.apply(lambda x: len(x.split())==1))&
            (Court_DF.Has_Pref==0), 'Ignore'] = True

    # prep for court matching by adding the ignore column for filtering
    Send_To_CourtMatching = Post_UCID.merge(
        Court_DF[['Points_To', 'court', 'Ignore']], how = 'left', on = ['Points_To', 'court'])

    # drop the ones we said to ignore
    Send_To_CourtMatching = Send_To_CourtMatching[~Send_To_CourtMatching.Ignore]
    # drop misc. columns I no longer need that I created in this function
    Send_To_CourtMatching.drop(['N_ucids','Ignore', 'is_party','Has_Pref'],axis=1, inplace=True)
    
    # finally send the updated data on to court level disambiguation
    return Send_To_CourtMatching

def COURT_PIPE_Build_Remapped_Lookup(court_map, unallocated_remainder, Post_UCID, grouped_df):
    node_IDs = []
    for court, objs in court_map.items():
        for each in objs:
            node_IDs.append({
                'court':court,
                'node_name': each.name, # this was the input points to which became the name
                'original_SID': each.serial_id,
                'is_FJC':each.is_FJC,
                'is_BA_MAG': each.is_BA_MAG,
                'is_eligible': each.eligible,
                'is_ambiguous': each.is_ambiguous,
                'Possible_Pointers': [(o.name, o.serial_id) for o in each.Possible_Pointers],
                'NID': each.NID,
                'BA_MAG_ID':each.BA_MAG_ID,
                'SJID': each.SJID
            })

    node_IDs = pd.DataFrame(node_IDs)

    Unallocated_DF = []
    for each in unallocated_remainder:
        Unallocated_DF.append({
            'court': None,
            'node_name': each.name,
            'original_SID':each.serial_id,
            'is_FJC': each.is_FJC,
            'is_BA_MAG': each.is_BA_MAG,
            'is_eligible': each.eligible,
            'is_ambiguous': each.is_ambiguous,
            'Possible_Pointers': each.Possible_Pointers,
            'NID': each.NID,
            'BA_MAG_ID': each.BA_MAG_ID,
            'SJID': each.SJID
        })

    Unallocated_DF = pd.DataFrame(Unallocated_DF)

    ALL_NODE_IDs = pd.concat([node_IDs, Unallocated_DF])

    TCM = build_triple_consonants_map(list(ALL_NODE_IDs.node_name.unique()))
    ALL_NODE_IDs["Triple_Consonant"] = ALL_NODE_IDs.node_name.map(TCM)


    remapping = []
    for court, objs in court_map.items():
        for each in objs:
            remapping.append({
                'court':court,
                'node_name_post_ucid': each.name, # this was the input points to which became the name
                'original_SID': each.serial_id,
                'Updated_Points_To': each.POINTS_TO,
                'Updated_Points_To_SID': each.POINTS_TO_SID,
            })

    REM = pd.DataFrame(remapping)

    node_lookup = REM.merge(
        ALL_NODE_IDs,
        how='left',
        left_on = ['court','Updated_Points_To_SID'],
        right_on = ['court','original_SID'],
        suffixes = ('','_endpoint')
    ).drop('Updated_Points_To_SID', axis=1)

    XWalk_to_Post_UCID = grouped_df.merge(
        node_lookup[['court','node_name_post_ucid','original_SID','original_SID_endpoint']],
        how='left',
        left_on = ['court','Points_To','_Serial_ID'],
        right_on = ['court','node_name_post_ucid','original_SID']
    ).drop(['_Serial_ID','node_name_post_ucid'],axis=1)

    ID_Mappings = Post_UCID.merge(
        XWalk_to_Post_UCID[['Points_To','court','original_SID','original_SID_endpoint']],
        how = 'left',
        left_on = ['court','Points_To'],
        right_on = ['court','Points_To']
    )

    return ID_Mappings, ALL_NODE_IDs
# #######

#     # again determine in a court how many times an entity was prefaced with judgey like terms
#     RePointed['Has_Pref'] = RePointed.Prefix_Categories != "No_Keywords"
#     Blanks = RePointed.groupby(['Updated_Points_To','court'], as_index = False).sum('Has_Pref')
#     Blanks = Blanks[['Updated_Points_To','court','Has_Pref']]

#     # determine number of unique ucids
#     Unique_Ucid_Counts = RePointed[['ucid','Cleaned_Entity','Points_To','Updated_Points_To','court']].groupby(
#         ['Updated_Points_To','court'], as_index=False)['ucid'].nunique()
#     # merge the blank counts and ucid counts into one frame
#     Court_DF = Unique_Ucid_Counts.merge(Blanks, how='left', on = ['Updated_Points_To','court'])

#     # special filter, we need to remove random single token entities that are just initials
#     # we can only confidently do this on triple consonants since vowels could mean it's a real name like Lee
#     letters = 'abcdefghijklmnopqrstuvwxyz'
#     vowels = 'aeiouy'
#     consonants = ''.join(l for l in letters if l not in vowels)
#     consearch = re.compile(fr'^[{consonants}]+$', flags=re.I)
#     conmap = {}
#     # if it's a triple consonant name, we will ignore it
#     for each in Court_DF.Updated_Points_To.unique():
#         if consearch.search(each) and len(each)<=4:
#             flag = True
#         else:
#             flag = False
#         conmap[each] = flag
    
#     # map the check for consonants
#     Court_DF['Ignore'] = Court_DF.Updated_Points_To.map(conmap)

#     # drop out entities with low frequency, single token, and no prefaced judgey text
#     Court_DF.loc[(Court_DF.ucid<=3)&
#             (Court_DF.Updated_Points_To.apply(lambda x: len(x.split())==1))&
#             (Court_DF.Has_Pref==0), 'Ignore'] = True
    
#     # merge together onto the SEL like df
#     FinPrep = RePointed.merge(
#         Court_DF[['Updated_Points_To', 'court', 'Ignore']], how = 'left', on = ['Updated_Points_To', 'court'])

#     # drop out the bad flagged rows
#     FinPrep = FinPrep[~FinPrep.Ignore]
    
#     return FinPrep

def build_triple_consonants_map(list_of_names):
    import re
    # special filter, we need to remove random single token entities that are just initials
    # we can only confidently do this on triple consonants since vowels could mean it's a real name like Lee
    letters = 'abcdefghijklmnopqrstuvwxyz'
    vowels = 'aeiouy'
    consonants = ''.join(l for l in letters if l not in vowels)
    consearch = re.compile(fr'^[{consonants}]+$', flags=re.I)
    conmap = {}
    # if it's a triple consonant name, we will ignore it
    for each in list_of_names:
        if consearch.search(each) and len(each)<4:
            flag = True
        else:
            flag = False
        conmap[each] = flag
    
    return conmap


def FJC_Node_Creator(fjc_active, serial_init):
    import JED_Globals_public

    # FJC_Nodes = []
    FJC_Nodes = defaultdict(list)

    # FJC df cannot have duplicated NID rows (it shouldnt)
    # iterate through each and create the nodes
    for index, row in fjc_active.iterrows():
        fjc_nid = row['nid']['']
        COURTS = row['Courts']['']

        NAME_FORMS = row['Name_Forms']['']
        if len(NAME_FORMS)==1:
            cleaned_name = NAME_FORMS[0]
            additional_reprs = []
        else:
            # the shortest name will be the most "truthlike" name as the FJC brackets indicate
            # that part of the name is unused (i.e. C[hristian] John Rozolis means the colloquial
            # name form is C John Rozolis)
            sorty = sorted(NAME_FORMS, key=lambda x: len(x), reverse=False)
            cleaned_name = sorty[0]
            additional_reprs = sorty[1:]

        if row['nid'][''] in JED_Globals_public.FJC_ADDITIONAL_REPRESENTATIONS:
            additional_reprs+= JED_Globals_public.FJC_ADDITIONAL_REPRESENTATIONS[row['nid']['']]
        
        if type(COURTS)== list:
            for court in COURTS:
                FJC_Nodes[court].append( 
                    JED_Classes.FreeMatch( 
                        cleaned_name, additional_reprs,
                        n_ucids = 0, courts = COURTS,FJC_NID = fjc_nid, BA_MAG_ID = None,
                        serial_id=serial_init
                        )
                    )
                serial_init+=1
        else:
            FJC_Nodes['Unallocated'].append( 
                JED_Classes.FreeMatch( 
                    cleaned_name, additional_reprs,
                    n_ucids = 0, courts = [],FJC_NID = fjc_nid, BA_MAG_ID = None,
                    serial_id=serial_init
                    )
                )
            serial_init+=1
                
    return FJC_Nodes, serial_init

def BA_MAG_Node_Creator(ba_mag, serial_init):
    
    # BA_MAG_Nodes = []
    BA_MAG_Nodes = defaultdict(list)

    # ba/mag df cannot have duplicated JUDGE_ID rows (it shouldnt)
    # iterate through each and create the nodes
    for index, row in ba_mag.drop_duplicates(["_cleaned_name","JUDGE_ID"]).iterrows():
        bama_id = row['JUDGE_ID']
        courts = row['_courts']
        cleaned_name = row['_cleaned_name']
        additional_reprs = None
        
        if type(courts)== list:
            for court in courts:
                BA_MAG_Nodes[court].append( 
                    JED_Classes.FreeMatch( 
                        cleaned_name, additional_reprs,
                        n_ucids = 0, courts = courts, FJC_NID = None, BA_MAG_ID = bama_id,
                        serial_id= serial_init
                        )
                    )
                serial_init+=1
        else:
            BA_MAG_Nodes['Unallocated'].append( 
                JED_Classes.FreeMatch( 
                    cleaned_name, additional_reprs,
                    n_ucids = 0, courts = [], FJC_NID = None, BA_MAG_ID = bama_id,
                    serial_id= serial_init
                    )
                )
            serial_init+=1

    return BA_MAG_Nodes, serial_init


def reduce_ground_truths(NODES):
    from collections import defaultdict
    
    
    nodes_bamag = defaultdict(list)
    nodes_fjc = defaultdict(list)
    
    for each in [N for N in NODES if N.is_BA_MAG]:
        nodes_bamag[each.BA_MAG_ID].append(each)
    for each in [N for N in NODES if N.is_FJC]:
        nodes_fjc[each.NID].append(each)
        
    for bid,bama in nodes_bamag.items():
        if len(bama) == 1:
            continue
        else:
            keep = bama[0]
            maps = bama[1:]
            for map_it in maps:
                keep.adopt_courts(map_it)
                map_it.points_to(keep)
                
    for nid,fjcs in nodes_fjc.items():
        if len(fjcs) == 1:
            continue
        else:
            keep = fjcs[0]
            maps = fjcs[1:]
            for map_it in maps:
                keep.adopt_courts(map_it)
                map_it.points_to(keep)
    
    return NODES

def PIPE_NODE_BUILDER(ID_Mappings, ALL_NODE_IDs):
    Post_Court_UCID_Counts = ID_Mappings.groupby(
        ['original_SID_endpoint','court'],
        as_index=False
    )['ucid'].nunique().sort_values('original_SID_endpoint')

    Post_Court_UCID_Counts.columns = ['SID','court','N_UCIDs']

    Nodes_from_dockets = ALL_NODE_IDs[(ALL_NODE_IDs.is_eligible)&
        (~ALL_NODE_IDs.is_ambiguous)&
        (~ALL_NODE_IDs.Triple_Consonant)&
        (ALL_NODE_IDs.node_name.apply(lambda x: len(str(x).split())>1))&
        (ALL_NODE_IDs.node_name.apply(lambda x: max( [len(tok) for tok in x.split()])>1))]

    Build_Set = Nodes_from_dockets.merge(
        Post_Court_UCID_Counts,
        how='left',
        left_on = ['court','original_SID'],
        right_on = ['court','SID']
    ).drop('SID',axis=1)
    Build_Set.N_UCIDs.fillna(0, inplace=True)

    ALL_NODE_IDs.loc[(~ALL_NODE_IDs.original_SID.isin(Build_Set.original_SID)), "is_eligible"] = False

    # 1. make a node for every node in the Build Set
    NODES = []

    for index, row in Build_Set.iterrows():
        additional_reprs = None
        if row['NID']:
            additional_reprs = JG.FJC_ADDITIONAL_REPRESENTATIONS.get(row['NID'])
        
        
        NODES.append(
            JED_Classes.FreeMatch(
                name= row['node_name'], additional_reprs=additional_reprs,
                n_ucids = row['N_UCIDs'], courts = [row['court']], 
                FJC_NID= row['NID'], BA_MAG_ID= row['BA_MAG_ID'],
                serial_id= row['original_SID']
            )
        )

    # first point all ba_mag at each other appropriately
    # second point any ba mag at nid if applicable
    # finally, point all nid at each other


    return NODES, ALL_NODE_IDs


def PIPE_REPOINT_Free_Match(ALL_NODES, ID_Mappings, ALL_NODE_IDs):
    input_lookup = []
    for node in ALL_NODES:
        input_lookup.append({
            'node_name': node.name, # this was the input points to which became the name
            'original_SID': node.serial_id,
            'is_FJC':node.is_FJC,
            'is_BA_MAG': node.is_BA_MAG,
            'is_eligible': node.eligible,
            'is_ambiguous': node.is_ambiguous,
            'Possible_Pointers': [(o.name, o.serial_id) for o in node.Possible_Pointers],
            'NID': node.NID,
            'BA_MAG_ID':node.BA_MAG_ID,
            'SJID': node.SJID
            
        })
    IL_NODES = pd.DataFrame(input_lookup)

    input_remap = []
    for node in ALL_NODES:
        input_remap.append({
            "original_name":node.name,
            "original_sid": node.serial_id,
            "points_to_name": node.POINTS_TO,
            "points_to_sid": node.POINTS_TO_SID
        })

    IREM = pd.DataFrame(input_remap)

    # inputs_mapped = IREM.merge(
    #     IL_NODES,
    #     how='left',
    #     left_on = ['points_to_sid'],
    #     right_on = ["original_SID"]
    # ).drop(["original_SID"], axis=1)

    PCID_Mappings = ID_Mappings.merge(
        IREM[['original_sid','points_to_sid']],
        how='left',
        left_on = 'original_SID_endpoint',
        right_on = 'original_sid'
    ).drop("original_sid",axis=1)
    PCID_Mappings.points_to_sid.fillna(PCID_Mappings.original_SID_endpoint, inplace=True)

    # combine original nodes that didnt enter post court + the post court nodes
    FINAL_NODE_LOOKUP = pd.concat([
        ALL_NODE_IDs[~ALL_NODE_IDs.original_SID.isin(IL_NODES.original_SID)],
        IL_NODES
    ])

    return PCID_Mappings, FINAL_NODE_LOOKUP, IREM

def PIPE_LABEL_AND_BUILD(PCID_Mappings, FINAL_NODE_LOOKUP, IREM):
    PCID_Mappings.loc[
        PCID_Mappings.Prefix_Categories.isin(
            ['assigned_judge', 'referred_judges']
        ), 'Prefix_Categories'] = 'Nondescript_Judge'

    temp = PCID_Mappings.groupby(['points_to_sid','Prefix_Categories'], as_index=False)['ucid'].nunique('ucid')

    # for each parent entity, determine the pretext category counts for judgey like terms
    Prefs = temp.set_index(['points_to_sid','Prefix_Categories']).unstack('Prefix_Categories')
    # fill nulls with 0
    Prefs.fillna(0,inplace=True)
    Prefs.reset_index(inplace=True)

    # get unique ucid counts for header appearances
    Header_Counts = PCID_Mappings[PCID_Mappings.docket_source!='line_entry'].groupby(
        "points_to_sid", as_index=False)['ucid'].nunique()
    Header_Counts.columns = ['points_to_sid','Head_UCIDs']
    # get toal unique ucid counts
    Total_Counts = PCID_Mappings.groupby("points_to_sid", as_index=False)['ucid'].nunique()
    Total_Counts.columns = ['points_to_sid','Total_UCIDs']

    # create the maps from entities to counts
    TC = {i:j for i,j in zip(Total_Counts.points_to_sid.values, Total_Counts.Total_UCIDs.values)}
    HC = {i:j for i,j in zip(Header_Counts.points_to_sid.values, Header_Counts.Head_UCIDs.values)}

    preffy_dict = {}
    # pref columns[1:] are all of the mutually exclusive labels
    # the column is a multi-index, so the label is col[1] 
    collabs = [col[1] for col in Prefs.columns[1:]]
    for each in [tuple(x) for x in Prefs.to_numpy()]:
        # for each entity with pretext counts
        # dict key is entity, value is a dict of columns + value where col is the actual column label
        preffy_dict[each[0]] = {col:val for col,val in zip(collabs, each[1:])}

    AMS = []
    # for every eligible parent entity
    for index, row in FINAL_NODE_LOOKUP[FINAL_NODE_LOOKUP.is_eligible].iterrows():
        # defaults are zeros
        pref = {}
        hucid = 0
        tucid = 0

        sid = row["original_SID"]
        
        if sid in preffy_dict:
            pref = preffy_dict[sid]
        if sid in HC:
            hucid = HC[sid]
        if sid in TC:
            tucid = TC[sid]

        if row['is_FJC']:
            FJC_Info = {"NID": row["NID"]}
        else:
            FJC_Info = {}
            
        if row['is_BA_MAG']:
            BA_MAG_Info = {"BA_MAG_ID": row["BA_MAG_ID"]}
        else:
            BA_MAG_Info = {}
            
        # make the object using our default information
        AMS.append(
            JED_Classes.Algorithmic_Mapping(
                row['node_name'], row['original_SID'],
                row["is_FJC"], FJC_Info,
                row["is_BA_MAG"], BA_MAG_Info,
                pref, hucid, tucid
            ))

    # instant rejection for entities with less than 3 unique ucids is not fjc and is a single token name
    bads = []
    for o in AMS:
        if o.Tot_UCIDs<=3:
            if not o.is_FJC and not o.is_BA_MAG and len(o.tokens_wo_suff)==1:
                bads.append(o)
        if len(o.tokens_wo_suff)==1 or len(o.base_tokens)==1:
            if o not in bads:
                bads.append(o)

    goods = [o for o in AMS if o not in bads]

    # for every good node, run the labelling algorithm
    for obj in goods:
        obj.Label_Algorithm()

    # once a label has been generated...
    # keep any node that was not denied, reject otherwise
    kept = [o for o in goods if 'deny' not in o.SCALES_Guess]
    rejected = [o for o in goods if 'deny' in o.SCALES_Guess]

    # now generate the SJIDs for the kept entities
    for i, obj in enumerate(kept):
        idn = str(i).zfill(6)
        obj.set_SCALES_JID(f"SJ{idn}")
    
    # mark the remaining entities as inconclusive
    for obj in rejected:
        obj.set_SCALES_JID("Inconclusive")

    # now for each of those entities, build a mapping of the SJIDs and Names
    SJID_MAP = {}
    for each in rejected + kept:
        SJID_MAP[each.serial_id] = each.SJID

    # should not see "t o" here now
    JEL = pd.DataFrame([o.JEL_row() for o in kept])

    sid2JEL = {sid:sjid for sid,sjid in JEL[['serial_id','SJID']].to_numpy()}
    PCID_Mappings['SJID'] = PCID_Mappings.points_to_sid.map(sid2JEL)

    additional_maps = {start:end for start, end in IREM[['original_sid','points_to_sid']].to_numpy()}

    remaining_ambiguities = FINAL_NODE_LOOKUP[FINAL_NODE_LOOKUP.is_ambiguous]
    ambiguous_maps = {}
    for index, row in remaining_ambiguities.iterrows():
        sid = row['original_SID']
        options = []
        for each in row['Possible_Pointers']:
            checksid = each[1]
            if checksid in sid2JEL:
                options.append(sid2JEL[checksid])
            else:
                checksid = additional_maps[checksid]
                if checksid in sid2JEL:
                    options.append(sid2JEL[checksid])
        if options:
            ambiguous_maps[sid] = options

    PCID_Mappings["Ambiguous_SJIDS"] = None
    PCID_Mappings.loc[PCID_Mappings.SJID.isna(),"Ambiguous_SJIDS"] = PCID_Mappings.points_to_sid.map(ambiguous_maps)
    PCID_Mappings.loc[~PCID_Mappings.Ambiguous_SJIDS.isna(),"SJID"] = "Ambiguous"
    PCID_Mappings.SJID.fillna("Inconclusive", inplace= True)

    JEL.drop("serial_id",axis=1, inplace=True)

    return JEL, PCID_Mappings


import os
import json
import multiprocessing as mp

def LOAD_JSONL(fpath):
    """Given a filepath to a JSONL SEL file, load it into memory as a list of the JSON objects

    Args:
        fpath (str): absolute or relative filepath to the SEL JSONL

    Returns:
        list: empty if the file does not exist, otherwise return a list of valid jsons each row representing an indexed entity from the ucid
    """
    # if its a real file, load it
    if os.path.isfile(fpath):
        with open(fpath, 'r') as json_file:
            json_list = list(json_file)
            results = []
            for json_str in json_list:
                results.append(json.loads(json_str))

        return results
    else:
        return []

def multiprocess_file_reader(all_dockets):
    """ load a bunch of ucid SEL files into memory

    Args:
        all_dockets (list): iterable list of filepaths we wish to load into memory (each filepath corresponds to a SEL JSONL)

    Returns:
        list: list of lists, each list corresponding to one ucids indexed entities
    """

    # establish the mp pool, save one processor so as not to overload the computer for the user
    pool = mp.Pool(mp.cpu_count()-1)
    print("FILE LOAD")
    raw_entries = list(tqdm.tqdm(pool.imap_unordered(LOAD_JSONL, all_dockets), total=len(all_dockets),  desc="[--File Assembly--]", leave=False))
    
    pool.close()
    pool.join()
    return raw_entries

def PIPE_LOAD_EXISTING_SELS(all_dockets):
    """Pipe that calls a multiprocessed loading of the desired ucid SEL rows

    Args:
        all_dockets (list): iterable list of filepaths we wish to load into memory (each filepath corresponds to a SEL JSONL)

    Returns:
        pandas.DataFrame: one DF of all ucid SEL rows, concatenated as one
    """

    all_selly = multiprocess_file_reader(all_dockets)
    
    all_selly = [item for sublist in all_selly for item in sublist]
    
    return pd.DataFrame(all_selly)

def PIPE_PREPARE_UPDATE_FREEMATCH(all_dockets, paths, timerange = ['2016', '2019']):
    """Pipe identifies existing SEL files for known ucids to use in a disambiguation update. Time bounds are filing date years, with a selected range of years to include from all courts

    Args:
        all_dockets (pandas.DataFrame): unique dockets data table loaded from the settings file
        paths (dict): dictionary of various filepaths for SEL files, JEL files, and FJC files
        timerange (list, optional): Bound-inclusive time range to select which ucids to include in disambiguation. Defaults to ['2016', '2019'].
    """

    def _IDENTIFY_POOL(all_dockets, sel_dir, timerange):
        """Given a directory, time range, and ucid rows from a docket table, generate a list of the filenames and ucids we should include in disambiguation

        Args:
            all_dockets (pandas.DataFrame): unique dockets data table loaded from the settings file
            sel_dir (str): absolute or relative path that points to the directory where the existing SEL ucid files exist. This directory is used in generating the lookup filepaths
            timerange (list, optional): Bound-inclusive time range to select which ucids to include in disambiguation. Defaults to ['2016', '2019'].
        """
    
        def __make_path(sel_dir, ucid):
            """[summary]

            Args:
                sel_dir (str): absolute or relative path that points to the directory where the existing SEL ucid files exist. This directory is used in generating the lookup filepaths
                ucid (str): individual ucid to splice into the corresponding SEL filepath location (location based on ucid nomenclature)

            Returns:
                str: filepath location of the SEL file corresponding to this particular ucid (there is no guarantee the path exists, this is the generated filepath if it did exist, or will be written there)
            """
            # the "year" dual digits are the sub-directory of the file in the court
            year = ucid.split(";;")[1].split(":")[1][0:2]
            # court is the first sub-directory within the SEL_dor
            court = ucid.split(';;')[0]
            # replace the colons and semi-colons with dashes
            fbase = ucid.replace(';;','-').replace(':','-')
            fname = f"{paths['SEL_DIR']}/{court}/{year}/{fbase}.jsonl"
            return fname

        # filter the ucids to the timerange desired
        pool = all_dockets[(all_dockets.year>=int(timerange[0])) & 
                           (all_dockets.year<=int(timerange[1]))]

        # the unique files table uses ucids as the index
        ucids = pool.index

        # generate tuple list of the filepath and corresponding ucid for that filepath to the SEL file
        fnames = [(__make_path(sel_dir, ucid),ucid) for ucid in ucids]

        return fnames

    # identify the ucids we want to include in our disambiguation based on filing date year
    grab_ucids = _IDENTIFY_POOL(all_dockets, paths['SEL_DIR'], timerange)

    # the list of tuples split into individual component lists (used in dev)
    loader_fnames = [g[0] for g in grab_ucids]
    loader_ucids = [g[1] for g in grab_ucids]
    
    # if in notebooks, load the file without multiprocessing
    # ODF = pd.read_json(paths['SEL'], lines=True)
    # oldSEL = ODF[ODF.ucid.isin(loader_ucids)]

    # script loader, use multiprocessing

    oldSEL = PIPE_LOAD_EXISTING_SELS(loader_fnames)
    # once the data is loaded, generate ucid counts per parent entity and merge that onto the data
    UCID_Counter = oldSEL.groupby(['court','Parent_Entity','SJID'], as_index=False).agg(N_ucids = ('ucid','nunique'))
    readySEL = oldSEL.merge(UCID_Counter, how='left',  on = ['court','Parent_Entity','SJID'])
    readySEL.rename(columns = {'Parent_Entity':'Updated_Points_To'}, inplace = True)
    
    return readySEL

    
def PIPE_UPDATE_NODE_BUILDER_FREEMATCH(Post_Court, readySEL, newest_fjc, fjc_active, oldJEL):
    """Build the node objects to be used in disambiguation. Build from existing SEL files, new dockets, and new FJC Article III judges.

    Args:
        Post_Court (pandas.DataFrame): one row per entity indexed location on a ucid, pre-SEL df from new dockets, already disambiguated intra-ucid and intra-court
        readySEL (pandas.DataFrame): one row per entity indexed location on a ucid, from existing mapped dockets
        newest_fjc (pandas.DataFrame): DF of only the newest Article III judges that have not previously been listed as FJC judges in the JEL
        fjc_active (pandas.DataFrame): All Article III judges from the FJC codebook, previously recognized and new ones
        oldJEL (pandas.DataFrame): the existing JEL that we will be updating after disambiguation

    Returns:
        list: list of objects (IntraMatch super class) that will be used in disambiguation
    """
    
    ##################################################
    ###### FJC BLOCK -- create nodes for the newest FJC judges

    # starter dicts
    nid = {}
    fullnames = {}
    simple_names = {}
    earliest_commission = {}
    latest_termination = {}

    # build FJC data -- for each unique NID, we want the earliest and latest dates
    # because there are multiple appointments for some judges (thus mulit-row in the fjc data)
    # we don't want multiple nodes representing the same entity since the code logic infers only one object should exist
    # that contains the specific NID on it.
    for index, row in newest_fjc.iterrows():
        key = row['nid']['']
        earliest_commission[key] = row['Commission Date']['min']
        latest_termination[key] = row['Termination Date']['max']
        nid[key] = key
        fullnames[key] = row['FullName']['']
        simple_names[key] = row['Simplified Name']['']

    # aggregations on the FJC data

    # unpack the dicts and make them matching objects
    FJC_Nodes = []
    for each in newest_fjc['nid'].unique():
        fjc_info = {
            "Full_Name":fullnames[each],
            "NID":nid[each],
            "Earliest_Commission":earliest_commission[each],
            "Latest_Termination": latest_termination[each]
        }
        simple_name = simple_names[each]
        # none of them will have an SJID yet
        FJC_Nodes.append(JED_Classes.FreeMatch(simple_name,0,[],fjc_info, SJID = "Inconclusive"))
    ########################################################
    ########################################################
    
    # these are all of the newest dockets, none have been identified with SJIDs so all start as inconclusive
    Post_Court['SJID'] = 'Inconclusive'

    # make a giant df of the existing SEL tagged rows and the new dockets
    PC_merged = pd.concat(
        [Post_Court[['Updated_Points_To','court','SJID', 'ucid']],
         readySEL[['Updated_Points_To','court','SJID', 'ucid']]
      ])

    # basically, the NID always gets screwed up, make sure it is cast as the correct string
    PC_merged['NID'] = PC_merged.SJID.map(
        {SJID:str(NID).split('.')[0] for SJID,NID in oldJEL[['SJID','NID']].to_numpy() if not pd.isna(NID)})

    # now group together all entities (existing dockets + enw ones) based on the final parent entity beofre free-matching disambiguation. Get UCID count
    nodes = PC_merged.groupby(
        ['Updated_Points_To', 'court', 'SJID'], 
        as_index=False)['ucid'].nunique().sort_values('Updated_Points_To')

    # begin grouping entities together by exact name matches now
    # court ucid counts can be summed as ucids are mutually exclusive across courts
    names = {}
    for name, court, sjid, ucids in [tuple(x) for x in nodes.to_numpy()]:
        key = name
        if name not in names:
            names[key] = {'courts':[court], 'N_ucids':ucids, "SJID":[sjid]}
        else:
            names[key]['courts'].append(court)
            names[key]['SJID'].append(sjid)
            names[key]['N_ucids']+=ucids


    # now for every name, if the name is multi-tokened, create a node object for matching
    PC_Nodes = []
    for name, values in names.items():

        # grab the SJID for the cleaned parent entity
        sjids = values["SJID"]
        # if there is only one label, that is the SJID
        if len(set(sjids))==1:
            sjid = sjids[0]
        # if there are multiple (2) but one is inconclusive, then the inconclusive are now mapped to the known SJID
        elif len(set([i for i in sjids if i!= "Inconclusive"]))==1:
            sjid = [i for i in sjids if i!= "Inconclusive"][0]
        # if there were multiple different SJIDs, then we cannot account for it and print a message. This has not happened yet.
        else:
            print("ope")

        # if the sjid is a known ID
        if sjid != "Inconclusive":
            # grab the old JEL data for the entity
            lookup = dict(oldJEL[oldJEL.SJID == sjid].iloc[0])
            # if it was an FJC judge as well, find the FJC data (most up to date FJC data too)
            if not pd.isna(lookup['NID']):
                str_nid = str(lookup['NID']).split(".")[0]

                date_lookup = dict(fjc_active[fjc_active.nid==int(str_nid)].iloc[0])
                fjc_info = {
                    "Full_Name":lookup["Full_Name"],
                    "NID":str_nid,
                    "Earliest_Commission":date_lookup[('Commission Date','min')],
                    "Latest_Termination": date_lookup[('Termination Date','max')]
                }
            # if it wasn't an FJC judge, that's fine, we instantiate with empty data
            else:
                fjc_info = None
        # if there was an inconclusive SJID, we also wont have NID or FJC data
        else:
            fjc_info = None

        # in free matching (no ucid or court) we dont handle single token entities
        if len(name.split())==1:
            continue

        # build the objects
        PC_Nodes.append(
            JED_Classes.FreeMatch(
                name, values['N_ucids'],values['courts'], FJC_Info = fjc_info, SJID = sjid
            )
        )

    # finally group the FJC nodes with the docket/extraction nodes
    Free_Nodes = PC_Nodes+ FJC_Nodes
    
    
    # ADD IN ANY OLDJEL ENTITIES THAT WERE OVERLOOKED or not present in the pool of selected dockets
    remainder = oldJEL[(~oldJEL.SJID.isin(set(n.SJID for n in Free_Nodes)))]
    rem_nodes = []
    for index,each in remainder.iterrows():
        name = each['name']
        nucids = each['Tot_UCIDs']
        sjid = each['SJID']

        # if it was an fjc judge, grab relevant date and nid info
        if not pd.isna(each['NID']):
            str_nid = str(each['NID']).split(".")[0]

            date_lookup = dict(fjc_active[fjc_active.nid==int(str_nid)].iloc[0])
            fjc_info = {
                "Full_Name":each["Full_Name"],
                "NID":str_nid,
                "Earliest_Commission":date_lookup[('Commission Date','min')],
                "Latest_Termination": date_lookup[('Termination Date','max')]
            }
        else:
            fjc_info = None
        # make the nodes
        rem_nodes.append(
            JED_Classes.FreeMatch(
                name, nucids,[], FJC_Info = fjc_info, SJID = sjid
            )
        )
        
    # this final list is: existing entities pretagged in the SEL, new docket entities to be tagged, new fjc judges to be tagged, existing JEL entities that didnt appear in the prior 3
    fin_nodes = Free_Nodes+ rem_nodes
    
    return fin_nodes


def PIPE_Update_Final_Repoint(Post_Court, FIN_NODES):
    """After disambiguation is complete, this function cycles through the docket rows and updates parent-child entity tags (repointing entities to the correct disambiguated parent))

    Args:
        Post_Court (pandas.DataFrame): one row per entity indexed location on a ucid DF ready for parent entity repointing
        FIN_NODES (list): list of objects (IntraMatch super class) that are now disambiguated

    Returns:
        pandas.DataFrame: DF in pre-SEL format that now points extracted entities to their disambiguated parent entities
    """

    # any remaining eligible node will point to itself as the parent
    self_points = []
    for obj in [o for o in FIN_NODES if o.eligible]:
        self_points.append(
                {'Updated_Points_To':obj.name,
                'Final_Pointer': obj.POINTS_TO,
                'is_FJC':obj.is_FJC,
                'NID':obj.NID,
                'SJID': obj.SJID}
            )
    # construct a dataframe of entities that are themselves the parent representative entity name
    SP = pd.DataFrame(self_points)

    # now generate the child entities data that takes the child entity representation and points it to the parent (final pointer)
    ineligible = []
    for obj in [o for o in FIN_NODES if not o.eligible]:
        ineligible.append(
                {'Updated_Points_To':obj.name,
                'Final_Pointer': obj.POINTS_TO,
                'is_FJC':obj.is_FJC,
                'NID':obj.NID,
                'SJID': obj.SJID}
            )

    IE = pd.DataFrame(ineligible)

    # create the lookup dataframe with appropriate suffixes
    RePointed = Post_Court.merge(
        SP, how='left', on = ['Updated_Points_To'], suffixes=['','_Self_Pointing']).merge(
        IE, how='left', on = ['Updated_Points_To'], suffixes=['','_Away_Pointing'])

    # fillna first with away pointing, then if none, the self entity
    RePointed['Final_Pointer'].fillna(RePointed.Final_Pointer_Away_Pointing, inplace=True)
    # this column comes from self pointing first so it is not suffixed
    RePointed['Final_Pointer'].fillna(RePointed.Updated_Points_To, inplace=True)

    # infill parent FJC/NID/SJID information
    RePointed['is_FJC'].fillna(RePointed.is_FJC_Away_Pointing, inplace=True)
    RePointed['NID'].fillna(RePointed.NID_Away_Pointing, inplace=True)
    RePointed['SJID_Self_Pointing'].fillna(RePointed.SJID_Away_Pointing, inplace=True)

    # if anybody is leftover pointing to themselves, set the parent entity to the string name
    RePointed.loc[RePointed.Final_Pointer==">>SELF<<", 'Final_Pointer'] = RePointed.Updated_Points_To

    # if anybody is self-pointing, take that SJID as ground truth
    RePointed['SJID'] = RePointed.SJID_Self_Pointing
    RePointed['is_FJC'].fillna(False, inplace=True) # if the entity was not labeled as FJC, fill it as false

    # drop my leftover columns from the merge
    RePointed.drop(['SJID_Self_Pointing', 'Final_Pointer_Away_Pointing',
           'is_FJC_Away_Pointing', 'NID_Away_Pointing', 'SJID_Away_Pointing'], axis=1, inplace=True)

    # header data's prefix categories would be null, at this point we label them as Nondescript
    RePointed.loc[
        RePointed.Prefix_Categories.isin(['assigned_judge', 'referred_judges']), 'Prefix_Categories'] = 'Nondescript_Judge'
    
    # any remaining entity without an SJID is now left inconclusive and can be considered for algorithmic labelling
    RePointed['SJID'].fillna("Inconclusive",inplace=True)
    
    # rename the column to match SEL file structure
    RePointed.rename(columns = {'extracted_entity':'Extracted_Entity'}, inplace = True)
    
    return RePointed

def reduce_ground_truths_update(NODES: list, oldJEL: pd.DataFrame):
    """After court disambiguation it is possible multiple copies of a ground truth entity exist
    (for example a judge that operated in the Eastern and Western District of KY would be in each courts
    list of nodes). In this function, we collapse those into one ground truth reference

    Args:
        NODES (list): list of IntraMatch objects 
        oldJEL (pd.DataFrame): prior identified ground truth entities if we have an existing JEL

    Returns:
        list: the same list of IntraMatch objects plus JEL rows if applicable, now mapped to each other
    """
    # create prior ground truth nodes
    JEL_objs = []
    jelsid = 0
    for index, row in oldJEL.iterrows():
        # we iterate the serial IDs into the negatives since the new unknown ones are positive
        jelsid-=1
        alternates = None
        # if there is an NID, confirm there are no additional marital names or other variants for the entities
        if not pd.isnull(row['NID']):
            alternates = JG.FJC_ADDITIONAL_REPRESENTATIONS.get(row['NID'])

        JEL_objs.append(
            JED_Classes.FreeMatch(
            row['name'], alternates, 0, courts=[],
            FJC_NID= row['NID'], BA_MAG_ID=row['BA_MAG_ID'], serial_id=jelsid, SJID = row['SJID']
            )
        )

    from collections import defaultdict

    # create all of the nodes from our ground truth sources
    # it doesnt matter if they already existed, they will be reduced or figured out later
    nodes_bamag = defaultdict(list)
    nodes_fjc = defaultdict(list)

    for each in JEL_objs:
        if each.is_FJC:
            nodes_fjc[each.NID].append(each)
        elif each.is_BA_MAG:
            nodes_bamag[each.BA_MAG_ID].append(each)
        else:
            continue
    
    # combine the input nodes that are BAMAG with the ground truth ones, same for FJC
    for each in [N for N in NODES if N.is_BA_MAG]:
        nodes_bamag[each.BA_MAG_ID].append(each)
    for each in [N for N in NODES if N.is_FJC]:
        nodes_fjc[each.NID].append(each)

    # this dictionary was keyed by unique identifier
    # for each unique judge, there is a list of duplicate entities
    # reduce them all into one object now
    for bid,bama in nodes_bamag.items():
        if len(bama) == 1:
            continue
        else:
            keep = bama[0]
            maps = bama[1:]
            for map_it in maps:
                keep.adopt_courts(map_it)
                map_it.points_to(keep)

    # exact same concept for the FJC judges
    for nid,fjcs in nodes_fjc.items():
        if len(fjcs) == 1:
            continue
        else:
            keep = fjcs[0]
            maps = fjcs[1:]
            for map_it in maps:
                keep.adopt_courts(map_it)
                map_it.points_to(keep)
                
    return NODES + JEL_objs


def assemble_meta_data(PCID: pd.DataFrame, old_SJID_Data: list, priors_map: dict):
    """This function runs after the final layer of disambiguation. The function assembled metadata
    for all of the input rows and prior tagged SJIDs that could be re-labelled algorithmically.
    The metadata includes UCID counts and prefix counts for the entity appearances

    Args:
        PCID (pd.DataFrame): extracted entities dataframe with pointers after disambiguation
        old_SJID_Data (list): list of ingested data on prior-tagged ucids including their prefixes and ucids
        priors_map (dict): a dictionary mapping the prior SJIDs to their node serial_ids

    Returns:
        dict, dict, dict: three dictionaries keyed by their node serial_ids - prefix counts, header ucid counts, and total ucid counts
    """
    temp = PCID.groupby(['points_to_sid','Prefix_Categories'], as_index=False)['ucid'].nunique('ucid')
    
    # for each parent entity, determine the pretext category counts for judgey like terms
    Prefs = temp.set_index(['points_to_sid','Prefix_Categories']).unstack('Prefix_Categories')
    # fill nulls with 0
    Prefs.fillna(0,inplace=True)
    Prefs.reset_index(inplace=True)

    preffy_dict = {}
    # pref columns[1:] are all of the mutually exclusive labels
    # the column is a multi-index, so the label is col[1] 
    collabs = [col[1] for col in Prefs.columns[1:]]
    for each in [tuple(x) for x in Prefs.to_numpy()]:
        # for each entity with pretext counts
        # dict key is entity, value is a dict of columns + value where col is the actual column label
        preffy_dict[each[0]] = {col:val for col,val in zip(collabs, each[1:])}
        
    # get unique ucid counts for header appearances
    Header_Counts = PCID[PCID.docket_source!='line_entry'].groupby(
        "points_to_sid", as_index=False)['ucid'].nunique()
    Header_Counts.columns = ['points_to_sid','Head_UCIDs']
    # get toal unique ucid counts
    Total_Counts = PCID.groupby("points_to_sid", as_index=False)['ucid'].nunique()
    Total_Counts.columns = ['points_to_sid','Total_UCIDs']

    # create the maps from entities to counts
    TC = {i:j for i,j in zip(Total_Counts.points_to_sid.values, Total_Counts.Total_UCIDs.values)}
    HC = {i:j for i,j in zip(Header_Counts.points_to_sid.values, Header_Counts.Head_UCIDs.values)}
    
    OSD = pd.DataFrame(old_SJID_Data)
    
    OSD_temp = OSD.groupby(['SJID','Prefix_Categories'], as_index=False)['ucid'].nunique('ucid')

    OSD_Prefs = OSD_temp.set_index(['SJID','Prefix_Categories']).unstack('Prefix_Categories')
    # fill nulls with 0
    OSD_Prefs.fillna(0,inplace=True)
    OSD_Prefs.reset_index(inplace=True)

    OSD_preffy_dict = {}
    # pref columns[1:] are all of the mutually exclusive labels
    # the column is a multi-index, so the label is col[1] 
    collabs = [col[1] for col in OSD_Prefs.columns[1:]]
    for each in [tuple(x) for x in OSD_Prefs.to_numpy()]:
        # for each entity with pretext counts
        # dict key is entity, value is a dict of columns + value where col is the actual column label
        dkey = priors_map[each[0]]
        OSD_preffy_dict[dkey] = {col:val for col,val in zip(collabs, each[1:])}
    
    # get unique ucid counts for header appearances
    OSD_Header_Counts = OSD[OSD.docket_source!='line_entry'].groupby(
        "SJID", as_index=False)['ucid'].nunique()
    OSD_Header_Counts.columns = ['SJID','Head_UCIDs']
    # get toal unique ucid counts
    OSD_Total_Counts = OSD.groupby("SJID", as_index=False)['ucid'].nunique()
    OSD_Total_Counts.columns = ['SJID','Total_UCIDs']

    # create the maps from entities to counts
    OSD_TC = {priors_map[i]:j for i,j in zip(OSD_Total_Counts.SJID.values, OSD_Total_Counts.Total_UCIDs.values)}
    OSD_HC = {priors_map[i]:j for i,j in zip(OSD_Header_Counts.SJID.values, OSD_Header_Counts.Head_UCIDs.values)}
    
    # assemble prefixes
    FIN_PREFFY = {}
    for key, dict_vals in preffy_dict.items():
        FIN_PREFFY[key] = dict_vals

    for key, dict_vals in OSD_preffy_dict.items():
        if key in FIN_PREFFY:
            for vkey, val in dict_vals.items():
                FIN_PREFFY[key][vkey]+=val
        else:
            FIN_PREFFY[key] = dict_vals

    # assemble total UCID counts per id
    FIN_TC = {}
    for key, vals in TC.items():
        FIN_TC[key] = vals

    for key, vals in OSD_TC.items():
        if key in FIN_TC:       
            FIN_TC[key]+=vals
        else:
            FIN_TC[key] = vals

    # assemble header UCID counts per id
    FIN_HC = {}
    for key, vals in HC.items():
        FIN_HC[key] = vals

    for key, vals in OSD_HC.items():
        if key in FIN_HC:       
            FIN_HC[key]+=vals
        else:
            FIN_HC[key] = vals
    
    return FIN_PREFFY, FIN_TC, FIN_HC

def PIPE_UPDATE_Assign_and_ReLabel(PCID: pd.DataFrame, FIN_LOOKUP: pd.DataFrame, IREM: pd.DataFrame, oldJEL: pd.DataFrame, old_SJID_Data: list):
    """GIven a post disambiguation dataframe and the node mapping lookups, relabel and re-guess the SJID values in the final cleanup steps here

    Args:
        PCID (pd.DataFrame): extracted entities dataframe with pointers after disambiguation
        FIN_LOOKUP (pd.DataFrame): Lookup df of all node serial IDs and who they eventually point to as their parent entity
        IREM (pd.DataFrame): intermediate lookup from court to freematch disambiguation
        oldJEL (pd.DataFrame): existing DF of known judge entities prior to this update
        old_SJID_Data (list): list of ingested data on prior-tagged ucids including their prefixes and ucids

    Returns:
        panda.DataFrame, pandas.DataFrame: massive SEL-ready dataframe with final disambiguation tags and labels; updated JEL df with newly identified judges
    """
    # header extractions need to be remapped so their prefixes are nondescript
    PCID.loc[PCID.Prefix_Categories.isin(['assigned_judge', 'referred_judges']), 'Prefix_Categories'] = 'Nondescript_Judge'

    # map the serial IDs to their SJIDs
    SJID_map = {k:v for k,v in FIN_LOOKUP[['original_SID','SJID']].drop_duplicates().to_numpy()}
    # apply
    PCID['SJID'] = PCID.points_to_sid.map(SJID_map)
    
    # generate a lookup of prior SJID tags based on Node serial IDs
    priors = FIN_LOOKUP[["SJID","original_SID","is_eligible"]]
    priors = priors[~priors.SJID.isna()]
    priors = priors[priors.is_eligible]
    priors_map = {k:v for k,v in priors[['SJID','original_SID']].to_numpy()}
    
    # given the post extraction data, assemble lookups for the metadat per entity (prefix counts, ucid counts)
    FIN_PREFFY, FIN_TC, FIN_HC = assemble_meta_data(PCID, old_SJID_Data, priors_map)
    
    # create a list of objects that will be pushed through the labelling algorithm to determine if new SJIDs need to be created
    AMS = []
    # for every eligible parent entity
    for index, row in FIN_LOOKUP[FIN_LOOKUP.is_eligible].iterrows():
        # defaults are zeros
        pref = {}
        hucid = 0
        tucid = 0

        sid = row["original_SID"]
        # pull the metadata by serial node identifier
        if sid in FIN_PREFFY:
            pref = FIN_PREFFY[sid]
        if sid in FIN_HC:
            hucid = FIN_HC[sid]
        if sid in FIN_TC:
            tucid = FIN_TC[sid]

        # pull ground truth labels if applicable
        if row['is_FJC']:
            FJC_Info = {"NID": row["NID"]}
        else:
            FJC_Info = {}

        if row['is_BA_MAG']:
            BA_MAG_Info = {"BA_MAG_ID": row["BA_MAG_ID"]}
        else:
            BA_MAG_Info = {}

        # make the object using our default information
        AMS.append(
            JED_Classes.Algorithmic_Mapping(
                row['node_name'], row['original_SID'],
                row["is_FJC"], FJC_Info,
                row["is_BA_MAG"], BA_MAG_Info,
                pref, hucid, tucid, Prior_SJID=row['SJID']
            ))

    # instant rejection for entities with less than 3 unique ucids is not fjc and is a single token name
    bads = []
    for o in AMS:
        if o.Tot_UCIDs<=3:
            if not o.is_FJC and not o.is_BA_MAG and len(o.tokens_wo_suff)==1:
                bads.append(o)
        if len(o.tokens_wo_suff)==1 or len(o.base_tokens)==1:
            if o not in bads:
                bads.append(o)

    goods = [o for o in AMS if o not in bads]

    # for every good node, run the labelling algorithm
    for obj in goods:
        obj.Label_Algorithm()

    # once a label has been generated...
    # keep any node that was not denied, reject otherwise
    kept = [o for o in goods if 'deny' not in o.SCALES_Guess]
    rejected = [o for o in goods if 'deny' in o.SCALES_Guess]

    # mark the remaining entities as inconclusive
    for obj in rejected:
        obj.set_SCALES_JID("Inconclusive")

    max_prior_sjid = max([int(oj.split('SJ')[1]) for oj in oldJEL.SJID.unique()])

    # now generate the SJIDs for the kept entities
    restart = max_prior_sjid+1
    for obj in kept:
        if pd.isna(obj.Prior_SJID):
            idn = str(restart).zfill(6)
            obj.set_SCALES_JID(f"SJ{idn}")
            restart+=1
        else:
            obj.set_SCALES_JID(obj.Prior_SJID)

    # now for each of those entities, build a mapping of the SJIDs and Names
    SJID_MAP = {}
    for each in rejected + kept:
        SJID_MAP[each.serial_id] = each.SJID

    # create an updated JEL dataframe
    newJEL = pd.DataFrame([o.JEL_row() for o in kept])
    sid2JEL = {sid:sjid for sid,sjid in newJEL[['serial_id','SJID']].to_numpy()}
    PCID['Updated_SJID'] = PCID.points_to_sid.map(sid2JEL)
    PCID['SJID'].fillna(PCID.Updated_SJID, inplace=True)
    PCID.drop('Updated_SJID',axis=1, inplace=True)

    additional_maps = {start:end for start, end in IREM[['original_sid','points_to_sid']].to_numpy()}
    # now reconcile the rows that had ambiguous labels so they at least point to the SJIDs they could be ambiguously tied to
    remaining_ambiguities = FIN_LOOKUP[FIN_LOOKUP.is_ambiguous]
    ambiguous_maps = {}
    for index, row in remaining_ambiguities.iterrows():
        sid = row['original_SID']
        options = []
        for each in row['Possible_Pointers']:
            checksid = each[1]
            if checksid in sid2JEL:
                options.append(sid2JEL[checksid])
            else:
                checksid = additional_maps[checksid]
                if checksid in sid2JEL:
                    options.append(sid2JEL[checksid])
        if options:
            ambiguous_maps[sid] = options

    PCID["Ambiguous_SJIDS"] = None
    PCID.loc[PCID.SJID.isna(),"Ambiguous_SJIDS"] = PCID.points_to_sid.map(ambiguous_maps)
    PCID.loc[~PCID.Ambiguous_SJIDS.isna(),"SJID"] = "Ambiguous"
    PCID.SJID.fillna("Inconclusive", inplace= True)

    PCID[~PCID.Ambiguous_SJIDS.isna()]
    
    # drop the column we no longer need
    newJEL.drop("serial_id",axis=1, inplace=True)
    
    if any(i not in newJEL.SJID.unique() for i in oldJEL.SJID.unique()):
        print("ERROR: LOST TRACK OF AN OLD SJID SOMEWHERE")
    
    # now create the newest version of our JEL
    check = newJEL.merge(oldJEL[['SJID','SCALES_Judge_Label','is_FJC','is_BA_MAG']],
            how = 'left', left_on = ['SJID'], right_on = ['SJID'],
            suffixes=['','_PRIOR'])
    # if an old SJID has a new guess, this is the rank order in which the new guess can supersede the old one
    # ie if the old guess was nondescript and the new one is Judicial Actor, it remains nondescript
    rank_order = [
        'FJC Judge', 'BA-MAG Judge','District_Judge','Magistrate_Judge', 'Bankruptcy_Judge',
        'Nondescript_Judge', 'Judicial_Actor', 'Maintain Prior JEL']
    # determine if any labels need to be changed
    for index, row in check.iterrows():
        prior = row['SCALES_Judge_Label']
        update = row['SCALES_Guess']
        fin_guess =''
        if pd.isna(prior):
            fin_guess = update
        elif prior!=update:
            ro_prior = rank_order.index(prior)
            ro_update = rank_order.index(update)
            fin_guess = rank_order[min([ro_prior, ro_update])]
        else:
            fin_guess = prior
        check.loc[index, 'Final_Guess'] = fin_guess
    # drop the old labels
    check.drop(["SCALES_Guess", "SCALES_Judge_Label"], axis=1, inplace=True)
    # update the column name for the final guess
    check.rename({'Final_Guess':'SCALES_Guess'}, axis=1, inplace=True)
    
    return PCID, check


def PIPE_UPDATE_FREE_MATCH(ID_Mappings: pd.DataFrame, ALL_NODE_IDs:pd.DataFrame, oldJEL: pd.DataFrame, old_SJID_Data: list, fjc_active: pd.DataFrame):
    """[summary]

    Args:
        ID_Mappings (pd.DataFrame): dataframe from post free match disambiguation that shows the node ids and where they point to
        ALL_NODE_IDs (pd.DataFrame): master lookup of all entity nodes and what parents they point to
        oldJEL (pd.DataFrame): prior existing JEL data
        old_SJID_Data (list): prior SEL tagged SJID data
        fjc_active (pd.DataFrame): the FJC codebook data

    Returns:
        pd.DataFrame, pd.DataFrame: the final SEL df ready to be written to a file; the newly updated JEL data
    """
    # build the nodes and reduce the ground truth ones
    NODES, ALL_NODE_IDs = PIPE_NODE_BUILDER(ID_Mappings, ALL_NODE_IDs)
    ALL_NODES = reduce_ground_truths_update(NODES, oldJEL)
    
    # newbs are nodes that were not accounted for in prior disambiguation, and thus need to be added to the
    # master node lookup table with the appropriate attributes
    newbs = [N for N in ALL_NODES if N.serial_id not in ALL_NODE_IDs.original_SID and N.eligible]
    APPY = []
    for each in newbs:
        ANI_formatted = {
            'court': None,
            'node_name': each.name,
            'original_SID':each.serial_id,
            'is_FJC': each.is_FJC,
            'is_BA_MAG': each.is_BA_MAG,
            'is_eligible': each.eligible,
            'is_ambiguous': each.is_ambiguous,
            'Possible_Pointers': each.Possible_Pointers,
            'NID': each.NID,
            'BA_MAG_ID': each.BA_MAG_ID,
            'SJID': each.SJID,
            'Triple_Consonant': False
            }
        APPY.append(ANI_formatted)
    APPY = pd.DataFrame(APPY)
    
    # update the node lookup table
    NODE_LOOKUP = pd.concat([ALL_NODE_IDs, APPY])
    
    # split nodes into likley active or dormant judges for our dockets
    ACTIVE,DORMANT = PIPE_FIND_DORMANT_FJC(ALL_NODES, fjc_active)
    
    # run free match disambiguation on the active ones
    ACTIVE = FREE_MATCH_CYCLER(ACTIVE)
    
    # recombine
    END_NODES = ACTIVE + DORMANT
    
    # update the entity parents and pointers after all of the disambiguation
    PCID, FIN_LOOKUP, IREM = PIPE_REPOINT_Free_Match(END_NODES, ID_Mappings, NODE_LOOKUP)
    
    # relabel and generate guesses algorithmically
    FPCID, newJEL = PIPE_UPDATE_Assign_and_ReLabel(PCID, FIN_LOOKUP, IREM, oldJEL, old_SJID_Data)
    
    return FPCID, newJEL