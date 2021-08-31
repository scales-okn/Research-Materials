import pandas as pd
import tqdm
import re

from collections import defaultdict
import JED_Classes_public as JED_Classes
import JED_Cleaning_Functions_public as JCF
import JED_Algorithms_public as JA
import JED_Utilities_public as JU

from fuzzywuzzy import fuzz

def PIPELINE_Disambiguation_Prep(entry_frame, header_frame, dockets):
    """Preliminary cleaning and respanning to prep the data for disambiguation

    Args:
        entry_frame (pandas.DataFrame): extracted entities using spacy model on docket entries
        header_frame (pandas.DataFrame): extracted entities from header metadata
        dockets (pandas.DataFrame): unique files table loaded using settings

    Returns:
        pandas.DataFrame: combined dataframe of all rows to be used in disambiguation
    """
    
    # merge the unique files table to get the filing date/year -- this might no longer be needed
    # edf = JCF.merge_to_uni(entry_frame, dockets)
    # hdf = merge_to_uni(header_frame, dockets)
    edf = entry_frame
    hdf = header_frame

    # run the cleaning and respanning processes on each dataframe independently
    edf = single_df_pipeline(edf, is_header=False)
    hdf = single_df_pipeline(hdf, is_header=True)
    
    # clean all the entities in preparation for disambiguation
    edf, hdf = pipe_junction(edf, hdf)
    
    # create quick counts of number of ucids in a court that an entity appears
    edf, hdf = pipe_UCID_Counts(edf, hdf)
    
    # drop bad/low-occurrence entities and fuse the header and entry data into one SEL-like dataframe
    fdf = pipe_concatenation(edf, hdf)

    return fdf


def single_df_pipeline(DF, is_header):
    """Cleaning and respanning functions for the raw spacy extracted entities

    Args:
        DF (pandas.DataFrame): raw extracted entities either from spacy extractions or header metadata
        is_header (bool): is this the header dataframe

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
        DF = JCF.reshuffle_exception_entities(DF)

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

def cleaning_pipe(df):
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

def pipe_junction(df_entries, df_headers):
    """take both the header and entry dataframe and combine their overall unique entities for cleaning

    Args:
        df_entries (pandas.DataFrame): entries entities df
        df_headers (pandas.DataFrame): header entities df

    Returns:
        pandas.DataFrame, pandas.DataFrame: entry and header dataframes with the cleaned entity column added
    """
    
    print("\nFirst Junction Pipe Running")
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


def pipe_UCID_Counts(df_entries, df_headers):
    """ Take the entity and header dataframes and determine for each entity,
    how many cases does it appear on in its respective court

    Args:
        df_entries (pandas.DataFrame): entries entities df
        df_headers (pandas.DataFrame): header entities df

    Returns:
        pandas.DataFrame, pandas.DataFrame: entry and header dataframes with the ucid count column added
    """
    
    print("\nPrimary UCID Count pipe running")
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

def pipe_concatenation(df_entries, df_headers):
    """Join header and entry dataframes into one large dataset pre-disambiguation

    Args:
        df_entries (pandas.DataFrame): cleaned and respanned entry entities
        df_headers (pandas.DataFrame): cleaned and respanned header entities

    Returns:
        pandas.DataFrame: Pre-SEL dataframe with row per entity to be used in disambiguation
    """
    
    print("\nLast Junction Pipe Running - Combining Header and Entry Data")
    
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
           'ucid','cid','court','year',#'filing_date',
           'original_text','extracted_entity','New_Pre_Tokens','New_Entity','New_Post_Tokens','Cleaned_Entity',
           'Prefix_Categories','Transferred_Flag','full_span_start','full_span_end','New_Span_Start','New_Span_End',
           'eligible','N_ucids']
    
    print(">> Concatenating Pre-SEL DataFrame")
    PRE_SEL_E = fin_e[SEL_COLS]
    PRE_SEL_H = fin_h[SEL_COLS]
    
    # merge into 1 df
    RET = pd.concat([PRE_SEL_E, PRE_SEL_H])
    
    return RET


def UCID_MATCH_PIPELINE(FDF, parties, counsels):
    """Disambiguation Pipeline for Intra-UCID entity matching

    Args:
        FDF (pandas.DataFrame): [description]
        parties (pandas.DataFrame): parties on the cases we are disambiguating
        counsels (pandas.DataFrame): counsels on the cases we are disambiguating

    Returns:
        pandas.DataFrame: Updated Df with entities and pointers to their parent entities after intra-ucid matching
    """

    ucid_map = UCID_PIPE_Object_Builder(FDF) # build the objects for disambiguation
    ucid_map, toss_map = JA.UCID_PIPE_Drop_Parties(ucid_map, parties, counsels) # drop parties and counsels that were misattributed as judges
    new_map = JA.PIPE_Fuzzy_Matching(ucid_map) # generic fuzzy match
    new_map = JA.PIPE_Tokens_in_Tokens(new_map, False, False, 'Plain') # token in token checks
    new_map = JA.PIPE_Tokens_in_Tokens(new_map, False, True, 'Plain') # TiT abbreviated middle initial
    new_map = JA.PIPE_Tokens_in_Tokens(new_map, True, True, 'Plain') # TiT abbreviated first and middle initial
    new_map = JA.PIPE_Tokens_in_Tokens(new_map, False, False, 'Unified') # universal name spellings
    new_map = JA.PIPE_Tokens_in_Tokens(new_map, False, False, 'Nicknames') # using nicknames
    new_map = JA.PIPE_Anchor_Reduction_UCID(new_map) # reduce using surnames
    new_map = JA.PIPE_Anchor_Reduction_II_UCID(new_map) # secondary surname reduction
    new_map = JA.PIPE_Anchor_Reduction_III_UCID(new_map) # third surname reduction
    UCID_df = UCID_PIPE_Build_Remapped_Lookup(new_map, toss_map, FDF) # remap into a Dataframe
    
    return UCID_df

def COURT_MATCH_PIPELINE(Post_UCID):
    """Disambiguation Pipeline for Intra-Court entity matching

    Args:
        Post_UCID (pandas.DataFrame): post-ucid matching df

    Returns:
        pandas.DataFrame: again disambiguated in court, now with a column mapping to newest parent entities
    """
    # build the objects and split into single or multi-token names
    court_map_long, court_map_single = COURT_PIPE_Object_Builder(Post_UCID)

    court_map_long = JA.PIPE_Fuzzy_Matching(court_map_long)
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, False, False, 'Plain')
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, False, True, 'Plain')
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, True, True, 'Plain')
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, False, False, 'Unified')
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, False, False, 'Nicknames')
    court_map_long = JA.PIPE_Tokens_in_Tokens(court_map_long, False, True, 'Nicknames')
    
    court_map = JA.PIPE_Anchor_Reduction_Court(court_map_long, court_map_single)
    
    Court_df = COURT_PIPE_Build_Remapped_Lookup(court_map, Post_UCID)

    return Court_df


def FREE_MATCH_PIPELINE(Post_Court, fjc_active):
    """Final round of disambiguation with no locks on court or ucid

    Args:
        Post_Court (pandas.DataFrame): df after court level disambiguation
        fjc_active (pandas.DataFrame): df of FJC judges we wish to include in the JEL

    Returns:
        pandas.DataFrame: final pre-SEL df
    """

    # build our pool of entities to be involved in court-agnostic free-matching. This will now incorporate judges from the FJC biographical dictionary
    NODES = PIPE_NODE_BUILDER(Post_Court, fjc_active)
    print("Total Nodes: ",len(NODES))
    ACTIVE_NODES, DORMANT_NODES = PIPE_FIND_DORMANT_FJC(NODES)

    ACTIVE_NODES = JA.PIPE_Free_Fuzzy(ACTIVE_NODES)
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Unified')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Nicknames')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Nicknames')

    ACTIVE_NODES = JA.PIPE_Free_Vacuum(ACTIVE_NODES) # vacuum out the middle names and compare entities
    ACTIVE_NODES = JA.PIPE_Free_Token_Sort(ACTIVE_NODES) # compare tokens in flipped ortders
    ACTIVE_NODES = JA.PIPE_Free_Van_Sweep(ACTIVE_NODES) # special patch for germanic names
    ACTIVE_NODES = JA.PIPE_Free_Initialisms(ACTIVE_NODES) # special patch for initial heavy judges
    ACTIVE_NODES = JA.PIPE_Free_Single_Letters(ACTIVE_NODES) # special patch for judges that go by a single letter of one of their names
    ACTIVE_NODES = JA.Abrams_Patch(ACTIVE_NODES) # special patch for marital name changes
    
    FIN_NODES = ACTIVE_NODES + DORMANT_NODES

    Fin_Matched = PIPE_RePoint_Free_Match(Post_Court, FIN_NODES)
    
    JEL, SJID_MAP = PIPE_LABEL_AND_BUILD(Fin_Matched, FIN_NODES)

    # build the SJIDs into the DF
    Fin_Matched['SJID'] = Fin_Matched.Final_Pointer.map(SJID_MAP)
    Fin_Matched['SJID'].fillna("Inconclusive", inplace=True)

    return Fin_Matched, JEL


def NEW_DISAMBIGUATION_FREEMATCH_PIPELINE(paths, all_dockets, newest_fjc, fjc_active, oldJEL, Post_Court):
    """Freematch pipeline that swaps for the original freematch pipeline when a new, updated JEL is being generated. This pipeline accounts for previously identified entities and previously tagged dockets.

    Args:
        paths (dict): dictionary of various filepaths for SEL files, JEL files, and FJC files
        all_dockets (pandas.DataFrame): unique files table default loaded from settings file
        newest_fjc (pandas.DataFrame): DF of new FJC Article III judges only
        fjc_active (pandas.DataFrame): DF of full FJC codebook as we know it at time of script running (1700s-present)
        oldJEL (pandas.DataFrame): DF of the old JEL loaded in from JSONL file
        Post_Court (pandas.DataFrame): DF of the new dockets post ucid and court disambiguation. These rows should be ready for free matching disambiguation

    Returns:
        pandas.DataFrame, pandas.DataFrame, pandas.DataFrame: 3 different DFs after disambiguation. readySEL is any updated old SEL file rows, RPDF is the repointed DF of the new dockets only (pointed to mapped disambiguated entities), and updated_JEL is the newest iteration of the JEL.
    """
    # prep for freematching by loading in the prior SEL dockets we want to pool into disambiguation
    readySEL = PIPE_PREPARE_UPDATE_FREEMATCH(all_dockets, paths, ['2016', '2019'])
    
    # build the object nodes we will use in disambiguation
    Free_Nodes = PIPE_UPDATE_NODE_BUILDER_FREEMATCH(Post_Court, readySEL, newest_fjc, fjc_active, oldJEL)

    # cull the pool before beginning to remove FJC judges that are no longer active, or not likely to show up in our data (i.e. retired by 1995 if we're using 2015-2020 dockets)
    active, dormant  = PIPE_FIND_DORMANT_FJC(Free_Nodes)
    
    # full disambiguation routine
    ACTIVE_NODES = JA.PIPE_Free_Fuzzy(active)
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Plain')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Unified')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, False, 'Nicknames')
    ACTIVE_NODES = JA.PIPE_Free_Tokens_in_Tokens(ACTIVE_NODES, False, True, 'Nicknames')
    ACTIVE_NODES = JA.PIPE_Free_Vacuum(ACTIVE_NODES) # vacuum out the middle names and compare entities
    ACTIVE_NODES = JA.PIPE_Free_Token_Sort(ACTIVE_NODES) # compare tokens in flipped ortders
    ACTIVE_NODES = JA.PIPE_Free_Van_Sweep(ACTIVE_NODES) # special patch for germanic names
    ACTIVE_NODES = JA.PIPE_Free_Initialisms(ACTIVE_NODES) # special patch for initial heavy judges
    ACTIVE_NODES = JA.PIPE_Free_Single_Letters(ACTIVE_NODES) # special patch for judges that go by a single letter of one of their names
    ACTIVE_NODES = JA.Abrams_Patch(ACTIVE_NODES) # special patch for marital name changes

    # when wrapping up, make sure we include all of the dormant nodes again as we still want them included when rebuilding and expanding the JEL
    FIN_NODES = ACTIVE_NODES + dormant
    
    # for the new dockets, repoint any child entities to the correct parent entities
    RPDF = PIPE_Update_Final_Repoint(Post_Court, FIN_NODES)
    
    # tag the old ucids with new entity labels if applicable, build a new JEL
    readySEL, updated_JEL = PIPE_UPDATE_NEW_LABELS_AND_BUILD(readySEL, RPDF, oldJEL, FIN_NODES)
    
    return readySEL, RPDF, updated_JEL

def PIPE_FIND_DORMANT_FJC(nodes, time_bound = '1995-01-01'):
    """Helper function that filters out FJC entities from a disambiguation pool based on their latest termination date as an Article III judge

    Args:
        nodes (list): list of IntraMatch child objects that will be used in a disambiguation pool
        time_bound (str, optional): Latest time-bound to cutoff and rule an Article III judge as dormant with respect to the years of the dockets in disambiguation. Defaults to '1995-01-01'.

    Returns:
        list, list: returns 2 lists of IntraMatch child objects whose lengths and items sum to the length and items of the original nodes input list. The active list are entities to be used in disambiguation, dormant are those believed to be no longer judges at the time of the dockets filing in the disambiguation pool.
    """

    # can only reliably filter FJC nodes
    fjc_nodes = [n for n in nodes if n.is_FJC]
    
    # dormant if the latest termination was before the input time bound
    fjc_dormant = [n for n in fjc_nodes if n.Latest_Termination <= pd.to_datetime(time_bound).date()]
    # active is not dormant
    fjc_active = [n for n in fjc_nodes if n not in fjc_dormant]

    # any remaining node that wasnt an FJC entitiy
    non_fjc = [n for n in nodes if not n.is_FJC]

    # those nodes to consider in dismabiguation
    active = fjc_active + non_fjc
    # those we can consider dormant.
    dormant = fjc_dormant

    return active, dormant


def UCID_PIPE_Object_Builder(concatenated_df):
    """build dictionary of objects per UCID for first round of disambiguation

    Args:
        concatenated_df (pandas.DataFrame): Pre-SEL df

    Returns:
        dict: key = ucid, values = list of objects (UCIDMatch objs)
    """
    
    print("\nPipe: UCID Object Builder")

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


def COURT_PIPE_Object_Builder(Pre_SEL):
    """build dict of objects grouped by court for court matching and disambiguation

    Args:
        Pre_SEL (panda.DataFrame): DF that has already been through intra-ucid matching and disambiguation

    Returns:
        dict, dict: dictionaries of multi-token entity objects vs. single token entity objects, grouped (keyed) by court
    """
    
    print("\nPipe: Object Builder")
    # IntraMatch dataframe is built using entity, what it points to after ucid matching, the court, and ucids
    IM = Pre_SEL[['Cleaned_Entity','Points_To','court','ucid']]

    # group the df by the updated entity (points to) and determine how many unique ucids each of those entities is on
    GDF = IM.groupby(["Points_To","court"], as_index=False)['ucid'].nunique()
    
    print(">> Building Objects")
    # with the grouped data, build the courtmatch objects
    court_map = defaultdict(list)
    for point_starter, court, n_ucids in tqdm.tqdm([tuple(x) for x in GDF.to_numpy()]):
        court_map[court].append(JED_Classes.CourtMatch(point_starter, court, n_ucids))
    
    # split the objects into single token names vs. multitoken names per court
    court_map_single = {}
    court_map_long = {}
    for court, objects in court_map.items():
        court_map_single[court] = [o for o in objects if o.token_length==1 or len(o.tokens_wo_suff)==1]
        court_map_long[court] = [o for o in objects if o not in court_map_single[court]]

    print(">> Objects Built")
    
    return court_map_long, court_map_single


def UCID_PIPE_Build_Remapped_Lookup(new_map, toss_map, concatenated_df):
    """After intra_ucid matching, we need to rejoin and remap the entities to point to the correct "parents" on the original dataframe

    Args:
        new_map (dict): keys = ucid, values = lists of objects on the ucid, mapped to each other if they were disambiguated
        toss_map (dict): keys = ucid, values = lists of any objects that were thrown out because they were parties or counsels the spacy model inadvertently extracted
        concatenated_df (pandas.DataFrame): SEL-like rows

    Returns:
        pandas.DataFrame: Updated dataframe of rows for entity lookup that now have a "points to" column filled if an entity points to another one
    """

    print("\nPipe: Building Remapped DataFrame")
    print(">>Applying remappings")
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

    print(">>Tossing junk entities")
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
    Post_UCID = concatenated_df.merge(REM, how='left', on = ['ucid','Cleaned_Entity'])

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

def COURT_PIPE_Build_Remapped_Lookup(court_map, Post_UCID):
    """after court matching, remap the entities in advance of free matching

    Args:
        court_map (dict): keys = court, values = list of entity objects in that court
        Post_UCID (pandas.DataFrame): one row per entity indexed location on a ucid

    Returns:
        pandas.DataFrame: one row per entity indexed location on a ucid with the remapped entities pointing to their parent entities after court matching
    """

    # we will loop through a courts objects and for any that are now ineligible (they were mapped to another)
    # we will create a row of data to track and update their data
    remapped=[]
    for court, objs in court_map.items():
        swapped = [o for o in objs if not o.eligible]
        # they started out pointing to an entity after ucid matching and now there is an updated pointing to
        for each in swapped:
            remapped.append(
                    {'court':court,
                    'Points_To':each.name,
                    'Updated_Points_To': each.POINTS_TO}
                )

    # make as df                
    REM = pd.DataFrame(remapped)

    print(">> Merging onto Larger DF")
    RePointed = Post_UCID.merge(REM, how='left', on = ['court','Points_To'])

    # if the entity did not have a new pointer, it is still eligible to be mapped to so fill the repointing with itself
    RePointed["Updated_Points_To"].fillna(RePointed.Points_To, inplace=True)

    # again determine in a court how many times an entity was prefaced with judgey like terms
    RePointed['Has_Pref'] = RePointed.Prefix_Categories != "No_Keywords"
    Blanks = RePointed.groupby(['Updated_Points_To','court'], as_index = False).sum('Has_Pref')
    Blanks = Blanks[['Updated_Points_To','court','Has_Pref']]

    # determine number of unique ucids
    Unique_Ucid_Counts = RePointed[['ucid','Cleaned_Entity','Points_To','Updated_Points_To','court']].groupby(
        ['Updated_Points_To','court'], as_index=False)['ucid'].nunique()
    # merge the blank counts and ucid counts into one frame
    Court_DF = Unique_Ucid_Counts.merge(Blanks, how='left', on = ['Updated_Points_To','court'])

    # special filter, we need to remove random single token entities that are just initials
    # we can only confidently do this on triple consonants since vowels could mean it's a real name like Lee
    letters = 'abcdefghijklmnopqrstuvwxyz'
    vowels = 'aeiouy'
    consonants = ''.join(l for l in letters if l not in vowels)
    consearch = re.compile(fr'^[{consonants}]+$', flags=re.I)
    conmap = {}
    # if it's a triple consonant name, we will ignore it
    for each in Court_DF.Updated_Points_To.unique():
        if consearch.search(each) and len(each)<=4:
            flag = True
        else:
            flag = False
        conmap[each] = flag
    
    # map the check for consonants
    Court_DF['Ignore'] = Court_DF.Updated_Points_To.map(conmap)

    # drop out entities with low frequency, single token, and no prefaced judgey text
    Court_DF.loc[(Court_DF.ucid<=3)&
            (Court_DF.Updated_Points_To.apply(lambda x: len(x.split())==1))&
            (Court_DF.Has_Pref==0), 'Ignore'] = True
    
    # merge together onto the SEL like df
    FinPrep = RePointed.merge(
        Court_DF[['Updated_Points_To', 'court', 'Ignore']], how = 'left', on = ['Updated_Points_To', 'court'])

    # drop out the bad flagged rows
    FinPrep = FinPrep[~FinPrep.Ignore]
    
    return FinPrep


def PIPE_NODE_BUILDER(Post_Court, fjc_active):
    """Build pool of entity nodes that will be used in free-matching disambiguation

    Args:
        Post_Court (pandas.DataFrame): SEL-like df with one row per entity indexed location on a ucid, after court-level disambiguation
        fjc_active (pandas.DataFrame): FJC demographics dataframe in custom long form

    Returns:
        list : list of node objects to be used for final round of pooled disambiguation
    """

    print("\nPipe: Building Free Match Nodes")
    # get unique ucid counts for each entity grouped by court
    nodes = Post_Court.groupby(['Updated_Points_To', 'court'], 
                           as_index=False)['ucid'].nunique().sort_values('Updated_Points_To')
    
    # defaults for the FJC data
    
    nid = {}
    fullnames = {}
    simple_names = {}
    earliest_commission = {}
    latest_termination = {}

    # build FJC data 
    for index, row in fjc_active.iterrows():
        key = row['nid']['']
        earliest_commission[key] = row['Commission Date']['min']
        latest_termination[key] = row['Termination Date']['max']
        nid[key] = key
        fullnames[key] = row['FullName']['']
        simple_names[key] = row['Simplified Name']['']

    # aggregations on the FJC data

    FJC_Nodes = []
    for each in fjc_active['nid'].unique():
        fjc_info = {
            "Full_Name":fullnames[each],
            "NID":nid[each],
            "Earliest_Commission":earliest_commission[each],
            "Latest_Termination": latest_termination[each]
        }
        simple_name = simple_names[each]

        FJC_Nodes.append(JED_Classes.FreeMatch(simple_name,0,[],fjc_info))

    # begin grouping entities together by exact name matches now
    # court ucid counts can be summed as ucids are mutually exclusive across courts
    names = {}
    for name, court, ucids in [tuple(x) for x in nodes.to_numpy()]:
        if name not in names:
            names[name] = {'courts':[court], 'N_ucids':ucids}
        else:
            names[name]['courts'].append(court)
            names[name]['N_ucids']+=ucids

    # now for every name, if the name is multi-tokened, create a node object for matching
    PC_Nodes = []
    for name, values in names.items():
        if len(name.split())==1:
            continue
        PC_Nodes.append(JED_Classes.FreeMatch(name, values['N_ucids'],values['courts']))

    # finally group the FJC nodes with the docket/extraction nodes
    Free_Nodes = PC_Nodes+ FJC_Nodes

    return Free_Nodes


def PIPE_RePoint_Free_Match(Post_Court, NODES):
    """After the final round of pooled disambiguation, update entities to point to final mapped parent entities

    Args:
        Post_Court (pandas.DataFrame): one row per entity indexed location on a ucid, pre-SEL df
        NODES (list): list of entity objects that have gone through disambiguation

    Returns:
        pandas.DataFrame: remapped column added post disambiguation
    """
    print("\nPipe: Repointing entities after free match")
    # again, if the entity was ruled ineligible during free match that means it was mapped onto another entity
    # we will make df rows of these remappings to merge onto the prior dataframe
    remapped=[]
    for obj in [o for o in NODES if not o.eligible]:
        remapped.append(
                {'Updated_Points_To':obj.name,
                'Final_Pointer': obj.POINTS_TO,
                'is_FJC':obj.is_FJC,
                'NID':obj.NID}
            )
    
    REM = pd.DataFrame(remapped)
    
    print(">> Merging onto Larger DF")
    RePointed = Post_Court.merge(REM, how='left', on = ['Updated_Points_To'])

    # if it does not point to another entity, it is the parent entity, fill with itself
    RePointed["Final_Pointer"].fillna(RePointed.Updated_Points_To, inplace=True)
    # if it was not an FJC node, this would be null
    RePointed["is_FJC"].fillna(False, inplace=True)

    # header data's prefix categories would be null, at this point we label them as Nondescript
    RePointed.loc[
        RePointed.Prefix_Categories.isin(['assigned_judge', 'referred_judges']), 'Prefix_Categories'] = 'Nondescript_Judge'
    
    return RePointed

def PIPE_LABEL_AND_BUILD(Fin_Match, NODES):
    """ After all disambiguation, build the final JEL and run the labelling algorithm over the entities

    Args:
        Fin_Match (pandas.DataFrame): pre-sel df row with one row per entity indexed location on a ucid
        NODES (list): list of disambiguated nodes

    Returns:
        pandas.DataFrame, dict: JEL dataframe of defined judges, lookup map of the SJID and node names
    """
    print("\nPipe: Generating Labels and SJIDs")
    # get a final count of unique ucids per entity using the parent entity (final pointer) labels
    temp = Fin_Match.groupby(['Final_Pointer', 'Prefix_Categories'], as_index=False)['ucid'].nunique('ucid')

    # for each parent entity, determine the pretext category counts for judgey like terms
    Prefs = temp.set_index(['Final_Pointer','Prefix_Categories']).unstack('Prefix_Categories')
    # fill nulls with 0
    Prefs.fillna(0,inplace=True)
    Prefs.reset_index(inplace=True)

    # get unique ucid counts for header appearances
    Header_Counts = Fin_Match[Fin_Match.docket_source!='line_entry'].groupby("Final_Pointer", as_index=False)['ucid'].nunique()
    Header_Counts.columns = ['Final_Pointer','Head_UCIDs']
    # get toal unique ucid counts
    Total_Counts = Fin_Match.groupby("Final_Pointer", as_index=False)['ucid'].nunique()
    Total_Counts.columns = ['Final_Pointer','Total_UCIDs']

    # create the maps from entities to counts
    TC = {i:j for i,j in zip(Total_Counts.Final_Pointer.values, Total_Counts.Total_UCIDs.values)}
    HC = {i:j for i,j in zip(Header_Counts.Final_Pointer.values, Header_Counts.Head_UCIDs.values)}

    preffy_dict = {}
    # pref columns[1:] are all of the mutually exclusive labels
    # the column is a multi-index, so the label is col[1] 
    collabs = [col[1] for col in Prefs.columns[1:]]
    for each in [tuple(x) for x in Prefs.to_numpy()]:
        # for each entity with pretext counts
        # dict key is entity, value is a dict of columns + value where col is the actual column label
        preffy_dict[each[0]] = {col:val for col,val in zip(collabs, each[1:])}

    # create the Algorithmic Mapping objects from the disambiguated nodes
    AMS = []
    # for every eligible parent entity
    for o in [o for o in NODES if o.eligible]:
        # defaults are zeros
        pref = {}
        hucid = 0
        tucid = 0

        if o.name in preffy_dict:
            pref = preffy_dict[o.name]
        if o.name in HC:
            hucid = HC[o.name]
        if o.name in TC:
            tucid = TC[o.name]

        if o.is_FJC:
            FJC_Info = {
                "Full_Name": o.Full_Name,
                "NID": o.NID,
                "Earliest_Commission": o.Earliest_Commission,
                "Latest_Termination": o.Latest_Termination}
        else:
            FJC_Info = {}

        # make the object using our default information
        AMS.append(JED_Classes.Algorithmic_Mapping(
            o.name, o.is_FJC, FJC_Info, pref, hucid, tucid
            ))
    
    # instant rejection for entities with less than 3 unique ucids is not fjc and is a single token name
    bads = [o for o in AMS if (o.Tot_UCIDs<=3 and not o.is_FJC and len(o.name.split())==1) or (len(o.name.split())==1)]
    # any single token name leftover is also tossed
    bads += [o for o in AMS if o not in bads and len(o.tokens_wo_suff[-1])==1]
    # good nodes are those that arent bad
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
        SJID_MAP[each.name] = each.SJID
    
    # Build the JEL using custom object function
    JEL = pd.DataFrame([o.JEL_row() for o in kept])
    
    return JEL, SJID_MAP


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

def PIPE_UPDATE_NEW_LABELS_AND_BUILD(readySEL, RPDF, oldJEL, FIN_NODES):
    """After a second disambiguation routine, update the entity labels and build the latest SEL and JEL data outputs

    Args:
        readySEL (pandas.DataFrame): DF of existing SEL labeled entities on dockets that will be remapped or updated
        RPDF (pandas.DataFrame): DF of newest SEL labeled entities on dockets that have not been put into files yet
        oldJEL (pandas.DataFrame): DF of existing JEL
        FIN_NODES (list): final list of nodes (IntraMatch super objects) after disambiguation

    Returns:
        [type]: [description]
    """
    ## PREPARE DOCKET PREFIX DATA
    ##############################
    readySEL.rename(columns = {'Updated_Points_To':'Final_Pointer'}, inplace = True)
    COUNT_DF = pd.concat(
        [
            RPDF[["docket_source","Final_Pointer","Prefix_Categories","ucid"]],
            readySEL[["docket_source","Final_Pointer","Prefix_Categories","ucid"]]
        ])

    # get a final count of unique ucids per entity using the parent entity (final pointer) labels
    temp = COUNT_DF.groupby(['Final_Pointer', 'Prefix_Categories'], as_index=False)['ucid'].nunique('ucid')

    # for each parent entity, determine the pretext category counts for judgey like terms
    Prefs = temp.set_index(['Final_Pointer','Prefix_Categories']).unstack('Prefix_Categories')
    # fill nulls with 0
    Prefs.fillna(0,inplace=True)
    Prefs.reset_index(inplace=True)

    # get unique ucid counts for header appearances
    Header_Counts = COUNT_DF[COUNT_DF.docket_source!='line_entry'].groupby("Final_Pointer", as_index=False)['ucid'].nunique()
    Header_Counts.columns = ['Final_Pointer','Head_UCIDs']
    # get toal unique ucid counts
    Total_Counts = COUNT_DF.groupby("Final_Pointer", as_index=False)['ucid'].nunique()
    Total_Counts.columns = ['Final_Pointer','Total_UCIDs']

    # create the maps from entities to counts
    TC = {i:j for i,j in zip(Total_Counts.Final_Pointer.values, Total_Counts.Total_UCIDs.values)}
    HC = {i:j for i,j in zip(Header_Counts.Final_Pointer.values, Header_Counts.Head_UCIDs.values)}

    preffy_dict = {}
    # pref columns[1:] are all of the mutually exclusive labels
    # the column is a multi-index, so the label is col[1] 
    collabs = [col[1] for col in Prefs.columns[1:]]
    for each in [tuple(x) for x in Prefs.to_numpy()]:
        # for each entity with pretext counts
        # dict key is entity, value is a dict of columns + value where col is the actual column label
        preffy_dict[each[0]] = {col:val for col,val in zip(collabs, each[1:])}


    ## Build Viable Nodes
    ##############################
    # create the Algorithmic Mapping objects from the disambiguated nodes
    AMS = []
    # for every eligible parent entity
    for o in [o for o in FIN_NODES if o.eligible]:
        # defaults are zeros
        pref = {}
        hucid = 0
        tucid = 0

        if o.name in preffy_dict:
            pref = preffy_dict[o.name]
        if o.name in HC:
            hucid = HC[o.name]
        if o.name in TC:
            tucid = TC[o.name]

        if o.is_FJC:
            FJC_Info = {
                "Full_Name": o.Full_Name,
                "NID": o.NID,
                "Earliest_Commission": o.Earliest_Commission,
                "Latest_Termination": o.Latest_Termination}
        else:
            FJC_Info = {}

        # make the object using our default information
        AMS.append(JED_Classes.Algorithmic_Mapping(
            o.name, o.is_FJC, FJC_Info, pref, hucid, tucid, o.SJID
            ))

    # instant rejection for entities with less than 3 unique ucids is not fjc and is a single token name
    bads = [o for o in AMS if (o.Tot_UCIDs<=3 and not o.is_FJC and len(o.name.split())==1) or (len(o.name.split())==1)]
    # any single token name leftover is also tossed
    bads += [o for o in AMS if o not in bads and len(o.tokens_wo_suff[-1])==1]
    # good nodes are those that arent bad
    goods = [o for o in AMS if o not in bads]


    ## Label Nodes
    ##############################
    # for every good node, run the labelling algorithm
    for obj in goods:
        obj.Label_Algorithm()

    kept = [o for o in goods if 'deny' not in o.SCALES_Guess]
    rejected = [o for o in goods if 'deny' in o.SCALES_Guess]

    # for every kept node, we want a dataframe of its data
    guesses = pd.DataFrame([o.build_row(update=True) for o in kept])

    # separate into previously identified and new entities
    needs_SJID = [o for o in kept if not o.Prior_SJID]
    needs_Confirmation = [o for o in kept if o.Prior_SJID]

    # now generate the SJIDs for the newest entities

    # identify the next available SJID to assign
    next_SJID = max([int(sj.split("SJ")[1]) for sj in oldJEL.SJID.unique()])+1
    for obj in needs_SJID:
        idn = str(next_SJID).zfill(6)
        next_SJID+=1
        obj.set_SCALES_JID(f"SJ{idn}")

    # for the existing known entities, keep their original SJIDs
    for obj in needs_Confirmation:
        obj.set_SCALES_JID(obj.Prior_SJID)

    # mark the remaining entities as inconclusive
    for obj in rejected:
        obj.set_SCALES_JID("Inconclusive")

    # generate the pretty name attribute for final kept entities (will be placed in JEL data)
    for obj in kept:
        obj.prettify_name()

    ## Build Updated JEL
    ############################## 
    # Build the JEL using custom object function
    
    # new JEL df
    newJEL = pd.DataFrame([o.JEL_row() for o in kept])

    JEL_Data = {}
    # make sure we didn't miss any rows from the old one, using an SJID mapping to do so
    for index, row in oldJEL.iterrows():
        sjid = row["SJID"]
        JEL_Data[sjid] = dict(row)

    for index, row in newJEL[
        ['name', 'Presentable_Name', 'SJID', 'SCALES_Guess', 'Head_UCIDs','Tot_UCIDs', 'Full_Name', 'NID']].iterrows():

        sjid = row["SJID"]
        new = dict(row)
        new['SCALES_Judge_Label'] = new['SCALES_Guess']
        new.pop('SCALES_Guess')

        # for the updated guesses in the JEL, make sure we update the entity labels appropriately based on the sample we used. Usually this will just mean Magistrates are upgraded to FJC Judges
        if sjid in JEL_Data:
            old = JEL_Data[sjid]
            if old["SCALES_Judge_Label"] != new["SCALES_Judge_Label"]:
                if new["SCALES_Judge_Label"] == "FJC Judge":
                    JU.log_message(f'Updating: {old["name"]} \t|Prior: {old["SCALES_Judge_Label"]}\t|New: {new["SCALES_Judge_Label"]}')
                    JEL_Data[sjid] = new
                    JEL_Data[sjid] = {}
                else:
                    # save the old jel row
                    continue
            else:
                # save the old jel row
                continue
        else:
            JU.log_message(f"New Judge: {new['name']} \t|New: {new['SCALES_Judge_Label']}")
            JEL_Data[sjid] = new

    # final updated JEL
    Updated_JEL = pd.DataFrame(JEL_Data.values())
    
    # return the new JEL and SEL ready for remapping
    return readySEL, Updated_JEL

def PIPE_UPDATE_FINAL_CROSSCHECK(readySEL, RPDF):
    """Final algorithmic crosscheck of misc. entities that did not get tagged on a ucid when updating the JEL/SEL. This crosscheck compares to identified entities on the ucid to see if the unidentified name could reliably match to the identified one (or if this is a meaningless entity on the case)

    Args:
        readySEL (pandas.DataFrame): prior SEL rows of data that had inconclusive entities on their case -- these will be crosschecked for updating
        RPDF (pandas.DataFrame): newest docket SEL rows that may have inconclusive entities as well to be crosschecked

    Returns:
        pandas.DataFrame, pandas.DataFrame : the 2 dataframes of old existing SELs or new SEL rows that will need to be written into JSONL files now that they have been fully disambiguated and labelled
    """
    # identify any ucids that have inconclusive entities that could be crosschecked
    XCheck_Old_Ucids = readySEL[readySEL.SJID=="Inconclusive"].ucid.unique()
    XCheck_Old = readySEL[readySEL.ucid.isin(XCheck_Old_Ucids)]
    # same deal here
    XCheck_New_Ucids = RPDF[RPDF.SJID=="Inconclusive"].ucid.unique()
    XCheck_New = RPDF[RPDF.ucid.isin(XCheck_New_Ucids)]
    Waiting_New = RPDF[~RPDF.ucid.isin(XCheck_New_Ucids)]

    # the mapper frame is the quick subset used for the crosscheck. UCID, the original entity, the entity parent, and an SJID
    mapper_frame = pd.concat(
        [
            XCheck_Old[['ucid','Extracted_Entity','Final_Pointer','SJID']],
            XCheck_New[['ucid','Extracted_Entity','Final_Pointer','SJID']]
        ]
    )

    # dont need duplicates, make it faster to iterate through
    mapper_frame = mapper_frame.drop_duplicates()


    # I will be building a map by ucid of all entities marked as good (known judge) or "inconclusive"
    the_map = {}
    for ucid, cleaned_ent, final_ent, SJID in [tuple(x) for x in mapper_frame.to_numpy()]:
        has_sjid = True
        if SJID == 'Inconclusive':
            has_sjid = False

        if ucid not in the_map:
            if has_sjid:
                the_map[ucid] = {"Good": [(final_ent, cleaned_ent, SJID)],"Inconclusive": []}
            else:
                the_map[ucid] = {"Good": [],"Inconclusive": [(final_ent, cleaned_ent, SJID)]}
        else:
            if has_sjid:
                the_map[ucid]["Good"].append((final_ent, cleaned_ent, SJID))
            else:
                the_map[ucid]["Inconclusive"].append((final_ent, cleaned_ent, SJID))

    # the entities to review will be any entity list from a ucid that is marked as inconclusive
    review = {}
    for ucid, ents in the_map.items():
        if ents['Inconclusive']:
            # if the ucid had inconclusive entities, keep the set of good and inconclusive ones in the reviewer dict
            review[ucid] = {k:set(v) for k,v in ents.items()}

    # updater will track which inconclusive entities we were able to update and point towards an SJID
    updater = []
    for ucid, ents in tqdm.tqdm(review.items()):
        for each in ents['Inconclusive']:
            badname = each[0] # the entity we will compare
            original = each[1] # what the entity originally looked like
            m_count = [] # match counter
            for good in ents['Good']:
                goodname = good[0] # name of entity with sjid to compare
                g_original = good[1] # original form of the entity

                # if the pointer entities match or the original entities match, add it as a match
                if fuzz.partial_ratio(badname,goodname)>=90 or fuzz.partial_ratio(original,g_original)>=90:
                    m_count.append(good)

            # if there is only one good match from the JEL entities
            # and the matched entity is a substring of the known judge, match it
            # this effectively matches ambiguous single token names on a docket
            if len(m_count)==1 and len([i for i in ents['Good'] if badname in i[0]])==1:
                good = m_count[0]
                updater.append({"ucid": ucid,
                                "Final_Pointer": each[0],
                                "New_Point": good[0],
                                "New_SJID": good[2],
                                "Absorb": True
                               })
    # build the DF for the updated mappings
    RECAST = pd.DataFrame(updater)  

    # log what we updated
    for each in updater:
        JU.log_message(f"Final Crosscheck | {each['ucid']:25} |{each['Final_Pointer']:25} --> {each['New_Point']}")

    print("Completing final SEL merge")
    # merge them together
    FMDF = XCheck_New.merge(RECAST, how='left', on = ['ucid','Final_Pointer'])

    OMDF = XCheck_Old.merge(RECAST, how='left', on = ['ucid','Final_Pointer'])

    UPDATED_UCIDS = RECAST.ucid.unique()

    # any of the update entities are marked with "Absorb" meaning we will overwrite their SJID (previously inconclusive) and their Final Parent entity (final pointer)
    FMDF.loc[FMDF.Absorb==True, 'SJID'] = FMDF.New_SJID
    OMDF.loc[OMDF.Absorb==True, 'SJID'] = OMDF.New_SJID
    FMDF.loc[FMDF.Absorb==True, 'Final_Pointer'] = FMDF.New_Point
    OMDF.loc[OMDF.Absorb==True, 'Final_Pointer'] = OMDF.New_Point
    # drop misc columns created
    FMDF.drop(['New_Point','New_SJID','Absorb'], axis=1, inplace=True)
    OMDF.drop(['New_Point','New_SJID','Absorb','N_ucids'], axis=1, inplace=True)

    # create the final output of SEL rows we will be writing to files
    New_Dockets = pd.concat([FMDF, Waiting_New])

    # from the existing dockets (Old) we only care about rewriting files that saw changes (mapped previously inconclusive entities to now known SJIDs)
    Old_Dockets = OMDF[OMDF.ucid.isin(UPDATED_UCIDS)]
    
    
    return Old_Dockets, New_Dockets