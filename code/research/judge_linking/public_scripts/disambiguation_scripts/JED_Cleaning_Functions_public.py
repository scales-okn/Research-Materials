import re
import tqdm
from collections import defaultdict

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[3]))
from support import court_functions as cf

import pandas as pd

import JED_Globals_public as JG
import JED_Utilities_public as JU

def ingest_raw_entities(dir_path):
    """ Ingest the Spacy mined raw data files from a directory. We assume every file is meant to be loaded in the directory

    Args:
        dir_path (string): relative location of data directory

    Returns:
        pandas.DataFrame, pandas.DataFrame: unaltered dataframes of the ingested data
    """

    # specific data files we want to load (in case the directory gets jumbled or log files appear)
    # ner_extraction_paths = [path for path in os.listdir(dir_path) if '.csv' in path]
    ner_extraction_paths = Path(dir_path).glob('**/*')
    ner_extraction_paths = [p for p in ner_extraction_paths if p.suffix=='.csv']

    # read and concat files into pd DF
    raw_data = []
    heads_df= []
    for nerpath in ner_extraction_paths:
        file_end = nerpath.stem.split("_")[-1]
        temp = pd.read_csv(nerpath)
        if file_end == 'headers':
            heads_df.append(temp)
        else:
            raw_data.append(temp)

    raw_df = pd.concat(raw_data)
    heads_df = pd.concat(heads_df)

    # in case we accidentally parsed some jsons twice, drop duplicates
    raw_df.drop_duplicates(inplace=True)
    heads_df.drop_duplicates(inplace=True)

    JU.log_message(f"-{len(ner_extraction_paths)} total files ingested")
    JU.log_message(f"-{len(raw_df.court.unique())} total courts represented")


    return raw_df, heads_df


def ingest_the_fjc(fpath):
    """ Ingest the FJC demographics data from our SCALES csv annotation version of the codebook

    Args:
        fpath (string): FJC fpath from settings or csv

    Returns:
        pandas.DataFrame: long-form DF of the fjc judge data, with 1 row per judge (collapsed the wideform information)
    """

    # customized pattern to handle the FJC's styling of bracketing names that go by initials
    brack_breaker = re.compile(r'((?<=[a-zA-Z])|^)[\[][a-zA-Z]+[\]](?= [a-zA-Z]|$)',flags=re.I) 

    def _prune_brackets(text: str):
        """given a pandas row, prune brackets out of the name if they exist

        Args:
            row (object): pandas row passed with apply

        Returns:
            list: list of cleaned name(s)
        """
        # split on spaces, join as single spaced
        _splitty = lambda x: " ".join(x.split())

        outs = []
        # determine if the name has brackets
        M = brack_breaker.search(text)
        if M:
            # create two forms of the bracketless name
            # voided where the bracketed text is entirely removed
            voided = f"{text[0:M.start()]}{text[M.end():]}"
            # replaced where the brackets are removed
            replaced = text.replace('[','').replace(']','')
            # apply the splitters
            outs.append(_splitty(voided))
            outs.append(_splitty(replaced))

        else:
            # if no brackets, just apply the splitters
            outs = [_splitty(text)]
        
        outs = [clean_ground_truth_name(x) for x in outs]
        
        # return list of name(s)
        return outs

    judge_demographics = pd.read_csv(fpath)
    if 'FullName' not in judge_demographics.columns:
        print('[FJC File Alteration] Building FullName Column')
        FullName = judge_demographics.apply(lambda row: ' '.join([str(x) for x in row[['First Name', 'Middle Name', 'Last Name','Suffix',]] if not pd.isnull(x)]), axis=1)
        FullName = FullName.apply(lambda row: row.replace('   ', ' '))
        judge_demographics.insert(2, 'FullName', FullName)
        judge_demographics.to_csv(fpath, index=False)


    # these are the columns I want that are named as is
    judge_demographics_cols_keys = ['nid', 'jid', 'FullName', 'Last Name', 'First Name', 'Middle Name', 'Suffix']

    # these columns need nuance to extract --  structurally they appear as "column name (#)"
    judge_demographics_cols_cast = [
        'Court Type', 'Court Name', 'Appointment Title', 'Confirmation Date', 'Commission Date', 'Termination Date']

    # extract rows from the dataframe, but make it a long frame instead of wide using dictionaries
    new_rows = []
    for index, row in judge_demographics.iterrows():
        # grab the columns named correctly as is
        baseline = {key: row[key] for key in judge_demographics_cols_keys}
        # there are 7 duplications of the wide columns, grab them all, we can filter nulls later
        for i in range(1,7):
            # save the same "key" information next to the appointment numbered information
            this_it = baseline.copy()
            this_it['Appointment Number'] = i
            for key in judge_demographics_cols_cast:
                this_it[key] = row[f'{key} ({i})']
            new_rows.append(this_it)

    # make into DF
    fjc_expanded = pd.DataFrame(new_rows)
    # drop NA's
    # NAs exist because we ranged to 7, but not all judges had 6 appointments
    fjc_expanded = fjc_expanded[(~fjc_expanded['Court Name'].isna())].copy() 

    # sort by nid and appt
    fjc_expanded.sort_values(["nid","jid","Appointment Number"], inplace=True)

    # convert district court names to abbreviations
    name2abb = {c: cf.classify(c) for c in fjc_expanded['Court Name'].unique().tolist()}
    fjc_expanded['Court Name Abb'] = fjc_expanded['Court Name'].map(name2abb)

    # fill nulls for termination to today (not yet terminated); convert date cols to datetimes
    fjc_expanded['Termination Date'].fillna(pd.to_datetime('today').date(), inplace=True)
    fjc_expanded['Commission Date'] = fjc_expanded['Commission Date'].apply(lambda x: pd.to_datetime(x).date())
    fjc_expanded['Termination Date'] = fjc_expanded['Termination Date'].apply(lambda x: pd.to_datetime(x).date())

    fjc_expanded['Simplified Name'] = fjc_expanded.FullName

    # fill NAs so they can be filtered if necessary
    # we assume a missing commission date is the same year as termination (?)
    fjc_expanded["Commission Date"].fillna(fjc_expanded["Termination Date"].apply(lambda x: pd.to_datetime(f"{x.year}-01-01").date()), inplace=True)

    fjc_active = fjc_expanded

    nid_courts = defaultdict(list)
    for nid, court in fjc_active[['nid','Court Name Abb']].to_numpy():
        # nones will be circuits, appeals, scotus, etc.
        if court:
            nid_courts[nid].append(court)
    nid_courts = {k:list(set(v)) for k,v in nid_courts.items()}

    # the output frame should be a cleaned name, original name, nid, earliest commission date and latest known termination date
    fjc_judges = fjc_active.groupby(["Simplified Name","FullName","nid"], as_index=False).agg({'Commission Date': ['min'], 'Termination Date': 'max'})

    fjc_judges['Name_Forms'] = fjc_judges["Simplified Name"].apply(lambda x: _prune_brackets(x))
    fjc_judges['Courts'] = fjc_judges['nid'].map(nid_courts)
    
    return fjc_judges

def ingest_ba_mag(fpath_judges: str, fpath_positions: str):
    """load the bankrupcty and magistrate judge dataset

    Args:
        fpath_judges (pathlib.Path or str): path to file for judges baseline sheet
        fpath_positions (pathlib.Path or str): path to file for judges position history

    Returns:
        list: list of magistrate and bankrupcty names
    """
    
    def _name_fusion(row):
        """assemble a full name using a row of the data

        Args:
            row (object): pandas row of data from the BA/MAG set

        Returns:
            str: assembled full name of a judge
        """
        # modelled here in case column names ever change
        nameset = {
            "FN": row['NAME_FIRST'],
            "MN": row['NAME_MIDDLE'],
            "LN": row['NAME_LAST'],
            "SUFF": row['NAME_SUFFIX']
        }

        # fill NAs as empty strings
        for k,v in nameset.items():
            if pd.isna(v):
                nameset[k] = ''
        # make a full name
        creation = f"{nameset['FN']} {nameset['MN']} {nameset['LN']} {nameset['SUFF']}"
        # return as the joined split to alleviate spacing oddities when a name part is missing
        return " ".join(creation.split())
    
    def _label_appointment(app: str):
        """Using the datasets ID, label the judge type using inference on the ID

        Args:
            app (str): appointment ID

        Returns:
            str: Bankruptcy, Magistrate, or None
        """
        if 'bnk' in app:
            return "Bankruptcy_Judge"
        elif 'mag' in app:
            return 'Magistrate_Judge'
        else:
            return None
    
    # load the data
    BK_MAG = pd.read_csv(fpath_judges)
    # make full names
    BK_MAG['_full_name'] = BK_MAG.apply(_name_fusion, axis=1)
    # tag appointment type
    BK_MAG['_tag'] = BK_MAG.JUDGE_ID.apply(_label_appointment)
    # create cleaned name forms
    BK_MAG['_cleaned_name'] = BK_MAG._full_name.apply(lambda x: clean_ground_truth_name(x))

    bm_map = defaultdict(list)
    for bmid, clean_name in BK_MAG[["JUDGE_ID","_cleaned_name"]].to_numpy():
        bm_map[clean_name].append(bmid)

    bm_map = {k:"/".join(values) for k,values in bm_map.items()}
    BK_MAG['__pseudo_JID'] = BK_MAG._cleaned_name.map(bm_map)
    
    ### LOAD POSITIONS
    pos_df = pd.read_csv(fpath_positions)
    toss_courts = ['county','tax','municipal','city','appeals','circuit', 'compensation', 'superior']

    # prune to the particular judge roles we care about
    subset = pos_df[
        (pos_df.TITLE.apply(lambda x: 'judge' in str(x).lower())) &
        (pos_df.INSTITUTION.apply(lambda x: 'court' in str(x).lower())) &
        (pos_df.INSTITUTION.apply(lambda x: not any(i in str(x).lower() for i in toss_courts))) &
        (pos_df.INSTITUTION.apply(lambda x: str(x)[0:4].lower()) == 'u.s.')
    ].copy()

    subset['_court_abbrv'] = subset.INSTITUTION.apply(lambda x: cf.classify(x))

    jid_courts = defaultdict(list)
    for jid, court in subset[['JUDGE_ID','_court_abbrv']].to_numpy():
        jid_courts[jid].append(court)
    jid_courts = {k:list(set(v)) for k,v in jid_courts.items()}

    BK_MAG['_courts'] = BK_MAG.JUDGE_ID.map(jid_courts)

    ELIGIBLE = BK_MAG[~BK_MAG._courts.isna()]

    return ELIGIBLE[['JUDGE_ID', '_full_name','_cleaned_name','_tag','_courts','__pseudo_JID']]


def clean_ground_truth_name(testname: str):
    """ given an FJC entity name, clean it

    Args:
        testname (str): the FullName column values from the fjc codebook

    Returns:
        str: cleaned, lower-cased version of the fjc fullname
    """
    # order matters in execution (i.e. assuming numbers exist until numbers are stripped out)
    testname = testname.translate(JG.accent_repl) # replace accented letters in case clerks did not use accents in entry
    
    testname = re.sub(r'(\'s[\s\b$]|\\\'s[\s\b$])', r' ', testname,flags=re.I) # replace as blanks possessive s
    testname = re.sub(r'(\\xc2|\\xa71)', r'', testname,flags=re.I) # replace as blanks
    testname = re.sub(r'(\\xc3|\\xa1)', r'a', testname,flags=re.I) # replace as a
    testname = re.sub(r'\\\'','\'',testname) # make these normal apostrophes
    
    testname = re.sub(r'[0-9]',r'', testname) # no numbers please
    testname = re.sub(r'(!|"|#|%|&|\*|\+|,|/|=|\?|@|\^|_|`|~|\$|\||\\)', r' ',testname) # dump meaningless punctuation
    testname = re.sub(r'[.](?=[^\s])', r' ',testname) # if a name is initial.initial, make that period a space
    testname = re.sub(r'[.](?=[\s])', r'',testname) # if a name has initial.space just strip the period
    
    # example: O' Brien or O 'Brien --> O'Brien
    testname = re.sub(r'[\'](?=[\s])|(?<=[\s])[\']', r'', testname) # if apostrophe space or space apostrophe, remove the space

    testname = re.sub(r'(-$|^-|\'$|^\')','',testname) # beginning or end string  hyphens or apostrophes

    # Pam Beesly- Halpert or Pam Beesly -Halpert --> Pam Beesly-Halpert
    testname = re.sub(r'(?<=[^\s]) [-](?=[^\s])|(?<=[^\s])[-] (?=[^\s])', '-',testname) #hyphen collapse
    
    # FJC does this whack thing for judges that go by an initial
    # C[hristian] Rozolis --> Christian Rozolis
    testname = re.sub(r'((?<=[a-zA-Z])|^)[\[](?=[a-zA-Z]+)',r'',testname) # front bracket
    testname = re.sub(r'(?<=[a-zA-Z])[\]](?=[\s]+)',r'',testname) # back bracket
    
    # any remaining periods go bye bye
    testname = testname.replace('.','')
    testname = testname.replace("'",'') # remove apostrophes now
    
    # default split will remove odd double spacing, then rejoin
    return ' '.join(testname.lower().split())

def ingest_header_entities(fpath: str):
    """ingest the extracted parties or counsels data. 

    Args:
        fpath (str): relative filepath in string form

    Returns:
        pandas.DataFrame: df of case entities from a specific header section. Expected Columns: ['ucid','court','cid','year','filing_date','Role','Entity']
    """
    
    in_df = pd.read_csv(fpath)

    return in_df

def reshuffle_exception_entities(RDF: pd.DataFrame, JEL = []):
    """Custom function that matches the SPACY V3 Extractor. The Model has english language bias and fails in a few notable ways
    - believes latinx names end after one token (Judge Ignacio Torres -- model thinks "Judge Ignacio")
    - Same concept for many non-english names
    - Fails with multi-syllable last names like Van Der Kloet (model thinks "Judge Van Der")
    - English word names as last names (judge keys, judge wood, judge settle, etc.)
    This function searches through the extracted entities, then locates the tokens in the surrounding neighborhood and confirms the model didn't miss any expected
    parts of a name. For example if the extracted entity is "Judge Alexander" it looks to see if "Mackinnon" is the next token after the entity. If so, it captures that token
    as part of the entity. This function will evolve given model updates and new names as well.

    Args:
        RDF (pandas.DataFrame): large dataframe of entry row extractions to be checked for neighborhood searches. Need ucid, docket_index, full_span_start, Ent_span_start to be used as indexes
        JEL (pandas.DataFrame, optional): if a prior run has been completed from disambiguation we can leverage those entities from the JEL
    Returns:
        pandas.DataFrame: same length as input dataframe, but with reshuffled entities (included tokens from neighborhoods) if the extracted entity qualified in the checks
    """
    from flashtext.keyword import KeywordProcessor

    def _reshuffle_subset(subset_df: pd.DataFrame, CPatts: dict, voids: dict):
        """This be the meat and potatoes. Algorithmic reshuffling function that takes an extracted entity
        and evaluates the text neighborhood surrounding it to determine if the extraction window should be expanded
        based on other extracted entities.

        Args:
            subset_df (pd.DataFrame): entity extractions data frame, split by court
            CPatts (dict): dictionary of name specific preceding and following patterns for entities
            voids (dict): dictionary of voids to consider

        Returns:
            pandas.DataFrame: the reshuffled dataset with newly extracted entities if their neighborhoods returned a match
        """
        remapped = []
        for index, row in subset_df.iterrows():
            # need keys to map the remapped entities to the original row (this is only done on the entries and not the headers df)
            # the entries df is join safe because docket index is guaranteed to be non-null and the entity span starts cannot overlap from the spacy model
            lookup = {'ucid':row['ucid'],
                    'docket_index':row['docket_index'],
                    'full_span_start': row['full_span_start'],
                    'Ent_span_start': row['Ent_span_start']}

            # every type of check will need the same base variables
            # original text
            OT = str(row['original_text'])
            # span locations
            FSS = row['full_span_start']
            ESS = row['Ent_span_start']
            ESE = row['Ent_span_end']   

            # default new neighborhoods are just the originals
            new_locs = {
                '_triggered': False,
                'New_span_start':ESS,
                'New_span_end': ESE,
                'New_pre_5': row['extracted_pre_5'],
                'New_Entity': row['extracted_entity'],
                'New_post_5': row['extracted_post_5']
                }

            CE = row["CLEANSED_ENTITY"]
            not_voided = True

            # if the entity qualifies as one that could be voided (like Will) determine if it voidable
            if CE in voids:
                VPATT = voids[CE]
                posttext = OT[ESE-FSS:]
                if VPATT.search(f"{CE} {posttext}"):
                    # this is a voidable catch
                    new_locs['_triggered'] = True
                    new_locs['New_Entity'] = ''
                    not_voided = False

            PRE = None
            POST = None
            if CE in CPatts:
                PRE = CPatts[CE]['PRE']
                POST = CPatts[CE]['POST']                          

            if PRE and not_voided:
                pretext = str(row['extracted_pre_5'])
                attempt = PRE.extract_keywords(pretext, span_info=True)
                if attempt:
                    lpt = len(pretext)
                    # if the match ends at the last 3 characters of the pre-string
                    probables = [a for a in attempt if a[2] in [lpt, lpt-1, lpt-2]]
                    if probables:
                        # find the closest one to the end of the string
                        m = sorted(probables, key=lambda tup: tup[2], reverse=True)[0]
                        m_start = m[1]

                        # if it is a match, then we need to reshuffle the entity using what matched from the pretext
                        # match start + full span start gives the new entity start w.r.t to the full docket text
                        new_ss = m_start+FSS
                        new_ee = OT[m_start:ESE-FSS] # string of the new entity
                        new_pre5 = OT[0:m_start] # new pretext
                        # remapping data
                        new_locs['_triggered'] = True
                        new_locs['New_span_start'] = new_ss
                        new_locs['New_pre_5'] =  new_pre5
                        new_locs['New_Entity'] =  new_ee

            if POST and not_voided:
                # this does not get changes in new_locs in above control block, so no need to check
                post_text = str(row['extracted_post_5'])
                attempt = POST.extract_keywords(post_text, span_info=True)
                if attempt:
                    # if the match indexes at the first 3 characters of the posttext
                    probables = [a for a in attempt if a[1] in [0,1,2]]
                    # if pattern matched, respan using post tokens
                    if probables:

                        # this is likley going to find the longest match if eligible
                        m = sorted(probables, key=lambda tup: tup[2], reverse=True)[0]
                        m_end = m[2] # where the match ends in the post string

                        # if we already ran pre-string adjustments
                        if new_locs['_triggered']:
                            ESS = new_locs['New_span_start']

                        new_se = int(ESE + m_end+1)

                        new_ee = OT[ESS-FSS:new_se-FSS]
                        new_post5 = OT[new_se-FSS:]
                        new_locs['_triggered'] = True
                        new_locs['New_span_end'] =  new_se
                        new_locs['New_Entity'] =  new_ee
                        new_locs['New_post_5'] =  new_post5                   
            # remap it
            appy =  {
                **lookup,
                **new_locs
            }
            remapped.append(appy)    

        REM = pd.DataFrame(remapped)
        if REM.empty:
            return REM
        CHANGED = REM[REM._triggered==True]
        
        return CHANGED

    def _pattern_constructor(name: str, alts: list):
        """Given a name and the alternative variants that include this name, 
        return a tuple of Trie processors that are built to search forwards
        and backwards for related name tokens that this name could be missing 

        Args:
            name (str): original extracted name
            alts (list): alternative name variants that wholly encapsulate the name
        Returns:
            KeywordProcessor, KeyWordProcessor: returns Nones or Keyword Processor Tries for pattern detection
        """
        
        def _expander(name: str, the_search: list, flag: str = 'preceding'):
            """
            Expand the search windows for the entities text

            Args:
                name (str): the originally extracted named entity
                the_search (list): the list of possible preceding tokens that could preface this token or follow
                flag (str, optional): is this for the preceding or following neighborhood. Defaults to 'preceding'.

            Returns:
                list: cleaned list of stringss that tries should be built from to search before the entity
            """

            strip_spaces = lambda x: x.strip()

            # we are about to expand our search neighborhood using punctuation, spacing, etc.
            # for example we have an original entity "Smith" and a preceding window saying "John Robert"
            # our window will become "search for any of" [john robert, j.r., jr, etc.]
            EXP = []
            for each in the_search:
                space_stripped =  strip_spaces(each)
                tokens = space_stripped.split()

                if not space_stripped:
                    continue

                # try adding periods after single letters
                perio = " ".join([f"{t}." if len(t)==1 else t for t in tokens])
                EXP.append(perio)
                
                fuze_periods = re.sub(r'(?<=\s[a-z]\.)\s(?=[a-z]\.)','',perio,flags=re.I)
                EXP.append(fuze_periods)
                
                fuze_singulars = re.sub(r'(?<=\s[a-z])\s(?=[a-z]\s)','',space_stripped,flags=re.I)
                EXP.append(fuze_singulars)

                if flag == 'preceding':
                    EXP.append(space_stripped)
                    # remove middle inits
                    if len(tokens)>1:
                        trial = f"{tokens[0]} {' '.join(t for t in tokens[1:] if len(t)>1)}"
                        if trial:
                            EXP.append(trial)

                if flag == 'following':
                    # void bad William matches asap
                    if name == 'will' and each[0:4]=='iam ':
                        continue

                    EXP.append(space_stripped)

                    # remove a middle initial if applicable
                    if len(tokens)>1:
                        trial = f"{' '.join(t for t in tokens[0:-1] if len(t)>1)} {tokens[-1]} "
                        if trial:
                            EXP.append(trial)

                    # if the first token is a suffix, add the comma and period
                    if tokens[0] in JG.suffixes_titles:
                        EXP.append(f", {space_stripped}")
                        EXP.append(f", {space_stripped}.")
                    
                    # if the last token is a suffix, add in a comma and period
                    if tokens[-1] in JG.suffixes_titles:
                        EXP.append(f"{space_stripped}.")
                        EXP.append(f"{' '.join(tokens[0:-1])}, {tokens[-1]}")
                        EXP.append(f"{' '.join(tokens[0:-1])}, {tokens[-1]}.")


            with_commas = []
            if 'preceding' and name in JG.suffixes_titles:
                with_commas = [f"{each}," for each in EXP]

            clean_out = list(set([strip_spaces(n) for n in with_commas+EXP]))
            return clean_out
        
        nl = len(name)
        nls = len(name.split())

        build_preceding_search = []
        build_following_search = []
        for alt in alts:
            # if the alternative variant ends with this extraction
            if alt[-nl:] == name:
                # add the preceding string portion as eligible preceding text
                build_preceding_search.append(alt[0:-nl])
            # if the alternative variant starts with this extraction
            elif alt[0:nl] == name:
                # the following text is remaining
                follows = alt[nl:]            
                # these are all bunk, bypass
                if follows.strip() in ['as','by','from','to','presentence', 'standing']:
                    continue
                # if the name was possessive or ended in s and the other option is a possessive apostrophe
                if name[-1] == 's' and follows[0:2] == "' ":
                    continue
                # if the alternative is just a tacked on possesive s, move on and ignore
                if follows[0:2] in ['s ']:
                    continue
                # any of these are also bad, goodbye
                if follows[0:3] in ['as ','by ','to ',"'s "]:
                    continue
                # hey if you made it here, you are a valid, eligible piece of text following the original extraction
                build_following_search.append(alt[nl:])
            # else conditions mean the extraction is in the middle of one of the name variations
            else:
                splat = alt.split()
                nsplat = name.split()
                # if the name is one token, and it is in the final token of the alternative name
                # we continue
                # this voids possessive matches like "thompson" being in "Dave Thompsons" and creating a weird
                # "look for Dave preceding and 'S' following"
                if nls <=2 and nsplat[-1] in splat[-1] and nsplat[-1]!=splat[-1]:
                    continue
                            
                ai = alt.index(name)
                pre = alt[0:ai]
                post = alt[ai+nl:]
                if pre[-1] ==' ':
                    build_preceding_search.append(pre)
                if post[0] == ' ':
                    build_following_search.append(post)
        
        BPS = _expander(name, build_preceding_search, 'preceding')
        BFS = _expander(name, build_following_search, 'following')
                
        BPTrie = None
        BFTrie = None
        if BPS:
            BPTrie = KeywordProcessor(case_sensitive=False)
            BPTrie.add_keywords_from_list(BPS)
        if BFS:
            BFTrie = KeywordProcessor(case_sensitive=False)
            BFTrie.add_keywords_from_list(BFS)
        
        return BPTrie, BFTrie
                
    def _pre_clean(text: str):
        """Custom cleaning function that standardizes extracted name strings for punctuation and casing

        Args:
            text (str): extracted entity string

        Returns:
            text: cleaned form of the entity
        """
        
        # if somehow only numbers were extracted, guarantee we have a string to work with for the rest of this function
        text = str(text)
        
        # remove leading periods
        m = re.search(r'^(\s*)(\.+)(\s*)', text)
        if m:
            text = text[m.end():]
        # remove beginning of string to or by or from, specifically followed by a space or bound
        m = re.search(r'^(\s*)(by|to|from)(\s+|\b)', text, flags=re.I)
        if m:
            text = text[m.end():]
        
        # leading and trailing .-\' and 's
        b = re.search(r'([\.\\\'\-]+)$|(\'s)$', text, flags=re.I)
        f = re.search(r'^([\.\\\'\-]+)', text, flags=re.I)
        if b:
            text = text[0:b.start()]
        if f:
            text = text[f.end():]
        
        # strip all periods and hyphens
        text = text.lower().replace('.','').replace('-',' ').replace(',','')
        
        # create uniform spacing
        text = " ".join(text.lower().split())
        
        # remove re and us. These proved to be weirdos that lingered and had been extracted
        if text in ['re','us','n/a','as','to','from','by'] or text=='':
            return None
        
        return text

    ###########################################
    ###########################################
    ### BEGINNING OF FUNCTION AFTER HELPERS ###
    ###########################################
    ###########################################
    
    # names that we include manual overrids for because the model always failed to grab them
    manual_overrides = {
        'gud':['joaquin ve manibusan jr', 'joaquin v e manibusan jr'],
        'insd':["joseph s van bokkelen"],
        'flmd': ["mark l van valkenburgh"],
        'kyed':["gregory f van tatenhove", "gregory f vantatenhove"],
        'txed': ["james van valkenberg"]
    }

    # known corpora of words that come after will that indicate Will is not a Proper Noun and is such is voidable as an entity
    voids = {'will': re.compile(r'will(\s|\b)+(address|adjust|adopt|appear|appoint|be |consider|continue|convene|coordinate|decide|defer|determine|either|enter|establish|extend|further|handle|have|hear |hold |issue|make|necessarily|not |preside|promptly|recommend|rely|remain|review|rule|save|schedule(d)?|set|sign|take|the|upon|update)', flags=re.I)}

    # on the input dataframe, map the original extracted entity to its cleaned name form
    # i.e. J.R. Smith and JR Smith both now map to jr smith
    cleansed_map = {n: _pre_clean(n) for n in RDF.extracted_entity.unique().tolist()}
    RDF['CLEANSED_ENTITY'] = RDF.extracted_entity.map(cleansed_map)

    # create a court level grouping of the cleaned entities
    court_ee = defaultdict(list)
    for court, cleansed in RDF[['court','CLEANSED_ENTITY']].drop_duplicates().to_numpy():
        if cleansed:
            court_ee[court].append(cleansed)
 
    # if we can leverage prior JEL entities, let's
    if len(JEL)>0:
        JEL_names = JEL.name.unique()
        JEL_Adds = [_pre_clean(n) for n in JEL_names]
        for court, names in court_ee.items():
            court_ee[court] = names
            court_ee[court] += JEL_Adds

    # the alternatives come from our manual overrides, add them in their respective courts
    ELIGIBLE_ALTS = []
    for court, names in manual_overrides.items():
        for n in names:
            cleansed = _pre_clean(n)
            court_ee[court].append(cleansed)
            ELIGIBLE_ALTS.append(cleansed)

    # after pre_cleaning, it's possible to reduce some of them as they are identical. Leverage set method
    court_ee = {k:set(v) for k,v in court_ee.items()}

    # for the cleaned entities, add all of them to the eligible alternatives if it appeared more than 25 times
    ELIGIBLE_ALTS+= [k for k,v in dict(RDF.CLEANSED_ENTITY.value_counts()).items() if v>25]

    # for a given court at a time
    COURT_PATTERNS = {}
    for COURT, NAMES in tqdm.tqdm(court_ee.items()):
        COURT_PATTERNS[COURT] = {}
        # for every name in the court
        for name in NAMES:
            # first barrier to passing criteria for a "valid entity"
            # cant be numbers and cannot be a single letter
            if re.search(r'^[\d]+$', name) or len(name)==1:
                continue
            # if the extracted entity was 2 tokens or less, let's double check the neighboring tokens in case we can expand the extraction
            if len(name.split())<=2:
                # alternative options are any names that fully encapsulate this name
                # for example: name = joaquin a, and an eligible alt is "joaquin a manibusan"
                alts = [i for i in ELIGIBLE_ALTS if name in i and name!=i and len(i.split())>=2]
                # make sure the algorithmically generated alts did not have judge or magistrate in them
                # otherwise we could accidentally open the window to honorifics
                alts = [a for a in alts if not any(pre in a for pre in ['judge','magistrate'])]
                if alts:
                    # determine all possible preceding and following tokens that should be searched for on this extraction
                    Preceding, Following = _pattern_constructor(name, alts)
                    if not Preceding and not Following:
                        continue
                    # if there were any, add them to the map
                    COURT_PATTERNS[COURT][name] = {'PRE':Preceding, 'POST': Following}
    
    all_RDFs = []
    # we will only cycle through entities we know we have a search neighborhood for
    # go court by court and make a df for each if any reshuflle
    for this_court, CPatts in tqdm.tqdm(COURT_PATTERNS.items()):
        search_eligible = list(CPatts.keys()) + list(voids.keys())
        this_subset = RDF[
            (RDF.CLEANSED_ENTITY.isin(search_eligible)) &
            (RDF.court==this_court)
        ]
        temp_rdf = _reshuffle_subset(this_subset, CPatts, voids)
        if temp_rdf.empty:
            continue
        all_RDFs.append(temp_rdf)

    # if we actually found remappings, go through with all of the merge processing now
    if all_RDFs:
        resh = pd.concat(all_RDFs)
        NDF = RDF.merge(resh,
                    how='left',
                    on = ['ucid', 'docket_index', 'full_span_start', 'Ent_span_start'])
        if "_triggered" not in NDF.columns:
            NDF["_triggered"] = False
        # log discrepancies before we overwrite them
        JU.log_message(">>Token Neighborhood Search for Corrective Entity Extraction")
        for index, row in NDF[NDF._triggered==True].iterrows():
            ucid = str(row['ucid'])
            di = str(row['docket_index'])
            ot = str(row['original_text'])
            old_ent = str(row['extracted_entity'])
            new_ent = str(row['New_Entity'])
            JU.log_message(f"{ucid:25} | {di} | {old_ent:25} --> {new_ent:25} \t| FROM: {ot}")


        # if the entity got remapped, it would be flagged as _triggered
        # for those rows, overwrite the extracted entity information fields
        NDF.loc[NDF._triggered==True, 'Ent_span_start'] = NDF['New_span_start']
        NDF.loc[NDF._triggered==True, 'Ent_span_end'] = NDF['New_span_end']
        NDF.loc[NDF._triggered==True, 'extracted_pre_5'] = NDF['New_pre_5']
        NDF.loc[NDF._triggered==True, 'extracted_entity'] = NDF['New_Entity']
        NDF.loc[NDF._triggered==True, 'extracted_post_5'] = NDF['New_post_5']

        # drop my custom columns used in this function, wont need them anymore
        NDF.drop(
            ['New_span_start', 'New_span_end','New_pre_5','New_Entity','New_post_5',
            '_triggered','CLEANSED_ENTITY'], inplace = True, axis=1)

        return NDF
    else:
        # if no changes happened, return the oirginal dataframe
        return RDF

def eligibility(df: pd.DataFrame, colname_to_check: str='extracted_entity'):
    """eligibility function that runs through a dataframe and determines what rows to filter out for exclusion in disambiguation

    Args:
        df (pandas.DataFrame]): header or entry df with entity fields and flags
        colname_to_check (str, optional): Based on the df (header or entry) which entity column should be checked for eligibility. Defaults to 'extracted_entity'.
    Returns:
        pandas.DataFrame: df of eligible sel rows for disambiguation (filtered out ineligible rows)
    """

    def _judges_in_pref(x: str):
        """simple substring check for the plural of judge

        Args:
            x (str/None): prefix text for an entity

        Returns:
            Bool: True if "judges" appears in the string
        """
        return 'judges' in str(x).lower()

    def _not_comma_suffix(x: str):
        """ custom function to check if an entity string has a comma followed by a non-suffix word. Presumably we should cut these entities with weird commas

        Args:
            x (str/None): entity to be checked for the comma suffix patterns

        Returns:
            Bool: true if an entity has a comma followed by non-suffix words
        """
        # cast as string if not, strip extra spaces
        x = str(x).strip()
        # comma in entity
        if "," in x:
            # comma followed by non-jr-sr-etc. words
            if re.search(fr',(?! ({"|".join(JG.suffixes_titles)})\.*( |$))', x, flags=re.I):
                return True
            else:
                return False
        else:
            return False

    # if the dataframe has never had an eligibility check before, we need to create the eligibility column
    if 'eligible' not in df.columns:
        # default all rows eligible, none are exceptions
        df['eligible'] = True
        df['is_exception'] = False
        
        # if the dataframe has prefix text (header data wont)
        if 'extracted_pre_5' in df.columns:
            # determine if these should be exception columns either due to plural judges OR weird commas
            # exception entities are handled differently
            df.loc[(df.extracted_pre_5.apply(_judges_in_pref)) &
                   (df.extracted_entity.apply(_not_comma_suffix)), 'is_exception'] = True
    
    # if the entity column we are checking is null and this row was not a flagged exception --> eligibility is now false
    df.loc[(df[colname_to_check].isna())&(~df.is_exception), 'eligible'] = False
    # if it is now a single letter entity --> eligibility is now false (We cannot match single letter names)
    df.loc[df[colname_to_check].apply(lambda x: len(str(x))<=1), 'eligible'] = False
    # if the entity is just jr, sr, ii,iv, etc. then it's a bad grab --> eligibility is now false
    df.loc[df[colname_to_check].apply(lambda x: str(x).strip().lower() in JG.suffixes_titles), 'eligible'] = False
    # if the magistrate judge prefixy labels are infixed on the entity, these need special handling, flag as exceptions
    df.loc[df[colname_to_check].apply(lambda x: True if JG.magjud_infix.search(str(x)) else False), 'is_exception'] = True

    return df


def apply_respanning(df: pd.DataFrame, is_header: bool=False):
    """Once an entity has been cleaned, respan the location in the text so that it is updated to reflect the cleaned entity

    Args:
        df (pandas.DataFrame): The SEL-like rows we will be respanning
        is_header (bool, optional): is this a header dataframe (otherwise it is entries). Defaults to False.

    Returns:
        pandas.DataFrame: SEL-like dataframe that has extracted entities and their new spans and cleaned forms of the entities
    """
    # if this is entities from header metadata, we need to create the full span columns
    if is_header:
        # fill all nulls with 0
        for each in ['Ent_span_start','Ent_span_end','Pre_span_start','Pre_span_end']:
            df[each].fillna(0,inplace=True)
        # create the full span columns
        df['full_span_start'] = 0 #df[['Ent_span_start','Pre_span_start']].min(axis=1)
        df['full_span_end'] = df[['Ent_span_end','Pre_span_end']].max(axis=1)
    
    # copy the dataframe, only taking what is eligible and not an exception
    # note the infix judges are no longer exceptions after exception handling, 
    # they were all cleared to non-exceptions after cleaning pipe
    wf = df[(df.eligible)&(~df.is_exception)].copy()
    
    # the incoming frame has a respanned starting point with respect to the original starting point as "0"
    # new span is original start + offset
    wf['New_Span_Start'] = wf.Ent_span_start + wf.Respanned_Start
    # new end is new start + the new entity length
    wf['New_Span_End'] = wf.Ent_span_start + wf.Respanned_Start + wf.Respanned_Length
    
    # scaling the starting point is taking the new point and subtracting the original full span start
    # we used the scaled start and end points to redefine the pretext and posttext around an entity
    wf['Scaled_SS'] = wf.New_Span_Start - wf.full_span_start
    wf['Scaled_SE'] = wf.New_Span_End - wf.full_span_start
    wf['Scaled_SS'] = wf['Scaled_SS'].astype(int)
    wf['Scaled_SE'] = wf['Scaled_SE'].astype(int)
    
    # build a pseudo index to map with
    wf['Pseudo_Index'] = range(len(wf))

    re_text_pre = {}
    re_text = {}
    re_text_post = {}
    
    # using the new span locations regenerate pre and post texts surrounding the entity
    for pi, ot, Scaled_Start, Scaled_End in zip(wf.Pseudo_Index, wf.original_text, 
                                                wf.Scaled_SS, wf.Scaled_SE):
        re_text_pre[pi] = ot[0:Scaled_Start]
        re_text[pi] = ot[Scaled_Start:Scaled_End]
        re_text_post[pi] = ot[Scaled_End:]
    
    # now remap and add the new text columns to the dataframe
    wf['New_Pre_Tokens'] = wf.Pseudo_Index.map(re_text_pre)
    wf['New_Entity'] = wf.Pseudo_Index.map(re_text)
    wf['New_Post_Tokens'] = wf.Pseudo_Index.map(re_text_post)
    
    return wf

def handle_exception_entities(df: pd.DataFrame):
    """specialty function to handle known special entity strings or to handle plural judge entities

    Args:
        df (pandas.DataFrame): full dataframe of eligible rows for disambiguation

    Returns:
        pandas.DataFrame/list: DF of updated exception SEL rows or an empty list if no exceptions were handled
    """
    # known entities the model picks out that are weird exception cases
    # the honorifics somehow land in the middle of the entity name, and obviously require a special cleaning method
    specials =["Edward B. Judge Atkins",
    "Elaine E. - MagJud Bucklo",
     "Rebecca R.- MagJud Pallmeyer", 
     "Iain D. - Mag. Judge Johnston",
     "Iain D. - Mag. Judge Johnston",
     "Joan B. - MagJud Gottschall",
     "Mary M.-MagJud Rowland",
     "Naomi R. (Magistrate) Buchwald on 1/",
     "Naomi R. (Magistrate) Buchwald on 10/",
     "Thomas J. Judge Rueter"]

    ## these were cases where the NER extracted them, but the neighboring text makes them one-off outliers
    # comma separated dual judge entities
    comm_sep = ["Cassady, Bivens", "Hartz, Matheson"]
    # specialty weird entities
    jud_sep = ["f Judge David Nuffer","b y Judge Christina A. Snyder"]

    # infix pattern for any type of judge label in the middle of an entity string
    insplit_judge = re.compile(r'\s*(honorable judge|magistrate judge|mj|b judge|judge|honorable|hon)\.*\s*', flags=re.I)

    out_rows = [] #init
    # loop through our eligible exceptions and clean them
    for index, row in df[(df.is_exception)&(df.eligible)].iterrows():
        # copy the SEL row of data, we will overwrite the necessary fields or use it to create multiple rows in the case of plural entities
        meta = dict(row.copy())
        # reset exception to false as we are now about to handle it
        meta['is_exception'] = False
        # remove post text for now, label as handled exception
        meta['extracted_post_5'] = '<-?->'
        meta['Entity_Extraction_Method'] = 'SPACY_EXCEPTION_HANDLING'
        
        # the exception entity we will consider
        EE = row['extracted_entity']

        # if it's one of the weird judge separated beginning word entities, handle it
        if any(ex in EE for ex in jud_sep):
            # re-span the entity to be post-Judge
            new_entity = EE[EE.index("Judge")+6:]
            new_span_start = EE.index("Judge")+6
            new_span_length = len(new_entity)

            new = meta.copy()
            # change prefix to be Judge
            new['extracted_pre_5'] = 'Judge'
            new['extracted_entity'] = new_entity
            # respan
            new['Ent_span_start'] = new['Ent_span_start']+ new_span_start
            new['Ent_span_end'] = new['Ent_span_start']+ new_span_start + new_span_length
            out_rows.append(new)
        # if its a special entity with magjud or other judgey labels infixed in the name
        elif EE in specials:
            # maintain that they are exceptions, the entity span will need to include these labels
            new = meta.copy()
            new['is_exception'] = True
            out_rows.append(new)
            continue
        # for the comma separated plural entities
        elif EE in comm_sep:
            star = 0
            # make a new row for each comma separation
            while re.search(r',\s*',EE):
                m = re.search(r',\s*',EE)
                new_entity = EE[0:m.start()]
                new_span_start = star
                star+= m.end()
                new_span_length = len(new_entity)

                new = meta.copy()
                new['extracted_entity'] = new_entity
                new['Ent_span_start'] = new['Ent_span_start']+ new_span_start
                new['Ent_span_end'] = new['Ent_span_start']+ new_span_start + new_span_length
                out_rows.append(new)
                EE = EE[m.end():]
            # once all commas have been removed from the entity, we know there is one remaining entity to add
            # a, b, [c] -- c is the final one
            if EE:
                new_entity = EE
                new_span_start = star
                new_span_length = len(new_entity)
                new = meta.copy()
                new['extracted_entity'] = new_entity
                new['Ent_span_start'] = new['Ent_span_start']+ new_span_start
                new['Ent_span_end'] = new['Ent_span_start']+ new_span_start + new_span_length
                out_rows.append(new)
        # FOR ALL OTHER EXCEPTION ROWS

        else:
            star = 0
            passed = False
            # we know there must be judge-y words infixed somewhere
            # WARNING: this while loop could potentially get stuck on newer cases
            # the assumption is the entity is "Hawkins Judge Smith" so we want to split that
            # into 2 distinct rows of possible entities --> [Hawkins Judge] and [Smith]
            while insplit_judge.search(EE):
                passed = True
                m = insplit_judge.search(EE)
                # if judge was found, take the string from that point on
                # we have an assumption judge is not the first part of the string.
                new_entity = EE[0:m.start()]
                new_span_start = star
                star+= m.end()
                new_span_length = len(new_entity)

                new = meta.copy()
                new['extracted_pre_5'] = m.group()
                new['extracted_entity'] = new_entity
                new['Ent_span_start'] = new['Ent_span_start']+ new_span_start
                new['Ent_span_end'] = new['Ent_span_start']+ new_span_start + new_span_length
                out_rows.append(new)
                EE = EE[m.end():]
            # again after the while loop if it was entered (passed) then we have one final entity trailing to capture
            if EE and passed:
                new_entity = EE
                new_span_start = star
                new_span_length = len(new_entity)

                new = meta.copy()
                new['extracted_pre_5'] = m.group()
                new['extracted_entity'] = new_entity
                new['Ent_span_start'] = new['Ent_span_start']+ new_span_start
                new['Ent_span_end'] = new['Ent_span_start']+ new_span_start + new_span_length
                out_rows.append(new)
    
    # if we went thru and added new rows while handling exceptions
    if out_rows:
        # log the exceptions and what they became
        for exc in out_rows:
            JU.log_message(f"{exc['ucid']:25} | Specialty: {exc['extracted_entity']:25} | FROM: {exc['original_text']}")

        # build the dataframe of new ents
        outframe = pd.DataFrame(out_rows)

        # update the respanning locations on this new df, specifically the remaining exceptions need respanning features
        outframe.loc[(outframe.eligible)&(outframe.is_exception), "Respanned_Entity"]= outframe.extracted_entity
        outframe.loc[(outframe.eligible)&(outframe.is_exception), "Respanned_Start"]= 0
        outframe.loc[(outframe.eligible)&(outframe.is_exception), "Respanned_Length"]= outframe.extracted_entity.apply(lambda x: len(str(x)))

        # return the new dataframe (this will be small, only the updated rows)
        return outframe
    else:
         return []

def prefix_categorization(df: pd.DataFrame, is_header: bool = False):
    """Given pretext preceding an entity, attempt to categorize it into one of our existing
    mutually exclusive buckets.

    Args:
        df (pandas.DataFrame): SEL-like dataframe
        is_header (bool, optional): Does this DF come from header metadata (False means entries). Defaults to False.
    """
    # trie based searching patterns with their mutually exclusive labels
    _JTries = [
        (JG.BJ_TRIE, 'Bankruptcy_Judge'),
        (JG.CJ_TRIE, 'Circuit_Appeals'),
        (JG.DJ_TRIE, 'District_Judge'),
        (JG.MJ_TRIE, 'Magistrate_Judge'),
        (JG.ND_TRIE,'Nondescript_Judge'),
        (JG.JA_TRIE, 'Judicial_Actor')
    ]
    def _try_patts(each: str):
        """Local function used to try a trie-based search pattern on prefix text

        Args:
            each (str): pretext string we will search for hotwords

        Returns:
            str: the label (mutually exclusive) this pretext belongs to
        """
        if each in ["assigned_judge","referred_judges"]:
            # these are header metadata labels that are pretagged on the header data
            # keep them if they show up
            return each

        # in the order they are listed in, try each trie pattern
        # note nondescript judges need to be tried last because "judge" is in "bankruptcy judge" and we want to
        # capture the more descriptive label first
        for pattern,label in _JTries:
            # if the match is found, return that label
            if pattern.extract_keywords(str(each)):
                return label
 
        return "No_Keywords"

    # verbiage check if the words transferred to or from surround the entity
    _transferred_patt = re.compile(r'[\b\s^](transferred|(re)?assigned)( (to|from))[\b\s$]', flags=re.I)
    def _determine_transfer(each: str):
        """call the transferring pattern above on a string

        Args:
            each (str): pretext string

        Returns:
            Bool: True or False, is transferring language preceding this judge
        """
        m = _transferred_patt.search(each)
        if m:
            return True
        else:
            return False
    # if this was the header df
    if is_header:
        # if there was no prefix text, infill it as None
        df.loc[(df.New_Pre_Tokens.isna())|(df.New_Pre_Tokens==''),'New_Pre_Tokens'] = None
        # fill the Nones with the entity extraction methods (this will be assigned or referred)
        df["New_Pre_Tokens"].fillna(df.Entity_Extraction_Method, inplace=True)
    
    # determine if it was transferred
    df['Transferred_Flag'] = df.New_Pre_Tokens.apply(_determine_transfer)

    # take all the existing unique pretext strings and categorize them
    existing_pretext = list(df[(df.eligible)].New_Pre_Tokens.unique())
    pretext_map = {}
    for each in tqdm.tqdm(existing_pretext):
        pretext_map[each] = _try_patts(each)
        
    # remap categorized pretext onto the SEL-like DF before disambiguation.
    df['Prefix_Categories'] = df.New_Pre_Tokens.map(pretext_map)

    return df

def string_cleaning_hierarchy(each: str):
    """stacked cleaning function that iterates through an entity string and identifies if parts of it need to be shaved off the front or back. 
    The order of execution is important in this function. Inserting new cleaning patterns should be okay, but be careful rearranging existing ones.

    Args:
        each (str): entity string to be cleaned

    Returns:
        str, list: cleaned string, list of exceptions if the original string was flagged as an exception
    """
    # keep the original string
    ori = each
    # empty exceptions list
    exceptions=[]

    # if the pattern says by/to "judge", we will strip that from the front
    m = JG.longform_extraction_pattern.search(each)
    if m:
        each = each[m.end():]

    # strip any number of leading periods
    m = re.search(r'^\.+', each)
    if m:
        each = each[m.end():]

    # middle patterns are patterns that we expect to detect after a judge's name, so strip at the beginning
    # of them if we find them
    m = JG.mpp.search(each)
    if m and m.start() not in range(0,5):
        each = each[0:m.start()]

    # if a number or punctuation is all at the beginning or end
    m = JG.num_punct_start.search(each)
    if m:
        each = each[m.end():]

    # if a . or [ now starts the string
    m = re.search(r'^(\.|\[)',each)
    if m:
        each = each[m.end():]
    

    # lots of crazy potential docket punctuation patterns
    mepatts=[".*",".:",".;",'.)',".(",'.[',".'",".-",".=",". (",
                ":",";","**", "--",
                ",)",")*",").",")(","(",
                '[',']','{',"}", "§",
                "(r)","(s)",")", " - ", '<b',"<./b",'</font',]
    # find any/all of these in the string
    splatty = [(pat, each.lower().index(pat)) for pat in mepatts if pat in each.lower()]
    # find the earliest one (if multiple)
    splatty = sorted(splatty, key=lambda tup: tup[1])
    if splatty:
        i = splatty[0][1]
        each = each[0:i]
    
    # word judge or justice at beginning of string
    m = JG.front_judge_pattern.search(each)
    if m:
        each = each[m.end():]
        
    # determine if "by" is the beginning  of the string, if so, strip it
    m = JG.by_pattern.search(each)
    if m:
        each = each[m.end():]

    # literal word "re" at end of judge string (often grouped in after judge names "judge thomas: re motion to dismiss")
    m = JG.endre_pattern.search(each)
    if m:
        each = each[0:m.start()]

    # locate any sort of dates or monthy words after a judge name
    m = JG.dates_pattern.search(each)
    if m:
        each = each[0:m.start()]
    
    # special docket pattern happened enough where it was Judge NameThe first
    m = JG.the_pattern.search(each)
    if m:
        each = each[0:m.start()]
        
    # identify possessive 's on names, if it is there, cut string after the apostrophe s
    m = JG.possessive_pattern.search(each)
    if m:
        sindex = m.start()+len(m.group("POSS"))
        each = each[0:sindex]
    
    # any type of case numbering pattern located in the end of the string
    m = JG.back_short_case_pattern.search(each)
    if m:
        each = each[0:m.start()]

    # more calendar and scheduley words
    m = JG.gregorian_pattern.search(each)
    if m:
        each = each[0:m.start()]
    
    # brute force, case specific "CAPS CAPS CAPS."
    m = JG.multi_name_pattern.search(each)
    if m:
        each = each[0:m.end()]
    
    # magistrate judge r&r abbreviation types got picked up a lot in entities
    m = JG.RandR_pattern.search(each)
    if m:
        each = each[0:m.start()]
    
    # large corpus of possible single token words or multitoken word patterns that appeared alongside or as entities
    m = JG.wopat.search(each)
    if m:# and m.start() not in range(3):
        each = each[0:m.start()]
    # words that must be searched specifically as the final word in a pattern
    m = JG.single_back_patt.search(each.strip())
    if m:
        each = each[0:m.start()]
    # specific words that are trailing tokens as lowercase after a full caps name
    m = JG.trailing_toks_pattern.search(each)
    if m:
        each = each[0:m.start()] 
    
    # any number of abbreviated judge patterns
    m = JG.affixed_judge.search(each)
    if m:
        # best guess locate if it's at the beginning or end of the string based on midpoint
        # USDJ Jones vs. Jones, USDJ
        midpoint = len(each)//2
        if m.start() <midpoint:
            each = each[m.end():]
        else:
            each = each[0:m.start()]
    
    # iterate through possible judge type labels in the entity string
    for patt in [JG.magi, JG.distr, JG.appy, JG.bankr, JG.judge_plain]:
        m = patt.search(each)
        if m:
            # if it's the beginning of the string, use whats after
            if m.start()==0:
                each = each[m.end():]
                break
            # vice versa
            elif m.end() == len(each):
                each = each[0:m.start()]
                break
            # judge labels appeared in middle of name, flag as exception
            # "Elaine E. MagJud Bucklo" for example
            else:
                exceptions.append(ori)
                break
    # if the string is long and there are misc. words after a comma in the entity string
    m = JG.post_comma.search(each[21:])
    if m:
        if len(each[m.start():].split())>3:
            each = each[0:m.start()]
    
    # judge misspelled patterns
    m = JG.jmp.search(each)
    if m:
        each = each[m.end():]
    
    # judge scott got a special pattern, his clerk was wild
    m = JG.scott_pattern.search(each)
    if m:
        each = each[0:m.end()]
    
    # there is an actualjudge named chambers so some extra logic was needed to determine if THE judge chambers
    # was being referenced, or if another judge's chambers were
    m = JG.chambers_exception_pattern.search(each)
    if m:
        preceding = each[0:m.start()].lower().split()
        if not preceding:
            each = each[0:m.start()]
        elif preceding and preceding[-1] not in ['robert','c','c.']:
            each = each[0:m.start()]
    
    # find text after a known suffix like jr, sr and cut it out
    m = JG.suff_post_text.search(each)
    if m:
        each = each[0:m.end()]
    
    # locate special honorarium prefixes
    m = JG.sr_judge_prefix.search(each)
    if m:
        each = each[m.end():]
        
    # basically identify triples preceding a perio and assume it is a sentence end
    m = JG.brute_sent.search(each)
    if m and len(each.split())>3 and each.split()[-1] not in JG.suffixes_titles:
        if 'lee' not in each.lower():
            each = each[0:m.end()]
    
    # special suffixes on peoples names
    m = JG.suffixed_pattern.search(each)
    if m:
        each = each[0:m.start()]

    # if a number or punctuation is all at the beginning or end
    m = JG.num_punct_start.search(each)
    if m:
        each = each[m.end():]

    m = JG.num_punct_end__.search(each)
    if m:
        ender = each.split()[-1].lower()
        # confirm the trailing punct isnt an abbreviation period
        if not any(suff in each.split()[-1].lower() for suff in JG.suffixes_titles if suff not in ['i','v']):
            each = each[0:m.start()]
    
    # self documenting trailing whitespaces
    m = JG.trailing_whitespace.search(each)
    if m:
        each = each[0:m.start()]
    
    # if ANY of these patterns trigger at the end, that means the entity is bad, we toss it
    x = JG.numbers_only.search(each) # just numbers
    y = JG.dba_obo_pattern.search(each) # a doing business as or on behalf of means it's not a judge
    z = JG.prisoner_patt.search(each) # prisoner numbers
    a = JG.address_num_patt.search(each) # address like pattern
    b = JG.weird_triple.search(each) #letter-space-letter was a weird one
    c = JG.re_beg_patt.search(each) # if re or other odd beginners existed
    d = JG.odd_numeric_pattern.search(each) # special case label type SACR###
    if x or y or z or a or b or c or d:
        each = None

    return each, exceptions

def stacked_cleaning(testname: str):
    """This cleaning is performed at the beginning of disambiguation. 
    It levels the disambiguation playing field omitting punctuation, omitting possessive s and does other similar things

    Args:
        testname (str): the entity string we will clean

    Returns:
        str: cleaned entity string
    """
    testname = testname.translate(JG.accent_repl) # replace accented letters in case clerks did not use accents in entry
    testname = re.sub(r'(mag\. judge|judge|magjud|- ?MagJud|magistrate)','',testname, flags=re.I) # infill the judgey words that got stuck between tokens
    testname = re.sub(r'(\\xc2|\\xa71)', r'', testname,flags=re.I) # replace as blanks
    testname = re.sub(r'(\\xc3|\\xa1)', r'a', testname,flags=re.I) # replace as a
    testname = re.sub(r'\'s($|\s+(?!(jr|sr)\.*))|\' s$',r' ',testname, flags=re.I) #replace possessive 's_ with nothing
    testname = re.sub(r'[0-9]',r'', testname) # no numbers please
    testname = re.sub(r'(!|"|#|%|&|\*|\+|,|/|=|\?|@|\^|_|`|~|\$|\||\\)', r' ',testname) # dump meaningless punctuation
    testname = re.sub(r'[.](?=[^\s])', r' ',testname) # if a name is initial.initial, make that period a space
    testname = re.sub(r'[.](?=[\s])', r'',testname) # if a name has initial.space just strip the period
    testname = re.sub(r'[\'](?=[\s])|(?<=[\s])[\']', r'', testname) # if apostrophe space or space apostrophe, remove the space
    testname = re.sub(r'(?<=[^\s]) [-](?=[^\s])|(?<=[^\s])[-] (?=[^\s])', '-',testname) #hyphen collapse

    testname = re.sub(r'[-]',' ',testname)
    testname = re.sub(r'[\']','',testname)

    # FJC does this whack thing for judges that go by an initial
    # C[hristian] Rozolis --> Christian Rozolis
    testname = re.sub(r'(?<=[a-zA-Z])[\[](?=[a-zA-Z]+)',r'',testname) # front bracket
    testname = re.sub(r'(?<=[a-zA-Z])[\]](?=[\s]+)',r'',testname) # back bracket
    testname = testname.replace('.','')
    
    # suffix normalization
    testname = re.sub(r' (sr|snr|senior)',' sr',testname, flags=re.I) # make all normal looking senior
    testname = re.sub(r' (jr|jnr|junior)',' jr',testname, flags=re.I) # make all normal looking jr
    
    # join split omits any funky double spaces
    return ' '.join(testname.lower().split())


def cast_as_entry_model(row: dict):
    """Given a row of json data from a SEL file, cast it into the disambiguation data model

    Args:
        row (dict): SEL data loaded in from a jsonL file, this should be an encapsulated valid json

    Returns:
        dict: the original data, but now cast into the properly keyed data model
    """
    ot = row['original_text']
    scaled_ent_start = int(row['Entity_Span_Start'] - row['full_span_start'])
    scaled_ent_end = int(row['Entity_Span_End'] - row['full_span_start'])

    # need to generate text before and after the entity
    pretext = ot[0:scaled_ent_start]
    posttext = ot[scaled_ent_end:]
    
    out = {
        'ucid': row['ucid'],
        'court': row['court'],
        'year': row['year'],
        'cid': row['cid'],
        'docket_index': row['docket_index'],
        'entry_date': '',
        'original_text': row['original_text'],
        'full_span_start': int(row['full_span_start']),
        'full_span_end': int(row['full_span_end']),
        'extracted_pre_5': pretext,
        'extracted_entity': row['Extracted_Entity'],
        'extracted_post_5': posttext,
        'Ent_span_start': int(row['Entity_Span_Start']),
        'Ent_span_end': int(row['Entity_Span_End']),
        'docket_source': row['docket_source'],
        'Entity_Extraction_Method': row['Entity_Extraction_Method']
    }
    return out


def cast_as_heads_model(row: dict):
    """Given a row of json data from a SEL file, cast it into the disambiguation data model

    Args:
        row (dict): SEL data loaded in from a jsonL file, this should be an encapsulated valid json

    Returns:
        dict: the original data, but now cast into the properly keyed data model
    """
    ot = row['original_text']
    scaled_ent_start = int(row['Entity_Span_Start'] - row['full_span_start'])
    # only need to generate text before the entity for header data
    pretext = ot[0:scaled_ent_start]

    out = {
        'ucid': row['ucid'],
        'court': row['court'],
        'cid': row['cid'],
        'year': row['year'],
        'filing_date': '',
        'original_text': row['original_text'],
        'extracted_pretext': pretext,
        'extracted_entity': row['Extracted_Entity'],
        'Ent_span_start': int(row['Entity_Span_Start']),
        'Ent_span_end': int(row['Entity_Span_End']),
        'Pre_span_start': row['full_span_start'],
        'Pre_span_end': row['Entity_Span_Start']-1,
        'docket_source': row['docket_source'], 
        'Entity_Extraction_Method': row['Entity_Extraction_Method'],
        'judge_enum': row['judge_enum'],
        'party_enum': row['party_enum'],
        'pacer_id': row['pacer_id'],
        'docket_index': row['docket_index']
    }
    return out

    
def Transform_SEL_to_Disambiguation_Data_Model(old_Inconclusive_Data: list):
    """Given a list of jsons/dicts loaded in from prior SEL files, transform them into
    the disambiguation data model so they can be reconsidered during disambiguation

    Args:
        old_Inconclusive_Data (list): valid jsons loaded from SEL jsonLs

    Returns:
        pandas.DataFrame, pandas.DataFrame: the remodelled data for line entries and header entities
    """
    as_raw_lines = []
    as_raw_heads = []
    for row in old_Inconclusive_Data:
        if row['docket_source'] == 'line_entry':
            as_raw_lines.append(cast_as_entry_model(row))
        else:
            as_raw_heads.append(cast_as_heads_model(row))
            
    old_entries = pd.DataFrame(as_raw_lines)
    old_heads = pd.DataFrame(as_raw_heads)

    return old_entries, old_heads