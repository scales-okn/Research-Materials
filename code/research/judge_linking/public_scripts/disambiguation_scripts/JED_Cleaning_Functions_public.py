from pathlib import Path
import re
import sys
import tqdm

sys.path.append('../../../../')
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


def ingest_the_fjc(fpath, low = '1700-01-01', high = '2022-12-31', is_update = False):
    """ Ingest the FJC demographics data from our SCALES csv annotation version of the codebook

    Args:
        fpath (string): FJC fpath from settings or csv
        low (str, optional): Low end of active judicial dates. Defaults to '1700-01-01'.
        high (str, optional): High end of active judicial dates. Defaults to '2022-12-31'.
        is_update(bool, optional): bool if this run is meant for updating the JEL or not. Defaults to False

    Returns:
        pandas.DataFrame: long-form DF of the fjc judge data, with 1 row per judge (collapsed the wideform information)
    """

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
    judge_demographics_cols_cast = ['Court Type', 'Court Name', 'Appointment Title', 'Confirmation Date', 'Commission Date', 'Termination Date']

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
    # drop NA's and ## DEPR: non District Court Judges
    fjc_expanded = fjc_expanded[(~fjc_expanded['Court Name'].isna())].copy() # & (fjc_expanded['Court Type'] == 'U.S. District Court')].copy()
    fjc_expanded.sort_values(["nid","jid","Appointment Number"], inplace=True)
    # convert district court names to abbreviations
    name2abb = {c: cf.classify(c) for c in fjc_expanded['Court Name'].unique().tolist()}
    fjc_expanded['Court Name Abb'] = fjc_expanded['Court Name'].map(name2abb)
    fjc_expanded['Termination Date'].fillna(pd.to_datetime('today').date(), inplace=True)
    fjc_expanded['Commission Date'] = fjc_expanded['Commission Date'].apply(lambda x: pd.to_datetime(x).date())
    fjc_expanded['Termination Date'] = fjc_expanded['Termination Date'].apply(lambda x: pd.to_datetime(x).date())

    # clean the full names
    fjc_expanded['Simplified Name'] = fjc_expanded.FullName.apply(lambda x: clean_fjc_name(x))
    # fill NAs so they can be filtered if necessary
    fjc_expanded["Commission Date"].fillna(fjc_expanded["Termination Date"].apply(lambda x: pd.to_datetime(f"{x.year}-01-01").date()), inplace=True)
    my_date_range = {"low": pd.to_datetime(low).date(),
                    "high": pd.to_datetime(high).date()}

    # filter to specified time range
    if not is_update:
        fjc_active = fjc_expanded[
            (fjc_expanded['Commission Date'] <= my_date_range["high"]) &
            (fjc_expanded['Termination Date'] >= my_date_range["low"])]
    else:
        fjc_active = fjc_expanded

    # the output frame should be a cleaned name, original name, nid, earliest commission date and latest known termination date
    fjc_judges = fjc_active.groupby(["Simplified Name","FullName","nid"], as_index=False).agg({'Commission Date': ['min'], 'Termination Date': 'max'})

    return fjc_judges


def clean_fjc_name(testname):
    """ given an FJC entity name, clean it

    Args:
        testname (str): the FullName column values from the fjc codebook

    Returns:
        str: cleaned, lower-cased version of the fjc fullname
    """
    # order matters in execution (i.e. assuming numbers exist until numbers are stripped out)
    testname = testname.translate(JG.accent_repl) # replace accented letters in case clerks did not use accents in entry
    
    testname = re.sub(r'(\'s|\\\'s|\\xc2|\\xa71)', r'', testname,flags=re.I) # replace as blanks
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
    
    # default split will remove odd double spacing, then rejoin
    return ' '.join(testname.lower().split())

def ingest_header_entities(fpath):
    """ingest the extracted parties or counsels data. 

    Args:
        fpath (str): relative filepath in string form

    Returns:
        pandas.DataFrame: df of case entities from a specific header section. Expected Columns: ['ucid','court','cid','year','filing_date','Role','Entity']
    """
    
    in_df = pd.read_csv(fpath)

    return in_df

def reshuffle_exception_entities(RDF):
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

    Returns:
        pandas.DataFrame: same length as input dataframe, but with reshuffled entities (included tokens from neighborhoods) if the extracted entity qualified in the checks
    """

    # running dict of possible "single token extractions" the spacy model makes that could be accurate single tokens, but could also have trailing (post) tokens after
    # them that we want to double check are not there

    # the regex patterns are intended to be "search"ed on the neighborhood of post-entity tokens, and if the last name appears, then it matches
    sgl_post_loop = {
        'alan': re.compile(r'baverman', flags=re.I),
        'alexander': re.compile(r'mackinnon', flags=re.I),
        'amul': re.compile(r'thapar', flags=re.I),
        'amy': re.compile(r'totenberg', flags=re.I),
        'anne': re.compile(r'b[ue]rton', flags=re.I),
        'anthony': re.compile(r'porcelli', flags=re.I),
        'arlander': re.compile(r'keys?', flags=re.I),
        'arthur': re.compile(r'schwab', flags=re.I),
        'analisa': re.compile(r'torres', flags=re.I),
        'andre': re.compile(r'birotte,? jr\.?', flags=re.I),
        'andrea': re.compile(r'wood', flags=re.I),
        'andrew': re.compile(r'schopler', flags=re.I),
        'barbara': re.compile(r'major|a\.* mcauliffe', flags=re.I),
        'barry': re.compile(r't(\.*|ed) moskowitz|seltzer', flags=re.I),
        'benjamin': re.compile(r'(goldgar|settle)', flags=re.I),
        'bernard': re.compile(r'friedman', flags=re.I),
        'billy': re.compile(r'mcdade', flags=re.I),
        'blanche': re.compile(r'(m\.* )?manning', flags=re.I),
        'brooke': re.compile(r'wells', flags=re.I),
        'bruce': re.compile(r'(mcgiverin|guyton|h(\.*|owe) hendricks)|parent', flags=re.I),
        'cameron': re.compile(r'm(\.*|cgowan) currie', flags=re.I),
        'candy': re.compile(r'w\.* dale', flags=re.I),
        'carl': re.compile(r'barbier', flags=re.I),
        'catherine': re.compile(r'steenland', flags=re.I),
        'charles': re.compile(r'(price|"skip" rubin|a stampelos|p?\.* kocoras|mills bleil,? sr\.*)', flags=re.I),
        'choe': re.compile(r'(-)?groves', flags=re.I),
        'claire': re.compile(r'cecchi', flags=re.I),
        'clarence': re.compile(r'cooper(s)?', flags=re.I),
        'colin': re.compile(r'lindsay', flags=re.I),
        'curtis': re.compile(r'gomez', flags=re.I),
        'cynthia': re.compile(r'rufe|eddy', flags=re.I),
        'dan': re.compile(r'stack', flags=re.I),
        'david': re.compile(r'(g\.* )?larimer|(s\.* )?doty|peebles|rush|lawson|nuffer', flags=re.I),
        'dearcy': re.compile(r'hall', flags=re.I),
        'deb': re.compile(r'barnes', flags=re.I),
        'deborah': re.compile(r'barnes', flags=re.I),
        'denise': re.compile(r'larue|page hood', flags=re.I),
        'derrick': re.compile(r'watson', flags=re.I),
        'diana': re.compile(r'saldana', flags=re.I),
        'dolly': re.compile(r'(m\.* )?gee', flags=re.I),
        'donald': re.compile(r'nugent', flags=re.I),
        'douglas': re.compile(r'mccormick', flags=re.I),
        'ed': re.compile(r'(m\.* )?chen', flags=re.I),
        'edward': re.compile(r'nottingham', flags=re.I),
        'elainee': re.compile(r'bucklo', flags=re.I),
        'eldon': re.compile(r'(e\.* )?fallon', flags=re.I),
        'eleanor': re.compile(r'(l\.* )?ross', flags=re.I),
        'eli': re.compile(r'richardson', flags=re.I),
        'elizabeth': re.compile(r'hey|wolford', flags=re.I),
        'eric': re.compile(r'j\.* markovich|long', flags=re.I),
        'frank': re.compile(r'geraci|whitney', flags=re.I),
        'freddie': re.compile(r'burton', flags=re.I),
        'gabriel': re.compile(r'fuentes|gorenstein', flags=re.I),
        'garrit': re.compile(r'howard', flags=re.I),
        'george': re.compile(r'caram steeh(,? iii\.*)?|levi russell(,? iii\.*)?|jarrod hazel|h\.* wu', flags=re.I),
        'geraldine': re.compile(r'(soat-?\s?)?brown', flags=re.I),
        'gershwin': re.compile(r'drain', flags=re.I),
        'glenn': re.compile(r'norton', flags=re.I),
        'graham': re.compile(r'mullen', flags=re.I),
        'gray': re.compile(r'(m\.*(ichael)? )?borden', flags=re.I),
        'greg': re.compile(r'gerard guidry', flags=re.I),
        'gregg': re.compile(r'costa', flags=re.I),
        'gregory': re.compile(r'van tatenhove', flags=re.I),
        'gustavo': re.compile(r'gelpi', flags=re.I),
        'gustave': re.compile(r'diamond', flags=re.I),
        'halil': re.compile(r'ozerden', flags=re.I),
        'harry': re.compile(r'leinenweber|mattice(,? jr\.*)?', flags=re.I),
        'hayden': re.compile(r'head', flags=re.I),
        'helen': re.compile(r'gillmor', flags=re.I),
        'henry': re.compile(r'e(\.* |dward )?autrey|lee adams,? jr\.*|coke morgan\,?( jr\.*)?', flags=re.I),
        'hernandez': re.compile(r'covington', flags=re.I),
        'hildy': re.compile(r'bowbeer', flags=re.I),
        'ignacio': re.compile(r'torteya,? iii', flags=re.I),
        'jacqueline': re.compile(r'rateau|chooljian', flags=re.I),
        'james': re.compile(r'gwin|ryan|lawrence king|russell grant|shadid|ed(gar)? kinkeade|patrick hanlon|knepp|knoll gardner|(a\.* )?soto|holderman|(c\.* )?francis(,? iv\.*)?|cacheris', flags=re.I),
        'jan': re.compile(r'dubois', flags=re.I),
        'janis': re.compile(r'vanmeerveld', flags=re.I),
        'jay': re.compile(r'c\.* zainey', flags=re.I),
        'jed': re.compile(r's\.* rakoff', flags=re.I),
        'jeffery': re.compile(r'frensley', flags=re.I),
        'jeffrey': re.compile(r'cole|gilbert', flags=re.I),
        'jerome': re.compile(r'semandle', flags=re.I),
        'jesus': re.compile(r'g\.* bernal', flags=re.I),
        'jill': re.compile(r'otake|l\.* burkhardt', flags=re.I),
        'joan': re.compile(r'b\.* gottschall', flags=re.I),
        'joe': re.compile(r'brown', flags=re.I),
        'joel': re.compile(r'(f\.* )?dubina', flags=re.I),
        'john': re.compile(r'nivison|(w\.* )?debelius(,? iii)?|darrah|ross|love|preston[\s]?bailey', flags=re.I),
        'jon': re.compile(r'phipps mccalla|(s\.* )?tigar', flags=re.I),
        'jorge': re.compile(r'alonso', flags=re.I),
        'jose': re.compile(r'fuste|linares', flags=re.I),
        'joseph': re.compile(r'spero|(s )?van bokkelen|dickson|lane|anthony diclerico,? jr\.*|saporito|robert goodwin', flags=re.I),
        'juan': re.compile(r'alanis', flags=re.I),
        'julian': re.compile(r'abele cook', flags=re.I),
        'kandis': re.compile(r'westmore', flags=re.I),
        'karoline': re.compile(r'mehalchick', flags=re.I),
        'kelly': re.compile(r'rankin', flags=re.I),
        'kenneth': re.compile(r'mchargh', flags=re.I),
        'kevin': re.compile(r'gross', flags=re.I),
        'kimberly': re.compile(r'swank', flags=re.I),
        'kiyo': re.compile(r'matsumoto', flags=re.I),
        'kristen': re.compile(r'(l\.* )?mix', flags=re.I),
        'lacey': re.compile(r'a\.* collier', flags=re.I),
        'lance': re.compile(r'africk', flags=re.I),
        'laura': re.compile(r'taylor swain', flags=re.I),
        'laurel': re.compile(r'beeler', flags=re.I),
        'lawrence': re.compile(r'e\.* kahn', flags=re.I),
        'lee': re.compile(r'h\.* rosenthal|yeakel', flags=re.I),
        'leon': re.compile(r'schy[d]?lower', flags=re.I),
        'leonard': re.compile(r'davis', flags=re.I),
        'leslie': re.compile(r'(e\.* )?kobayas[hk]i', flags=re.I),
        'linda': re.compile(r'caracappa', flags=re.I),
        'loretta': re.compile(r'preska', flags=re.I),
        'louis': re.compile(r'stanton|guirol( )?a(,? jr\.*)?', flags=re.I),
        'luciano': re.compile(r'panici', flags=re.I),
        'lynwood': re.compile(r'smith', flags=re.I),
        'mac': re.compile(r'mccoy', flags=re.I),
        'mae': re.compile(r'd\'agostino', flags=re.I),
        'marc': re.compile(r'thomas treadwell', flags=re.I),
        'marcos': re.compile(r'lopez', flags=re.I),
        'margaret': re.compile(r'goodzeit', flags=re.I),
        'margo': re.compile(r'brodie', flags=re.I),
        'marilyn': re.compile(r'go', flags=re.I),
        'mark': re.compile(r'filip', flags=re.I),
        'martin': re.compile(r'(l\.*c\.* )?feldman|(c\.* )?ashman|carlson', flags=re.I),
        'marvin': re.compile(r'aspen|isgur', flags=re.I),
        'mary': re.compile(r's\.* scriven|alice', flags=re.I),
        'michael': re.compile(r'baylson|newman|hammer|davis|wilner|mihm|mason|scopelitis|(john )?aloi|urbanski', flags=re.I),
        'miles': re.compile(r'davis', flags=re.I),
        'morton': re.compile(r'denlow', flags=re.I),
        'nanette': re.compile(r'laughrey', flags=re.I),
        'nannette': re.compile(r'jolivette brown', flags=re.I),
        'nathanael': re.compile(r'cousins', flags=re.I),
        'nelson': re.compile(r'(stephen )?rom[aá]n', flags=re.I),
        'nina': re.compile(r'gershon', flags=re.I),
        'orinda': re.compile(r'evans', flags=re.I),
        'paul': re.compile(r'(singh )?grewal|plunkett|Gardephe', flags=re.I),
        'pedro': re.compile(r'delgado', flags=re.I),
        'percy': re.compile(r'anderson', flags=re.I),
        'peter': re.compile(r'beer|buchsbaum|leisure', flags=re.I),
        'philip': re.compile(r'lammens', flags=re.I),
        'raag': re.compile(r'singhal', flags=re.I),
        'raul': re.compile(r'ar[ei]as((-|\s)?marxuach)?', flags=re.I),
        'reed': re.compile(r'[0o]\'?connor', flags=re.I),
        'richard': re.compile(r'puglisi|lloret|story', flags=re.I),
        'robert': re.compile(r'gettleman|(m\.* )?dow|bacharach|junell|chambers|numbers|n\.* scola|b\.* jones,? jr\.*', flags=re.I),
        'rodney': re.compile(r'sippel', flags=re.I),
        'rolando': re.compile(r'olvera', flags=re.I),
        'ronnie': re.compile(r'abrams', flags=re.I),
        'rosemary': re.compile(r'marquez', flags=re.I),
        'roslyn': re.compile(r'silver', flags=re.I),
        'ruben': re.compile(r'castill[o0]s?', flags=re.I),
        'sallie': re.compile(r'kim', flags=re.I),
        'samuel': re.compile(r'mays( jr\.*)?', flags=re.I),
        'scott': re.compile(r'frost|vanderkarr', flags=re.I),
        'sheila': re.compile(r'finnegan', flags=re.I),
        'sheri': re.compile(r'py[mn]', flags=re.I),
        'sidney': re.compile(r'schenkier', flags=re.I),
        'smith': re.compile(r'camp', flags=re.I),
        'sonja': re.compile(r'bivins', flags=re.I),
        'st': re.compile(r'eve', flags=re.I),
        'staci': re.compile(r'm\.* yandle', flags=re.I),
        'stanley': re.compile(r'(a\.* )?boone', flags=re.I),
        'steven': re.compile(r'(i\.* )?locke|nordquist', flags=re.I),
        'stewart': re.compile(r'dalzell|aaron', flags=re.I),
        'sue': re.compile(r'myerscough', flags=re.I),
        'susan': re.compile(r'wigenton|van(\s)?keulen', flags=re.I),
        'suzanne': re.compile(r'conlon', flags=re.I),
        'therese': re.compile(r'wiley(\s)?dancks', flags=re.I),
        'thomas': re.compile(r'thrash|durkin|russell|mcavoy|coffin', flags=re.I),
        'timothy': re.compile(r'batten(,? sr\.*)?', flags=re.I),
        'tonianne': re.compile(r'bongiovanni', flags=re.I),
        'troy': re.compile(r'(l\.* )?nunley', flags=re.I),
        'vernon': re.compile(r'speede broderick', flags=re.I),
        'velez': re.compile(r'rive', flags=re.I),
        'victor': re.compile(r'bianchini', flags=re.I),
        'waverly': re.compile(r'(d\.* )?crenshaw', flags=re.I),
        'wayne': re.compile(r'andersen', flags=re.I),
        'wendy': re.compile(r'beetlestone', flags=re.I),
        'william': re.compile(r'stafford|cobb|fremming nielsen', flags=re.I)
    }

    # same concept, but when the model extracts 2 tokens only but a third intuitively should exist
    dbl_post_loop = {
        'alan b': re.compile(r'johnson', flags=re.I),
        'allyne r': re.compile(r'ross', flags=re.I),
        'anita b': re.compile(r'brody', flags=re.I),
        'barbara l': re.compile(r'(l(\.*|ynn) )?major', flags=re.I),
        'barbara lynn': re.compile(r'(l(\.*|ynn) )?major', flags=re.I),
        'benjamin h': re.compile(r'settle', flags=re.I),
        'candace j': re.compile(r'smith', flags=re.I),
        'carla b': re.compile(r'carry', flags=re.I),
        'christine m': re.compile(r'arguello', flags=re.I),
        'darrin p': re.compile(r'gayles', flags=re.I),
        'deborah m': re.compile(r'fine', flags=re.I),
        'donald c': re.compile(r'nugent', flags=re.I),
        'eduardo c': re.compile(r'robreno', flags=re.I),
        'edward g': re.compile(r'smith', flags=re.I),
        'edward j': re.compile(r'davila', flags=re.I),
        'edward r': re.compile(r'korman', flags=re.I),
        'elizabeth s': re.compile(r'chestney', flags=re.I),
        'frederick j': re.compile(r'kapala', flags=re.I),
        'gerald a': re.compile(r'mchugh', flags=re.I),
        'gerald j': re.compile(r'pappert', flags=re.I),
        'gregory f': re.compile(r'van( )?tatenthove', flags=re.I),
        'guillermo r': re.compile(r'garcia', flags=re.I),
        'gustavo a': re.compile(r'gelpi', flags=re.I),
        'henry s': re.compile(r'perkin', flags=re.I),
        'hugh b': re.compile(r'scott', flags=re.I),
        'james d': re.compile(r'caldwell', flags=re.I),
        'james f': re.compile(r'holder', flags=re.I),
        'james r': re.compile(r'case', flags=re.I),
        'jan e': re.compile(r'dubois', flags=re.I),
        'janie s': re.compile(r'mayeron', flags=re.I),
        'jean p': re.compile(r'rosenbluth', flags=re.I),
        'jeffrey s': re.compile(r'frensley', flags=re.I),
        'joan b': re.compile(r'gottschall', flags=re.I),
        'joan h': re.compile(r'lefkow', flags=re.I),
        'joaquin v': re.compile(r'manibusan(,? jr\.*)?', flags=re.I),
        'joaquin ve': re.compile(r'manibusan(,? jr\.*)?', flags=re.I),
        'joel h': re.compile(r'slomsky', flags=re.I),
        'john a': re.compile(r'nordberg', flags=re.I),
        'john d': re.compile(r'early', flags=re.I),
        'john f': re.compile(r'grady', flags=re.I),
        'john m': re.compile(r'gerrard', flags=re.I),
        'john p': re.compile(r'cronan', flags=re.I),
        'john r': re.compile(r'tunheim|adams|padova', flags=re.I),
        'john w': re.compile(r'degravelles|primomo', flags=re.I),
        'joseph f': re.compile(r'bataillon', flags=re.I),
        'joseph s': re.compile(r'van( )?bokkelen', flags=re.I),
        'juan r': re.compile(r'sanchez', flags=re.I),
        'karen l': re.compile(r'stevenson', flags=re.I),
        'kiyo a': re.compile(r'matsumoto', flags=re.I),
        'lee h': re.compile(r'rosenthal', flags=re.I),
        'legrome d': re.compile(r'davis', flags=re.I),
        'leonard p': re.compile(r'stark', flags=re.I),
        'leslie e': re.compile(r'kobayashi', flags=re.I),
        'lynn n': re.compile(r'hughes', flags=re.I),
        'mark l': re.compile(r'van valkenburgh', flags=re.I),
        'mark r': re.compile(r'hornak', flags=re.I),
        'martin c': re.compile(r'ashman', flags=re.I),
        'mary m': re.compile(r'rowland', flags=re.I),
        'matthew f': re.compile(r'kennelly', flags=re.I),
        'matthew w': re.compile(r'brann', flags=re.I),
        'meredith a': re.compile(r'jury', flags=re.I),
        'michael j': re.compile(r'roemer', flags=re.I),
        'milton i': re.compile(r'shadur', flags=re.I),
        'mitchell s': re.compile(r'goldberg', flags=re.I),
        'morris c': re.compile(r'england(,? jr\.*)?', flags=re.I),
        'morrison c': re.compile(r'england(,? jr\.*)?', flags=re.I),
        'naomi r': re.compile(r'buchwald', flags=re.I),
        'paul l': re.compile(r'abrams', flags=re.I),
        'paul s': re.compile(r'diamond', flags=re.I),
        'peter e': re.compile(r'ormsby', flags=re.I),
        'petrese b': re.compile(r'tucker', flags=re.I),
        'richard l': re.compile(r'puglisi|bourgeois(,? jr\.*)?', flags=re.I),
        'robert e': re.compile(r'jones', flags=re.I),
        'robert f': re.compile(r'kelly', flags=re.I),
        'robert j': re.compile(r'krask', flags=re.I),
        'robert t': re.compile(r'numbers(,? ii\.*)?', flags=re.I),
        'robert w': re.compile(r'gettleman', flags=re.I),
        'ronald a': re.compile(r'guzman', flags=re.I),
        'rozella a': re.compile(r'oliver', flags=re.I),
        'stephen v': re.compile(r'wilson', flags=re.I),
        'susan e': re.compile(r'cox', flags=re.I),
        'thomas j': re.compile(r'rueter', flags=re.I),
        'timothy r': re.compile(r'rice', flags=re.I),
        'troy l': re.compile(r'nunley', flags=re.I),
        'william h': re.compile(r'walls', flags=re.I),
        'william j': re.compile(r'hibbler', flags=re.I),
        'william k': re.compile(r'sessions(,? iii\.*)?', flags=re.I)
    }

    # when the model fails in reverse, we get just a last name but it's possible first names existed before it
    # these patterns search the prior token neighborhoods
    sgl_pre_loop = {
        'jr': re.compile(r'joaquin v\.?e\.? manibusan(\s)?', flags=re.I),
        'johnson': re.compile(r'((k)?imberly )?(c\.? )?priest(\s)?', flags=re.I),
        'amy': re.compile(r'totenberg(\s)?', flags=re.I),
        'woods': re.compile(r'kay(\s)?', flags=re.I),
        'yeghiayan': re.compile(r'(\s|\b)der(\s)?', flags=re.I),
        'james': re.compile(r'-elena(\s)?', flags=re.I)
    }

    # again but with 2 tokens
    dbl_pre_loop = {
        'jr fifth': re.compile(r'joaquin v\.?( )?e\.? manibusan(\s)?', flags=re.I),
        'manibusan jr': re.compile(r'joaquin v\.?( )?e\.?( manibusan(\s)?)?', flags=re.I)
    }

    # loop of tokens where if we find this token and the the next token in the post-neighborhood matches these patterns, we know it's not an entity
    # and we also know we should void the match
    sgl_void_loop = {
        'will': re.compile(r'will(\s|\b)+(address|adjust|adopt|appear|appoint|be |consider|continue|convene|coordinate|decide|defer|determine|either|enter|establish|extend|further|handle|have|hear |hold |issue|make|necessarily|not |preside|promptly|recommend|rely|remain|review|rule|save|schedule(d)?|set|sign|take|the|upon|update)', flags=re.I),
        'nef': re.compile(r'regen', flags=re.I),
        'hai': re.compile(r'precision', flags=re.I),
        'jeff': re.compile(r'sessions', flags=re.I),
        'jefferson': re.compile(r'sessions', flags=re.I),
        'joe': re.compile(r'^s company', flags=re.I)
        }

    dbl_void_loop = {}

    # combine the singles and doubles into one dict since they are checked in the same way
    post_loop = {**sgl_post_loop, **dbl_post_loop}
    pre_loop = {**sgl_pre_loop, **dbl_pre_loop}
    void_loop = {**sgl_void_loop, **dbl_void_loop}

    # in order to determine if an entity qualifies to be checked, it needs to be lowercased and stripped of simple punctuation
    # make that a flag on the df so the pandas apply efficiency can be used    
    RDF['FLAG_CHECK'] = RDF.extracted_entity.apply(lambda x: str(x).replace('.','').replace("-","").replace(",","").strip().lower())

    # create the "hunting" dataframe of entities we will search through and check their neighborhoods
    # use dict containment as the simple pandas efficient check to filter the DF
    goodwill_hunting = RDF[RDF.FLAG_CHECK.isin(list(pre_loop.keys()) + list(post_loop.keys())+list(void_loop.keys()))]

    # begin the process of checking each row.
    # remapped will be where we store the new information
    remapped = []
    for index, row in tqdm.tqdm(goodwill_hunting.iterrows(), total=len(goodwill_hunting)):
        
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
        # transformed  entity string
        elow = row['FLAG_CHECK']

        # where the entity starts in the original text
        Enaught = ESS-FSS
        # extract the pretext 
        pretext = OT[0:Enaught]
        # extract the posttext
        posttext = OT[ESE-FSS:]

        go = True
        if elow in pre_loop:
            # if the entity qualifies to be checked
            # take the regex patern and search it on the pretext
            patt = pre_loop[elow]
            m = patt.search(pretext)
            if m:
                # if it is a match, then we need to reshuffle the entity using what matched from the pretext
                # match start + full span start gives the new entity start w.r.t to the full docket text
                new_ss = m.start()+FSS
                new_se = ESE # end is unchanged
                new_ee = OT[m.start():ESE-FSS] # string of the new entity
                new_pre5 = OT[0:m.start()] # new pretext
                new_post5 = row['extracted_post_5'] # new posttext
                # remapping data
                remapped.append(
                    {
                        **lookup,
                        'New_span_start':new_ss,
                        'New_span_end': new_se,
                        'New_pre_5': new_pre5,
                        'New_Entity': new_ee,
                        'New_post_5': new_post5
                    }
                )
                go = False
                continue  
        
        if go and elow in post_loop:
            # if the entity hasn't already been remapped (go=True still)
            # if the entity qualifies to be checked for post neighborhoods
            # grab pattern and check
            patt = post_loop[elow]
            m = patt.search(posttext)
            if m:
                # if pattern matched, respan using post tokens
                new_ss = ESS
                new_se = ESE + m.end()
                new_ee = OT[ESS-FSS:new_se-FSS]
                new_pre5 = row['extracted_pre_5']
                new_post5 = OT[new_se-FSS:]
                remapped.append(
                    {
                        **lookup,
                        'New_span_start':new_ss,
                        'New_span_end': new_se,
                        'New_pre_5': new_pre5,
                        'New_Entity': new_ee,
                        'New_post_5': new_post5
                    }
                )
                go = False
                continue   

        if go and elow in void_loop:
            # if the entity hasn't already been remapped (go=True still)
            # if the entity qualifies to be checked for void tokens
            # grab pattern and check
            patt = void_loop[elow]
            m = patt.search(elow+' '+posttext)
            if m:
                # if it is voidable, rewrite the entity to empty string. 
                # nothing else really matters since we'll throw it out
                remapped.append(
                    {
                        **lookup,
                        'New_span_start':row['Ent_span_start'],
                        'New_span_end': row['Ent_span_end'],
                        'New_pre_5': row['extracted_pre_5'],
                        'New_Entity': '',
                        'New_post_5': row['extracted_post_5']
                    }
                )
                go = False
                continue   
 
    
    # make the remappings into a DF
    REM = pd.DataFrame(remapped)
    # set a flag on these rows to indicate we should use their newly remapped entities
    REM['INCOMING'] = True

    # merge the remappings onto the original input DF, using the lookup values
    NDF = RDF.merge(REM, how='left', on = ['ucid', 'docket_index', 'full_span_start', 'Ent_span_start'])

    # log discrepancies before we overwrite them
    JU.log_message(">>Token Neighborhood Search for Corrective Entity Extraction")
    for index, row in NDF[NDF.INCOMING==True].iterrows():
        ucid = str(row['ucid'])
        di = str(row['docket_index'])
        ot = str(row['original_text'])
        old_ent = str(row['extracted_entity'])
        new_ent = str(row['New_Entity'])
        JU.log_message(f"{ucid:25} | {di} | {old_ent:25} --> {new_ent:25} | FROM: {ot}")


    # if the entity got remapped, it would be flagged as INCOMING
    # for those rows, overwrite the extracted entity information fields
    NDF.loc[NDF.INCOMING==True, 'Ent_span_start'] = NDF['New_span_start']
    NDF.loc[NDF.INCOMING==True, 'Ent_span_end'] = NDF['New_span_end']
    NDF.loc[NDF.INCOMING==True, 'extracted_pre_5'] = NDF['New_pre_5']
    NDF.loc[NDF.INCOMING==True, 'extracted_entity'] = NDF['New_Entity']
    NDF.loc[NDF.INCOMING==True, 'extracted_post_5'] = NDF['New_post_5']

    # drop my custom columns used in this function, wont need them anymore
    NDF.drop(['New_span_start', 'New_span_end','New_pre_5','New_Entity','New_post_5','FLAG_CHECK'], inplace = True, axis=1)

    return NDF

def eligibility(df, colname_to_check='extracted_entity'):
    """eligibility function that runs through a dataframe and determines what rows to filter out for exclusion in disambiguation

    Args:
        df (pandas.DataFrame]): header or entry df with entity fields and flags
        colname_to_check (str, optional): Based on the df (header or entry) which entity column should be checked for eligibility. Defaults to 'extracted_entity'.
    Returns:
        pandas.DataFrame: df of eligible sel rows for disambiguation (filtered out ineligible rows)
    """

    def _judges_in_pref(x):
        """simple substring check for the plural of judge

        Args:
            x (str/None): prefix text for an entity

        Returns:
            Bool: True if "judges" appears in the string
        """
        return 'judges' in str(x).lower()

    def _not_comma_suffix(x):
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
    
    # if the entity column we are checking is not null and this row was not a flagged exception --> eligibility is now false
    df.loc[(df[colname_to_check].isna())&(~df.is_exception), 'eligible'] = False
    # if it is now a single letter entity --> eligibility is now false (We cannot match single letter names)
    df.loc[df[colname_to_check].apply(lambda x: len(str(x))<=1), 'eligible'] = False
    # if the entity is just jr, sr, ii,iv, etc. then it's a bad grab --> eligibility is now false
    df.loc[df[colname_to_check].apply(lambda x: str(x).strip().lower() in JG.suffixes_titles), 'eligible'] = False
    # if the magistrate judge prefixy labels are infixed on the entity, these need special handling, flag as exceptions
    df.loc[df[colname_to_check].apply(lambda x: True if JG.magjud_infix.search(str(x)) else False), 'is_exception'] = True

    return df


def apply_respanning(df, is_header=False):
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

def handle_exception_entities(df, is_header=False):
    """specialty function to handle known special entity strings or to handle plural judge entities

    Args:
        df (pandas.DataFrame): full dataframe of eligible rows for disambiguation
        is_header (bool, optional): is this a df of header entities or not (entry). Defaults to False.

    Returns:
        pandas.DataFrame/list: DF of updated exception SEL rows or an empty list if no exceptions were handled
    """
    # known entities the model picks out that are weird exception cases
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
            # again after the while loop if it was entered (passed) then we have one finally entity trailing to capture
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

def prefix_categorization(df, is_header = False):
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
    def _try_patts(each):
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
    def _determine_transfer(each):
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

def string_cleaning_hierarchy(each):
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
    if m:
        each = each[0:m.start()]

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
    if m:
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

def stacked_cleaning(testname):
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