import re
import sys
import json
import numpy as np
from pathlib import Path
from itertools import chain, groupby
from datetime import datetime

import pandas as pd
from tqdm.autonotebook import tqdm
import zlib
import base64
from hashlib import blake2b

sys.path.append(str(Path(__file__).resolve().parents[1]))
from support import settings
from support import docket_entry_identification as dei
from support import judge_functions as jf
from support import bundler as bundler
from support.core import std_path
from support import fhandle_tools as ftools

tqdm.pandas()



def sign(cookie):
    h = blake2b(digest_size=12, key=b'noacri-loc-generica')
    h.update(cookie)
    return h.hexdigest().encode('utf-8')

def make_span(match, span_offset):
    span_orig = match.span()
    return {'start': span_orig[0]+span_offset, 'end': span_orig[1]+span_offset}

def find_matching_lines(docket, phrases, exclusion_phrases=[], make_lowercase=True, beginning_only=False):
    line_ids = []
    for i, entry in enumerate(docket):
        text = entry['docket_text'].lower() if make_lowercase else entry['docket_text']
        if beginning_only:
            if not any((x == text[:len(x)] for x in exclusion_phrases)) and any((x == text[:len(x)] for x in phrases)):
                line_ids.append(i)
        else:
            try:
                if not any((x in text for x in exclusion_phrases)) and any((x in text for x in phrases)):
                    line_ids.append(i)
            except (AttributeError, TypeError, KeyError): # retained from Adam's code
                pass
    # print(line_ids)
    return line_ids

def has_edges(docket):
    for entry in docket:
        if 'edges' in entry and bool(entry['edges']):
            return True
    return False

def get_edges_to_target(target_ind, edges):
    matches = []
    if edges:
        for edge in edges:
            if bool(edge) and edge[1] == target_ind:
                matches.append(edge)
    return matches



def create_docket_core(case_id):
    '''
    Take a case id 1:15-cr-23422 or 1-15-cr-23422 and turn it into a docket core
    that can be used with the fjc data
    '''
    if ':' in case_id:
        case_parts = case_id.split(':')[1].split('-')
        dcore = case_parts[0] + case_parts[2]
    else:
        id_parts = case_id.split('-')
        dcore = id_parts[1] + id_parts[3]
    return dcore

def nos_number_extractor(nos_text):
    '''
    Given the Nature of Suit text, attempts to return the number
    '''
    import numpy as np
    try:
        first_elem = int(nos_text.split(' ')[0].strip(':'))
        return first_elem
    except:
        return np.nan

def transform_date_slash_to_dash(slashdate):
    '''
    Take a m/y/d and transform to y-m-d
    '''
    try:
        m, d, y = slashdate.split('/')
        return '-'.join([y, m, d])
    except AttributeError:
        return slashdate

def load_recap_dump(indir):
    '''
    Loads a recap dump and transforms it into a dataframe
    input: indir, json folder
    output: dataframe
    '''
    import glob, json, sys
    import pandas as pd
    sys.path.append('..')
    import support.fhandle_tools as ftools

    data = {'filedate':[], 'court':[], 'nos':[], 'recap_case_id':[], 'id':[], 'termdate':[],
            'docket_core':[], 'case_name':[]}
    for jfhandle in glob.glob(indir + '*json'):
        jfile = json.load( open(jfhandle) )
        #IDs
        data['recap_case_id'].append( jfile['docket_number'])
        data['id'].append( jfile['id'] )
        data['docket_core'].append( jfile['docket_number_core'] )
        data['case_name'].append( jfile['case_name'] )
        #Case data
        data['court'].append( jfile['court'] )
        data['nos'].append( jfile['nature_of_suit'] )
        #Dates
        data['filedate'].append( jfile['date_filed'] )
        data['termdate'].append( jfile['date_terminated'] )
    #Convert to the dataframe
    df = pd.DataFrame(data)
    df['case_id'] = df.recap_case_id.apply( ftools.clean_case_id )
    return df

def load_noacri_data(indir):
    '''
    Loads a downloaded json data folder into a dataframe
    input: indir, json folder
    '''
    import glob, json
    import pandas as pd

    noacri_data = {'noacri_case_id':[], 'court':[], 'nos':[], 'filedate':[], 'termdate':[]}
    for jfhandle in glob.glob(indir + '/*json'):
        tfile = json.load( open(jfhandle) )
        #Pull in the data
        noacri_data['noacri_case_id'].append(tfile['case_id'])
        #Get the download court
        if 'download_court' in tfile:
            dlcourt = tfile['download_court']
        else:
            dlcourt = court_trans[jfhandle.split('/json/')[0].split('/')[-1]]
        noacri_data['court'].append(dlcourt)
        #Nature of suit, filedate
        noacri_data['nos'].append(tfile['nature_suit'])
        noacri_data['filedate'].append( transform_date_slash_to_dash(tfile['filing_date']) )
        noacri_data['termdate'].append( transform_date_slash_to_dash(tfile['terminating_date']) )
    #Load it into a dataframe
    noacri_df = pd.DataFrame(noacri_data)
    #Now get the original case id and docket core
    noacri_df['case_id'] = noacri_df.noacri_case_id.apply(create_case_id_colon)
    noacri_df['docket_core'] = noacri_df.noacri_case_id.apply(create_docket_core)
    #Get the nos number
    noacri_df['nos_num'] = noacri_df.nos.apply(nos_number_extractor)
    return noacri_df

def load_query_file(query_file, main_case=True):
    '''
    Loads and cleans a query search result
    input: query_file, html search result,
           main_cases - T/F, restrict to main cases or not
    output: query_df, query dataframe with clean
    '''
    import pandas as pd
    import support.fhandle_tools as ftools

    #Load the dataframe, get rows, and drop the NA dead rows
    dfset = pd.read_html(query_file)
    df = dfset[0]
    df.columns = ['query_case_id', 'name', 'details']
    df.dropna(inplace=True)
    #Pull the state court
    court_abbrev = query_file.split('/')[-1].split('_')[0]
    #Clean up the case ids
    df['case_id'] = df.query_case_id.apply(ftools.clean_case_id)
    df['main_case'] = df.query_case_id.apply(lambda x: ftools.main_limiter(x) )
    df['case_type'] = df.query_case_id.apply(lambda x: x.split('-')[1])
    #Get the other attributes
    df['court'] = df.query_case_id.apply(lambda x: court_abbrev)
    # df['filedate'] = df.details.apply(ftools.extract_file_date)
    # df['termdate'] = df.details.apply(ftools.extract_term_date)
    #Restrict to the primary case only
    if main_case==True:
        maindf = df[df.main_case==True]
    else:
        maindf = df
    return maindf



def line_detagger(string):
    '''
    Removes HTML tags from text (for use in parse_pacer.py)
    input: string (text to clean)
    '''
    if string != None:
        while string.count('***') > 1: # sometimes Pacer does, e.g., "***DO NOT FILE IN THIS CASE***"
            string = string.split('***')[0] + '***'.join(string.split('***')[2:])
        return re.sub('\<[^<]+?>', '', string).strip('<>?! ') or None
    else:
        return None

def line_cleaner(string):
    '''
    Cleans up messy text (for use in parse_pacer.py)
    input: string (text to clean)
    '''
    if string != None:
        string = string.replace('&amp;','&').replace('&nbsp;',' ').replace('\\\'','\'').replace('\\n','').replace('\\t','')
        string = string.lstrip(')').rstrip('(')
        string = ' '.join(string.split()).strip()
        return string or None
    else:
        return None

def parse_extra_info(chunk, from_recap=False):
    '''
    Helper method for remap_recap_data() as well as process_entity_and_lawyers() in parse_pacer.py
    Inputs:
        - chunk (str): a chunk of HTML text (split_chunk[0] when called from process_entity_and_lawyers())
        - from_recap (bool): whether the chunk came from a RECAP field (if it did, it's already been stripped of HTML tags)
    Outputs:
        - extra_info (str): any info contained in the italicized text block below the party name in PACER
        - terminating_date (str): a parsed-out date when the italicized text block specifies the 'TERMINATED' date
    '''
    def _replace_breaks_with_spaces(string, from_recap):
        if string != None:
            breaks_re = '\n' if from_recap else '<br>|<br/>|<br />'
            return re.sub(breaks_re, ' ', string)
        else:
            return None

    delim = 'TERMINATED: '
    extra_info = _replace_breaks_with_spaces(chunk,from_recap) if from_recap else ( line_detagger(_replace_breaks_with_spaces(
        '<i>'.join(chunk.split('<i>')[1:]).split('</td>')[0],from_recap)) if '<i>' in chunk else None )
    terminating_date = None
    while extra_info and delim in extra_info: # this loop will take whichever terminating date is encountered last in the italicized string
        terminating_date = delim.join(extra_info.split(delim)[1:])
        if len(extra_info.split(delim)[0]) > 0 or ' ' in terminating_date: # is there any extra info besides the terminating date?
            extra_info = extra_info.split(delim)[0] + ' '.join(terminating_date.split(' ')[1:])
            terminating_date = terminating_date.split(' ')[0]
        else:
            extra_info = extra_info.split(terminating_date)[1]

    if extra_info and (len(extra_info) == 0 or extra_info == 'and'):
        extra_info = None
    extra_info = line_cleaner(extra_info)
    extra_info = [extra_info] if extra_info else None # convert extra_info to list (like role name) to handle parties listed w/multiple titles
    return extra_info, line_cleaner(terminating_date)

def update_party(party_dict, name, new_party):
    '''
    Update a party dictionary with info about a newly parsed party, merging this info with existing info about the same party if necessary
    Inputs:
        - party_dict (dict): one of the five party dictionaries in the SCALES parse schema (plaintiff, defendant, bk_party, other_party, misc)
        - name (str): the name of the person/corporation/etc comprising this party, to be used as a key into party_dict
        - new_party (dict): the info we want to add, which (in this implementation) will be a dict containing 'roles', 'is_pro_se', 'counsel', 'terminating_date', & 'extra_info'
    Outputs:
        - party_dict (dict): the passed-in party dictionary updated with the new info
    '''
    if name not in party_dict.keys():
        party_dict.update(new_party)
    else:
        old = party_dict[name]
        new = new_party[name]

        old['roles'] += new['roles']
        old['is_pro_se'] = (old['is_pro_se'] or new['is_pro_se'])

        if not bool(old['counsel']):
            old['counsel'] = new['counsel']
        else:
            old['counsel'].update(new['counsel'] or {})

        date_delta = difference_in_dates(new['terminating_date'], old['terminating_date'])
        if date_delta and date_delta>0:
            old['terminating_date'] = new['terminating_date']

        if not bool(old['extra_info']):
            old['extra_info'] = new['extra_info']
        elif bool(new['extra_info']):
            for ei in new['extra_info']:
                if ei not in old['extra_info']:
                    old['extra_info'].append(ei)

    return party_dict

def remap_recap_data(recap_fpath=None, rjdata=None):
    '''
    Given a recap file, normalizes the process
    * recap_fpath
    output:
    *jdata
    '''

    def standardize_date(tdate):
        '''y-m-d to m/d/y'''
        if not tdate:
            return None
        try:
            y,m,d = tdate.split('-')
            return '/'.join([m, d, y])
        except AttributeError:
            return None

    def get_recap_docket(court, docket_entries):
        '''
        Remap the recap docket
        Inputs:
            - court (str): the court abbreviation
            - docket_entries (list): the value from the 'docket_entries' key in recap
        Output:
            - list of docket entries same as parsed format
        '''

        def get_doc_links(row):
            ''' Get links to documents (most rows don't have attachments, some do)'''
            documents = {}
            for doc in row.get('recap_documents', []):

                # Recap encodes document_type=1 for line doc and document_type=2 for attachment
                if doc.get('document_type') == 1:
                    ind = 0
                elif doc.get('document_type') == 2 and doc.get('attachment_number', False):
                    ind = int(doc['attachment_number'])
                else:
                    # Fallback option, use doc_id
                    ind = f"_{doc['pacer_doc_id']}"

                document_data = {
                    'url': ftools.get_pacer_url(court,'doc_link') + '/' + str(doc['pacer_doc_id']), 'span': {},
                    **{f"recap_{k}": doc[k] for k in ('page_count','filepath_ia', 'filepath_local', 'description', 'is_available')}
                }
                documents[ind] = document_data
            return documents

        rows = [
            {'date_filed': standardize_date(row['date_filed']),
             'ind': row['entry_number'],
             'docket_text': row['description'],
             'documents': get_doc_links(row),
             'edges': None
            }
            for row in docket_entries
        ]
        return rows

    #Load the data
    try:
        if not rjdata:
            recap_fpath = std_path(recap_fpath)
            jpath = settings.PROJECT_ROOT / recap_fpath
            rjdata = json.load(open(jpath), encoding="utf-8")
    except:
        print(f"Error loading file {recap_fpath}")
        return {}
    #Get the termination date
    tdate = standardize_date(rjdata['date_terminated'])
    case_status = 'closed' if tdate else 'open'

    # parties/counts
    case_type = rjdata['docket_number'].split('-')[1]
    is_cr = bool(case_type == 'cr')
    parties = {'plaintiff':{}, 'defendant':{}, 'bk_party':{}, 'other_party':{}, 'misc':{}}
    pending_counts, terminated_counts, complaints = ({},{},{}) if is_cr else (None,None,None)

    for party in rjdata['parties']:
        name = party['name']
        extra_info, terminating_date = parse_extra_info(party['extra_info'], from_recap=True)

        # lawyer dictionary
        lawyer_dict = {}
        is_pro_se = 'PRO SE' in str(party)
        if not is_pro_se and 'attorneys' in party.keys():
            for lawyer in party['attorneys']:
                is_lead = 'LEAD ATTORNEY' in str(lawyer)
                is_pro_hac = 'PRO HAC VICE' in str(lawyer)
                info = lawyer['contact_raw']
                office = info.split('\n')[0]
                addtl_info = {}
                if 'Designation' in info:
                    addtl_info['designation'] = re.search('Designation: ([A-Za-z \'\-]{1,100})', info).group(1)
                if any(x in info for x in ['Trial Bar Status', 'Trial bar Status']): # this seems to appear only in ILND
                    addtl_info['trial_bar_status'] = re.search('tatus: ([A-Za-z \'\-]{1,100})', info).group(1)
                elif 'Bar Status' in info:
                    addtl_info['bar_status'] = re.search('tatus: ([A-Za-z \'\-]{1,100})', info).group(1)
                lawyer_dict[lawyer['name']] = {'office':office,'is_lead_attorney':is_lead,'is_pro_hac_vice':is_pro_hac,'additional_info':addtl_info}

        # role titles, terminating date, extra info
        with open(settings.ROLE_MAPPINGS, 'r') as f:
            mappings = json.load(f)
        for pt in party['party_types']:
            role = pt['name']

            # sometimes these fields vary from party name to party name
            local_extra_info, local_terminating_date = parse_extra_info(pt['extra_info'], from_recap=True)
            extra_info = extra_info+local_extra_info if extra_info and local_extra_info and (
                local_extra_info[0] not in extra_info) else local_extra_info or extra_info
            date_delta = difference_in_dates(local_terminating_date, terminating_date)
            terminating_date = local_terminating_date if date_delta and date_delta>0 else local_terminating_date

            if role not in mappings.keys():
                party_title = role
                party_type = 'misc'
            else:
                party_title = mappings[role]['title']
                party_type = mappings[role]['type']
            dicti = {name: {'roles':[party_title], 'counsel':(lawyer_dict or None), 'is_pro_se':is_pro_se, 'terminating_date':terminating_date, 'extra_info':extra_info}}
            parties[party_type] = update_party(parties[party_type], name, dicti)

        # criminal counts
        if is_cr:
            criminal_counts = [count for pt in party['party_types'] for count in pt['criminal_counts']]
            pending, terminated = [], []
            for cc in criminal_counts:
                cc_parsed = [cc['name'], cc['disposition']]
                if 'dismissed' in cc['disposition'].lower(): # pretty coarse heuristic, maybe update later
                    terminated.append(cc_parsed)
                else:
                    pending.append(cc_parsed)
            if len(pending) > 0:
                pending_counts[name] = pending
            if len(terminated) > 0:
                terminated_counts[name] = terminated

    # Convert the data
    fdata = {
        'bankruptcy_parties':parties['bk_party'],
        'case_flags': '',
        'case_id': ftools.clean_case_id(rjdata['docket_number']),
        'case_name': rjdata['case_name'],
        'case_status': case_status,
        'case_type': case_type,
        'cause': rjdata['cause'],
        'complaints': complaints,
        'defendants': parties['defendant'],
        'docket': get_recap_docket(rjdata['court'], rjdata['docket_entries']) ,
        'download_court': rjdata['court'],
        'filing_date': standardize_date(rjdata['date_filed']),
        'header_case_id':None,
        'judge': rjdata['assigned_to_str'],
        'jurisdiction': rjdata['jurisdiction_type'],
        'jury_demand': rjdata['jury_demand'],
        'lead_case_id':None,
        'misc_participants':parties['misc'],
        'monetary_demand':None,
        'nature_suit': rjdata['nature_of_suit'],
        'other_court':None,
        'other_parties':parties['other_party'],
        'pacer_case_id':rjdata['pacer_case_id'],
        'pending_counts':pending_counts,
        'plaintiffs': parties['plaintiff'],
        'referred_judge': rjdata['referred_to_str'],
        'terminated_counts':terminated_counts,
        'terminating_date': tdate,
        'source':'recap',
        'ucid': ucid(rjdata['court'], ftools.clean_case_id(rjdata['docket_number'])),
        # MDL/Multi keys
        **{k:None for k in ['mdl_code', 'mdl_id_source','is_mdl', 'is_multi']},
        # Billing keys
        **{k:None for k in ['billable_pages', 'cost','n_docket_reports',]},
        # Scraper things
        **{k:None for k in ['download_timestamp', 'download_url', 'docket_available', 'member_case_key']}
    }
    return fdata



def year_check(dstring, year_pull):
    '''
    Checks to see if the file year is the same as year_pull specified
    '''
    #Do the year check
    if year_pull != None:
        try:
            year_diff = int(dstring.split('/')[-1]) - year_pull
        except:
            year_diff =  -9999
    else:
        year_diff = 0
    return year_diff

def generate_unique_filepaths(outfile=None, nrows=None):
    '''
    Create a list of unique filepaths for all case json in the PACER folder and export to .csv
    Inputs:
        - outfile (str or Path) - the output file name (.csv) relative to the project root if none doesn't output
        - nrows (int) - no. of cases to use (for testing)
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    import pandas as pd
    tqdm.pandas()

    case_jsons = [court_dir.glob('json/*.json') for court_dir in settings.PACER_PATH.glob('*')
                    if court_dir.is_dir()]

    file_iter = chain(*case_jsons)

    df = convert_filepaths_list(file_iter=file_iter, nrows=nrows)

    #Write the file
    if outfile:
        df.to_csv(std_path(outfile))

    return df

def unique_filepaths_updater(table_file, update_iter, outfile=None, force=False):
    '''
    A method to update a unique filepaths file

    Inputs:
        - table_file (str or Path): the .csv file with the table to be updated
        - update_iter (iterable): iterable over the new filepaths to check (of json files)
        - outfile (str or Path): path to output the new file to
        - force (bool): if true, will recalculate and overwrite rows for all cases in update_iter

    Output:
        DataFrame of file metadata (also output to outfile if output=True)
    '''
    def _clean_fpath_(x):
        p = std_path(x)
        if settings.PROJECT_ROOT.name in p.parts:
            return str(p.relative_to(settings.PROJECT_ROOT))
        else:
            return str(p)

    # Load the original table (but not using load_unique_files_df loader, as we want to work with dates as strings)
    df = pd.read_csv(table_file, index_col=0)

    # Get the fpaths of the ones to be added to the table
    to_update = [_clean_fpath_(x) for x in update_iter]
    if not force:
        # Filter down to ones that aren't already in df
        new_fpaths_series = pd.Series(to_update)
        to_update = new_fpaths_series[~new_fpaths_series.isin(df.fpath)].to_list()

    new_df = convert_filepaths_list(file_iter=to_update)

    # Concatenate to make table for output
    out_df = pd.concat((df, new_df))

    # Drop the original lines if force is true
    if force:
        out_df = outdf[~outdf.index.duplicated(keep='last')]

    if outfile:
        out_df.to_csv(outfile)

    return out_df




def convert_filepaths_list(infile=None, outfile=None, file_iter=None, nrows=None):
    '''
    Convert the list of unique filepaths into a DataFrame with metadata and exports to csv

    Inputs:
        - infile (str or Path) - the input file, relative to the project root, expects csv with an 'fpath' column
        - outfile (str or Path) - the output file name (.csv) relative to the project root, if None doesn't write to file
        - file_iter (iterable) - list of filepaths, bypasses infile and reads list directly
        - nrows (int) - number of rows, if none then all
    Outputs:
        DataFrame of file metadata (also output to outfile if output=True)
    '''

    # Map of keys to functions that extract their values (avoids keeping separate list of keys/property names)
    #c: case json, f: filepath
    dmap = {
        'court': lambda c,f: c['download_court'] if is_recap(f) else Path(f).parent.parent.name,
        'year': lambda c,f: c['filing_date'].split('/')[-1],
        'filing_date': lambda c,f: c['filing_date'],
        'terminating_date': lambda c,f: c.get('terminating_date'),
        'case_id': lambda c,f: ftools.clean_case_id(c['case_id']),
        'case_type': lambda c,f: c['case_type'],
        'nature_suit': lambda c,f: dei.nos_matcher(c['nature_suit'], short_hand=True) or '',
        'judge': lambda c,f: jf.clean_name(c.get('judge')),
        'recap': lambda c,f: 'recap' in c['source'],
        'is_multi': lambda c,f: c['is_multi'],
        'is_mdl': lambda c,f: c['is_mdl'],
        'mdl_code': lambda c,f: c['mdl_code'],
        'has_html': lambda c,f: 'pacer' in c['source'],
        'source': lambda c,f: c['source']

    }

    properties = list(dmap.keys())

    def get_properties(fpath):
        ''' Get the year, court and type for the case'''
        case = load_case(fpath)
        return tuple(dmap[key](case,fpath) for key in properties)

    # Load fpaths from list or else from infile
    if file_iter is not None:
        if nrows:
            paths = [next(file_iter) for _ in range(nrows)]
        else:
            paths = file_iter
        # Build dataframe of paths relative to PROJECT_ROOT
        df = pd.DataFrame(paths, columns=['fpath'])
    elif infile:
        # Read in text file of filepath names
        df = pd.read_csv(std_path(infile), names=['fpath'], nrows=nrows)
    else:
        raise ValueError("Must provide either 'infile' or 'file_list'")

    # Convert filepath to relative format
    def _clean_fpath_(x):
        p = std_path(x)
        if settings.PROJECT_ROOT.name in p.parts:
            return str(p.relative_to(settings.PROJECT_ROOT))
        else:
            return str(p)

    df.fpath = df.fpath.apply(lambda x: _clean_fpath_(x))
    # Build year and court cols

    # Only do progress bar if it's more than 1
    if len(df) > 1:
        print('\nExtracting case properties...')
        properties_vector = df.fpath.progress_map(get_properties)
    else:
        properties_vector = df.fpath.map(get_properties)
    prop_cols = zip(*properties_vector)

    # Insert new columns, taking names from ordering of properties
    for i, new_col in enumerate(prop_cols):
        df[properties[i]] = new_col

    # Set UCID index
    df['ucid'] = ucid(df.court, df.case_id)#, series=True) #Not sure why this was here
    df = df.set_index('ucid')

    # Judge matching
    jmap = jf.unique_mapping(df.judge.unique())
    df.judge = df.judge.map(jmap)

    columns = properties.copy()
    columns.insert(2,'fpath')

    if outfile:
        df[columns].to_csv(std_path(outfile))
    return df[columns]

def load_docket_filepaths(court_pull = 'all', year_pull = None):
    '''
    input:
        * court - the court to load data for, "all" does everything, otherwise it's the abbreviation
        * year_pull (integer)- the year to pull data for
    output:
        * list of filepaths
    '''
    import pandas as pd

    df = pd.read_csv(settings.UNIQUE_FILES_CSV)
    if court_pull != 'all':
        df = df[df.court==court_pull].copy()
    if year_pull:
        year_pull = int(year_pull)
        df = df[df.year==year_pull].copy()

    return df.fpath.values

def load_dockets(court_pull = 'all', year_pull = None):
    '''
    input:
        * court - the court to load data for, "all" does everything, otherwise it's the abbreviation
        * year_pull (integer)- the year to pull data for
    output:
        * dockets - list of jsons for case files
    '''
    import json, sys

    paths = load_docket_filepaths_unique(court_pull, year_pull)
    dockets = []
    for fpath in paths:
        jdata = json.load(open(settings.PROJECT_ROOT + fpath, encoding="utf-8"))
        dockets.append(jdata)
    return dockets

def load_unique_files_df(file=settings.UNIQUE_FILES_TABLE, fill_cr=False, **kwargs):
    '''
        Load the unique files dataframe

        Inputs:
            - xarelto (bool): whether to include xarelto cases
            - fill_cr (bool): whether to fill nature_suit for all criminal cases to criminal
    '''

    dff = pd.read_csv(file, index_col=0, **kwargs)

    for col in ['filing_date','terminating_date']:
        dff[col] = pd.to_datetime(dff[col], format="%m/%d/%Y")

    # Set nature of suit for all criminal cases to 'criminal'
    if fill_cr and ('nature_suit' in dff.columns) :
        cr_idx = dff[dff.case_type.eq('cr')].index
        dff.loc[cr_idx, 'nature_suit'] = 'criminal'

    return dff

def get_case_counts(gb_cols=[], qstring=None):
    '''
    Returns a dataframe of the counts of cases grouped by year, court and any additional columns
    inputs:
        - cols (list) - a list of strings of column names from unique filepaths table to group by
    '''
    import pandas as pd

    df = load_unique_files_df().query(qstring) if qstring else load_unique_files_df()
    return df.groupby(['year', 'court', *gb_cols], dropna=False).size().reset_index(name='case_count')

def load_case(fpath, html=False, recap_orig=False):
    '''
    Loads the case given its filepath

    input:
        fpath (string/Path): a path relative to the project roots
        html (bool): whether to return the html (only works for Pacer, not Recap)
        recap_orig (bool): whether to return the original recap file, rather than the mapped

    output:
        the json of the case (or html if html is True)
    '''
    # Standardise across Windows/OSX and make it a Path object
    fpath = std_path(fpath)

    # Absolute path to json file
    if settings.PROJECT_ROOT.name in fpath.parts:
        # Treat as absolute if its not relative to project root
        jpath = fpath
    else:
        jpath = settings.PROJECT_ROOT / fpath

    if html:
        hpath = get_pacer_html(jpath)
        if hpath:
            return str( open(settings.PROJECT_ROOT / hpath, 'rb').read() )
        else:
            raise FileNotFoundError('HTML file not found')
    else:
        jdata = json.load( open(jpath, encoding="utf-8") )
        jdata['case_id'] = ftools.clean_case_id(jdata['case_id'])

        # TODO: recap orig- include recap orig
        # if recap_orig:
        #     if 'recap' in jdata['source']:
        #         # recap_id = #This needs to be in case json
        #         return json.load(open(settings.RECAP_PATH/f"{recap_id}.json", encoding="utf-8"))

        return jdata

def get_pacer_html(jpath):
    ''' Get a pacer html from the json filepath'''
    jpath = Path(str(jpath))
    hpath = Path(str(jpath).replace('json', 'html'))
    if hpath.exists():
        return hpath

def difference_in_dates(date_x, date_0):
    '''
    Calculate the the number of days after day_0 something on day_x occurs from docket text date strings
    inputs
        - date_x(string): the date of interest from docket
        - day_0(string): the baseline date from docket (e.g. filing date)

    return
        (int) no. of days
    '''

    try:
        dt_x = datetime.strptime(date_x,'%m/%d/%Y')
        dt_0 = datetime.strptime(date_0,'%m/%d/%Y')

        return (dt_x - dt_0).days

    except:
        return None

# Generate unique case id
def ucid(court, case_id, clean=False):
    '''
    Generate a unique case id (ucid)

    Inputs:
        - court (str or Series): court abbreviation
        - case_id (str or Series): either colon or hyphen format, will be standardised
        - clean (bool): whether the case_id is already clean (speeds up calculation)
    Output:
        (str or Series) like 'nced;;5:16-cv-00843'
    '''
    if type(case_id)==pd.Series:
        if not clean:
            return court + ';;' + case_id.map(ftools.clean_case_id)
        else:
            return court + ';;' + case_id
    else:
        if not clean:
            return f"{court};;{ftools.clean_case_id(case_id)}"
        else:
            return f"{court};;{case_id}"

get_ucid = ucid

def ucid_from_scratch(court, office, year, case_type, case_no):
    ''' Generate a ucid from all base elements (year is 2-digit)'''
    if type(court)==pd.Series and type(case_no)==pd.Series:
        case_id = office + ':' + year + f"-{case_type}-" + case_no
    else:
        case_id = f"{office}:{year}-{case_type}-{case_no}"
    return ucid(court, case_id)

def get_ucid_weak(ucid):
    '''
    Get a weakened ucid with the office removed

    Inputs:
        - ucid (str): a correctly formed ucid
    Output:
        (str) a weakened ucid e.g. 'ilnd;;16-cv-00001'
     '''

    if type(ucid)==pd.Series:
        return ucid.str.replace(rf"{ftools.re_com['office']}:", '')
    else:
        return re.sub(rf"{ftools.re_com['office']}:", '', ucid)

def parse_ucid(ucid):
    '''
    Split a ucid back into its component parts
    Inputs:
        -ucid (str or Series)
    Output:
        dict (if ucid is a str) or DataFrame (if ucid is a Series)
    '''
    re_ucid = "(?P<court>[a-z]{2,5});;(?P<case_no>.*)"

    if type(ucid)==pd.Series:
        return ucid.str.extract(re_ucid)
    else:
        match = re.match(re_ucid, ucid)
    if match:
        return match.groupdict()

def bundle_from_list(file_list, name, mode='ucid', notes=None, overwrite=False):
    '''
    Bundle files together from list of files
    Inputs:
        - file_list (arr of strings)
        - name (str): name of directory to pass to bundler method
        - mode ('ucid' or 'fpath'): whether the list contains ucids or fpaths
    '''

    df = load_unique_files_df()
    if mode=='ucid':
        df = df[df.index.isin(file_list)]
    elif mode=='fpath':
        df = df[df.fpath.isin(file_list)]

    bundle_from_df(df, name, notes, overwrite)

def bundle_from_df(df, name, notes=None, overwrite=False):
    '''
        Bundle up a collection of files
        Inputs:
            - df (DataFrame): any dataframe with a fpath column
            - name (str): name of directory to bundle into (will be put in /data/{name})
    '''
    bundler.bundler(df,name, notes, overwrite)

def is_recap(fpath):
    '''Determine if a case is a recap case based on the filepath'''
    return 'recap' in std_path(fpath).parts

def group_dockets(docket_fpaths, court=None, use_ucids=None, summary_fpaths=None):
    '''
    Group filepaths to docket htmls by ucid (will be multiple for docket updates)

    Inputs:
        - docket_fpaths (list of str): list of docket filepaths
        - court (str): the court abbreviation, if none will infer from directory structure
        - use_ucids (list-like): a list of ucids to include. If supplied, will filter out
            any ucids that are not in this list from the output
        - summary_fpaths (list): list of str or Path objects of summary fpaths to connect to docket_fpaths
    Outputs:
        - a list of dicts grouped by ucid with
            - 'docket_paths': tuples of paths
            - 'summary_path': a single Path (may be of type float('nan') if no summary)

    '''
    df = pd.DataFrame(docket_fpaths, columns=['fpath'])

    # Sort on filename, assumes that this builds the correct order for updated filenames
    df.sort_values('fpath', inplace=True)

    # Get the ucid column
    if court==None:
        #Infer from path
        court = df.fpath.iloc[0].parents[1].name

    df['ucid'] = df.fpath.apply(lambda x: ftools.filename_to_ucid(x, court=court))

    # Filter which ones to use
    df['use'] = True if type(use_ucids) is type(None) else df.ucid.isin(use_ucids)

    df['summary_fpath'] = pd.Series(dtype=str)

    if summary_fpaths:

        df_summaries = pd.DataFrame(summary_fpaths, columns=['fpath'])
        df_summaries.sort_values('fpath', inplace=True)
        df_summaries['ucid'] = df_summaries.fpath.apply(lambda x: ftools.filename_to_ucid(x, court=court))

        # Assumes no duplication of summaries
        summary_map = df_summaries.set_index('ucid')['fpath']
        df['summary_path'] = df.ucid.map(summary_map)

    # cases = df[df.use].groupby('ucid')['fpath'].apply(tuple).to_list()
    cases = df[df.use].groupby('ucid').agg(docket_paths=('fpath',tuple), summary_path=('summary_path','first')).to_dict('records')
    return cases

# n.b.: compress_data and decompress_data may no longer be needed after updates to parse_pacer.py (11/2/20)
def compress_data(data):
    '''Compress complex data into a JSON-serializable format (written to generate the 'member_cases' field in JSONs from parse_pacer.py)'''
    return base64.b64encode(zlib.compress(json.dumps(data).encode('utf-8'),9)).decode('utf-8')

def decompress_data(data):
    '''Reverse the effects of compress_data'''
    return json.loads(zlib.decompress(base64.b64decode(data.encode('utf-8'))).decode('utf-8'))

def pacer2scales_ind_all(docket):
    '''
    Get a mapping from pacer index (the 'ind' value in docket, and the # column in table),
    to scales index (the order from the case json docket array)

    Note: this is mainly for testing/exploring purposes.
    There is no guarantee a well-defined mapping exists from pacer ind to scales ind
    e.g. two docket lines neither of which have a pacer ind (both blank in # column)

    Inputs:
        - docket (list): the 'docket' value from a case json
    Output:
        (dict) mapping from pacer index (str) to scales index (int)
        e.g. { '1': 0, '2':1, '3':2, '7':3, ...}
    '''
    return dict( (pacer,scales) for scales, pacer in enumerate([x['ind'] for x in docket]))

def lookup_docket_pacer_ind(ucid, pacer_ind, dff):
    '''
    Helper function to quickly lookup docket lines based on the pacer ind (the # column in docket)
    Note: this is mainly for testing/exploring purposes, see note in pacer2scales_ind_all

    Inputs:
        - ucid (str): the case ucid
        - pacer_ind (int,str or list): throw anything at me! can take int, str or list of neither
        - dff (pd.DataFrame): dataframe of files, either the unique files dataframe,
                        or any subset of it that includes the ucid and fpath
    Output:
        (dict) where the key is the scales ind and the value is the entry from the case docket array

        e.g. lookup_docket_pacer_ind(ucid, [27,28])
            -> { 24: {'ind': '27', 'date_filed': ___, 'docket_text': ___, ... },
                 25: {'ind': '28', 'date_filed': ___, 'docket_text': ___, ... }
               }
    '''

    #Handle either a list or single value for ind
    ind_arr = pacer_ind if type(pacer_ind)==list else [pacer_ind]
    ind_arr = [str(x) for x in ind_arr]

    docket = load_case(dff.loc[ucid].fpath)['docket']
    pacer2scales = pacer2scales_ind_all(docket)
    keepers = [pacer2scales[ind] for ind in ind_arr]

    return {scales_ind:line for scales_ind, line in enumerate(docket) if scales_ind in keepers}

def case_link(ucids, ext='html' , incl_token=False, server_index=-1):
    '''
    Generate clickable case links for a set of ucids, from within a jupyter notebook
    Inputs:
        - ucids (iterable or str): iterable of ucids to create links for (if you supply a single ucid it will case to a list)
        - ext ('html' or 'json')
        - incl_token (bool): whether to include the token query string (e.g. if you're switching between browsers)
        - server index (int): if you have multiple jupyter servers running, specify which one, defaults to last
    '''
    from notebook import notebookapp
    from IPython.core.display import display, HTML
    # Get server details
    server = list(notebookapp.list_running_servers())[server_index]
    notebook_dir = Path(server['notebook_dir'])
    token = server['token'] if incl_token else None

    # Coerce to iterable if a single ucid supplied
    if type(ucids) is str:
        ucids = (ucids,)

    # Iterate over all ucids
    for ucid in ucids:
        path = ftools.get_expected_path(ucid, ext=ext)

        if not path.exists():
            print(f'{ucid}: No such file exists at {path}')
            continue

        rel_path = path.relative_to(notebook_dir)
        base_url = f"http://{server['hostname']}:{server['port']}/view/"
        full_url = base_url + str(rel_path)

        if incl_token:
            full_url +=  f'?token={token}'

        link = f'{ucid}: <a target="_blank" href="{full_url}">{full_url}</a>'
        display(HTML(link))