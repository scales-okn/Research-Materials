'''
File: build_judge_recusal_data.py
Author: Greg Mangan
Description:
Builds a datatable of sealed docket lines  information
'''

#Standard imports
from __future__ import division, print_function
import glob
import json
import sys
import re
import csv
from datetime import datetime
from collections import Counter
from pathlib import Path
import numpy as np

#Non-standard imports
import click
import pandas as pd
from tqdm import tqdm
tqdm.pandas()

#Local imports
sys.path.append('../..')
import support.settings as settings
import support.data_tools as dt
import support.docket_functions as dof

RLIM = 200
# Common components
WIDE_NET_TERMS = ['seal','restrict','redact','protective']
RE_WIDE_NET = r'|'.join(WIDE_NET_TERMS)

RE_ARTICLE = r'(motion|order|document|reply|response|affidavit|attachment|transcript|declaration|image|exhibit|indictment|judgement|vesrion)s?'
RE_OPT_DOCKET_NO = r'\(?\d{1,4}\)?'
RE_SEAL = r'(seal|file (\w+ )?under seal)'
RE_UNSEAL = r'(?<!\(case )unseal' # ignores (case unsealed...) which seem retroactive
RE_MOTION = r'motion( for leave)?'
pats = {

    # Motion
    'seal_motion' : rf'({RE_MOTION}|order) (by (\S+ ){{1,15}})?(to {RE_SEAL}|sealing)', #motion to seal, order unsealing, motion by X Y Z to seal
    'unseal_motion' : rf'(unsealing)|(({RE_MOTION}|order) (by (\S+ ){{1,15}})?(to unseal|unsealing))',# unsealing, motion to unseal, order unsealing, order by X Y Z unsealing

    # Grant/deny
    'seal_grant' : rf'granting ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to {RE_SEAL}',
    'seal_grant_p' : rf'granting in part ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to {RE_SEAL}',

    'seal_deny' : rf'denying (as moot |without prejudice )?({RE_OPT_DOCKET_NO} )?{RE_MOTION} to {RE_SEAL}',
    'seal_deny_p' : rf'denying in part ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to {RE_SEAL}',

    'unseal_grant' : rf'granting ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to unseal',
    'unseal_grant_p' : rf'granting in part ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to unseal',

    'unseal_deny' : rf'denying (as moot |without prejudice )?({RE_OPT_DOCKET_NO} )?{RE_MOTION} to unseal',
    'unseal_deny_p' : rf'denying in part ({RE_OPT_DOCKET_NO} )?{RE_MOTION} to unseal',

    'part_grant_deny' : r'((granted|granting),? in part and (denied|denying) in part|granting\s?/\s?denying in part)',

    # Misc
    'parenthesis' : rf'\(sealed( {RE_ARTICLE})?\)',
    'case_unsealed' : r'case unsealed', #Case unsealed as to Name X, maybe don't care about?
    'system_entry' : r'^system entry', #Want to ignore
    'sealed_article' : rf'(?<!if there are )sealed {RE_ARTICLE}',
    'e_gov': r'e-government',
    'redacted' : rf'^redaction|(un)?redacted {RE_ARTICLE}',
    'protective' : r'^(agreed |joint |notice of motion.{1,55})?(motion for )?protective order',
    'restricted' : rf'^(\*)?restrict|restricted {RE_ARTICLE}|{RE_ARTICLE} restricted|\(restricted\)',

    #Patents
    # 'discovery_control': r"(agreed|joint|unopposed) motion .{1,100} (protective order|discovery control|docket control order)"
    'discovery_control': r"((stipulated|stipulation for)|(agreed|joint|unopposed)( sealed)? motion .{1,100}) (protective order|discovery control|docket control order)",
}

def build_seal_idx():
    '''
    Build a text file with a list of indexes (from unique file df)
    of cases that contain the words 'seal/redact/protetive/restriced'
    '''
    seal_idx = []
    dff = dt.load_unique_files_df()

    for i,row in tqdm(dff.iterrows(),total=dff.shape[0]):
        case = dt.load_case(row.fpath)
        if dof.find_pattern(case.get('docket', []), RE_WIDE_NET, rlim=RLIM):
            seal_idx.append(row.name)

    with open('seal_idx.txt', 'w+') as wfile:
        wfile.writelines([f'{x}\n' for x in seal_idx])

    print(f'Succesfully built seal_idx.txt file, {len(seal_idx)} cases found\n')

def filter_cases_seal(df):
    '''
    Filter the dataframe to only have cases that have a
    reference to 'seal/redact/protetive/restriced'
    inputs:
        - df (DataFrame): the unique files DataFrame
    output:
        the filtered DataFrame
    '''

    if not Path('seal_idx.txt').exists():
        print('No seal_idx.txt file found, building...')
        build_seal_idx()

    with open('seal_idx.txt', 'r') as rfile:
        idx_lines = rfile.readlines()
        seal_idx = [x.rstrip('\n') for x in idx_lines]

    print(f'\nFiltering dataframe from {len(df):,} unique cases to {len(seal_idx):,}'\
            f' cases with reference to {"/".join(WIDE_NET_TERMS)}')

    return df.loc[seal_idx]

def tidy_variables(df):
    '''
        All the post-processing tidying of variables
    '''
    df['sealed_without_motion'] = df.system_entry | df.parenthesis

    df['unseal_grant_deny'] = df.part_grant_deny & df.text.str.contains('unseal', case=False, na=False)
    df['seal_grant_deny'] = df.part_grant_deny & ~df.text.str.contains('unseal', case=False, na=False)

    # Amalgamate "partials"
    pvars = [x for x in df.columns if x.endswith('_p')]
    for pvar in pvars:
        # The corresponding full variable
        fvar = pvar.rstrip('_p')
        # Incroporate partial matches into full matches as long as it's not grant/deny
        df[fvar] = df[fvar] | (df[pvar] & ~ df['part_grant_deny'])

    # "is granted"
    df.unseal_grant = df.unseal_grant  | (df.text.str.contains('granted', case=False, na=False) & df.text.str.contains(RE_UNSEAL, case=False, na=False) & ~df.unseal_grant_deny)
    df.seal_grant = df.seal_grant | (df.text.str.contains('granted', case=False, na=False) & ~df.text.str.contains(RE_UNSEAL, case=False, na=False) & ~df.seal_grant_deny)

    # "is denied"
    df.unseal_deny = df.unseal_deny | (df.text.str.contains('denied', case=False, na=False) & df.text.str.contains(RE_UNSEAL, case=False, na=False) & ~ df.unseal_grant_deny & ~df.text.str.contains('if there are sealed',case=False,na=False))
    df.seal_deny = df.seal_deny | (df.text.str.contains('denied', case=False, na=False) & ~df.text.str.contains(RE_UNSEAL, case=False, na=False) & ~df.unseal_grant_deny)

    # Untangle outcomes from motions
    pattern_flags = [x for x in df.select_dtypes('bool').columns if x not in ['is_mdl','is_multi']]

    motion_seal_excl_cols = [x for x in pattern_flags if x not in ['seal_motion']]
    df.seal_motion = df.seal_motion & ~np.logical_or.reduce( df[motion_seal_excl_cols].T.values )

    motion_unseal_excl_cols = [x for x in pattern_flags if x not in ['unseal_motion']]
    df.unseal_motion = df.unseal_motion & ~np.logical_or.reduce( df[motion_unseal_excl_cols].T.values )

    # drop unnecessary columns
    for col in ['system_entry', 'parenthesis', 'part_grant_deny', *pvars]:
        del df[col]
    return df

def load_sealed_df(file):
    ''' Load an output file from this script'''
    df = pd.read_csv(file)
    for col in ('is_multi', 'is_mdl'):
        df[col].fillna(False)

    # Deal with binary variables stored as 0/1
    bool_cols = [*df.columns[df.columns.get_loc('seal_motion'):] ]
    df = df.astype({col:bool for col in bool_cols})

    dff = dt.load_unique_files_df()
    df.insert(6, 'source', df.ucid.map(dff.source))
    return df

@click.command()
@click.argument('outfile', )
@click.option('--year-inp', '-y',  default=0, help="Year to run on")
@click.option('--court-inp', '-c',  default=None, help="Court to run on e.g. ilnd")
@click.option('--nos-inp', '-n',  default=None, help="Nature of suit code to run on ")
@click.option('--sample-n', '-s', default=0, help="Size of random sample to run on, default is to run on full set")
@click.option('--allow-non-matches', default=False, is_flag=True, show_default=True, help="Whether to include lines in the dataset with no indicator matches, if True it keeps them")
def main(outfile, sample_n, year_inp, court_inp ,nos_inp, allow_non_matches):
    '''
    Process all of the courts to build dataset
    '''
    # Gather the filepaths csv
    files_df = dt.load_unique_files_df()

    # Filter by relevant "seal" cases
    files_df = filter_cases_seal(files_df).copy()

    if year_inp:
        files_df = files_df[files_df.year==year_inp].copy()
        print(f'Running only on cases from {year_inp}, reduced dataset to {len(files_df):,} cases')

    if court_inp:
        files_df = files_df[files_df.court==court_inp].copy()
        print(f'Running only on cases from {court_inp}, reduced dataset to {len(files_df):,} cases')

    if nos_inp:
        files_df = files_df[files_df.nature_suit.fillna('').str.startswith( str(nos_inp) )].copy()
        print(f'Running only on cases from {court_inp}, reduced dataset to {len(files_df):,} cases')

    # If sample size specified, run on random subset
    if sample_n:
        files_df = files_df.sample(sample_n).copy()
        print(f'Running on random subset of size {sample_n:,}')

    print(f'Processing {len(files_df):,} cases...\n')

    #Build the csv file line-by-line
    out_file_name = outfile
    col_names = ['court', 'judge', 'case_id', 'ucid', 'line_ind','fpath', 'case_type',
                 'nature_suit','text', 'date_docket_line', 'days_from_filing',
                 'is_multi','is_mdl','mdl_code', *pats.keys()]
    w_count = 0  #Keep count of lines written

    with open(out_file_name, 'w+', encoding="utf-8") as wfile:
        writer = csv.writer(wfile)
        writer.writerow(col_names)

        # Iterate through all relevant files
        for i, row in tqdm(files_df.iterrows(), total=len(files_df), desc="Files Processed"):

            case = dt.load_case(row.fpath)
            if 'docket' not in case.keys():
                continue

            if type(case['docket'][0])==list:
                tqdm.write(i)
                continue

            for line_ind, line in enumerate(case['docket']):

                # Calculate days from filing, if negative (case probably transferred court/district) then skip to prevent double count
                days_from_filing = dt.difference_in_dates(line['date_filed'], case['filing_date'])
                if (days_from_filing and days_from_filing < 0):
                    continue

                if re.search(RE_WIDE_NET, line['docket_text'][:RLIM], re.I):
                        # Build the basis of the df row
                        docket_row = {
                            'court': row.court,
                            'judge': row.judge,
                            'case_id': case['case_id'],
                            'ucid': row.name,
                            'line_ind':line_ind,
                            'fpath' : row.fpath,
                            'case_type' : case.get('case_type'),
                            'nature_suit': row.nature_suit,
                            'text': line['docket_text'][:400], # Cuts out outliers with long docket lines (class action etc)
                            'date_docket_line': line['date_filed'],
                            'days_from_filing': days_from_filing,
                            'is_multi': row.get('is_multi'),
                            'is_mdl': row.get('is_mdl'),
                            'mdl_code': row.mdl_code,

                            # Pattern matches
                            **{k: bool(re.search(v, line['docket_text'][:RLIM], re.I)) for k,v in pats.items()}
                        }
                        # Write line
                        writer.writerow(docket_row.values())
                        w_count += 1

    # Read in the raw csv
    df = pd.read_csv(out_file_name, encoding="utf-8")

    df = tidy_variables(df).copy()
    indicator_vars = df.select_dtypes('bool').columns
    print(f"{allow_non_matches=}")
    if not allow_non_matches:
        print(f'Initial dataset of {w_count:,} docket lines \nDropping lines with no matches\n')
        df = df.query(" or ".join(indicator_vars)).copy()
    df.astype({col: int for col in indicator_vars}).to_csv(out_file_name, index=False)
    print(f'Successfully built dataset of {len(df):,} docket lines')

if __name__ == '__main__':
    main()
