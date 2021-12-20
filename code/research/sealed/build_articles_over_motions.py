import re
import sys
from pathlib import Path

import click
import numpy as np
import pandas as pd
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[2]))
from support import settings
from support import data_tools as dtools
from build_sealed_data import load_sealed_df

# source: localpatentrules.com
with open('plr_districts.txt', 'r') as rfile:
    PLR_DISTRICTS = [x.strip() for x in rfile.readlines()]

PATENT_NOS = '830: patent property rights'

def build_df_dur(df, year=2016):
    '''
    Build table with relevant case duration data (need to open each case to verify latest date)
    Inputs:
        - df (pd.DataFrame): the main docketline level dataframe of sealed data
        - year (int): the year

    '''

    dff = dtools.load_unique_files_df()
    # Get the subset of cases from unique files table that are patent cases
    cases_pat = dff[dff.nature_suit.eq(PATENT_NOS) & dff.year.eq(year) & ~dff.is_multi.eq(True)].copy()
    cases_pat['is_txed'] = cases_pat.court.eq('txed')

    duration = []
    for ucid,row in tqdm(cases_pat.iterrows(),total=cases_pat.shape[0]):
        case = dtools.load_case(row.fpath)
        if not case.get('docket'):
            continue
        case_data = {'fpath': row.fpath,
                    'ucid' : ucid,
                    'filing_date': pd.to_datetime(case['filing_date']),
                    'terminating_date': pd.to_datetime(case['terminating_date']),
                    'status': case['case_status'],
                    'n_lines': len(case['docket'])}

        # Get latest date
        dates = [x['date_filed'] for x in case['docket']]
        case_data['latest_date'] = pd.to_datetime(dates).max()

        duration.append(case_data)

    df_dur = pd.DataFrame(duration)
    df_dur['days'] = (df_dur[['terminating_date','latest_date']].max(axis=1) - df_dur.filing_date).apply(lambda x: x.days)
    df_dur['is_txed'] = df_dur.ucid.str.startswith('txed')
    df_dur['sealed'] = df_dur.ucid.isin(df.ucid.unique())
    df_dur['discovery'] = df_dur.ucid.isin(df[df.protective | df.discovery_control].ucid.unique())
    return df_dur

def make_sealed_item_col(dframe):
    '''
    Generate a new category/column 'sealed item' that :
        Includes: sealed/restricted documents
        Excludes: motions to seal/unseal and any resolutions of them

    '''
    return dframe.sealed_article | dframe.restricted \
                            &  ~(dframe.seal_motion | dframe.unseal_motion
                                  | dframe.seal_grant  | dframe.unseal_grant
                                  | dframe.unseal_grant | dframe.unseal_deny
                                  | dframe.seal_grant_deny | dframe.unseal_grant_deny)


def build_quotient_table(df_in, df_dur):
    '''Build dataframe of quotients (articles/motions etc.)'''

    data = df_in.merge(
        df_dur.query("status=='closed' and discovery")[['ucid','days']],
        'inner', left_on='ucid', right_on='ucid'
    ).copy()
    data['days_in_lifespan'] = data.days_from_filing / data.days

    post_protective_idx = []
    data.groupby('ucid').apply(lambda dfg: post_protective_idx.extend(dfg.iloc[dfg.discovery_control.to_list().index(True)+1 :].index))

    data['protective_timeframe'] = 'before'
    data.loc[post_protective_idx, 'protective_timeframe'] = 'after'
    data.loc[data[data.discovery_control].index, 'protective_timeframe'] = 'protective'

    data['sealed_item'] = make_sealed_item_col(data)
    dfq = data.groupby('court').agg({'ucid':['nunique','count'], **{k: 'sum' for k in ('sealed_article', 'seal_motion', 'sealed_item')}})
    dfq.columns = ['case_count','docket_lines', 'sealed_article', 'seal_motion', 'sealed_item']
    dfq['articles/motions'] = dfq.sealed_article / dfq.seal_motion
    dfq['sealed_items/seal_motions'] = dfq.sealed_item/dfq.seal_motion

    return dfq


@click.command()
@click.option('--infile', '-i', help='Input file of sealed docket lines')
@click.option('--outfile', '-o', help='Output file name', default='articles_over_motions.csv', show_default=True )
def main(infile, outfile):
    # Load dataset of sealed events
    if infile!=None:
        infile = Path(infile).resolve()
    else:
        infile = settings.PROJECT_ROOT/'code/research/sealed/docket_lines_sealed_2016_v2.3.csv'

    df = load_sealed_df(infile)
    cr_idx = df[df.case_type.eq('cr')].index
    df.loc[cr_idx, 'nature_suit'] = 'criminal'
    df = df[~df.is_multi.eq(1)].copy()

    # Set nature of suit for all criminal cases to 'criminal'
    cr_idx = df[df.case_type.eq('cr')].index
    df.loc[cr_idx, 'nature_suit'] = 'criminal'

    # DF_DUR
    df_dur = build_df_dur(df)
    import pdb;pdb.set_trace()
    # dfq
    #Get subset of sealing activity NTO in txed
    sealed_pat = df[df.nature_suit.eq(PATENT_NOS)].copy()
    ## COMPILING PROTECTIVE INTO DISCOVERY CONTROL
    sealed_pat.discovery_control = sealed_pat.discovery_control | sealed_pat.protective

    df_articles_over_motions = build_quotient_table(sealed_pat, df_dur)
    df_articles_over_motions['plr'] = df_articles_over_motions.index.isin(PLR_DISTRICTS)
    df_articles_over_motions['articles/motions'].replace([np.inf],np.nan,inplace=True)
    df_articles_over_motions['sealed_items/seal_motions'].replace([np.inf],np.nan,inplace=True)
    df_articles_over_motions['is_txed'] = df_articles_over_motions.index.map(lambda x: 'txed' if x=='txed' else 'other')

    outfile = Path(outfile).resolve()
    df_articles_over_motions.to_csv(outfile, index=False)

if __name__ == '__main__':
    main()