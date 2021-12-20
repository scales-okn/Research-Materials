'''
File: build_judge_ifp_data.py
Authors: Adam Pah & Scott Daniel (2020-2021)
Description: Builds an ifp dataset on judges
'''

# standard path imports
from __future__ import division, print_function
import click
import glob
import json
import csv
import sys, os
import re
import datetime
import string
import numpy as np
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
from scipy import stats

# non-standard imports
import pandas as pd
from tqdm import tqdm

# support imports
sys.path.append('../..')
import support.settings as settings
import support.docket_entry_identification as dei
import support.data_tools as dtools
import support.judge_functions as jfunc
import support.court_functions as cfunc
import ifp_regexes as ifp_re

# keyword globals
ifp_basic_phrases = ['pauperis', 'ifp', 'without prepay', 'without the pay', 'without pay', '1915']
ifp_all_phrases = ifp_basic_phrases + ['consolidate', 'transfer', 'remand', 'dismiss', 'no longer pending', 'reduced filing fee procedure']
ifp_exclusion_phrases = ['order of service']
appeal_phrases = ['notice of appeal']

# misc globals
IGNORE_CASE = 'Void'
EMPTY_SEL_DF = pd.DataFrame(columns=['Entity_Extraction_Method', 'docket_source', 'judge_enum', 'party_enum', 'pacer_id', 'docket_index',
                            'ucid', 'cid', 'court', 'year', 'original_text', 'Extracted_Entity', 'Prefix_Categories', 'Transferred_Flag',
                            'full_span_start', 'full_span_end', 'Entity_Span_Start', 'Entity_Span_End', 'Parent_Entity', 'SJID'])
output_docketlines_cols = ['ucid','judge_name','scales_ind','span_start','span_end','matched_status','application_line',
                           'resolution_line','resolution_category','resolution']
today = datetime.datetime.today()
timestamp = str(today.day) + str(today.month) + str(today.year)

# datapath for outputs
DATAPATH = Path(__file__).parent/'data'
DATAPATH.mkdir(exist_ok=True)

# file that maps hand-matched ucids to resolution categories e.g. {"txsd;;1:16-XXXXX" : "grant"}
hand_matched_fpath = 'hand_matched.json'
try:
    with open(hand_matched_fpath, 'r', encoding='utf-8') as rfile:
        hand_matched = json.load(rfile)
except:
    hand_matched = {}



################################
### General helper functions ###
################################

lowercase_splitter = '|'.join(list(string.ascii_lowercase))

def _flatten_edge_dict(edges):
    return [x for sublist in edges.values() for x in sublist] if edges else None

def _identify_keyword_entry_type(docket_entry): # TODO change approach
    split_by_lowercase = re.split(lowercase_splitter, docket_entry) # take only caps statements at beginning of entry
    keywords = ['APPLICATION', 'MOTION', 'REQUEST', 'COMPLAINT', 'AFFIDAVIT', 'MINUTE ORDER', 'ORDER', 'DECISION', 'OPINION']
    for k in keywords:
        if k in split_by_lowercase[0]:
            return k

def _entry_splitter(docket_entry):
    period_splitter = '(?<=(?:[^\.]{4}[a-z]|[^\.]{3}[^\. ][A-Z]))\. (?=[A-Z\(][^\.]{4})'
    return re.split('(?: and | but |;|: #|, #|%s)' % period_splitter, docket_entry)

def _status_to_span_text(status, docket_entries):
    return docket_entries[status[0]]['docket_text'][status[2]['start']:status[2]['end']] if status else None

def _get_min_ifp_span(statuses, docket_entries):
    statuses = [s for s in statuses if any([x in _status_to_span_text(s, docket_entries).lower() for x in ['pauperis', 'ifp']])]
    if not statuses:
        return None
    else:
        min_span = statuses[0][2]
        min_length = statuses[0][2]['end']-statuses[0][2]['start']
        for status in statuses[1:]:
            new_length = status[2]['end']-status[2]['start']
            if new_length < min_length:
                min_span = status[2]
                min_length = new_length
        return min_span



#################################################
### Dataframe-transformation helper functions ###
#################################################

def _nos_cr_map(row):
    if row['case_type'] == 'cr':
        return 'cr'
    else:
        return row['nos_code']

def _nos_number(x):
    prisoner_nos = [510, 530, 535, 540, 550, 555, 560, 'cr']
    if x in prisoner_nos:
        return 1
    else:
        return 0

def _case_total(xset):
    return len(xset)

def _decision_total(xset):
    return len([x for x in xset if x in [1, -1]])

def _decision_average(xset):
    decisions = [x for x in xset]
    granted = len([x for x in decisions if x==1])
    if len(decisions)>0: # maybe CHANGE THIS to drop zero-outcomes from the denominator
        return granted/len(decisions)
    else:
        return np.nan



#################################################
### Post-processing dataframe transformations ###
#################################################

def transform_for_living_report(df, export_unencrypted):
    df['year'] = df.filedate.apply(lambda x: int(x.split('/')[-1]))
    df['case_type'] = df.case_id.apply(lambda x: x.split('-')[1])
    df['nos_code'] = df.apply(_nos_cr_map, axis=1)
    df['nature_of_suit_prisoner'] = df.nos_code.apply(_nos_number)
    df['circuit'] = df.court.apply(lambda x: cfunc.courtdf.at[x,'circuit'])

    df = df.drop(['filedate','nos_code'], axis=1)
    acountdf = df.loc[:, ['judge_name', 'resolution']].groupby('judge_name').agg(
        [_case_total, _decision_total, _decision_average]).reset_index()
    acountdf.columns = ['judge_name', 'case_total', 'decision_total', 'decision_average']
    df = df.merge(acountdf, on='judge_name', how='left')
    ycountdf = df.loc[:, ['judge_name', 'year', 'resolution']].groupby(['judge_name', 'year']).agg(
        [_case_total, _decision_total, _decision_average]).reset_index()
    ycountdf.columns = ['judge_name', 'year', 'ycase_total', 'ydecision_total', 'ydecision_average']
    df = df.merge(ycountdf, on=['judge_name', 'year'], how='left')

    if export_unencrypted:
        df.to_csv(DATAPATH/f'ifp_cases_unencrypted_{timestamp}.csv')

    df['case_id'] = df.case_id.apply(lambda x: dtools.sign(x.encode('UTF-8')))
    df['judge_name'] = df.judge_name.apply(encrypt_judge)

    # if args.debug:
    #     print(df)
    #     print()
    return df

def transform_for_validation(ifp_df, datadf):
    columns = ['fpath','jpath','app','res']#,'app_str','res_str']
    val_df = pd.DataFrame(columns=columns)

    for i,docket in enumerate([x[1] for x in ifp_df.iterrows()]):
        ucid = docket.ucid or list(datadf.index)[i]
        app = docket.application
        res = docket.resolution

        fpath = datadf.at[ucid,'fpath']
        fpath_string = str(settings.PROJECT_ROOT) + '/' + fpath.replace('json','html')
        jpath_string = fpath_string.replace('html','json')
        jdata = dtools.load_case(fpath)

        app_status = f'{app[1]} on line {app[0]}' if app else 'none'
        res_status = f'{res[1]} on line {res[0]}' if res else 'none'
        # app_string = _status_to_span_text(app, jdata['docket']) if app else None
        # res_string = _status_to_span_text(res, jdata['docket']) if res else None

        val_df.loc[ucid] = [fpath_string, jpath_string, app_status, res_status]

    return val_df



######################################################
### Judge ID functions, specific to IFP            ###
######################################################

def encrypt_judge(name):
    if name not in jfunc.NAN_NAMES.values():
        return dtools.sign(name.encode('UTF-8'))
    else:
        return name

# def jid_to_judge_name(jid, JEL):
#     if jid not in jfunc.NAN_NAMES.values():
#         #TODO load JEL once and pass through
#         # JEL = get_jel()
#         subdf = JEL[JEL['SCALES_JID']==float(jid)]
#         if len(subdf.index) == 1:
#             return subdf.iloc[0]['Presentable_Name']
#     return None

def check_neighborhood_exact_match(case_SEL, starting_point, order, statuses):

    preceding = [stat for stat in statuses if stat[0] < starting_point]
    succeeding = [stat for stat in statuses if stat[0] > starting_point]

    preceding = sorted(preceding, key = lambda tup: tup[0], reverse=True)
    succeeding = sorted(succeeding, key = lambda tup: tup[0], reverse=False)

    if order == 'preceding_first':
        run = [preceding, succeeding]
    else:
        run = [succeeding, preceding]

    for each in run:

        for i, line in enumerate(each):
            exact = check_if_direct_entry_match(case_SEL, line[0],line[2])
            if exact:
                return exact

    return None

def check_mode_between_lines(case_SEL, app_line, reso_line):
    docket_lines = case_SEL

    relevant_lines = docket_lines[(docket_lines.docket_index <= reso_line) &
                                 (docket_lines.docket_index >= app_line)]
    relevant_lines = relevant_lines[~relevant_lines.SJID.isna()]

    if len(relevant_lines) == 0:
        #print("No Judges Between IFP Lines")
        return None

    mode_judges = relevant_lines[['SJID']].mode()
    if len(mode_judges) == 1:
        SJID = mode_judges.SJID.iloc[0]
        if pd.isnull(SJID):
            return None
        else:
            return SJID
    else:
        #print("Multi-Modal")
        return None


def check_only_one_span_overlap(span_of_interest, SEL_spans):
    sois = span_of_interest['start']
    soie = span_of_interest['end']
    # if only 1 SEL span starts within the bounds of the ifp span or ends within the bounds
    keeps = []
    for span in SEL_spans:
        starter = span[0]
        ender = span[1]
        if starter <= soie and starter >=sois:
            keeps.append(span)
        elif ender >= sois and ender <= soie:
            keeps.append(span)

    if len(keeps) == 1:
        match = keeps[0]
        return (match[0],match[1])
    else:
        return None

def check_if_direct_entry_match(case_SEL, line_of_interest, span_of_interest):
    docket_lines = case_SEL

    direct_line_match = docket_lines[docket_lines.docket_index == line_of_interest]
    # unique scales judges for this exact line
    dlm = direct_line_match.copy()
    dlm.drop_duplicates('SJID', inplace=True)
    dlm = dlm[~dlm.SJID.isna()].copy()

    # no direct match, use other logic
    if len(dlm) == 0:
        return None
    elif len(dlm) == 1:
        SJID = dlm.SJID.iloc[0]
        if pd.isnull(SJID):
                return None
        else:
            return SJID

    elif len(dlm)>1:
        #print("MORE THAN ONE MATCH")
        SEL_spans = [(i,j) for i, j in zip(direct_line_match.Entity_Span_Start.values, direct_line_match.Entity_Span_End.values)]

        single_span_overlap = check_only_one_span_overlap(span_of_interest, SEL_spans)
        if single_span_overlap:
            new_dlm = direct_line_match[(direct_line_match.Entity_Span_Start == single_span_overlap[0]) &
                                        (direct_line_match.Entity_Span_End == single_span_overlap[1])]

            SJID = new_dlm.SJID.iloc[0]
            if pd.isnull(SJID):
                    return None
            else:
                return SJID
        else:
            # sentences splitting logic
            #{'Mapping': 'Needs Additional Logic Multi-DLM'}
            return None

def number_judges_ucid(case_SEL):
    docket_lines = case_SEL

    actual_judges = docket_lines[~docket_lines.SJID.isna()]
    if len(actual_judges)>0:
        total_docket_judges = len(actual_judges.SJID.unique())
        return total_docket_judges
    else:
        return 0

def single_judge_ucid_check(case_SEL):
    n_judges = number_judges_ucid(case_SEL)

    return n_judges == 1

def check_if_single_judge(case_SEL):

    if single_judge_ucid_check(case_SEL):
        docket_lines = case_SEL
        the_judge = docket_lines[~docket_lines.SJID.isna()]
        SJID = the_judge.SJID.iloc[0]
        if pd.isnull(SJID):
            return None
        else:
            return SJID

    else:
        return None

def take_header_judge(case_SEL):
    docket_lines = case_SEL
    header = docket_lines[docket_lines.docket_index==-1]
    header = header[~header.SJID.isna()].copy()
    if len(header)>0:
        ## check for assigned judge first
        if 'assigned_judge' in header.Entity_Extraction_Method.values:
            this_judge = header[header.Entity_Extraction_Method == 'assigned_judge']
            SJID = this_judge.SJID.iloc[0]
            return SJID
        else:
            # take the referred_judge
            this_judge = header[header.Entity_Extraction_Method == 'referred_judges']
            SJID = this_judge.SJID.iloc[0]
            return SJID
    else:
        return None

def jed_sel_crosswalker(ucid, resolution, statuses, application_line, debug=False):

    if debug:
        return jfunc.NAN_NAMES['not_found']

    # Get the SEL lines for this single case, only time SEL is read, on a per-case basis
    case_SEL = jfunc.load_SEL(ucid)
    if case_SEL is None:
        case_SEL = EMPTY_SEL_DF

    # LOGIC: no resolution ucids
    if resolution == None: # no line flagged as the resolution line
        # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
        single_judge = check_if_single_judge(case_SEL)
        if single_judge:
            return single_judge

        app_line = [tup for tup in statuses if tup[1] == 'application' and tup[0] == application_line][0]
        spans_of_interest = app_line[2]
        direct_line = check_if_direct_entry_match(case_SEL, application_line, spans_of_interest)
        if direct_line:
            return direct_line

        neighbor = check_neighborhood_exact_match(case_SEL, application_line, 'succeeding_first', statuses)
        if neighbor:
            return neighbor

        # find mode of all IFP Lines
        min_line = min([t[0] for t in statuses])
        max_line = max([t[0] for t in statuses])
        modey = check_mode_between_lines(case_SEL, min_line, max_line)
        if modey:
            return modey

        header = take_header_judge(case_SEL)
        if header:
            return header

        return jfunc.NAN_NAMES['not_found']

    # LOGIC: grant or deny
    else:
        # local vars
        line_of_interest = resolution[0] # index in scales
        endstate = resolution[1] # resolution label
        spans_of_interest = resolution[2] # ifp span relative to docket entry

        # LOGIC: resolution is on line 0, need to know if application came with it
        if line_of_interest == 0:
            # all line 0 statuses
            relevant = [stat for stat in statuses if stat[0]==0]

            # if there is an application, then we are good for attribution check
            if any(stat for stat in relevant if stat[1]=='application'):
                # good to go there is a pairing

                # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
                single_judge = check_if_single_judge(case_SEL)
                if single_judge:
                    return single_judge

                direct_line = check_if_direct_entry_match(case_SEL, line_of_interest, spans_of_interest)
                if direct_line:
                    return direct_line

                neighbor = check_neighborhood_exact_match(case_SEL, line_of_interest,'preceding_first', statuses)
                if neighbor:
                    return neighbor

                min_line = min([t[0] for t in statuses])
                max_line = max([t[0] for t in statuses])
                modey = check_mode_between_lines(case_SEL, min_line, max_line)
                if modey:
                    return modey


                header = take_header_judge(case_SEL)
                if header:
                    return header

                return jfunc.NAN_NAMES['not_found']

            # no application, void ucid
            else:
                # bad, no pairing
                return IGNORE_CASE

        # LOGIC: RESOLUTION NOT ON LINE 0
        else:
            # attribution hierarchy
            # IF OUR ENTITY LIST ONLY HAS A SINGLE JUDGE TIED TO THIS CASE, WIN EARLY AND ASSIGN THEM
            single_judge = check_if_single_judge(case_SEL)
            if single_judge:
                return single_judge

            direct_line = check_if_direct_entry_match(case_SEL, line_of_interest, spans_of_interest)
            if direct_line:
                return direct_line

            neighbor = check_neighborhood_exact_match(case_SEL, line_of_interest, 'preceding_first', statuses)
            if neighbor:
                return neighbor

            modey = check_mode_between_lines(case_SEL, application_line, line_of_interest)
            if modey:
                return modey

            header = take_header_judge(case_SEL)
            if header:
                return header

            return jfunc.NAN_NAMES['not_found']

        return jfunc.NAN_NAMES['not_found']



####################################
### Judge significance functions ###
####################################

def bootstrap_ttest(df, min_cases=5, exclude_prisoner=False, outliers_prop=0):

    '''
    Check for statistical significance of variation of judge grant.
    Compares individual judge to average from all other judges within their district

    Inputs:
        - df (pd.DataFrame): data frame (expected columns: court, judge_name, case_id, resolution)
        - min_cases (int): skip judge if they have less that this many cases
        - outliers_prop(float): proportion (out of 1) of outliers to exclude based on decision_average e.g. 0.1 -> excludes <5% and >95%
    Output:
        (pd.DataFrame) results table with ['judge_name','court', 'diff', 'lb', 'ub']

    '''
    checkdf = df.copy()
    if outliers_prop > 0:
        out_lb = outliers_prop/2
        out_ub = 1 - out_lb
        checkdf = checkdf[(checkdf.decision_average>=out_lb) & (checkdf.decision_average<=out_ub)].copy()
    if exclude_prisoner:
        checkdf = checkdf[checkdf.nature_of_suit_prisoner==0].copy()

    checkdf = checkdf.loc[:, ['court', 'judge_name', 'case_id', 'resolution']].copy()
    checkdf.columns = ['court',  'judge', 'ucid', 'grant']

    judge_data = []
    courts = [x for x in checkdf.court.unique() if x!='nmid']
    for court in courts:
        #Just subset to keep the naming shorter
        cdf = checkdf[checkdf.court == court]
        #Get the judge list
        judges = cdf.judge.unique()
        #District differences
        for j in judges:
            jdf = cdf[cdf.judge==j]
            njdf = cdf[cdf.judge!=j]

            if len(jdf)>min_cases and len(njdf)>0:
                mu_1, var_1 = np.mean(jdf.grant), np.var(jdf.grant)
                mu_2, var_2 = np.mean(njdf.grant), np.var(njdf.grant)
                s_1 = np.std(jdf.grant)
                s_2 = np.std(njdf.grant)
                Ndf = len(cdf) - 2
                diff = (mu_1-mu_2)

                #even samples
                sp_2 =  (((len(jdf)-1)*s_1**2) + ((len(njdf)-1)*s_2**2))/ Ndf
                se = np.sqrt(s_1**2/len(jdf) + s_2**2/len(njdf))
                t = diff/se
                #se = np.sqrt(var_1/len(jdf.grant) + var_2/len(njdf.grant))
                lb = diff - stats.t.ppf(0.975, Ndf)*se
                ub = diff + stats.t.ppf(0.975, Ndf)*se

                #Uneven samples
                se = np.sqrt(s_1**2/len(jdf) + s_2**2/len(njdf))
                d = diff/se
                nndf = (se**2)**2/( (s_1**2/len(jdf))**2/(len(jdf)-1) + (s_2**2/len(njdf))**2/(len(njdf)-1) )
                if np.sign(diff) == -1:
                    lb = diff + stats.t.ppf(0.975, nndf)*se
                    ub = diff - stats.t.ppf(0.975, nndf)*se
                else:
                    lb = diff - stats.t.ppf(0.975, nndf)*se
                    ub = diff + stats.t.ppf(0.975, nndf)*se

                judge_data.append([j, court, diff, lb, ub])

    scidf = pd.DataFrame(judge_data, columns = ['judge_name','court', 'diff', 'lb', 'ub'])

    identify_sig = lambda row: int(np.sign(row['lb'])==np.sign(row['ub']) )

    scidf['sig'] = scidf.apply(identify_sig, axis=1)
    # print(f"Proportion significant: {scidf.sig.sum()/len(scidf)}")
    return scidf

def create_judge_var_sig_tables(dataset):
    '''
    Check for judge variance signifcance with boostrapping.
    Ouputs two tables (1) 'judge_var_sig.csv' and an aggregated (2) 'judge_var_sig_lookup.csv'

    Inputs:
        - dataset (pd.DataFrame or str/Path): dataframe formatted for living report
    '''

    if type(dataset) is not pd.DataFrame:
        # Read in dataset if argument is a string or path
        dataset = pd.read_csv(dataset)

    # Run separate bootstrap for excl_prisoner_outliers and not
    bt1 = bootstrap_ttest(dataset)
    bt2 = bootstrap_ttest(dataset, exclude_prisoner=True, outliers_prop=0.1)

    bt1['excl_prisoner_outliers'] = False
    bt2['excl_prisoner_outliers'] = True
    bt_both = pd.concat([bt1,bt2])
    bt_both.to_csv(DATAPATH/f'judge_var_sig_{timestamp}.csv', index=False)

    # Aggregate by court to produce a lookup table
    dfvar = bt_both.groupby(['excl_prisoner_outliers','court']).sig.agg(['sum','count','mean']).reset_index()
    # Reduce columns and rename
    dfvar = dfvar[ ['court','excl_prisoner_outliers','sum', 'count','mean'] ]
    dfvar.columns = ['court','excl_prisoner_outliers','jsum', 'jcount','jmean']

    dfvar = dfvar.sort_values('court').reset_index(drop=True)
    threshold = 0.05
    dfvar['isSig'] = dfvar.jmean > threshold
    dfvar.to_csv(DATAPATH/f'judge_var_sig_lookup_{timestamp}.csv',index=False)



######################################
### IFP identification & selection ###
######################################

# Used to map resolution categories to {-1,0,1}
resolution_map = {
    'grant': 1,
    'deny': -1,
    'no_resolution': 0 #ifp_resolution_filter returns None
    # 'other': None, # ifp_resolution_filter returns 'other', we exclude this case before we get to this point
}

def ifp_line_identification(sentences, docket_entries, ind): # TODO use tuples, nest loops in outer loop, regex ignore case instead of .lower, maybe concat into single regex per category
    '''
    Uses regular expressions to identify whether ifp was granted
    Note: we can throw out the regex spans because they're relative to the sentences (not the entries) and thus probably not useful

    Output:
        - list of lists of form (ind, matched_status, span)
        where matched_status in ['application', 'grant','deny', 'other']
        and span is dict like {'start': 50, 'end':60}
    '''


    # TODO: implement a status dictionary that looks like:
    # {'ucid': 'ilnd;;1:16-cv-00001', 'scales_ind':7, span: {'start':1, 'end':20}}
    # and have the statuses array be made up of dicts like above
    # Note: involves changing everywhere in the code where status entries are sliced (by index) to get certain value

    statuses = []
    # print(sentences)
    for sentence in [x for x in sentences if len(x)>0]:
        #TODO: These offsets should probably be calculated at time of sentence splitting if possible, using .split(sentence) may be inefficient
        # Instead of passing sentences around, pass spans for sentences?
        span_offset = len(docket_entries[ind]['docket_text'].split(sentence)[0]) # reformat sentence here, maybe use regex (time?)
        if len([True for xphrase in ifp_exclusion_phrases if xphrase in sentence.lower()])==0:
            for group_name, group in ifp_re.groups.items():
                for regex in group:
                    temp_search = regex.search(' '.join(sentence.lower().split()))
                    if temp_search!=None:
                        # print(sentence)
                        statuses.append([ind, group_name, dtools.make_span(temp_search, span_offset)])
    return statuses

def application_filter(statuses, caps_statements, actions_dict, docket_entries): # TODO make if-else?
    '''

    Output:
        - an entry from statuses (or possibly None)
    '''
    # Applications, we want to take the first one
    scores = []
    possible_apps = [x for x in statuses if x[1]=='application']
    for poss_app in possible_apps:
        # if debug:
        #     print(poss_app)
        tscore = 0
        # Entry type matches
        if caps_statements[poss_app[0]] in ['APPLICATION', 'MOTION', 'REQUEST', 'COMPLAINT']:
            # if debug:
            #     print('+1 caps statement')
            tscore += 1
        # Maximum index
        if poss_app[0] == max([x[0] for x in possible_apps]): # TODO call this a tiebreaker, maybe make docket order a separate dimension for better conceptual clarity
            # if debug:
            #     print('+0.5 max index')
            tscore += 0.5
        # No appeal phrases
        if not any([x in docket_entries[poss_app[0]]['docket_text'].lower() for x in appeal_phrases]):
            # if debug:
            #     print('+1 no appeal phrases')
            tscore += 1
        # # Only applications
        # if len(actions_dict[poss_app[0]])==1:
        #     tscore += 1
        scores.append(tscore)
        # if debug:
        #     print("TOTAL SCORE:", tscore)
    # I want the maximum score that's over 0
    if possible_apps == []:
        ifp_application = None
    else:
        if max(scores)>0: # TODO confirm that this will never happen
            winning_ind = scores.index(max(scores))
            ifp_application = possible_apps[winning_ind]
        else:
            ifp_application = None
    return ifp_application

def resolution_filter(statuses, caps_statements, actions_dict, docket_entries):
    '''

    Output:
        - an entry from statuses (or possibly None)
    '''
    # Outcomes
    scores = []
    endpoints = [x for x in statuses if x[1]!='application']
    min_span = _get_min_ifp_span(endpoints, docket_entries)
    for poss_res in endpoints:
        # if debug:
        #     print(poss_res)
        tscore = 0

        # Throw out ones that are not of Interest


        # Entry type matches
        if caps_statements[poss_res[0]] in ['ORDER', 'DECISION', 'OPINION']:
            # if debug:
            #     print('+1 caps statement')
            tscore += 1
        # Maximum index
        if poss_res[0] == max([x[0] for x in endpoints]):
            # if debug:
            #     print('+0.5 max index')
            tscore += 0.5
        # # Minimum span
        # if poss_res[2] == min_span:
        #     print('+0.5 min span')
        #     tscore += 0.5
        # Consistent in its identification

        # If it's the only matched_resolution in its category, give it a bonus point?
        if len([x for x in actions_dict[poss_res[0]] if x!='application'])==1:
            # if debug:
            #     print('+1 consistent')
            tscore += 1
        # No appeal phrases
        if not any([x in docket_entries[poss_res[0]]['docket_text'].lower() for x in appeal_phrases]):
            # if debug:
            #     print('+1 no appeal phrases')
            tscore += 1
        # Preference for grant -> denial -> other
        if poss_res[1] == 'grant':
            # if debug:
            #     print('+1.5 grant')
            tscore+= 1.5
        elif poss_res[1] == 'deny':
            # if debug:
            #     print('+1 deny')
            tscore += 1
        scores.append(tscore)
        # if debug:
        #     print("TOTAL SCORE:", tscore)
    # we want the maximum non-zero score
    if endpoints == []:
        ifp_resolution = None
    else:
        if max(scores)>0:
            winning_ind = scores.index(max(scores)) # per implem. of max(), tie goes to the earliest docket entry / highest up in regex list TODO make this behavior explicit
            ifp_resolution = endpoints[winning_ind]
        else:
            ifp_resolution = None
    return ifp_resolution

def filter_by_edges(statuses, edges, ifp_application):
    '''
    Once an application has been found, look at possible resolution filters in the statuses list.
    If any of them have an edge that points specifically to the application, include these in a filtered subset.
    Return filtered subset if it's non-empty, else abandon edge filtering and pass back the full statuses list.
    '''
    filtered_statuses = []
    if edges and ifp_application:
        matching_inds = [x[0] for x in dtools.get_edges_to_target(ifp_application[0], edges)]
        for poss_res in [x for x in statuses if x[1] in ('grant','deny')]: # if there's 'grant'/'deny', we're confident in discarding 'other'
            if poss_res[0] in matching_inds:
                filtered_statuses.append(poss_res)
    return filtered_statuses or statuses

def filter_by_appeals(statuses, edges, docket):
    '''
    Search for appeal activity in the docket.
    If any of the statuses correspond to docket lines within a window of appeal, discard them from the statuses list.
    '''
    appeal_statuses = []
    appeal_indices = dtools.find_matching_lines(docket, appeal_phrases, beginning_only=True)
    if appeal_indices:

        if edges: # note: we're assuming that if there are ~any~ edges then we have access to ~all~ appeal edge data (may not be the case!)
            appeal_windows = [(ind, max([x[0] for x in dtools.get_edges_to_target(ind, edges)] or [len(docket)]) + 1) for ind in appeal_indices]
        else:
            appeal_windows = []
            for scales_ind in appeal_indices:
                pacer_ind = docket[scales_ind]['ind']
                appeal_notice_base = f'{pacer_ind}(\.|\)|]| )? ?notice ?of ?appeal|notice ?of ?appeal ?(\(|[) ?(no ?\.?|#)? ?{pacer_ind}(\.|\)|]| )'
                appeal_notice_re = re.compile('('+appeal_notice_base+f'|(dkt|docket) ?(no ?\.?|#)? ?{pacer_ind}(\.|\)|]| ))')

                max_ind = scales_ind
                for i,entry in enumerate(docket[scales_ind+1:], start=scales_ind+1):
                    temp_search = appeal_notice_re.search(' '.join(entry['docket_text'].lower().split()))
                    if temp_search!=None:
                        max_ind = i
                appeal_windows.append((scales_ind, max_ind+1))

        # print(appeal_windows)
        for status in statuses:
            for window in appeal_windows:
                if window[0] <= status[0] and status[0] < window[1]: # for consistency, i'm using half-open intervals, i.e. [start,end)
                    appeal_statuses.append(status)

    return [x for x in statuses if x not in appeal_statuses]



######################
### Main functions ###
######################

def file_processor(fpath, outpath_docketlines, validation_columns, keep_ambig, track_progress, preloaded_jdata=None):
    '''
    Main method to process files

    Input:
        - fpath (str): fpath to case json
        - outpath_docketlines (str): output path to write the intermediate docketline level data (related to what's in statuses),
                if None, doesn't output docketline level data
        - validation_columns, keep_ambig, track_progress: command-line arguments passed in so as to avoid Click context during multithreading
        - preloaded_jdata (dict): the already-loaded case json (in case the caller prefers to pass that)
    Output:
        (list) [jdata['download_court'], jdata['case_id'], nos_code, judge_name, resolution, jdata['filing_date']]
    '''
    jdata = preloaded_jdata if preloaded_jdata else dtools.load_case(fpath, skip_scrubbing=True)
    ifp_indices = dtools.find_matching_lines(jdata['docket'], ifp_basic_phrases, ifp_exclusion_phrases)

    # We have entries with ifp mentions
    if ifp_indices:
        ifp_indices = dtools.find_matching_lines(jdata['docket'], ifp_all_phrases, ifp_exclusion_phrases)
        # Get the entries
        docket_entries = {i: jdata['docket'][i] for i in ifp_indices}
        # Wrap to sentences
        docket_sentences = {i: _entry_splitter(de['docket_text']) for i,de in docket_entries.items()}
        # Keyword entry types
        caps_statements = {i: _identify_keyword_entry_type(de['docket_text']) for i,de in docket_entries.items()}
        # Docket edges
        if dtools.has_edges(jdata['docket']):
            ifp_edges = _flatten_edge_dict({i: de['edges'] for i,de in docket_entries.items()})
            all_edges = _flatten_edge_dict({i: de['edges'] for i,de in enumerate(jdata['docket'])})
        else:
            ifp_edges, all_edges = None, None


        # order of elements within statuses list:
        # [ [docket_entry_ind, action, span], ...]
        statuses = []
        for ind, sentences in docket_sentences.items():
            # Get the statuses for this particular line and add them to the statuses list
            docketline_statuses = ifp_line_identification(sentences, docket_entries, ind)
            statuses.extend(stx for stx in docketline_statuses if len(stx))

        # track the action types (app/grant/deny/other) detected on each line
        # Groups by (scales_ind,status) like actions_dict =  { 13: {'application': list of statuses entries, 'grant': list of statuses entries}}
        actions_dict = {}
        for st in statuses:
            if st[0] not in actions_dict: # is this docket line in actions_dict?
                actions_dict[st[0]] = []
            if st[1] not in actions_dict[st[0]]: # is this action type in actions_dict?
                actions_dict[st[0]].append(st[1])

        # Identify the application
        statuses = filter_by_appeals(statuses, all_edges, jdata['docket'])
        # if debug:
        #     print('---------')
        ifp_application = application_filter(statuses, caps_statements, actions_dict, docket_entries)
        application_line = ifp_application[0] if ifp_application else None

        # Identify the resolution
        statuses = filter_by_edges(statuses, ifp_edges, ifp_application)
        # if debug:
        #     print('---------')
        ifp_resolution = resolution_filter(statuses, caps_statements, actions_dict, docket_entries)
        resolution_category = ifp_resolution[1] if ifp_resolution else 'no_resolution'

        # Catch the hand-annotated cases and set their resolution_category
        if jdata['ucid'] in hand_matched.keys():
            hand_resolution_category = hand_matched[jdata['ucid']]
            print(f"Manually setting {jdata['ucid']} -> {hand_resolution_category} ")
            resolution_category = hand_resolution_category

        # If resolution is 'other' and not keep_ambig (or if res is 'other' & no app, regardless of keep_ambig), drop the whole case from analysis
        # (If an application exists but resolution is 'no_resolution', we *do* want to include it, judge did not rule on it, map it to 0, see below)
        # More details: we check for app to avoid flagging, e.g., 'case transferred' by itself, but we lose 'stricken as moot' if it appears w/o app
        # To be clear, the application-less 'stricken as moot' bucket should be pretty small, though we could salvage it by further refining 'other'
        if resolution_category == 'other':
            if track_progress:
                print("Throwing out", jdata['ucid'], "(categorized as 'other')")
            if not keep_ambig or ifp_application==None:
                return None

        # If no application or resolution found, we want to throw whole case out
        if ifp_resolution==None and ifp_application==None:
            if track_progress:
                print('Throwing out', jdata['ucid'], '(no application/resolution)')
            return None

        # if debug:
        #     print('---------')
        if validation_columns:
            # if debug:
            #     print('ucid:', jdata['ucid'])
            #     print(f'application: {ifp_application} - {_status_to_span_text(ifp_application, docket_entries)}')
            #     print(f'resolution: {ifp_resolution} - {_status_to_span_text(ifp_resolution, docket_entries)}')
            #     print()
            if track_progress:
                print(jdata['ucid'])
            return [jdata['ucid'], ifp_application, ifp_resolution]
        else:
            nos_code = dtools.nos_number_extractor(jdata['nature_suit'])

            # Not distinguishing committees at this time
            judge_name = jed_sel_crosswalker(
                jdata['ucid'], ifp_resolution, statuses, application_line)
            # print(jfunc.jid_to_judge_name(judge_name))

            # If crosswalker returns 'Void' (see scenario above in function definition)
            if judge_name == IGNORE_CASE:
                if track_progress:
                    print("Throwing out", jdata['ucid'], "(resolution on line 0 and no application)")
                return None

            # Map to {-1,0,1} explicitly based on resolution_map
            resolution = resolution_map[resolution_category]

            # Output docketlines
            if outpath_docketlines and len(statuses):

                # Gather docketline level data as list of dicts
                docketline_output = [
                    {
                        'ucid': jdata['ucid'],
                        'judge_name':judge_name,
                        'scales_ind': status[0], #scales_ind
                        'matched_status': status[1], # status
                        'span_start':status[2]['start'], # span start
                        'span_end':status[2]['end'], # span end
                        'application_line': application_line,
                        'resolution_line':  ifp_resolution[0] if ifp_resolution else '',
                        'resolution_category': resolution_category,
                        'resolution': resolution
                    }
                    for status in statuses
                ]
                # Map each to a tuple and ensure correct ordering of csv output
                docketline_output_ordered = [tuple(d[col] for col in output_docketlines_cols) for d in docketline_output]
                with open(outpath_docketlines, 'a', encoding="utf-8") as wfile:
                    csv.writer(wfile).writerows(docketline_output_ordered)

            if track_progress:
                print(jdata['ucid'])
            return [jdata['court'], jdata['case_id'], nos_code, judge_name, resolution, jdata['filing_date']]



@click.command()
@click.option('--inpath', default=None, help='path to input file')
@click.option('--single_court', default=None, help='only build dataset for single court')
@click.option('--outpath', default=None, help='path to output file')
@click.option('--outpath_docketlines', default=None, help='path to output docketlines level data on matches' )
@click.option('--validation_columns', default=False, is_flag=True, help=
    'toggles a concise output designed for validating the algorithm (rather than exporting to a statistical notebook')
@click.option('--keep_ambig', default=False, is_flag=True, help=
    'instructs the processor not to throw out cases with ambiguous IFP resolutions (i.e. resolutions of type "other")')
@click.option('--export_unencrypted', default=False, is_flag=True, help=
    'exports a version of the dataframe with unencrypted judges and ucids')
# @click.option('--debug', default=False, is_flag=True, help='toggles debug mode')
@click.option('--no_sig', default=False, is_flag=True, help='turns off judge significance functions')
@click.option('--track_progress', default=False, is_flag=True, help='prints ucids as they finish')
@click.option('--n_workers', default=8, help='how many processes to run')
def main(inpath, single_court, outpath, outpath_docketlines, validation_columns, keep_ambig, export_unencrypted, no_sig, track_progress, n_workers):
    # Read in dataframe
    datadf_orig = pd.read_csv(inpath, index_col=0) if inpath else dtools.load_unique_files_df()

    # # in debug mode, filter dataframe to a single case
    # if debug:
    #     ucid_to_debug = 'hid;;1:16-cv-00260'
    #     datadf_orig = pd.DataFrame([datadf_orig.loc[ucid_to_debug]],columns=datadf_orig.columns)
    #     print()

    if outpath_docketlines:
        # Clear the outpath docketlines file and write the header
        with open(outpath_docketlines, 'w', encoding='utf-8') as wfile:
            csv.writer(wfile).writerow(output_docketlines_cols)

    ifp_df = None
    # Iterate over the desired set of courts
    for current_court in ([single_court] if single_court else sorted(set(list(datadf_orig.court)))):

        # Filter datadf to only one court
        datadf = datadf_orig[datadf_orig.court.eq(current_court)]

        # Get the sel dframe and filter it on court
        # if not validation_columns:
        #     jfunc.get_sel(court, reload_sel=True)

        ex = ThreadPoolExecutor(max_workers=int(n_workers))
        fp_args = [(f, outpath_docketlines, validation_columns, keep_ambig, track_progress) for f in datadf.fpath.values]
        results = list(ex.map(lambda x: file_processor(*x), fp_args))
        print('Finished processing ' + current_court + '!')

        # clean up the newest results
        cleaned_results = [x for x in results if x!=None]
        print(f'Processed {len(datadf.index)} cases, found {len(cleaned_results)} ifp cases')
        new_ifp_df = pd.DataFrame([[None, None, None] if x==None else x for x in results] if validation_columns else cleaned_results, columns=
            ['ucid','application','resolution'] if validation_columns else [
            'court','case_id','nos_code','judge_name','resolution','filedate'])

        # concatenate to the existing results
        if ifp_df is None:
            ifp_df = new_ifp_df
        else:
            ifp_df = ifp_df.append(new_ifp_df)

    # name the export
    if not outpath:
        wfname = DATAPATH/f'ifp_cases_{timestamp}.csv'
    else:
        wfname = outpath
    # format and export the data
    final_df = transform_for_validation(ifp_df, datadf) if validation_columns else transform_for_living_report(ifp_df, export_unencrypted)
    final_df.to_csv(wfname)
    if not no_sig:
        create_judge_var_sig_tables(final_df)

if __name__ == '__main__':
    main()
