'''
File: build_judge_ifp_data.py
Author: Adam Pah
Description:
Builds an ifp dataset on judges
'''

#Standard path imports
from __future__ import division, print_function
import argparse
import glob
import json
import sys, os
import os
import re
from collections import Counter

#Non-standard imports
import pandas as pd
import spacy
nlp = spacy.load('en_core_web_sm')

import ifp_support as ifp_dtools

#Global directories and variables
columns =  ['jurisdiction', 'case_id', 'judge_name', 'entry_date', 'entry_text']
ifp_columns = ['jurisdiction', 'case_id', 'judge_name', 'entry_date', 'resolution']

##Regular expressions
filler = '[a-zA-Z0-9 ()\[\]:#\,\.\'\"\-\$]'

##Granting phrases
grant_fp_re = re.compile('grant%s{1,400}(in forma pauperis|ifp)' % filler)
grant_fp_re_rev = re.compile('(in forma pauperis|ifp)%s{1,400}grant[seding]{1,3}' % filler)
grant_order_re = re.compile('order%s{1,10}proceed (in forma pauperis|ifp)' % filler)
grant_natlang_re = re.compile('order%s{1,10}grant%s{1,80}without prepaying' % (filler, filler))
grant_natlang2_re = re.compile('prepay%s{1,100}grant' % (filler))
grant_natlang3_re = re.compile('waive filing fee grant')

##Denying phrases
deny_fp_re = re.compile('den[yingieds]{1,4}%s{1,400}(in forma pauperis|ifp)' % filler)
deny_fp_re_rev = re.compile('(in forma pauperis|ifp)%s{1,400}den[iedyings]{1,4}' % filler)
deny_moot_re = re.compile('(in forma pauperis|ifp)%s{1,400}moot' % filler)
deny_natlang_re = re.compile('order%s{1,10}den%s{1,80}without prepaying' % (filler, filler))
deny_natlang2_re = re.compile('prepay%s{1,100}den[iedyings]{1,4}' % (filler))
deny_natlang3_re = re.compile('waive filing fee den[iedyings]{1,4}')
deny_partial_re = re.compile('initial partial filing fee')
deny_payment_re = re.compile("directing monthly payments be made from prison account")

##Noninsantion of ifp by plaintiff, but with mention of ifp in case
noninst_appeal_re = re.compile('appeal%s{1,100}good faith%s{1,100}should not%s{1,100}in forma pauperis' % (filler, filler, filler))
noninst_consolidate_re = re.compile('order to consolidate cases%s{1,500}all future pleadings' % filler)
noninst_wrongassign_re = re.compile('case was assigned incorrectly%s{1,400}hereby ordered%s{1,400}transferred' % (filler, filler))
noninst_transfer_re = re.compile('transfer[ringed]{0,4}%s{1,30}(to|from)%s{1,30}district' % (filler, filler))
noninst_instruction_re = re.compile('(direct|order)%s{1,40}fil%s{1,400}forma pauperis' % (filler, filler) )
noninst_instruction2_re = re.compile('has not%s{0,80}submit%s{1,80}(fil|motion)%s{1,200}forma pauperis' % (filler, filler, filler) )
noninst_moot_re = re.compile('finding as moot%s{1,400}forma pauperis' % filler)
noninst_nolonger_re = re.compile('no longer pending in this court')

##Case dismissal
dismissal_re = re.compile('order of dismissal')
dismissal2_re = re.compile('notice of [voluntary ]{0,11}dismissal')
dismissal3_re = re.compile('order dismiss%s{0,40}(prejudice|case)'%filler)
dismissal4_re = re.compile('dismiss%s{0,40}case%s{0,20}entirety' % (filler, filler))

##Excluding phrases
exclusion_phrases = ['order of service']


def clean_case_id(case_name):
    '''
    Takes a listed case name and clean anything that isn't the year, case type, and id

    input: case_name, name of the case from the query
    output: clean_name, cleaned name
    '''
    sx = case_name.split('-')
    if 'cv' in sx[0]:
        return sx[0].strip('cv') + '-cv-' + sx[1]
    elif 'cr' in sx[0]:
        return sx[0].strip('cr') + '-cr-' + sx[1]
    else:
        try:
            return sx[0] + '-' + '-'.join([sx[1], sx[2]])
        except:
            return x

def remap_recap_data(recap_fpath):
    '''
    Given a recap file, normalizes the process
    * recap_fpath
    output:
    *jdata
    '''
    def standardize_date(tdate):
        '''y-m-d to m/d/y'''
        try:
            y,m,d = tdate.split('-')
            return '/'.join([m, d, y])
        except AttributeError:
            return None

    #Load the data
    try:
        rjdata = json.load(open(recap_fpath))
    except:
        rjdata = json.load(open(recap_fpath, encoding="utf8"))
    #Get the termiantion date
    try:
        tdate = standardize_date(jdata['date_terminated'])
        case_status='closed'
    except:
        tdate=None
        case_status='open'
    #Convert the data
    fdata = {
            'case_id': clean_case_id(rjdata['docket_number']),
            'case_name': rjdata['case_name'],
            'case_status': case_status,
            'case_type': rjdata['docket_number'].split('-')[1],
            'cause': rjdata['cause'],
            'defendants':'',
            'docket': [[standardize_date(tentry['date_filed']), tentry['entry_number'], tentry['description']]\
                    for tentry in rjdata['docket_entries']],
            'download_court': rjdata['court'],
            'filing_date': standardize_date(rjdata['date_filed']),
            'judge': rjdata['assigned_to_str'],
            'jurisdiction': rjdata['jurisdiction_type'],
            'jury_demand': rjdata['jury_demand'],
            'nature_suit': rjdata['nature_of_suit'],
            'pending_counts':'',
            'plaintiffs':'',
            'terminated_counts':'',
            'terminating_date': tdate
            }
    return fdata


def find_ifp_entry_line(docket):
    '''
    Identifies the earliest docket entry with forma pauperis in it

    returns line number or 9999 if not found in the entry
    '''
    line_ids = []
    for i,content in enumerate(docket):
        try:
            if 'forma pauperis' in content[-1].lower() and len([True for xphrase in exclusion_phrases if xphrase in content[-1].lower()])==0:
                line_ids.append(i)
            # elif ' ifp ' in content[-1].lower() and len([True for xphrase in exclusion_phrases if xphrase in content[-1].lower()])==0:
                # line_ids.append(i)
        except (AttributeError, TypeError):
            pass
    if line_ids:
        return min(line_ids)
    else:
        return 9999

def binarize_case_ifp(line_num):
    '''
    binarizes the found ifp line numbers into positive and negative (9999) findings
    '''
    if line_num == 9999:
        return 0
    else:
        return 1

def ifp_grant_identification(entry_text):
    '''
    Identifies if ifp was granted using regular expressions
    '''
    grant_status = 0
    if len([True for xphrase in exclusion_phrases if xphrase in entry_text.lower()])==0:
        #Grants
        for regex in [grant_fp_re, grant_fp_re_rev, grant_order_re, grant_natlang_re, grant_natlang2_re, grant_natlang3_re]:
            temp_search = regex.search(entry_text.lower())
            if temp_search != None:
                return 1
        #Denials
        for regex in [deny_fp_re, deny_fp_re_rev, deny_natlang_re, deny_natlang2_re, deny_natlang3_re, deny_partial_re, deny_payment_re]:
            temp_search = regex.search(entry_text.lower())
            if temp_search != None:
                return -1
        #Non-instances
        for regex in [noninst_appeal_re, noninst_consolidate_re,  noninst_wrongassign_re, noninst_transfer_re, noninst_instruction_re, noninst_instruction2_re, noninst_nolonger_re]:
            temp_search = regex.search(entry_text.lower())
            if temp_search != None:
                return -999
        #Dismissal
        for regex in [dismissal_re, dismissal2_re, dismissal3_re, dismissal4_re]:
            temp_search = regex.search(entry_text.lower())
            if temp_search != None:
                return -10
    return 0

def check_docket(dlines):
    '''
    checks to see if there are transfers in the docket
    '''
    #handle the check
    if len(dlines)>0:
        entry_text = string_sanitizer(dlines[0][-1])
    else:
        entry_text = ''
    #Conditions on transfers
    if len(dlines)==0:
        return False
    elif 'EXECUTIVE COMMITTEE ORDER' in entry_text:
        return False
    elif len([x for x in dlines if type(x[-1])==float])>0:
        return False
    elif len([x for x in dlines if 'clerical error' in x[-1]])>0:
        return False
    elif 'in error' in entry_text:
        return False
    elif 'shall not be used for any other proceeding' in entry_text:
        return False
    else:
        return True

def nonetype_sanitizer(avar):
    '''
    Ensures that the variable is not a none type
    '''
    if type(avar) not in [str, int, float]:
        return ''
    else:
        return avar

def string_sanitizer(avar):
    '''
    Sanitizes a variable to a string
    '''
    avar = nonetype_sanitizer(avar)
    if type(avar) !='str':
        return str(avar)
    else:
        return avar

def clean_name(judge_name, punc=True):
    #deletions
    titles = ['Judge ', 'Senior Judge ', 'Magistrate Judge ', 'Chief Judge ', 'Honorable ']
    puncs = ['\.', ',']
    #Clean the titles
    try:
        for title in titles:
            if title in judge_name:
                judge_name = judge_name.split(title)[-1]
            if punc == True:
                for punc in puncs:
                    judge_name = re.sub('.,', '', judge_name)
            judge_name = judge_name.strip(' ')
    except TypeError:
            pass
    return judge_name

def identify_judge(docket_block):
    '''
    Identifies/predicts a judge using spacy's entity detection
    input:
        docket_block - list of docket entries
    output: 
        judge_name
    '''
    honorifics = ['Magistrate', 'Honorable', 'Judge']
    #Get the names
    judge_names = []
    for dline in docket_block:
        doc = nlp(dline[-1])
        for ent in doc.ents:
            if ent.label_ == 'PERSON':
                if doc[ent.start-1].text in honorifics:
                    judge_names.append(ent.text)
    #Check name occurences and decide on judge
    if len(judge_names)==0:
        judge_name = ' '
    elif len(judge_names)==1:
        judge_name =  judge_names[0]
    else:
        counted = Counter(judge_names)
        #If names are all equal then pull the first name, since that comes first during a reassignment
        if len( set(counted.values()) ) == 1:
            judge_name = judge_names[0]
        else:
            judge_name = counted.most_common()[0][0]
    return judge_name

def identify_judge_entries(docket, district, case_id, djudge):
    '''
    Attributes each docket entry to a judge
    input:
            docket - list of docket entries
            district - court for case
    output:
       judge_ind_entries, list that is ripe for a dataframe
    '''
    #Data and transfer phrase
    judge_ind_entries = []
    transfer_phrase = 'EXECUTIVE COMMITTEE ORDER: It appearing that cases previously assigned'
    #Check to see if there are transfers
    dcheck = check_docket(docket)
    transfer_indices = []
    if dcheck:
        for i, dline in enumerate(docket):
            if transfer_phrase in dline[-1]:
                transfer_indices.append(i)
    #If there are transfers, divide it up
    if transfer_indices != []:
        #need to check against the last index being the last entry
        if transfer_indices[-1] == len(docket)-1 and len(transfer_indices)==1:
            tindices = [0,-1]
        elif transfer_indices[-1] == len(docket)-1:
            tindices = [0] + transfer_indices
        #If not then iterate normally with the last entry
        else:
            tindices = [0] + transfer_indices + [-1]
        #Block iteration
        if tindices:
            #Iterate through the blocks
            for ti, block_index in enumerate(tindices[:-1]):
                end_index = tindices[ti+1]
                if end_index == -1:
                    docket_block = docket[block_index:]
                else:
                    docket_block = docket[block_index:end_index+1]
                pjudge = identify_judge(docket_block)
                if pjudge==' ':
                    print(case_id, 'blank judge')
                judge_ind_entries += [ map(nonetype_sanitizer, [district, case_id, clean_name(pjudge,punc=False), [0], x[-1]]) \
                                       for x in docket ]
    #Only one judge
    else:
        #Error case check
        wrong_assignment=False
        for dline in docket:
            try:
                if 'assigned in error' in dline[-1]:
                    wrong_assignment = True
                elif 'assigned' in dline[-1] and 'in error' in dline[-1]:
                    wrong_assignment = True
                elif 'clerical error' in dline[-1]:
                    wrong_assignment = True
                elif 'shall not be used for any other proceeding' in dline[-1]:
                    wrong_assignment = True
            except TypeError:
                #Type error is a suppressed case
                print(district, case_id)
        #If the wrong assignment is there, use the function to identify the judge
        if wrong_assignment == True:
            pjudge = identify_judge(docket)
        else:
            pjudge = djudge
        #Now add the lines
        judge_ind_entries += [ map(nonetype_sanitizer, [district, case_id, clean_name(pjudge, punc=False), x[0], x[-1]]) \
                               for x in docket ]
    return judge_ind_entries

def main(args):
    '''
    Processes all unique filepaths detected
    '''
    # Dataset
    dataset = []
    # File handler
    for jfhandle in [x.strip() for x in open('unique_docket_filepaths.txt').readlines()]:
        #Load the data files directly to pull out the data
        if 'recap' in jfhandle:
            jdata = remap_recap_data(jfhandle)
        else:
            jdata = json.load( open(jfhandle) )
        #Check to see if there is IFP and even a reason to continue
        if 'docket' in jdata:
            line_num = find_ifp_entry_line(jdata['docket'])
        else:
            print('Empty docket: ', jfhandle)
            line_num = 9999
        #Process into the individual docket entries if ifp exists
        if line_num < 9999:
            print(jfhandle)
            ind_entries = identify_judge_entries(jdata['docket'], jdata['download_court'], jdata['case_id'], jdata['judge'])
            df = pd.DataFrame(ind_entries, columns = columns)
            possible_ifp_motions = list(df.entry_text.apply(ifp_grant_identification))
            #try to find the index where it was granted
            grant_index, deny_index, noninst_index, dismissal_index, no_decision = None, None, None, None, None
            if 1 in possible_ifp_motions:
                grant_index = possible_ifp_motions.index(1)
            elif -1 in possible_ifp_motions:
                deny_index = possible_ifp_motions.index(-1)
            elif -999 in possible_ifp_motions:
                noninst_index = possible_ifp_motions.index(-999)
            elif -10 in possible_ifp_motions:
                dismissal_index = possible_ifp_motions.index(-10)
            #If both are none set no decision to true
            if grant_index==None and deny_index==None and noninst_index==None and dismissal_index==None:
                no_decision = True
            #Now assemble the row, pull the judge first. if it was granted or denied then we want the judge that did that action
            #Otherwise we want the judge it was asked of
            if no_decision!=True:
                if grant_index!=None:
                    judge_name = df.iloc[grant_index].judge_name
                    entry_date = df.iloc[grant_index].entry_date
                    resolution = 1
                elif deny_index!=None:
                    judge_name = df.iloc[deny_index].judge_name
                    entry_date = df.iloc[deny_index].entry_date
                    resolution = -1
                elif noninst_index!=None:
                    judge_name = df.iloc[noninst_index].judge_name
                    entry_date = df.iloc[noninst_index].entry_date
                    resolution = -999
                elif dismissal_index!=None:
                    judge_name = df.iloc[dismissal_index].judge_name
                    entry_date = df.iloc[dismissal_index].entry_date
                    resolution = -10
            else:
                judge_name = df.iloc[line_num].judge_name
                entry_date = df.iloc[line_num].entry_date
                resolution = 0
            #Build the dataset
            dataset.append( [df.iloc[line_num].jurisdiction, df.iloc[line_num].case_id, judge_name, entry_date, resolution] )
    #Conver to dataframe and save
    ifp_df = pd.DataFrame(dataset, columns = ifp_columns)
    final_df = ifp_dtools.process_dataframe(ifp_df)
    final_df.loc[:, ['jurisdiction', 'case_id', 'hash_name', 'entry_date', 'resolution', 'total_decisions']].to_csv('ifp_cases.csv')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="")
    args = parser.parse_args()
    main(args)
