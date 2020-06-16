import re
import pandas as pd

committee_names = {'Respondent':10000001, 'Unassigned':10000002, \
				   'Movant':10000003, 'Fugitive Calendar':10000004, \
				   'Executive Committee':10000005, 'Status':10000006, \
				   '':10000000}

def clean_names(jname):
    #If the 'judge_name' is in the committee names then get rid of it and return none
    if jname in committee_names:
        return None
    #Now lets clean up punctuation
    try:
        jname = re.sub('.,', '', jname)
    except TypeError:
        print(jname)
    return jname

def name_identifier(row, unique_set):
    jname = row['clean_name']
    jurisdiction = row['jurisdiction']
    #Need to need to check if the name is a part of another name
    match_indices = []
    for i,uname in enumerate(unique_set):
        if jname in uname and len(jname)<len(uname):
            match_indices.append(i)
    #Now we need to go through this
    #If there aren't any matches
    if match_indices == []:
        #and it's more than one word then we can return the name
        if len(jname.split(' ')) > 1:
            return jname
        #What to do with a one word name that doesn't have a match
        else:
            print('one word, no match', jname)
            return None
    else:
        #If theres only one match thats longer, then this should be easy and we return the one name
        if len(match_indices)==1:
            return unique_set[match_indices[0]]
        #If there's more than one, we need to figure out which one is better
        else:
            print('multiple matches', jname, match_indices)
            return None

######################
#Decision Functions
######################
def decision_average(xset):
    decisions = [x for x in xset if x in [1, -1]]
    granted = len([x for x in decisions if x==1])
    if len(decisions)>0:
        return granted/len(decisions)
    else:
        return np.nan

def total_cases(xset):
    return len(xset)

def total_decisions(xset):
    return len([x for x in xset if x in [1, -1]])

######################
# Processor
######################
def process_dataframe(df):
    df.dropna(subset=['judge_name'], inplace=True)
    df['clean_name'] = df.judge_name.apply(clean_names)
    df.dropna(subset=['clean_name'], inplace=True)
    unique_set = list(set( df.clean_name.unique() ))
    df['unique_name'] = df.apply(lambda x: name_identifier(x, unique_set), axis=1)
    df.dropna(subset=['unique_name'], inplace=True)
    #Total decisions
    jcountdf = df.loc[:, ['unique_name', 'resolution']].groupby('unique_name').agg(total_decisions).reset_index()
    jcountdf.columns = ['unique_name', 'total_decisions']
    df = df.merge(jcountdf, on='unique_name', how='left')
    return df
