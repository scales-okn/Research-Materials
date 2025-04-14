# ported by Scott from research_dev/code/research/entity_resolution/judges_public/entity_extraction/extract_new_cases.py and extract_header_judges.py

import pandas as pd
import glob
import json
import sys
import os
import re
import argparse
import multiprocessing as mp
from multiprocessing.dummy import Pool as ThreadPool
import tqdm
import spacy
from pathlib import Path

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)   

droppers = ['ebc','executive committee','unassigned','magistrate judge','magistrate judge unassigned magistrate',
'judge unassigned judge','judge unassigned', 'cvb judge', 'debt-magistrate','mia duty magistrate',
'CVRecovery Case CRStatistics Unassigned', 'Cvrecovery Case Crstatistics', 'civ','mdl']

base_prefixes = ['magistrate judge judge','us district judge','district judge','mag\. judge','magistrate judge','bankruptcy judge',
                 'honorable judge','mag judge','dist judge',
                 'magistrate','honorable', 'hon\.', 'judge', 'mj', 'justice']
base_prefixes_snr = ['senior '+b for b in base_prefixes]
base_prefixes_chief = ['chief '+b for b in base_prefixes]

all_prefixes = base_prefixes + base_prefixes_chief + base_prefixes_snr
all_prefixes = sorted(all_prefixes, key=lambda x: len(x.split()), reverse=True)

header_cause_pattern = re.compile(r'\s*cause:\s*',flags=re.I)
header_referred_pattern = re.compile(r'\s*referred\s*(to)?\s*', flags=re.I)
header_designated_pattern = re.compile(r'^designated\s*',flags=re.I)
dash_mj_pattern = re.compile(r'-mj$', flags=re.I)
header_prefixes = re.compile(rf'\b({"|".join(all_prefixes)})[\.]*($|(?=\s))', flags=re.I)

jnlp = spacy.load('en_pipeline')


def judge_detection(judge_str):

    if not judge_str or len(judge_str)==0 or any(i for i in droppers if i.lower() == judge_str.lower()):
        return None

    if ' and ' in judge_str.lower():
        print(judge_str)

    cause = header_cause_pattern.search(judge_str)
    if cause:
        judge_str = judge_str[0:cause.start()]    

    referred = header_referred_pattern.search(judge_str)
    if referred:
        judge_str = judge_str[0:referred.start()]      

    designated = header_designated_pattern.search(judge_str)
    if designated:
        new_star = designated.end()
        judge_str = judge_str[new_star:]
    else:
        new_star = 0
    
    dash_mj = dash_mj_pattern.search(judge_str)
    if dash_mj:
        judge_str = judge_str[0:dash_mj.start()]


    m = header_prefixes.search(judge_str)
    if m:
        pref_span_start =  m.start()
        pref_span_end = m.end()

        pretext = judge_str[0:m.end()]
        name = judge_str[m.end()+1:]
        if name:
            name_span_start = m.end()+1 + new_star
            name_span_end = name_span_start + len(name)
        else:
            name_span_start = None
            name_span_end = None
    else:
        name = judge_str
        pretext = None
        name_span_start = 0 + new_star
        name_span_end = len(judge_str) + new_star
        pref_span_start =  None
        pref_span_end = None

    return {
        'extracted_pretext':pretext,
        'extracted_entity': name,
        'Ent_span_start':name_span_start,'Ent_span_end':name_span_end,
        'Pre_span_start':pref_span_start,'Pre_span_end':pref_span_end}


def extract_header_entities(header_judges):

    out_dicts = []
    
    if 'referred_judges' in header_judges:
        RJ_enum = 0
        for judge_string in header_judges['referred_judges']:
            label = f"RJ-{RJ_enum}"
            RJ_enum+=1
            detected_judge = judge_detection(judge_string)
            if detected_judge:
                out_dicts.append(
                    {
                    'original_text': judge_string,
                    **detected_judge,                 
                    'docket_source': 'case_header',                      
                    'Entity_Extraction_Method': 'referred_judges',
                    'judge_enum': RJ_enum,
                    'party_enum': None,
                    'pacer_id': None,                    
                    'docket_index': None                    
                    })

    if 'judge' in header_judges:
        label = "AJ"
        detected_judge = judge_detection(header_judges['judge'])
        if detected_judge:
            out_dicts.append(
                {
                'original_text': header_judges['judge'],
                **detected_judge,                 
                'docket_source': 'case_header',                      
                'Entity_Extraction_Method': 'assigned_judge',
                'judge_enum': 0,
                'party_enum': None,
                'pacer_id': None,                
                'docket_index': None                
                })

    return out_dicts


def extract_party_judge_entities(party_list):
    out_dicts = []
    for assigned, referred, pacer_id, p_enum in party_list:
        if referred:
            r_enum = 0
            for ref in referred:
                label = f"RJ-{r_enum}"
                r_enum+=1
                detected_judge = judge_detection(ref)
                if detected_judge:
                    out_dicts.append(
                        {
                        'original_text': ref,
                        **detected_judge,   
                        'docket_source': 'case_parties',                      
                        'Entity_Extraction_Method': 'referred_judges',
                        'judge_enum': r_enum,
                        'party_enum': p_enum,
                        'pacer_id': pacer_id,                        
                        'docket_index': None                        
                        })

        if assigned:
            label = f"AJ"
            detected_judge = judge_detection(assigned)
            if detected_judge:
                out_dicts.append(
                    {
                    'original_text': assigned,
                    **detected_judge,                     
                    'docket_source': 'case_parties',                      
                    'Entity_Extraction_Method': 'assigned_judge',
                    'judge_enum': 0,
                    'party_enum': p_enum,
                    'pacer_id': pacer_id,                    
                    'docket_index': None                    
                    })    
    
    return out_dicts


def extract_entities_sub_pipe(doc):
    
    out_dicts = []

    for jj in [ent for ent in doc.ents if ent.label_ == 'JUDGE']:

        pretext=None
        postext=None
        pre_ss=None
        pre_se=None
        post_ss=None
        post_se=None
        
        star = jj.start
        fin = jj.end
        # find previous n tokens
        n=7
        pre_n = doc[max(jj.start-n,0):jj.start]
        star = max(jj.start-n,0)
        if pre_n:
            pretext = pre_n.text
            pre_ss = pre_n.start_char
            pre_se = pre_n.end_char
            
        # find following n tokens
        n=7
        post_n = doc[jj.end:jj.end+n]
        fin = min(len(doc),jj.end+n)
        if post_n:
            postext = post_n.text
            post_ss = post_n.start_char
            post_se = post_n.end_char

        out_dicts.append(
                {
            'original_text': doc[star:fin].text,
            'full_span_start': doc[star:fin].start_char,
            'full_span_end': doc[star:fin].end_char,
            'extracted_pre_5': pretext,
            'extracted_entity': jj.text,
            'extracted_post_5': postext,
            'Ent_span_start': jj.start_char,
            'Ent_span_end': jj.end_char,                                  
            'docket_source': 'line_entry',
            'Entity_Extraction_Method': 'SPACY JNLP2'
            }
        )

    return out_dicts


def counsels_and_parties(case_parties, meta):

    counsels = []
    parties = []
    for party in case_parties:

        parties.append(
            {
            **meta,
            "Role":party['role'],
            "Entity": party['name']
            }
        )


        if 'counsel' in party:
            if not party['counsel']:
                continue
                
            for each in party['counsel']:
                counsels.append(
                    {
                    **meta,
                    "Role":party['role'],
                    "Entity": each['name']
                    }
                )
    
    return parties, counsels


def load_data(fpath, pbar):

    # open json
    with open(fpath) as f:
        case = json.load(f)

    year = int(case['filing_date'].split('/')[-1])
    if 'court' not in case:
        court = ucid.split(";;")[0]
    else:
        court = case['court']

    meta = {
        'ucid': case['ucid'],
        'court': court,
        'cid': case['case_id'],
        'year': year,
        'filing_date': case['filing_date']
    }
    
    ######################
    # PARTIES & COUNSELS #
    ######################
    if 'parties' in case and case['parties']:
        parties, counsels = counsels_and_parties(case['parties'], meta)
    else:
        parties, counsels = [], []

    #################
    # HEADER JUDGES #
    #################
    case_type = case['case_type']

    # we want to limit how much of the case jsons we are moving around in memory, so we cut out all the bulk quick and only 
    # pass along the values we care about
    header_keys_interested = ['judge','referred_judges']
    header_data = {key:case[key] for key in header_keys_interested if case[key]}

    header_judges = []
    if case_type == 'cr' and case['parties']:
        # LOOK THRU THE PARTIES
        party_judges = [(party['judge'], party['referred_judges'], party['pacer_id'], enum) 
                    for enum, party in enumerate(case['parties']) 
                    if party['judge'] or party['referred_judges']]
        if party_judges:
            party_info = extract_party_judge_entities(party_judges)
            padded_with_meta = [{**meta ,**each} for each in party_info]
            header_judges += padded_with_meta

    # CURRENT APPROACH, STANDARD META_KEYS
    # do it for every case type
    if header_data:
        header_info = extract_header_entities(header_data)
        padded_with_meta = [{**meta ,**each} for each in header_info]
        header_judges += padded_with_meta

    ################
    # ENTRY JUDGES #
    ################

    # extract all of the entries so we can leverage nlp.pipe after
    text_entries = []
    if 'docket' in case and case['docket']:
        meta = {
            'ucid': case['ucid'],
            'court': court,
            'year': year,
            'cid': case['case_id']
        }
        entries = [(d['docket_text'], d['date_filed']) for d in case['docket']]

        threeples = []
        threeples = [(*e, i, meta) for i,e in enumerate(entries)]
        text_entries += [(tup[0],tup[1:]) for tup in threeples]

    pbar.update()
    return (parties, counsels, header_judges, text_entries)


def run_thru_entries(texts, cores=8):
    processed_data = []
    for doc, context_tuple in tqdm.tqdm(
        jnlp.pipe(texts,
         n_process=cores, batch_size=1000,  as_tuples=True, disable = ['lemmatizer','tagger','parser','attribute_ruler','morphologizer','textcat']),
        total=len(texts), desc=f'[--Extraction--]', leave=True
        ):
        if 'JUDGE' in [ent.label_ for ent in doc.ents]:
            entry_date, enum_index, meta = context_tuple
            rows = extract_entities_sub_pipe(doc)

            rows = [
                {
                    **meta,
                    'docket_index': enum_index,
                    'entry_date': entry_date,
                    **row
                } for row in rows ]
            if rows:
                processed_data+=rows
    return processed_data


def main_run(indir, outdir, cores=10):

    fpaths = glob.glob(str(Path(indir).resolve()/'*.json'))
    pbar = tqdm.tqdm(total = len(fpaths))
    pool = ThreadPool(cores)
    returned_tuples = pool.starmap(load_data, ((fpath, pbar) for fpath in fpaths))

    pdf_rows = []
    cdf_rows = []
    hj_rows = []
    entry_rows = []
    for p,c,h,t in returned_tuples:
        if p:
            pdf_rows+=p
        if c:
            cdf_rows+=c
        if h:
            hj_rows+=h
        if t:
            entry_rows+=t

    PDF = pd.DataFrame(pdf_rows)
    CDF = pd.DataFrame(cdf_rows)
    Headers = pd.DataFrame(hj_rows)

    outdir = Path(outdir).resolve()
    PDF.to_csv(outdir/'parties.csv', index=False)
    CDF.to_csv(outdir/'counsels.csv', index=False)
    Headers.to_csv(outdir/'headers.csv', index=False)

    del(PDF)
    del(CDF)
    del(Headers)

    print("Now performing model extraction")
    extractions = run_thru_entries(entry_rows, cores)
    EDF = pd.DataFrame(extractions)
    EDF.to_csv(outdir / 'entries.csv', index=False)