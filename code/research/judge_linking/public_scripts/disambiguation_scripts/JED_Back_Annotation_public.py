import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[4]))

from support import settings
import support.judge_functions as JF
import support.court_functions as CF

from JED_Utilities_public import write_to_jsonl_ucid_file

import multiprocessing as mp
from multiprocessing.dummy import Pool as ThreadPool

import argparse
import pandas as pd
from collections import defaultdict, Counter
import tqdm
import time

def load_fjc_appointments(fpath):
    """Load the FJC biographical dictionary and transform it into longitudinal data, separated by distinct appointments

    Args:
        fpath (pathlib.Path): location of the FJC csv

    Returns:
        pandas.DataFrame: the longitudinal appointments data from the FJC
    """
    
    # load the data
    FJC = pd.read_csv(fpath)
    # these are the columns I want that are named as is
    judge_demographics_cols_keys = ['nid', 'jid', 'FullName', 'Last Name', 'First Name', 'Middle Name', 'Suffix']

    # these columns need nuance to extract --  structurally they appear as "column name (#)"
    judge_demographics_cols_cast = [
        'Court Type', 'Court Name', 'Appointment Title', 'Confirmation Date', 'Commission Date', 'Termination Date']

    # extract rows from the dataframe, but make it a long frame instead of wide using dictionaries
    new_rows = []
    for index, row in FJC.iterrows():
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
    name2abb = {c: CF.classify(c) for c in fjc_expanded['Court Name'].unique().tolist()}
    fjc_expanded['Court Name Abb'] = fjc_expanded['Court Name'].map(name2abb)

    # fill nulls for termination to today (not yet terminated); convert date cols to datetimes
    fjc_expanded['Termination Date'].fillna(pd.to_datetime('today').date(), inplace=True)
    fjc_expanded['Commission Date'] = fjc_expanded['Commission Date'].apply(lambda x: pd.to_datetime(x).date())
    fjc_expanded['Termination Date'] = fjc_expanded['Termination Date'].apply(lambda x: pd.to_datetime(x).date())
    
    return fjc_expanded

def load_bankruptcy_magistrate_positions(fpath):
    """Load the UVA bankrupcty and magistrate appointment positions data

    Args:
        fpath (pathlib.Path): path to file

    Returns:
        pandas.DataFrame: the ba-mag appointment data by chronological windows of appointments
    """
    ### LOAD POSITIONS
    BAMAG = pd.read_csv(fpath)

    # prune to the particular judge roles we care about
    BAMAG = BAMAG[
        (BAMAG.TITLE.apply(lambda x: 'judge' in str(x).lower())) &
        (BAMAG.INSTITUTION.apply(lambda x: 'court' in str(x).lower())) &
        (BAMAG.INSTITUTION.apply(lambda x: str(x)[0:4].lower()) == 'u.s.')
    ].copy()

    # classify their court of assignment into an abbreviation we are familiar with
    BAMAG['_court_abbrv'] = BAMAG.INSTITUTION.apply(lambda x: CF.classify(x))
    
    return BAMAG

def create_lookup_bamag_roles(JEL):
    """Given a JEL dataframe, identify the Bankruptcy and Magistrate judges that have ground truth appointments, and using
    their ID's determine if they are bankruptcy or magistrate judges

    Args:
        JEL (pandas.DataFrame): SCALES JEL data

    Returns:
        dict: keyed by SJID, values are inferred labels from the ground truth IDs
    """
    
    # init empty dict
    sjid_bamag_lookup = {}
    # for every judge that comes from the ground truth data source (but not FJC)
    for index, row in JEL[JEL.SCALES_Judge_Label=='BA-MAG Judge'].iterrows():
        # infer the label
        if 'bnk' in row['BA_MAG_ID']:
            inferred = 'Bankruptcy_Judge'
        elif 'mag' in row['BA_MAG_ID']:
            inferred = 'Magistrate_Judge'
        else:
            inferred = 'Nondescript_Judge'
        # code it into the dict
        sjid_bamag_lookup[row['SJID']] = inferred
        
    return sjid_bamag_lookup

def create_lookup_fjc_dates(fjc_expanded):
    """Given longitudinal appointment histories for FJC judges, assemble them into a single data object per NID

    Args:
        fjc_expanded (pandas.DataFrame): the longitudinal FJC appointments data

    Returns:
        dict: dictionary of dictionaries. Outer key is the FJC NID, the inner dictionary is keyed by commission date for each appointment
    """
    # init empty dict
    fjc_dict = {}
    # for every appointment in the FJC data (one NID could have multiple rows)
    for index, row in fjc_expanded.iterrows():

        nid = row['nid']
        commdate = row['Commission Date']
        termdate = row['Termination Date']
        court = row['Court Name Abb']
        ctype = row['Court Type']
        # if this judge hasnt had an appointment yet in the dict
        if nid not in fjc_dict:
            fjc_dict[nid] = {commdate: (termdate, court, ctype, 'Article III')}
        # else update this judges appointments
        else:
            fjc_dict[nid][commdate] = (termdate, court, ctype, 'Article III')
            
    return fjc_dict
    
def create_lookup_bamag_dates(BAMAG):
    """Given appointment histories for bankruptcy and magistrate judges, assemble them into a single data object per ID

    Args:
        BAMAG (pandas.DataFrame): the position appointment histories from the BA/MAG dataset

    Returns:
        dict: dictionary of dictionaries. Outer key is the BA/MAG JUDGE_ID, the inner dictionary is keyed by start date for each appointment
    """
    # init empty dict
    bamag_dict = {}
    # for every appointment row (one ID could have multiple rows)
    for index, row in BAMAG.iterrows():
        jid = row['JUDGE_ID']
        start_date = row['DATE_START']
        end_date = row['DATE_END']
        court = row['_court_abbrv']
        institution = row['INSTITUTION']

        # there is a lot of wonkiness in the date formatting, we went for speed and bipassed fixing the bad entries
        try:
            start_date = pd.to_datetime(start_date).date()
        except:
            continue
        # write the data into the dict by ID
        if jid not in bamag_dict:
            bamag_dict[jid] = {start_date: (end_date, court, institution, "Bankruptcy-Magistrate")}
        else:
            bamag_dict[jid][start_date] = (end_date, court, institution, "Bankruptcy-Magistrate")
            
    return bamag_dict
    
def create_lookup_SJID_dates_labels(JEL, FJC_path, BAMAG_Positions):
    """Given the ground truth appointments data and the post-disambiguation JEL, create a lookup for the
    SJID appointments, and a lookup for their disambiguation estimated labels

    Args:
        JEL (pandas.DataFrame): pre-loaded JEL data
        FJC_path (pathlib.Path): path to the FJC_path file
        BAMAG_Positions (pathlib.Path): path to the BAMAG_Positions file

    Returns:
        tuple: dict, dict the lookups by SJID for the appointment labels and times for a judge
    """
    
    # load the ground truth data and transform them into longitudinal dataframes
    fjc_expanded = load_fjc_appointments(FJC_path)
    BAMAG = load_bankruptcy_magistrate_positions(BAMAG_Positions)
    
    # convert the data frames into dictionary data models, 
    # with 1 key per ID that contains a dictionary of all judicial appointments for that ID
    fjc_dict = create_lookup_fjc_dates(fjc_expanded)
    bamag_dict = create_lookup_bamag_dates(BAMAG)
    
    # empty dicts to be added to
    sjid_lookup = {}
    JEL_Labs = {}
    # for every known judge/SJID
    for index, row in JEL.iterrows():
        sjid = row['SJID']
        nid_data = {}
        ba_mag_data = {}

        # if there is an NID, grab the NID appointment data
        if not pd.isna(row['NID']):
            nid_data = fjc_dict.get(int(row['NID']))
            if not nid_data:
                nid_data = {}
        # if there is a BAMAG ID, grab the related appointment data
        if not pd.isna(row['BA_MAG_ID']):
            ba_mag_data = bamag_dict.get(row['BA_MAG_ID'])
            if not ba_mag_data:
                ba_mag_data = {}
        # unpack the two ground truth sources into one dictionary for the SJID
        sjid_lookup[sjid] = {
            **nid_data,
            **ba_mag_data
        }

        # for every JEL row, also create a lookup of the post-disambiguation entity label
        JEL_Labs[sjid] = row['SCALES_Judge_Label']
    
    return sjid_lookup, JEL_Labs

def label_categorizer(each):
    """Given a label either from the FJC appointments or the BA/MAG dataset,
    parse it into simple categories of "at-the-time" judge labels

    Args:
        each (tuple or str): the label for the judge appointment (could be a tuple or just a string)
    """
    
    #Helper that replaces underscores with spaces
    _replace_ = lambda x: x.replace("_", " ")
    
    # if the label is a tuple, it came in as (data_source, court type, court location)
    if type(each) == tuple:
        source, ctype, locale = each
        ctype = ctype.lower()
        if source == 'Article III':
            if 'district court' in ctype:
                return "District Judge"
            elif 'appeals' in ctype:
                return "Appellate Judge"
            elif 'supreme court' in ctype:
                return 'Supreme Court Justice'
            elif 'other' in ctype:
                return 'Article III Judge'
            else:
                return each
        elif source == 'Bankruptcy-Magistrate':
            if 'district court' in ctype:
                return "Magistrate Judge"
            elif 'bankruptcy' in ctype:
                return "Bankruptcy Judge"
            else:
                return _replace_(each)
        else:
            return _replace_(each)
    else:            
        # if it wasn't a tuple, it's the standard JEL string label for the entity
        return _replace_(each)
    
    return _replace_(each)

def back_annotate_ucid_sel(fpath, sjid_lookup, sjid_bamag_lookup, JEL_Labs, pbar):
    """meta function that back-annotates a whole SEL file 
    (completely independent of any other files also being back-annotated)

    Args:
        fpath (pathlib.Path): a pathlib path to the SEL file
        sjid_lookup (dict): a lookup dictionary, keyed by sjid for the date of appointments
        sjid_bamag_lookup (dict): a lookup dictionary, keyed by the ba/mag id and contains the inferred entity label
        JEL_Labs (dict): a lookup dictionary, keyed by sjid and contains the JEL post-disambiguation labels
        pbar (tqdm.tqdm.): progress bar object
    """

    # iterate the progress bar
    pbar.update()

    # the filepath stem will be the file-name form UCID
    ucid = fpath.stem
    # transform it into our ucid structure
    ucid = ucid.split('.')[0].split('-')
    new_ucid = f"{ucid[0]};;{ucid[1]}:{'-'.join(ucid[2:])}"

    # load the SEL corresponding to this ucid (could also have just loaded the jsonl directly to the input fpath)
    sample_SEL = JF.load_SEL(new_ucid, as_df=False)
    # if there was no data in this file, move on
    if not sample_SEL:
        return

    # keep track of the SJIDs on this case and what we labeled them as
    case_labels = defaultdict(list)
    # track the indices of the data rows we could not tag in the first pass because they had no entry or filing dates
    case_misses = []

    new_data = []
    # for every row of SEL data (a  json object)
    for index, DATA in enumerate(sample_SEL):

        D_date = DATA.get('entry_or_filing_date')
        D_sjid = DATA['SJID']
    
        # this is weird, but a few files somehow were written without them. I suspect these are old SELs that did not get cleaned out or overwritten
        if 'entry_or_filing_date' not in DATA:
            print(new_ucid, "does not have entry or filing date??")

        # try converting the SEL date to a real date
        try:
            D_date = pd.to_datetime(D_date).date()
        except:
            # if not, the date was null, or something else is whack about it
            case_misses.append(index)
            continue
        
        # for this SJID, grab the appointment history (dictionary keyed by dates, values are the positions)
        dates = sjid_lookup.get(D_sjid)
        if dates:
            # if there was ground truth data, let's sort the keys ascending so the earliest appointments are first in the list
            sorted_dates = sorted(dates)
            # we then want all appointments that came before but not after this docket entry date
            keeps = [d for d in sorted_dates if D_date >= d]
            if keeps:
                # if there were appointments before this date, great the most recent appointment before this date is presumed to be this judges position at the time
                fin = keeps[-1]
                grab = dates[fin]
                label = (grab[3], grab[2], grab[1])
            else:
                # if there were no appointments before this time, we either take their inferred label from the ground truth BA/MAG set, or we take the JEL label
                if D_sjid in sjid_bamag_lookup:
                    label  = sjid_bamag_lookup[D_sjid]
                elif JEL_Labs[D_sjid] == 'FJC Judge':
                    # if the JEL label was just generic FJC judge and we got to here, it means this docket entry came before their Article III appointment,
                    # we make an assumption that they were acting as a magistrate judge at this time
                    label = "Magistrate Judge"
                else:
                    # otherwise, somehow this judge precedes any appointment we knew about and we could not categorize them
                    label = 'Precedes Ground Truth'
        else:
            # if there was no ground truth data and their labels are inconclusive or ambiguous, persist those labels
            if D_sjid in ['Inconclusive', 'Ambiguous']:
                label = D_sjid
            else:
                # else we can either infer from their BA/MAG IDs (not all of them had appointment history, but the ID is still decidedly bankruptyc or magistrate)
                if D_sjid in sjid_bamag_lookup:
                    label = sjid_bamag_lookup[D_sjid]
                else:
                    # or leverage the JEL labels
                    label = JEL_Labs[D_sjid]
                # label = 'No Ground Truth'

        # if we succesfully labeled this judge, track it
        case_labels[D_sjid].append(label)
        # create the new output data row
        out_data = {
            **DATA,
            'JUDGE_LABEL':  label_categorizer(label)
        }
        new_data.append(out_data)

    # if we could not label the judge before,
    if case_misses:
        for miss in case_misses:
            # if this judge was labeled in another row, see if there is a majority label we can use
            rowdat = sample_SEL[miss]
            rowsjid = rowdat['SJID']

            if case_labels[rowsjid]:
                label = Counter(case_labels[rowsjid]).most_common()[0][0]
            else:
                label = 'No Case Data'

            new_data.append({**rowdat,'JUDGE_LABEL': label_categorizer(label)})
            
    # if we get to the end of the process and have not created the same amount of output rows as we ingested from the SEL file, flag it as an error
    # (this should never happen)
    if len(new_data) != len(sample_SEL):
        print(F"ERROR, LOST SOME DATA: {ucid}")
    else:
        # write the new data back to the original path
        writes = (fpath, new_data)
        write_to_jsonl_ucid_file(writes)
    
    return

def back_annotate_directory(paths):
    """Given a group of paths (one of which points to the directory to back-annotate), back-annotate
    the SEL files

    Args:
        paths (dict): dictionary of file locations
    """
    
    # get the SEL directory
    rundir = paths['SEL_DIR']
    
    # instantiate JEL data
    JEL = pd.read_json(paths['JEL_FILE'], lines=True)

    # create the necessary data lookups that will be used
    sjid_lookup, JEL_Labs = create_lookup_SJID_dates_labels(JEL, paths['FJC_FILE'], paths['BAMAG_FILE'])
    sjid_bamag_lookup = create_lookup_bamag_roles(JEL)

    # it is assumed that the highest level directory is the SEL Directory and that is where we are starting
    for court in rundir.iterdir():
        for year in court.iterdir():
            N = len(list(year.glob('*.jsonl')))
            pbar = tqdm.tqdm(total=N, desc=f"{court.stem}-{year.stem}")

            # pool = ThreadPool(mp.cpu_count()-1)
            pool = ThreadPool(4)
            results = pool.starmap(back_annotate_ucid_sel,  [(jsonl, sjid_lookup, sjid_bamag_lookup, JEL_Labs, pbar) for jsonl in year.glob('*.jsonl')])
            pool.close()
            pool.join()
            pbar.close()
    
    return

def locate_files(args):
    """Given the script args, find or create PATH objects for the necessary ground truth data

    Args:
        args (argparse.Args): parsed args

    Returns:
        dict: dictionary of file paths
    """

    paths = {}
    if args.runtype == 'default':
        paths['FJC_FILE'] = settings.JUDGEFILE
        paths['BAMAG_FILE'] = settings.BAMAG_POSITIONS
        paths['JEL_FILE'] = settings.JEL_JSONL
        paths['SEL_DIR'] = settings.DIR_SEL
    else:
        # not sure this will ever be a use case?
        print("Hello future Chris, look at this problem you left for yourself!")

    return paths

def build_arg_parser():
    """isolate argparser creation to a function

    Returns:
        argparse.ArgumentParser: <--
    """

    parser = argparse.ArgumentParser(description="")
    parser.add_argument('--runtype', default= 'default', action='store',help = 'is this a standard run on the SEL Directory')

    return parser

if __name__ == "__main__":
    start = time.time()

    AP = build_arg_parser()
    args = AP.parse_args()

    paths = locate_files(args)

    back_annotate_directory(paths)

    end = time.time()
    print(f"{round((end-start)/60, 3)} minutes to complete.")


