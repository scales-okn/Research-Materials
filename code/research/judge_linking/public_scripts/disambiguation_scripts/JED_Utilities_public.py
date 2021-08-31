import logging
from pathlib import Path
import configparser
import multiprocessing as mp
import os
import tqdm
import json  
import pandas as pd

def ingest_config(config_path):
    """ function used in baseline disambiguation to parse the config arguments, make appropriate directories, and pass the args on to the script

    Args:
        config_path (str): string based relative path to the config file

    Returns:
        dict: object contains various args for the disambiguation script
    """
    # need it for file naming only, don't need it in rest of scripts
    import time

    def _build_base():
        """localized function only, assumes the variables have been created or called in the parent function.
        Creates the base data output directory and project directory for the disambiguation run

        Returns:
            PosixPath: Pathlib object pointing to the project outputs folder
        """
        # parsed config base directory and project names
        BASE_DIR = Path(config['OUT']['base_directory'])
        RUN_NAME = config['META']['name_of_proj']

        # if the base folder doesnt exist, create it
        if not BASE_DIR.is_dir():
            BASE_DIR.mkdir()

        # if the project run does not exist, create it
        assembled = BASE_DIR / Path(RUN_NAME)
        if assembled.is_dir():
            return assembled
        else:
            assembled.mkdir()
            print(f"Instantiated output directory here: {assembled}")
            return assembled

    def _build_logs():
        """Instantiate the log file and log location
        """
        def __establish_logger():
            """actually create the log file

            Returns:
                PosixPath: relative path to the log file
            """
            
            t = time.localtime()
            current_time = time.strftime("%H_%M_%S", t)
            # create the path to the .log file
            the_log = log_path / Path(f"JED_log_{current_time}.log")

            ## instantiate the log and set level to info so that we can document all of our cross-entity mapping for review
            logging.basicConfig(filename= the_log, format='%(message)s', level = logging.INFO)
            print(f"Instantiated logfile here: {the_log}")
            
            # test post
            test_post = logging.getLogger()
            test_post.info(f"|||"*30)
            test_post.info(f"\nJudge Entity Disambiguation")
            test_post.info(f"{config['META']['name_of_proj']}\n")
            test_post.info(f"|||"*30)
            
            return the_log

        # given the config output directory, instantiate a child "Logs" directory to store run logs
        log_path = OUT_DIR / Path('Logs')
        if log_path.is_dir():
            LOG_PATH = __establish_logger()
            return LOG_PATH
        else:
            log_path.mkdir()
            print(f"Instantiated logging directory here: {log_path}")

            LOG_PATH = __establish_logger()
            return LOG_PATH

    def _establish_out_files():
        """Generate the paths for the eventual output data files and data directories to be used at the end of the disambiguation run.
        The timestamp matches the initiating script run, not when the file is first written to (a 15-20 minute delay)

        Returns:
            PosixPath, PosixPath, PosixPath: location of JEL file, location of SEL file, location to write individual case-level SEL files
        """

        # if the data outputs directory doesnt exist, create it
        DATA_OUTS = OUT_DIR / Path(config['OUT']['data_outputs'])
        if not DATA_OUTS.is_dir():
            DATA_OUTS.mkdir()

        # create paths for all of the respective outfiles that get written to at the end of disambiguation
        t = time.localtime()
        current_time = time.strftime("%H_%M_%S", t)
        jel_path = DATA_OUTS / Path(f"JEL_{current_time}.jsonl")
        sel_path = DATA_OUTS / Path(f"SEL_{current_time}.jsonl")
        sel_dir = DATA_OUTS / Path(f"SEL_DIR/")
        # if the sel directory hasn't been created, create it
        if not sel_dir.is_dir():
            sel_dir.mkdir()

        return jel_path, sel_path, sel_dir

    # standard parsing library
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # build our directories
    OUT_DIR = _build_base()
    LOG_PATH = _build_logs()
    jel_path, sel_path, sel_dir = _establish_out_files()

    # return the parsing as a structured dict for the rest of the script
    pythonic_config = {
        'OUT_DIR': OUT_DIR,
        'LOG_PATH': LOG_PATH,
        'DATA_FILES': config['DATA']['NER_Extractions'],
        'DATA_PARTIES':config['DATA']['parties_csv'],
        'DATA_COUNSELS':config['DATA']['counsels_csv'],
        'FJC': {
            'fjc_file':config['FJC']['fjc_file'],
            'Low': config['FJC']['commission_start'],
            'High': config['FJC']['commission_end']
        },
        'OUT_PATHS':{
            'JEL':jel_path,
            'SEL':sel_path,
            'SEL_DIR':sel_dir
        }
    }

    return pythonic_config

def ingest_new_tagger_config(config_path):
    """ function used in ad hoc tagging to parse the config arguments, make appropriate directories if necessary, and pass the args on to the script

    Args:
        config_path (str): string based relative path to the config file

    Returns:
        dict: object contains various args for the disambiguation script
    """
    
    import time

    def _build_base():
        """localized function only, assumes the variables have been created or called in the parent function.
        Creates the base data output directory and project directory for the disambiguation run

        Returns:
            PosixPath: Pathlib object pointing to the project outputs folder
        """

        BASE_DIR = Path(config['OUT']['base_directory'])
        RUN_NAME = config['META']['name_of_proj']

        assembled = BASE_DIR / Path(RUN_NAME)
        if assembled.is_dir():
            return assembled
        else:
            assembled.mkdir()
            print(f"Instantiated output directory here: {assembled}")
            return assembled

    def _build_logs():
        """Build log file
        """
        def __establish_logger():
            """actually create the log file

            Returns:
                PosixPath: relative path to the log file
            """
            t = time.localtime()
            current_time = time.strftime("%H_%M_%S", t)
            the_log = log_path / Path(f"JED_tagging_log_{current_time}.log")
            ## instantiate the log and set level to info so that we can document all of our cross-entity mapping for review
            logging.basicConfig(filename= the_log, format='%(message)s', level = logging.INFO)
            print(f"Instantiated logfile here: {the_log}")
            
            test_post = logging.getLogger()
            test_post.info(f"|||"*30)
            test_post.info(f"\nJudge Entity Tagging")
            test_post.info(f"{config['META']['name_of_proj']}\n")
            test_post.info(f"|||"*30)
            
            return the_log

        # make the log directories
        log_path = OUT_DIR / Path('Logs')
        if log_path.is_dir():
            LOG_PATH = __establish_logger()
            return LOG_PATH
        else:
            log_path.mkdir()
            print(f"Instantiated logging directory here: {log_path}")

            LOG_PATH = __establish_logger()
            return LOG_PATH

    def _establish_out_files():
        """Generate the paths for the eventual output data files and data directories to be used at the end of the tagging run.
        The timestamp matches the initiating script run, not when the file is first written to (a 15-20 minute delay)

        Returns:
            PosixPath, PosixPath: location of SEL file, location to write individual case-level SEL files
        """
        DATA_OUTS = OUT_DIR / Path(config['OUT']['data_outputs'])
        if not DATA_OUTS.is_dir():
            DATA_OUTS.mkdir()

        t = time.localtime()
        current_time = time.strftime("%H_%M_%S", t)
        sel_path = DATA_OUTS / Path(f"SEL_Update_{current_time}.jsonl")
        sel_dir = DATA_OUTS / Path(f"SEL_DIR/")

        if not sel_dir.is_dir():
            sel_dir.mkdir()

        return sel_path, sel_dir

    # standard parsing
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # run our path creators
    OUT_DIR = _build_base()
    LOG_PATH = _build_logs()
    sel_path, sel_dir = _establish_out_files()

    # pass the args on as a restructured dict
    pythonic_config = {
        'OUT_DIR': OUT_DIR,
        'LOG_PATH': LOG_PATH,
        'DATA_FILES': {
            'entries': config['DATA']['new_entries'],
            'headers': config['DATA']['new_headers'],
            'parties': config['DATA']['new_parties'],
            'counsels': config['DATA']['new_counsels']
        },
        'FJC': {
            'fjc_file':config['FJC']['fjc_file'],
            'Low': config['FJC']['commission_start'],
            'High': config['FJC']['commission_end'],
            'Active_Period': config['FJC']['active_period_min']
        },
        'JEL': config['JEL']['jel_location'],
        'OUT_PATHS':{
            'Updated_SEL':sel_path,
            'SEL_DIR':sel_dir
        }
    }

    return pythonic_config


def ingest_disambiguation_update_config(config_path):
    """ function used in disambiguation updating to parse the config arguments, make appropriate directories if necessary, and pass the args on to the script

    Args:
        config_path (str): string based relative path to the config file

    Returns:
        dict: object contains various args for the disambiguation script
    """
    
    import time

    def _build_base():
        """localized function only, assumes the variables have been created or called in the parent function.
        Creates the base data output directory and project directory for the disambiguation run

        Returns:
            PosixPath: Pathlib object pointing to the project outputs folder
        """

        BASE_DIR = Path(config['OUT']['base_directory'])
        RUN_NAME = config['META']['name_of_proj']

        assembled = BASE_DIR / Path(RUN_NAME)
        if assembled.is_dir():
            return assembled
        else:
            assembled.mkdir()
            print(f"Instantiated output directory here: {assembled}")
            return assembled

    def _build_logs():
        """Create the log files
        """
        def __establish_logger():
            """actually create the log file

            Returns:
                PosixPath: relative path to the log file
            """
            
            t = time.localtime()
            current_time = time.strftime("%H_%M_%S", t)
            the_log = log_path / Path(f"JED_Update_log_{current_time}.log")
            ## instantiate the log and set level to info so that we can document all of our cross-entity mapping for review
            logging.basicConfig(filename= the_log, format='%(message)s', level = logging.INFO)
            print(f"Instantiated logfile here: {the_log}")
            
            test_post = logging.getLogger()
            test_post.info(f"|||"*30)
            test_post.info(f"\nJudge Entity Disambiguation Update")
            test_post.info(f"{config['META']['name_of_proj']}\n")
            test_post.info(f"|||"*30)
            
            return the_log

        log_path = OUT_DIR / Path('Logs')
        if log_path.is_dir():
            LOG_PATH = __establish_logger()
            return LOG_PATH
        else:
            log_path.mkdir()
            print(f"Instantiated logging directory here: {log_path}")

            LOG_PATH = __establish_logger()
            return LOG_PATH

    def _establish_out_files():
        """Generate the paths for the eventual output data files and data directories to be used at the end of the disambiguation run.
        The timestamp matches the initiating script run, not when the file is first written to (a 15-20 minute delay)

        Returns:
            PosixPath, PosixPath, PosixPath: location of JEL file, location of SEL file, location to write individual case-level SEL files
        """
        DATA_OUTS = OUT_DIR / Path(config['OUT']['data_outputs'])
        if not DATA_OUTS.is_dir():
            DATA_OUTS.mkdir()

        t = time.localtime()
        current_time = time.strftime("%H_%M_%S", t)
        jel_path = DATA_OUTS / Path(f"Updated_JEL_{current_time}.jsonl")
        sel_path = DATA_OUTS / Path(f"Updated_SEL_{current_time}.jsonl")
        sel_dir = DATA_OUTS / Path(f"SEL_DIR/")

        if not sel_dir.is_dir():
            sel_dir.mkdir()

        return jel_path, sel_path, sel_dir


    config = configparser.ConfigParser()
    config.read(config_path)
    
    OUT_DIR = _build_base()
    LOG_PATH = _build_logs()
    jel_path, sel_path, sel_dir = _establish_out_files()

    pythonic_config = {
        'OUT_DIR': OUT_DIR,
        'LOG_PATH': LOG_PATH,
        'DATA_FILES': config['DATA']['NER_Extractions'],
        'DATA_PARTIES':config['DATA']['parties_csv'],
        'DATA_COUNSELS':config['DATA']['counsels_csv'],
        'FJC': {
            'fjc_file':config['FJC']['fjc_file'],
            'Low': config['FJC']['commission_start'],
            'High': config['FJC']['commission_end']
        },
        'JEL': config['JEL']['jel_location'],
        'OUT_PATHS':{
            'New_JEL':jel_path,
            'New_SEL':sel_path,
            'SEL_DIR':sel_dir
        }
    }

    return pythonic_config


def log_message(msg):
    """utility to grab the current environments log and write a message to the info tier

    Args:
        msg (str): logging message
    """
    my_log = logging.getLogger()
    my_log.info(msg)
    return


def UPDATE_TO_JSONL(Post_UCID, paths):
    """when updating new cases with entities, use this writer

    Args:
        Post_UCID (pandas.DataFrame): SEL df for the new cases only
        paths (dict): dict of directory paths to write to
    """
    # filter out bade entities
    PRE_SEL = Post_UCID[~((Post_UCID.SJID == "Inconclusive") & (Post_UCID.Prefix_Categories=="No_Keywords"))]
    # get relevant columns
    SEL = PRE_SEL[['Entity_Extraction_Method', 'docket_source', 'judge_enum', 'party_enum',
        'pacer_id', 'docket_index', 'ucid', 'cid', 'court', 'year',
        'original_text','New_Entity', 'Prefix_Categories',
        'Transferred_Flag', 'full_span_start', 'full_span_end',
        'New_Span_Start', 'New_Span_End','Points_To','SJID']].copy()
    
    # rename
    rename_cols = {
        'New_Entity': 'Extracted_Entity',
        'New_Span_Start':'Entity_Span_Start' , 
        'New_Span_End':'Entity_Span_End' ,
        'Points_To':'Parent_Entity'}

    SEL.rename(columns=rename_cols, inplace=True)
    SEL.to_json(paths['Updated_SEL'], orient='records', lines=True)


    # EXACT SAME CODE CONCEPT AS THE ORIGINAL WRITER FUNCTION BELOW
    DATA = {}
    temp = SEL[['court','year', 'ucid']].copy()
    temp.drop_duplicates(inplace=True)
    for court, year, ucid in temp.to_numpy():
        if court not in DATA:
            DATA[court] = {year:{ucid:[]}}
        else:
            if year not in DATA[court]:
                DATA[court][year] = {ucid:[]}
            else:
                if ucid not in DATA[court][year]:
                    DATA[court][year][ucid] = []    

    basepath = paths["SEL_DIR"]

    for court, years in DATA.items():
        if not os.path.isdir(f"{basepath}/{court}/"):
            os.mkdir(f"{basepath}/{court}/")
        for year in years.keys():
            if not os.path.isdir(f"{basepath}/{court}/{year}/"):
                os.mkdir(f"{basepath}/{court}/{year}/")

    for each in tqdm.tqdm(SEL.to_numpy(), total=len(SEL)):
        ucid_index = list(SEL.columns).index('ucid')
        court_index = list(SEL.columns).index('court')
        year_index = list(SEL.columns).index('year')
        
        ucid = each[ucid_index]
        court = each[court_index]
        year = each[year_index]
        
        DATA[court][year][ucid].append({cn:e for cn,e in zip(list(SEL.columns),each)})

    unwound = []
    for court, stuff in DATA.items():
        for year, more_stuff in stuff.items():
            for ucid, data in more_stuff.items():
                outfile = f"{paths['SEL_DIR']}/{court}/{year}/{ucid}.jsonl"
                outfile = outfile.replace(";;","-").replace(":","-")
                outdata = data
                unwound.append((outfile, outdata))

    
    print("NOW WRITING TO FILE")
    for fpath, datum in tqdm.tqdm(unwound):
        with open(fpath, 'w') as fout:
            json.dump(datum,fout)

    return

def TO_JSONL(PRE_SEL, JEL, paths, single_file_or_multi = 'multi'):
    """Final Function that creates then writes the SEL files as well writes the JEL

    Args:
        PRE_SEL (pandas.DataFrame): Nearly complete large df with all dismabiguated entities mapped to their ucid-docket-index-spans
        JEL (pandas.DataFrame): df of the known judges we kept after disambiguation
        paths (dict): dictonary of various out-paths for files we are writing to
        single_file_or_multi (str, optional): how do we want to write the SEL - 1 big file or also write into unique ucid level files. Defaults to 'multi'.

    Returns:
        pandas.DataFrame, pandas.DataFrame: returns the JEL and SEL (if in .ipynb this can be handy to see how it looks in the end)
    """
    # filter the dataframe to remove any inconclusive entities that did not have "judgey" pretext
    PRE_SEL = PRE_SEL[~((PRE_SEL.SJID == "Inconclusive") & (PRE_SEL.Prefix_Categories=="No_Keywords"))]
    # select the columns we want in a particular order
    SEL = PRE_SEL[['Entity_Extraction_Method', 'docket_source', 'judge_enum', 'party_enum',
           'pacer_id', 'docket_index', 'ucid', 'cid', 'court', 'year',
           'original_text','New_Entity', 'Prefix_Categories',
           'Transferred_Flag', 'full_span_start', 'full_span_end',
           'New_Span_Start', 'New_Span_End','Final_Pointer','SJID']].copy()

    # give them more understandable names
    rename_cols = {
        'New_Entity': 'Extracted_Entity',
    'New_Span_Start':'Entity_Span_Start' , 
    'New_Span_End':'Entity_Span_End' ,
    'Final_Pointer':'Parent_Entity'}

    SEL.rename(columns=rename_cols, inplace=True)
    
    # take specific columns from the JEL
    JEL = JEL[['name', 'Presentable_Name', 'SJID', 'SCALES_Guess', 
       'Head_UCIDs', 'Tot_UCIDs', 'Full_Name', 'NID']].copy()

    rename_cols = {'SCALES_Guess': 'SCALES_Judge_Label'}
    JEL.rename(columns=rename_cols, inplace=True)
    JEL.loc[~JEL.NID.isna(), "NID"] = JEL.NID.apply(lambda x: str(x).split(".")[0])

    # if single_file_or_multi=='single':
    # write to files
    JEL.to_json(paths['JEL'], orient='records', lines=True)
    SEL.to_json(paths['SEL'], orient='records', lines=True)

    # else:
    DATA = {}
    # this is effectively the file list I need
    temp = SEL[['court', 'ucid']].copy()
    temp.drop_duplicates(inplace=True)

    # for each court, find the year digits
    # i.e. ilnd;;3:16-cr-00001, the digits are 16
    # the DATA dict will have 
    #   {court: {year:{ucid: [sel_row, sel_row],ucid:[sel_row...]}}}
    for court, ucid in temp.to_numpy():
        year = ucid.split(";;")[1].split(":")[1][0:2]
        if court not in DATA:
            DATA[court] = {year:{ucid:[]}}
        else:
            if year not in DATA[court]:
                DATA[court][year] = {ucid:[]}
            else:
                if ucid not in DATA[court][year]:
                    DATA[court][year][ucid] = []

    # grab the path of the directory we are writing sel files to
    basepath = paths["SEL_DIR"]

    # for every iteration of court-years, make the directory for it
    for court, years in DATA.items():
        if not os.path.isdir(f"{basepath}/{court}/"):
            os.mkdir(f"{basepath}/{court}/")
        for year in years.keys():
            if not os.path.isdir(f"{basepath}/{court}/{year}/"):
                os.mkdir(f"{basepath}/{court}/{year}/")

    # now allocate each SEL row into its respective court-year-ucid 
    for each in tqdm.tqdm(SEL.to_numpy(), total=len(SEL)):
        ucid_index = list(SEL.columns).index('ucid')
        court_index = list(SEL.columns).index('court')
        year_index = list(SEL.columns).index('year')
        
        ucid = each[ucid_index]
        court = each[court_index]
        year = ucid.split(";;")[1].split(":")[1][0:2]
        
        DATA[court][year][ucid].append({cn:e for cn,e in zip(list(SEL.columns),each)})

    # now we will unwind the DATA dict and write out the ucid-lists into their respective files
    # unwound will be a giant list of (fpath, [json-data-rows])
    unwound = []
    for court, stuff in DATA.items():
        for year, more_stuff in stuff.items():
            for ucid, data in more_stuff.items():
                outfile = f"{paths['SEL_DIR']}/{court}/{year}/{ucid}.jsonl"
                outfile = outfile.replace(";;","-").replace(":","-")
                outdata = data
                unwound.append((outfile, outdata))

    # go through the list and write it out
    print("NOW WRITING TO FILE")
    multiprocess_file_writer(unwound)

    return JEL, SEL

def multiprocess_file_writer(unwound):
    """Parallelized writing function that leverages the multiprocessing module for file writing all of the JSONLs

    Args:
        unwound (list): Iterable list of tuples, (fpath, JSONL data) that can be mapped across the threads/processes
    """

    # establish the pool
    pool = mp.Pool(mp.cpu_count()-1)
    
    # map the writing function across the pool
    catch_list = list(tqdm.tqdm(pool.imap_unordered(write_to_jsonl_ucid_file, unwound), total=len(unwound),  desc="[--File Assembly--]", leave=False))
    
    # close and join the pool
    pool.close()
    pool.join()
    return

def write_to_jsonl_ucid_file(inp):
    """Single file writer function that takes a tuple input and writes data to a file

    Args:
        inp (tuple): tuple consisting of a filepath and JSONL data. TODO: check if the filepath is relative or absolute -- I believe either works
    """
    fpath, datum = inp
    with open(fpath, 'w') as fout:
        # ty greg for the valid jsonl code
        fout.writelines('\n'.join(json.dumps(line) for line in datum))
    return


def UPDATED_DISAMBIGUATION_TO_JSONL(paths, Updated_JEL, Old_docks, New_docks):
    """Final writing function when updating a full JEL and running disambiguation. This function will overwrite old SEL files and will also create new ones

    Args:
        paths (dict): dict of local or absolute paths pointing towards the location for new SEL and JEL files
        Updated_JEL (pandas.DataFrame): DF representing the new JEL entities
        Old_docks (pandas.DataFrame): DF representing the old ucids we have run through disambiguation before but now have updated entity tags
        New_docks (pandas.DataFrame): DF representing the new ucids we have now included in disambiguation
    """
    # discard SEL rows that were inconclusive and had no pretext indicators of judge-like terms
    PRE_SEL = New_docks[~((New_docks.SJID == "Inconclusive") & (New_docks.Prefix_Categories=="No_Keywords"))]

    # select the columns we want in a particular order
    SEL = PRE_SEL[['Entity_Extraction_Method', 'docket_source', 'judge_enum', 'party_enum',
           'pacer_id', 'docket_index', 'ucid', 'cid', 'court', 'year',
           'original_text','New_Entity', 'Prefix_Categories',
           'Transferred_Flag', 'full_span_start', 'full_span_end',
           'New_Span_Start', 'New_Span_End','Final_Pointer','SJID']].copy()

    # give them more understandable names
    rename_cols = {
        'New_Entity': 'Extracted_Entity',
    'New_Span_Start':'Entity_Span_Start' , 
    'New_Span_End':'Entity_Span_End' ,
    'Final_Pointer':'Parent_Entity'}

    SEL.rename(columns=rename_cols, inplace=True)
    Old_docks.rename(columns={"Final_Pointer":"Parent_Entity"}, inplace=True)

    # the new JEL will be written to a file, we want the NID to be represented as a string and not floating point number.0
    Updated_JEL.loc[~Updated_JEL.NID.isna(), "NID"] = Updated_JEL.NID.apply(lambda x: str(x).split(".")[0])
    Updated_JEL.to_json(paths['New_JEL'], orient='records', lines=True)

    # now create a final dataframe for output that consists of all rows we need to overwrite
    FIN_OUT = pd.concat([SEL,Old_docks])
    DATA = {}
    # this is effectively the file list I need
    temp = FIN_OUT[['court', 'ucid']].copy()
    temp.drop_duplicates(inplace=True)

    # for each court, find the year digits
    # i.e. ilnd;;3:16-cr-00001, the digits are 16
    # the DATA dict will have 
    #   {court: {year:{ucid: [sel_row, sel_row],ucid:[sel_row...]}}}
    for court, ucid in temp.to_numpy():
        year = ucid.split(";;")[1].split(":")[1][0:2]
        if court not in DATA:
            DATA[court] = {year:{ucid:[]}}
        else:
            if year not in DATA[court]:
                DATA[court][year] = {ucid:[]}
            else:
                if ucid not in DATA[court][year]:
                    DATA[court][year][ucid] = []

    # grab the path of the directory we are writing sel files to
    basepath = paths["SEL_DIR"]

    # for every iteration of court-years, make the directory for it if it doesn exist
    for court, years in DATA.items():
        if not os.path.isdir(f"{basepath}/{court}/"):
            os.mkdir(f"{basepath}/{court}/")
        for year in years.keys():
            if not os.path.isdir(f"{basepath}/{court}/{year}/"):
                os.mkdir(f"{basepath}/{court}/{year}/")

    # now allocate each SEL row into its respective court-year-ucid 
    for each in tqdm.tqdm(FIN_OUT.to_numpy(), total=len(FIN_OUT)):
        ucid_index = list(FIN_OUT.columns).index('ucid')
        court_index = list(FIN_OUT.columns).index('court')
        year_index = list(FIN_OUT.columns).index('year')

        ucid = each[ucid_index]
        court = each[court_index]
        year = ucid.split(";;")[1].split(":")[1][0:2]

        DATA[court][year][ucid].append({cn:e for cn,e in zip(list(FIN_OUT.columns),each)})

    # now we will unwind the DATA dict and write out the ucid-lists into their respective files
    # unwound will be a giant list of (fpath, [json-data-rows])
    unwound = []
    for court, stuff in DATA.items():
        for year, more_stuff in stuff.items():
            for ucid, data in more_stuff.items():
                outfile = f"{paths['SEL_DIR']}/{court}/{year}/{ucid}.jsonl"
                outfile = outfile.replace(";;","-").replace(":","-")
                outdata = data
                unwound.append((outfile, outdata))

    print("NOW WRITING TO FILE")
    # call the multiprocess file writer
    multiprocess_file_writer(unwound)

    return