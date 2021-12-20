import re
from flashtext.keyword import KeywordProcessor

###################################
## Global Name Related Variables ##
###################################


# all accent-based letters to swap into plain-english
accent_repl = str.maketrans("áàéêéíóöúüñ","aaeeeioouun")

suffixes_titles = ['i', 'ii', 'iii', 'iv', 'v', 'jr','jnr', 'snr', 'sr', 'senior','junior']
common_surnames = ['lee','smith','johnson','williams', 'moody', 'thomas']

# unified spellings of names
NAME_UNIFIER = {    
    "Allan":"Allen",
    "Allen":"Allen",
    "Bryan": "Bryan",
    "Brian": "Bryan",
    "Catherine": "Catherine",
    "Catharine": "Catherine",
    "Deborah": "Deborah",
    "Debora":"Deborah",
    "Debra":"Deborah",
    "Elisabeth": "Elizabeth",
    "Elizabeth": "Elizabeth",
    "Erick":"Erik",
    "Erik":"Erik",
    "Eric":"Erik",
    "Frederic": "Frederick",
    "Frederick": "Frederick",
    "Harold": "Harold",
    "Herold": "Herold",
    "Jacquelyn": "Jacqueline",
    "Jacqueline": "Jacqueline",
    "Janis": "Janice",
    "Janice": "Janice",
    "Johnathan": "Johnathan",
    "Jonathan": "Johnathan",
    "Jonothan": "Johnathan",
    "Katherine": "Catherine",
    "Katharine": "Catherine",
    "Kristin": "Kristen",
    "Kristen": "Kristen",
    "Lawrence":"Lawrence",
    "Laurence":"Lawrence",
    "Lewis": "Lewis",
    "Louis": "Lewis",
    "Marcia": "Marsha",
    "Marsha": "Marsha",
    "Megan": "Megan",
    "Meagan": "Megan",
    "Meghan": "Megan",
    "Michelle": "Michelle",
    "Michele": "Michelle",
    "Michael": "Michael",
    "Mikel":"Michael",
    # "Morrison": "Morris", # this incorrectly mapped first to last names
    "Nathaniel": "Nathaniel",
    "Nathaneal":"Nathaniel",
    "Nathanel":"Nathaniel",
    # super niche typo catch
    "Netburn":"Netburn", # niche
    "Netbum":"Netburn", # niche
    "Nickolas": "Nicholas",
    "Nicholas":"Nicholas",
    "Nicolas":"Nicholas",
    "Patrick": "Patrick",
    "Patric": "Patrick",
    "Randal":"Randall",
    "Randall":"Randall",
    'Ricard':'Richard',
    "Samuel":"Sam",
    "Sam":"Sam",
    "Sally": "Sally",
    "Sallie": "Sally",
    "Sonia": "Sonja",
    "Sonja": "Sonja",
    "Stepben":"Stephen",
    "Stephem": "Stephen",
    "Stephen": "Stephen",
    "Stephan": "Stephen",
    "Stewart": "Stuart",
    "Stuart": "Stuart",
    "Sylvia": "Sylvia",
    "Silvia": "Sylvia",
    "Susan": "Susan",
    "Suzanne":"Susan",
    "Theodore": "Theodore",
    "Theadore": "Theodore",
    "Teresa": "Theresa",
    "Theresea":"Theresa",
    "Thrse":"Therese",
    "Wm":"William"
}

# these names proved extremely difficult to map algorithmically either due to a maternal name drop, hyphenated name, or nicknamed middle name
FJC_ADDITIONAL_REPRESENTATIONS = {
    1394646: ["leslie abrams gardner"],
    6385001: ["raul manuel arias"],
    1394196: ["nelson stephen romn"],
    1391691: ["ed kinkeade"],
    1389116: ["frederick van sickle"],
    1377801: ["fred biery jr"],
    1378846: ["ed carnes"],
    1383696: ["roger h lawson jr"],
    4027846: ["chip campbell jr"]
}

# these are coded as nid: lowercase name
FJC_DYNASTIES = {
    '1383911': 'stephen nathaniel limbaugh',
    '1392721': 'stephen nathaniel limbaugh jr',
    '1385266': 'james maxwell moody',
    '1394351': 'james maxwell moody jr',
    '1394201': 'william horsley orrick iii',
    '1385986': 'william horsley orrick jr',
    '1392616': 'william lindsay osteen jr',
    '1385991': 'william lindsay osteen sr',
    '1386076': 'parker barrington daniels jr',
    '1386071': 'parker barrington daniels sr',
    '1386146': 'robert porter patterson jr',
    '1386151': 'robert porter patterson sr'}

# nicknames for possible names
NICKNAMES = {
    "Alexandra": ["Alex"],
    "Alexander": ["Alex"],   
    "Anuraag":['Raag'], 
    "Beth": ["Elizabeth"],
    "Billy": ["Bill"],
    "Catherine": ["Cathy", "Casey"],
    "Catharine": ["Cathy", "Casey"],
    "Christian": ["Chris"],
    "Christopher":["Chris"],
    "David": ["Dave"],
    "Daniel": ["Dan"],
    "Deborah": ["Deb"],
    "Debora": ["Deb"],
    "Debra": ["Deb"],
    "Edward": ["Ed"],
    # "Edmond": ["Ed"],
    "Elizabeth": ["Beth"],
    "Frederic": ["Fred"],
    "Frederick": ["Fred"],
    "Gabriel": ["Gabe"],
    "Gregory": ["Greg"],
    "Jacob": ["Jake"],
    "Jackson": ["Jack"],
    "Joseph": ["Joe", "J"],
    "Johnathan": ["John"],
    "Jonathan": ["John"],
    "Jonothan": ["John"],
    "Judith": ["Judy"],
    "Katherine": ["Kathy"],
    "Katharine": ["Kathy"],
    "Leroy": ["Lee"],
    "Margaret": ["Maggie"],
    "Martin": ["Marty"],
    "Matthew":["Matt"],
    "Michael": ["Mike"],
    "Megan":["Meg"],
    "Nathaniel": ["Nathan","Nate"],
    "Nathaneal": ["Nathan","Nate"],
    "Nathanel": ["Nathan","Nate"],
    "Nickolas":["Nick"],
    "Nicholas": ["Nick"],
    "Nicolas": ["Nick"],
    "Pamela": ["Pam"],
    "Patricia": ["Patti"],
    "Patrick": ["Pat"],
    "Richard":["Rich"],
    "Rodolfo": ["Rudy"],
    "Sam":["Sam"],
    "Samuel": ["Sam"],
    "Samantha": ["Sam"],
    "Samson": ["Sam"],
    "Simeon": ["Sim"],
    'Solomon':['Sol'],
    "Stephen": ["Steve"],
    "Steven": ["Steve"],
    "Stephan": ["Steve"],
    "Theodore": ["Ted"],
    "Theadore": ["Ted"],
    "Thomas": ["Tom"],
    "Timothy": ["Tim"],
    "William":["Will","Wm", "Chip"]
}

# convert my globals above to completely lowercased variables
name_unifier = {}
for k,v in NAME_UNIFIER.items():
    name_unifier[k.lower()] = v.lower()
nicknames = {}
for k,v in NICKNAMES.items():
    nicknames[k.lower()] = [v2.lower() for v2 in v]


###################
## ENTITY LABELS ##
###################

# possible_labels = list(prefix_categories.keys()) + ['No_Keywords','Assigned_Judge','Referred_Judge']
possible_labels = ['Bankruptcy_Judge','Circuit_Appeals','District_Judge','Magistrate_Judge','Nondescript_Judge', 'Judicial_Actor']+ ['No_Keywords','Assigned_Judge','Referred_Judge']


#### Pattern Factory to build them all

# MAGISTRATES
magistrates = []
for each in ['chief','senior','ch\.*','sr\.*','']:
    for e in ['federal','u\.*s\.*','united states','']:
        for f in ['magistrate','mag\.*', 'm\.*']:
            for g in ['ju[ds]ge(s)?','j\.*']:
                magistrates.append(" ".join(f"{each} {e} {f} {g}".split()))

for each in ['chief','senior','ch\.*','sr\.*','']:
    for e in ['federal','u\.*s\.*','united states','']:
        for f in ['m\.*']:
            for g in ['j\.*']:
                magistrates.append(" ".join(f"{each} {e} {f}{g}".split()))

magistrates.append("chief magistrate")
magistrates = sorted(magistrates,key = lambda x: len(x.split()), reverse=True)

# DISTRICT JUDGES
dist_judges = ['u.s.district court judge(s)?']
for each in ['chief','senior','ch\.*','sr\.*','']:
    for e in ['federal','u\.*s\.*','united states','']:
        for f in ['district','dist\.*', 'd\.*']:
            for g in ['court ju[ds]ge(s)?','ju[ds]ge(s)?','c\.*j\.*','j\.*']:
                dist_judges.append(" ".join(f"{each} {e} {f} {g}".split()))

for each in ['chief','senior','ch\.*','sr\.*','']:
    for e in ['federal','u\.*s\.*','united states','']:
        for f in ['d\.*']:
            for g in ['c\.*j\.*','j\.*']:
                dist_judges.append(" ".join(f"{each} {e} {f}{g}".split()))

dist_judges+=['U.S.D.J.']           
dist_judges = sorted(dist_judges,key = lambda x: len(x.split()), reverse=True)

# NONDESCRIPT JUDGES
nondescript_judges = ["visiting judge(s)?","honorable judge(s)?", "judge honorable", "hon judge(s)?",
"judge hon","honorable","hon","jud"]
for each in ['chief','senior','ch\.*','sr\.*','']:
    for e in ['federal','u\.*s\.*','united states','']:
        for g in ['court ju[ds]ge(s)?','ju[ds]ge(s)?','c\.*j\.*','j\.*']:
            nondescript_judges.append(" ".join(f"{each} {e} {g}".split()))
nondescript_judges.remove('j\.*')
nondescript_judges.remove('c\.*j\.*')
nondescript_judges.append('j/')
nondescript_judges.append('designated')
std_judges = sorted(nondescript_judges,key = lambda x: len(x.split()), reverse=True)

# CIRCUIT AND APPELATE JUDGES
appellate = ['chief justice(s)?','senior justice(s)?', 'associate justice(s)?', 'junior justice(s)?',
'appellate judge(s)?','justice(s)?']
for each in ['senior','']:
    for circuit in ['first','second','third','fourth','fifth','sixth','seventh','eighth','ninth','tenth','eleventh','']:
        for n in ['circuit judge(s)?','circuit j']:
            appellate.append(" ".join(f"{each} {circuit} {n}".split()))

appellate_judges = sorted(appellate,key = lambda x: len(x.split()), reverse=True)
appellate_judges.remove('justice(s)?')
appellate_judges.append('justices')

# BANKRUPTCY JUDGES
bankrupcty_judges = ['us bankruptcy judge(s)?', 'states bankrupcty judge(s)?','chief bankruptcy judge(s)?',
'bankruptcy chief judge(s)?','bankruptcy case judge(s)?', 'bankruptcy court judge(s)?',
'bankruptcy judge(s)?','U S Bankruptcy']
bankrupcty_judges = sorted(bankrupcty_judges,key = lambda x: len(x.split()), reverse=True)                    

# ALL OPTIONS COMBINED
all_judgey = magistrates+dist_judges+std_judges+appellate_judges+bankrupcty_judges
all_judgey = sorted(all_judgey,key = lambda x: len(x.split()), reverse=True)                    

# JUDICIAL ACTIONS
court_other = [
               'judgment signed by', 'order signed by' 'entry before','motions referred to','hereby ordered',
               'proceedings before', 'order signed by', 'before', 'assigned to', 'referred to','order by',
               'ordered by', 'motion before', 'proceed before', 'referred to','transferred to','refer to', 
               'transfer to', 'held before', 'reassigned to', 'to chambers', 'chambers']


# universal padding bounds for any of our words above
padding = '(\\b|^|[\s\.,;:\(\)])'

# compiled regex patterns
magi = re.compile(fr'{padding}({"|".join(magistrates)}){padding}', flags = re.I)
distr = re.compile(fr'{padding}({"|".join(dist_judges)}){padding}', flags = re.I)
judge_plain = re.compile(fr'{padding}({"|".join(std_judges)}){padding}', flags = re.I)
appy = re.compile(fr'{padding}({"|".join(appellate_judges)}){padding}', flags = re.I)
bankr = re.compile(fr'{padding}({"|".join(bankrupcty_judges)}){padding}', flags = re.I)
judicial_actions = re.compile(fr'{padding}({"|".join(court_other)}){padding}', flags = re.I)


## TRIE BASED LABELS
TRIE_bankrupcty_judges = [
    'bankruptcy judge', 'bankruptcy judges'
    'bankruptcy chief judge', 'bankruptcy chief judges',
    'bankruptcy case judge','bankruptcy case judges', 'bankruptcy court judge','bankruptcy court judges',
    'U S Bankruptcy', 'US Bankruptcy', 'usbj','u.s.b.j.', 'us b.j.', 'us bj']
TRIE_appellate = ['chief justice','chief justices',
             'senior justice','senior justices',
             'associate justice','associate justices',
             'junior justice','junior justices',
             'appellate judge','appellate judges','justices']
for circuit in ['first','second','third','fourth','fifth','sixth','seventh','eighth','ninth','tenth','eleventh','']:
    for n in ['circuit judge','circuit judges','circuit j']:
        TRIE_appellate.append(" ".join(f"{circuit} {n}".split()))

TRIE_nondescript_judges = ["visiting judge","visiting judges",
                      "honorable judge", "honorable judges",
                      "judge honorable", "hon judge", "hon j", 
"judge hon","honorable","hon","jud"]
for e in ['federal','us','u.s.','united states','']:
    for g in ['court judge','court judges',
              'court jusge','court jusges',
              'judge','judges','jusge','jusges',
              'cj', 'c.j.','j', 'j.']:
        TRIE_nondescript_judges.append(" ".join(f"{e} {g}".split()))
TRIE_nondescript_judges.remove('j')
TRIE_nondescript_judges.remove('cj')
# MAGISTRATES
TRIE_magistrates = []

for f in ['magistrate','mag','mag.' 'm', 'm.','usm','u.s.m.']:
    for g in ['judge','judges','jusge','jusges',
              'cj', 'c.j.','j', 'j.']:
        TRIE_magistrates.append(" ".join(f"{f} {g}".split()))

for f in ['m','m.','usm','u.s.m.']:
    for g in ['j', 'j.']:
        TRIE_magistrates.append(" ".join(f"{f}{g}".split()))

TRIE_magistrates.append("chief magistrate")
TRIE_dist_judges = ['u.s.district court judge', 'u.s.district court judges']
for f in ['district','dist','dist.' 'd', 'd.','usd','u.s.d.']:
    for g in ['court judge','court judges',
              'court jusge','court jusges',
              'judge','judges','jusge','jusges',
              'cj', 'c.j.','j', 'j.']:
        TRIE_dist_judges.append(" ".join(f"{f} {g}".split()))

for f in ['d','d.', 'usd','u.s.d.']:
    for g in ['cj', 'c.j.','j', 'j.']:
        TRIE_dist_judges.append(" ".join(f"{f}{g}".split()))

TRIE_dist_judges+=['U.S.D.J.', 'usdj']     

MJ_TRIE = KeywordProcessor(case_sensitive=False)
MJ_TRIE.add_keywords_from_list(TRIE_magistrates)

DJ_TRIE = KeywordProcessor(case_sensitive=False)
DJ_TRIE.add_keywords_from_list(TRIE_dist_judges)

ND_TRIE = KeywordProcessor(case_sensitive=False)
ND_TRIE.add_keywords_from_list(TRIE_nondescript_judges)

CJ_TRIE = KeywordProcessor(case_sensitive=False)
CJ_TRIE.add_keywords_from_list(TRIE_appellate)

BJ_TRIE = KeywordProcessor(case_sensitive=False)
BJ_TRIE.add_keywords_from_list(TRIE_bankrupcty_judges)

JA_TRIE = KeywordProcessor(case_sensitive=False)
JA_TRIE.add_keywords_from_list(court_other)


######################
######################
## CLEANING PATTERNS #
######################
######################

##~~~~~~~~~~~~~~~
## Stripping or Cleaning patterns
##~~~~~~~~~~~~~~~

# doing business as, on behalf of are indicators this entity is not a judge
dba_obo_pattern = re.compile(r'(\b|\s)([do][/\.]*b[/\.]*[ao][/\.]*|o/b/?)(\b|\s)', flags=re.I)

odd_numeric_pattern = re.compile(r'sacr[\d]+-[\d]+', flags=re.I)
# name with a number after it
prisoner_patt = re.compile(r'#\s*[\d]+')

# simple streetv address pattern
address_num_patt = re.compile(r'c\.f\.|p\.\s*\d+|\d+\s*\w+\s*(st|str|street)|\d+ [nesw]\.? [a-z]+(-\d+)?', flags=re.I)

# only numbers with punctuation or just numbers remain in the string
numbers_only = re.compile(r'^[\d]+$|^([\d]*[\.,:;\-=/]*)*$', flags=re.I)

# more than one character in a token preceding a period
brute_sent = re.compile(r'(?<!^)[a-zA-Z]{3,}\.(?!=[a-zA-Z]\.*\s|\w\.)',flags=re.I)

# detect multiple occurrences of names in all caps
# currently everything after is discarded. a period is a fixed punct at the end of the pattern
multi_name_pattern = re.compile(r'[A-Z]+ [A-Z]\.? [A-Z]+\.')

# punct
normy = '<>,/:;"[]!@#%&-`~_'
escapey = ["\*","\?","\'", "\.", "\(", "\)","\^","\$","\{","\}","\d"]
puncty = "|".join(normy) + "|"+ "|".join(escapey)

num_punct_end__ = re.compile(fr'(\s|{puncty})+$', flags=re.I)
num_punct_start = re.compile(fr'^({puncty}|\s)+', flags=re.I)

trailing_whitespace = re.compile(r'\s+$')

##~~~~~~~~~~~~~~~
## Beginning of string patterns
##~~~~~~~~~~~~~~~

# beginning of string has judge built into it
front_judge_pattern = re.compile(r'^judge(s?\b|)\s*|^\'ble justice', flags = re.I)

# entity string beginning with "byName" - case sensitive
by_pattern = re.compile(r'^(by|to)(?=[A-Z]|\s+)') 

#re: patterns
re_beg_patt = re.compile(r'^re [A-Z]|^[rR]e$|^ge [A-Z]|^Mr\.*\s+')

##~~~~~~~~~~~~~~~
## End of string patterns
##~~~~~~~~~~~~~~~
# end re pattern
endre_pattern = re.compile(r' re$', flags=re.I)

# entity string with token ending with "NameThe" - case sensitive
the_pattern = re.compile(r'(?<=[a-z])The') 

# identify a possessive label on a judge
possessive_pattern = re.compile(r'(?P<POSS>\'s|s\')\s+(?!(jr|sr)\.*)',flags=re.I)

# specialty words that could show up in other words and have a more restricted "end of string" pattern
single_back = ["court","time","rule","note", 'oral','pris']
single_back_patt = re.compile(fr'({"|".join(single_back)})$', flags=re.I)

# specialty words that show up as mixed case end of token words smashed together
trailing_toks_pattern = re.compile(r'(?<=[A-Z]{2})(in|as|re|[jJ]ury)')

# judge Scott's dockets always merged their name into the following words. Happened enough to warrant his own exception
scott_pattern = re.compile(r'Scott(?=[A-Z])|SCOTT(?=[A-Za-z][a-z])')

# specialty token search for the word chambers as there is a judge Robert C. Chambers, but many judges names were listed
# NAME N. NAME CHAMBERS in respect to their chambers, so spacy was confused
chambers_exception_pattern = re.compile(r'(in |to )?chambers', flags=re.I)

##~~~~~~~~~~~~~~~
## Infix patterns
##~~~~~~~~~~~~~~~

# is there text after a known suffix (not i or v as they could be initials)
suff_post_text = re.compile(fr'(\s|\b)({"|".join(i for i in suffixes_titles if len(i)>1 and i!="sr")})\.* (?=\w+)', flags=re.I)
sr_judge_prefix = re.compile(r'^(sr|ch(ief)?|mr)\.*(\s|$)',flags=re.I)

# determine if a comma is separating another name or words (note this excludes , Jr. type suffixes)
post_comma = re.compile(fr'(?<!(\b)[a-zA-Z])\.*,(?!\s*({"|".join(suffixes_titles+["j r"])})(\.|\b|\s))',flags=re.I)

# any sort of specialty infill USMJ, USDJ, etc. or judge pattern 
affixed_judge= re.compile(r'(\s|\b|^)(Sr\.* |s\.*)?(U\.*S\.*(D|M)\.*J\.*|(M|D)\.*J\.*D\.*C\.*)\s*|^vj\-', flags=re.I)
judge_misps = ['j udge', 'ju dge', 'jud ge', 'judg e']
jmp = re.compile(fr'({"|".join(judge_misps)})\s', flags=re.I)

# magistrate judge r and r reports
RandR_pattern = re.compile(r'\s*(per r&r|r&r|[rf] & r|[mf]&r|r and r|recommend)', flags=re.I)

# any sort of dates (numeric)
dates_pattern = re.compile(r'\s*(o\.* n|oon|on|by|for|entered:|scheduled|dated|:|--)*\s*[\d]+[-/\.][\d]+[-/\.][\d]+', flags=re.I)

# odd case patterns that look like initials JD-14 or MS JD-14
back_short_case_pattern = re.compile(r'(\b|\s)[a-zA-Z]{1,2}(\b|\s)[a-zA-Z]{1,2}-[\d]{1,2}|(\b|\s)[a-zA-Z]{1,2}-[\d]{1,2}', flags=re.I)

# gregorian calend like scheduling terms
# NOTE: April is a first name and May is a last that occassionally appears in these strings, so we exclude it as a precaution
calendars = ['january','february','march',#'april','may',
            'june','july','august','september','october','november','december',
             'monday','tuesday','wednesday','thursday','friday','saturday','sunday']
gregorian_pattern = re.compile(rf'((\b|\s)|dated\s|(\b|\s)on\s|(\b|\s)o n\s)({"|".join(calendars)})(\b|\s|$)', flags=re.I)

# magjud appears in middle of name tokens
magjud_infix = re.compile(r'[a-zA-Z]+[\s\b][a-zA-Z]*\.*( ?- ?Mag\. Judge| ?- ?MagJud| Judge| Hon\.| \(magistrate\))[\s\b][a-zA-Z]+', flags=re.I)

# when a name has a lot of text, we shave a lot of it quickly by locating the mention of judge if judge is in the string
longform_extraction_pattern = re.compile(r'(by|to)(\b|\s)(\w+\s)*(judge)(\s|\b)(?=(\w+\s)+)', flags=re.I)

# words that appear in the middle of strings that indicate prior text should be considered as possible entity
mid_patts = [
             "and", "(arr )?as to", "at",
             "by",
             'Cause', "c\.f\. p\.o",
             'discharging order', "document filed by",
             "(f .|a\.) document filed by", "for( the| a)?",
             'good cause', "g\[rant(ing)?",  
             'letter motion',          
             "mad pre-motion",
             "no motions( filed)?",
             "per", 'plaintiff', 'pro se',
             'r( )?andomly assigned',
             "shall", "so ordered",
             "that", 
             'to',
             'USM','USPO'
             "will( be| take|now)"
             ]
            
mid_patts = sorted(mid_patts, key = lambda x: (len(x.split()), len(x)), reverse=True)
mpp = re.compile(fr'(\b|\s)({"|".join(mid_patts)})(\b|\s)', flags=re.I)

# unbounded words that could be merged with entity names, or after them
# NOTE THERE ARE NO WORD BOUNDS, BE CAREFUL WITH SMALL WORDS LIKE "IS" IN CHRIS
wordy = [
"(^| )a (ll$|coa |big$|rule|first|second|third|fourth|prelim|pretrial|preconf|certified|revocation)",
"\.a ll$",
"about E-?$","about ","admit(s)?","advocate","(administrative|alternative|additional)(ly)?","affir(r)?m(ing|ed|s)?","after","against","altering","ambassador","amend(ed|ing)( doc)?","anddenying",
"and ",
"apologizes","(initial )?appear(ing|ances|s)?", "appoint(ed|ing|ment)",
"arraign(ment|ing)", "(none )?(un|re)?assign(ing|ed|ment)?", "attach(ing|ment|es|ed)?", "(^|\b|\s)att(orne?)?y","ausa",
"BAYER A.G","(on or )?before",
"certif(y|ying|ied)","chief","(\b|\s)civ(\b|\s)", "civil","claim","clarif(ied|ying|y)",
"(c\.o\.)|(c\.*/o\.*)","^co\.*$","company","conditionally","consent(ing)?","consonant", "construed","correct(ing|ion|ed)?","counsel",
"damage(s)", "debt","declination","(deft|defendant)(s)?","den(ied|ying)","direct(s)?", "discharging","discloses","dispositive","disqualifie(s|d)","district","docket [A-B]","dplc",
"e-?(mail|file|sign)(ed)?", "eastern","ECF","el paso","encl(osed)?", "esq(uire)?", "evidence","exec committee",
"f\.r\.c\.p\.","[fl]\.(r\.)?( )?civ","f\.r\.( )?bankr(\.p\.)?","f\.3d","fairness","fifth","final","find(ing)?","forall","(tor|for|fo r) the (foregoing|following)","for ","form ao","forwarded","from ","fugitive",
"grant(ing|s)", "grant (the|in|def|plaint)",
"hearing", 
"ia/(ac)?", "(itis |it is|is |)?(hereby|stricken|therefore|likewise|substitut(ed|ing)|cancel(led)?)", 
"\bi'm\b","impartiality","^in[\d]+ cr?","initial","instructions","interpret(er|ing)","(spanish |cantonese )?interpreter","(in)?voluntary",
"liable","liberally","limine","^L( )?L( )?C\.*$",
"magistrate","magisrate","magistate","magistriate","mailed","(pre-?)?motion(s)?","motion","movant",
"( |^)n\.*a\.","n/a" ,"Non-","not allowing","nef regenerated",
" oath"," on[\d]*$","ORDERScheduling","order(ing|ed|s|\b|\s)",
"p\.s?o\.*(\s|\b|$)" ,"-psi$","pslc",
"parajudicial officer","Parties( notified)?","pla ","plaintiff","pre(-)?(liminary|trial|motion)","prisoner","proposed","pro se","^p(\. | )?so$","psi^","pursuant",
"(^|\s|\b)qc(\s|\b|$)",
"r\.a\.s"," re ", "recuse(ing|s|d)?", "(un)?redact(ed)?","refer(ring|red|s)?","regard(s|ing)","regenerated","related","renoting","repl(y|ied|ies|ing)",
"report","requir(ing|ed|es)","reset","respective(ly)?",
"^sacr\d+","s(\s|\.)ct\.*","schedul(ed|ing)" ,"(p?re)?sentenc(ing|ed|e)", "served", "(for a |a |)settlement(s)?", "s( )?ign(ed|ing)", "status","staying","standing","stipulation",
"take(s cross)?","telephon(e|ic)","text","there$","trans((ferr|mitt)ed|(ferr|mitt)ing|crib|cribe(d|r)?)", "traveled","(\s|^)tro(/preliminary| hearing)?(\s|$)",
"united","upon ","under l\.r\.","under D\.Ak\.LMR", "(united states|u\. s\.|us|u.s.|u s) (district|courthouse)", "us[dm][cj]", "u\.s\.","usmj",
"vacat(ing|ed|e)","visiting","voir dire",
"where(fore|as)", "withdraw(ing|s)?", "withou(t)?",
"will (issue|address|recommend|handle|consider|review|the|enter|make|promptly|extend|set|be |defer|have|preside|sign|coordinate|rely|adopt|appear)",
"\\\\xc2\\\\xa7|§",
"Y/N",
"\|Zoom",
"\^e\s"
]
wordy = sorted(wordy, key = lambda x: (len(x.split()), len(x)), reverse=True)
wopat = re.compile(fr'({"|".join(wordy)})', flags=re.I)

weird_triple = re.compile(r'^[a-zA-Z]\s[a-zA-Z]$', flags=re.I)


abnormal_suffixes = ['cj','dr','na','de','jsc','md','mr','ms','ph','jg','pa', 'jkr','accm','aul', 'llc','abc'] #jkr may be a clerk??
abnormal_suffixes = ["\.?".join(l for l in each)+"\.?" for each in abnormal_suffixes]
suffixed_pattern = re.compile(fr',?(\s|\b)({"|".join(abnormal_suffixes)})$', flags=re.I)

###################################################
## DEPRECATED AND UNUSED, BUT POTENTIALLY USEFUL ##
###################################################

case_pattern = re.compile(r'([\d]:[\d]{2}-[a-zA-Z]{2}-[\d]{5})|([\d]{2}-[a-zA-Z]-[\d]{4})|([a-zA-Z]{3}-[\d]{4,6})', flags=re.I)
specialized_possessive_pattern = re.compile(r'(\'s[A-Z\s\b])|(\'[sS]\s*$)')
dot_com_pattern = re.compile(r'(\s|\b)?([a-zA-Z]+@)?[a-zA-Z]*\.(com|net|org|gov|edu)', flags=re.I)
judgey_pattern = re.compile(rf'\b({"|".join(all_judgey)})\b', flags=re.I)
alphanum_code_patt = re.compile(r'^[a-zA-Z]{1,3}[\d]+', flags=re.I)


reversed_name_pattern = re.compile(fr'[\w]+, (?!({"|".join(suffixes_titles)}))', flags=re.I)

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