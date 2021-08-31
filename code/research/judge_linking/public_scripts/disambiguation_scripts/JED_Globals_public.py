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
FJC_SURNAME_SWAP = {
    "leslie joyce abrams": "leslie abrams gardner",
    "raul manuel arias-marxuach":"raul manuel arias",
    "james e kinkeade": "ed kinkeade",
    "hugh lawson": "roger h lawson jr"
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
dba_obo_pattern = re.compile(r'[do][/\.]*b[/\.]*[ao][/\.]*|o/b/?', flags=re.I)

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
by_pattern = re.compile(r'^by(?=[A-Z])') 

#re: patterns
re_beg_patt = re.compile(r'^re [A-Z]|^[rR]e$|^ge [A-Z]|Mr\.*\s+')

##~~~~~~~~~~~~~~~
## End of string patterns
##~~~~~~~~~~~~~~~
# end re pattern
endre_pattern = re.compile(r' re$', flags=re.I)

# entity string with token ending with "NameThe" - case sensitive
the_pattern = re.compile(r'(?<=[a-z])The') 

# identify a possessive label on a judge
possessive_pattern = re.compile(r'(?P<POSS>\'s)\s+(?!(jr|sr)\.*)',flags=re.I)

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
affixed_judge= re.compile(r'\s(Sr\.* |s\.*)?(U\.*S\.*(D|M)\.*J\.*|(M|D)\.*J\.*D\.*C\.*)\s*|^vj\-', flags=re.I)
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
             'Cause',
             'discharging order', "document filed by",
             "(f .|a\.) document filed by", "for( the| a)?",
             'good cause', "g\[rant(ing)?",  
             'letter motion',          
             "mad pre-motion",
             "no motions( filed)?",
             "per", 'plaintiff', 'pro se',
             'r( )?andomly assigned',
             "shall", "so ordered",
             "that", 'to',
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
"about E$","about ","admit(s)?","advocate","(administrative|alternative|additional)(ly)?","affir(r)?m(ing|ed|s)?","after","against","altering","ambassador","amend(ed|ing)( doc)?","anddenying",
"apologizes","(initial )?appear(ing|ances|s)?", "appoint(ed|ing|ment)",
"arraign(ment|ing)", "(none )?(un|re)?assign(ing|ed|ment)?", "attach(ing|ment|es|ed)?", "(^|\b|\s)att(orne?)?y","ausa",
"BAYER A.G","(on or )?before",
"certif(y|ying|ied)","chief","(\b|\s)civ(\b|\s)", "civil","claim","clarif(ied|ying|y)",
"(c\.o\.)|(c\.*/o\.*)","^co\.*$","company","conditionally","consent(ing)?","consonant", "construed","correct(ing|ion|ed)?","counsel",
"damage(s)", "debt","declination","(deft|defendant)(s)?","den(ied|ying)","direct(s)?", "discharging","discloses","dispositive","disqualifie(s|d)","district","docket [A-B]","dplc",
"e-?(mail|file|sign)(ed)?", "eastern","ECF","el paso","encl(osed)?", "esq(uire)?", "evidence","exec committee",
"f\.r\.c\.p\.","f\.r\.( )?bankr(\.p\.)?","f\.3d","fairness","fifth","final","find(ing)?","forall","(tor|for|fo r) the (foregoing|following)","form ao","forwarded","from ","fugitive",
"grant(ing|s)", "grant (the|in|def|plaint)",
"hearing", 
"ia/(ac)?", "(itis |it is|is |)?(hereby|stricken|therefore|likewise|substitut(ed|ing)|cancel(led)?)", 
"impartiality","^in[\d]+ cr?","initial","instructions","interpret(er|ing)","(spanish |cantonese )?interpreter","(in)?voluntary",
"liable","liberally","limine","^L( )?L( )?C\.*$",
"magistrate","magisrate","magistate","magistriate","mailed","(pre-?)?motion(s)?", "movant",
"( |^)n\.*a\.","n/a" ,"Non-","not allowing","nef regenerated",
" oath"," on[\d]*$","ORDERScheduling","order(ing|ed|s|\b|\s)",
"p\.s?o\.*(\s|\b|$)" ,"-psi$","pslc",
"parajudicial officer","Parties( notified)?","pla ","plaintiff","pre(-)?(liminary|trial|motion)","prisoner","proposed","^p(\. | )?so$","psi^","pursuant",
"(^|\s|\b)qc(\s|\b|$)",
"r\.a\.s"," re ", "recuse(ing|s|d)?", "(un)?redact(ed)?","refer(ring|red|s)?","regard(s|ing)","regenerated","related","renoting","repl(y|ied|ies|ing)",
"report","requir(ing|ed|es)","reset","respective(ly)?",
"^sacr\d+","s(\s|\.)ct\.*","schedul(ed|ing)" ,"(p?re)?sentenc(ing|ed|e)", "served", "(for a |a |)settlement(s)?", "s( )?ign(ed|ing)", "status","staying","standing","stipulation",
"take(s cross)?","telephon(e|ic)","text","there$","trans((ferr|mitt)ed|(ferr|mitt)ing|crib|cribe(d|r)?)", "traveled","(\s|^)tro(/preliminary| hearing)?(\s|$)",
"united","upon ","under l\.r\.","under D.Ak.LMR", "(united states|u\. s\.|us|u.s.|u s) (district|courthouse)", "us[dm][cj]", "u\.s\.","usmj",
"vacat(ing|ed|e)","voir dire",
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