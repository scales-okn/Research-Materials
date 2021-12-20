'''
File: ifp_regexes.py
Author: Scott Daniel
Description: Provides regular expressions that detect IFP activity, to be used in build_judge_ifp_data.py
'''

import re

filler = '[a-zA-Z0-9 ()\[\]:#\,\.\'\"\-\$&/]'
filler_with_asterisk = filler[:-1] + '\*]'

prepay = 'without (pre|the )?pay'
statute = 'under 28 ?u\.?s\.?c\.? ?§? ?1915'

application_exclusions_short = '(?<![0-9] )(?<!re )(?<!re: )(?<!re [0-9] )(?<!re: [0-9] )(?<!the )(?<!\'s )(?<!^a )(?<!^an )(?<!^no )(?<! a )(?<! an )(?<! no )'
application_exclusions = f'{application_exclusions_short}(?<!grant )(?<!grants )(?<!granting )(?<!deny )(?<!denies )(?<!denying )(?<!supplemental )'
application_base = f'to proceed (in district court )?(in ?forma ?pauperis|ifp|{prepay}|{statute})(?! on appeal)(?! (o|i)n (his|her) appeal)(?! on transcripts)'

pre_ifp_exclusion = '(?<!appeal )(?<!case files )(?<!or an )'
post_ifp_exclusion = '(?! on appeal)(?! status on appeal)(?! (o|i)n (his|her) appeal)(?! status (o|i)n (his|her) appeal)(?! for the purpose of an appeal)(?! status for the purpose of an appeal)(?! on transcripts)'
post_deny_exclusion = '(?!as moot)(?!in part)(?!for the purpose of an appeal)'



application_re = [
    re.compile(f'(?:^|, )(?!order){filler_with_asterisk}{{0,30}}?{application_exclusions}(motion|application|request|affidavit|letter){filler}{{1,50}}?{application_base}')
]


grant_re = [

    ### primary options ###
    re.compile(f'grant(?!(s|ed|ing)? ((P|p)laintiff )?(thirty|30) days)(?!(s|ed|ing)? in part){filler}{{1,400}}?{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}'),
    re.compile(f'{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}{filler}{{1,400}}?(?<!if such leave should be )grant(?!ed in part)'),
    re.compile(f'(?<!if he is )(?<!if she is )allow{filler}{{1,400}}?{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}'),
    re.compile(f'{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}{filler}{{1,400}}?allow'),
    re.compile(f'grant(?!(s|ed|ing)? in part){filler}{{1,80}}?{pre_ifp_exclusion}({prepay}|{statute})'),
    re.compile(f'{pre_ifp_exclusion}({prepay}|{statute}){filler}{{1,100}}?grant(?!ed in part)'),

    ### other options ###
    re.compile(f'may proceed{filler}{{1,30}}(in ?forma ?pauperis|ifp|{prepay})'),
    re.compile(f'waive filing fee grant')

    # re.compile('order%s{1,60}?proceed (in ?forma ?pauperis|ifp)%s' % (filler, post_ifp_exclusion))
]


deny_re = [

    ### primary options ###
    re.compile(f'den(y |ies |ied |ying ){post_deny_exclusion}{filler}{{0,400}}?{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}'),
    re.compile(f'{pre_ifp_exclusion}(in ?forma ?pauperis|ifp){post_ifp_exclusion}{filler}{{1,400}}?(?!appeal is )den(y |ies |ied(\.| )|ying ){post_deny_exclusion}'),
    re.compile(f'(?!appeal is )den(y |ies |ied |ying ){post_deny_exclusion}{filler}{{0,100}}?{pre_ifp_exclusion}({prepay}|{statute})'),
    re.compile(f'{pre_ifp_exclusion}({prepay}|{statute}){filler}{{1,100}}?(?!appeal is )den(y |ies |ied(\.| )|ying ){post_deny_exclusion}'),

    ### other options ###
    re.compile(f'order{filler}{{1,60}}?den{filler}{{1,80}}?{pre_ifp_exclusion}{prepay}'),
    re.compile(f'waive filing fee den(y |ies |ied(\.| )|ying ){post_deny_exclusion}'),
    re.compile(f'ordered to pay (the)? full')

    # re.compile('initial partial filing fee')
    # re.compile('directing monthly payments be made from prison account')
]


other_re = [

    ### non-binary rulings ###
    re.compile(f'(in ?forma ?pauperis|ifp){filler}{{1,400}}?moot'),
    re.compile(f'(find|stricken|terminat|den)[iedyings]{{0,4}} as moot{filler}{{1,400}}?forma ?pauperis'),
    re.compile(f'grant(s|ed|ing)? in part'),

    ### case endings ###
    re.compile(f'order to consolidate cases{filler}{{1,500}}?all future pleadings'),
    re.compile(f'case was assigned incorrectly{filler}{{1,400}}?hereby ordered{filler}{{1,400}}?transferred'),
    re.compile(f'(transfer[redings]{{0,4}}|remand[edings]{{0,3}}){filler}{{1,30}}?to{filler}{{1,30}}?(district|division)'),
    re.compile(f'no longer pending in this court'),
    re.compile(f'notice of [voluntary ]{{0,11}}dismissal'),
    re.compile(f'dismiss{filler}{{0,40}}?case{filler}{{0,20}}?(entirety|frivolous)'),

    ### other options ###
    re.compile(f'appeal{filler}{{1,100}}?good faith{filler}{{1,100}}?should not{filler}{{1,100}}?in ?forma ?pauperis'),
    re.compile(f'(direct|order)(?! grant)(?! den){filler}{{1,40}}?fil{filler}{{1,400}}?forma ?pauperis'),
    re.compile(f'has not{filler}{{0,80}}?submit{filler}{{1,80}}?(fil|motion){filler}{{1,200}}?forma ?pauperis'),
    re.compile(f'reduced (payment|filing fee) procedure') # e.g. a procedure in ILCD that seemingly supersedes the IFP procedure

    # re.compile('order of dismissal')
    # re.compile('order dismiss%s{0,40}?(prejudice|case)'%filler)
]


# export everything to build_judge_ifp_data.py as a single dictionary
groups = {'application': application_re, 'grant': grant_re, 'deny': deny_re, 'other': other_re}
