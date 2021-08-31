import tqdm
from collections import defaultdict, Counter
import re

from fuzzywuzzy import fuzz
import pandas as pd

import JED_Globals_public as JG
import JED_Cleaning_Functions_public as JCF
import JED_Classes_public as JClasses
import JED_Utilities_public as JU


#######################
## Updating Function ##
#######################

def assess_new_cases(Post_UCID, Compy_JEL):
    """Given a DF of new cases to disambiguate for entities, run a disambiguation against the existing JEL

    Args:
        Post_UCID (pandas.DataFrame): extracted entities from new cases
        Compy_JEL (pandas.DataFrame): portion of the JEL containing active judges during the time period the new cases were active

    Returns:
        pandas.DataFrame: Pre-SEL like dataframe with attributed/matched judges
    """
    # list of entities that need to be mapped
    needs_mapping = list(Post_UCID.Points_To.unique())
    
    # find the entities that are single token vs. multi
    single_anchors = [e for e in needs_mapping if len(e.split())==1]
    remainder = [e for e in needs_mapping if e not in single_anchors]

    # build out objects for the new entities and the JEL entities
    new_anchors = [JClasses.UPDATER_NODE(each) for each in single_anchors]
    new_others = [JClasses.UPDATER_NODE(each) for each in remainder]
    JEL_NODES = []
    for name, sjid in Compy_JEL[['name','SJID']].to_numpy():
        JEL_NODES.append(JClasses.JEL_NODE(name, sjid))

    # for every multi-token name that needs to be matched
    # compare it to existing JEL nodes
    for each in tqdm.tqdm(new_others):

        # if it has an exact spelling match, win early
        matches = [o for o in JEL_NODES if o.name == each.name]
        if len(matches)==1:
            # assign the winner to this new entity
            each.assign_SJID(matches[0].SJID)
            continue

        # if a match was not found, we are going to attempt to iterate through a tokens-in-tokens checking routine
        # of this entity against JEL entities using various abbreviations and nickname stylings
        for AF, AM, STYLE in [(False, False, 'Plain'), (False, True, 'Plain'),(True, False, 'Plain'), 
                            (False, False, 'Unified'), (False, False, 'Nicknames'),
                            (False, True, 'Nicknames')]:
            # if it got mapped during this loop, break
            if not each.eligible:
                break
            for JN in JEL_NODES:
                # go thru every JEL node
                # if this entity was mapped at some point, break out of this
                if not each.eligible:
                    break
                # create the pool of comparison tokens
                this_pool, that_pool = pool_creator(each, JN, 
                                                    abbreviated_first=AF, abbreviated_middle=AM, style=STYLE)

                # if the pool of tokens qualifies as a match, then assign the JEL SJID to this entity
                if pool_runner(this_pool, that_pool):
                    each.assign_SJID(JN.SJID)
                    continue
        
        # if we got through all of the token in token checks with no match, let's attempt some fuzzy matches against known judges
        for JN in JEL_NODES:
            if not each.eligible:
                break
            if fuzz.ratio(each.name, JN.name)>90:
                each.assign_SJID(JN.SJID)
                continue
            if fuzz.token_set_ratio(each.name, JN.name)>95:
                each.assign_SJID(JN.SJID)
                continue

        # if this entity still doesnt have a match, we will try specialty naming conventions
        # this block attempts matching a name like "John Robert Smith" to the known "J Robert Smith"
        for JN in JEL_NODES:
            if not each.eligible:
                break
            for inf_toks in each.inferred_tokens[False][True]:
                if not each.eligible:
                    break
                for inf_tok_j in JN.inferred_tokens[False][True]:
                    if not each.eligible:
                        break
                    if fuzz.ratio(inf_toks, inf_tok_j)>95:
                        each.assign_SJID(JN.SJID)
                        break
    
    # now for the single token entities
    for each in new_anchors:
        this_anchor = each.anchor
        JAnchs = [JN for JN in JEL_NODES if JN.anchor==this_anchor]
        # only if there is a single known JEL judge with the same last name will we attribute that judge to that SJID
        if len(JAnchs)==1:
            each.assign_SJID(JAnchs[0].SJID)

        # otherwise we say it will remain inconclusive
        elif len(JAnchs)>1:
            each.assign_SJID('Inconclusive')

    # any remaining unmarked entities will be ruled inconclusive
    for each in [no for no in new_others if no.eligible]:
        each.assign_SJID("Inconclusive")
    for each in [na for na in new_anchors if na.eligible]:
        each.assign_SJID("Inconclusive")

    # now build out the map of new entities to their flagged SJID/"inconclusive" labels
    finmap = {}
    for each in new_others+new_anchors:
        finmap[each.name] = each.SJID

    # tack them on the row-by-row dataframe
    Post_UCID['SJID'] = Post_UCID.Points_To.map(finmap)

    return Post_UCID

##################################
## Final Intra-UCID Crosschecks ##
##################################

def FINAL_CLEANUP(Fin_Matched, JEL):
    """The end of the initial disambiguation process. This function takes a final dataframe with many entities disambiguated and
    attempts to disambiguate a few more within a UCID with the confident disambiguated entities as priors.

    Args:
        Fin_Matched (pandas.DataFrame): row-by-row data frame of entities with SJID tags now (some will be inconclusive)
        JEL (pandas.DataFrame): df of confidently selected judge entities from our data

    Returns:
        pandas.DataFrame: final output df of row-by-row entities with a few of the inconclusive entities now resolved
    """
    print("\nFinal Cleanup: Crosschecking remaining inconclusive entities")

    # I only need the unqieu list of entities by ucid to run through the final check
    mapper_frame = Fin_Matched[['ucid','extracted_entity','Final_Pointer', 'SJID']].drop_duplicates()
    
    # I will be building a map by ucid of all entities marked as good (known judge) or "inconclusive"
    the_map = {}
    for ucid, cleaned_ent, final_ent, SJID in [tuple(x) for x in mapper_frame.to_numpy()]:
        has_sjid = True
        if SJID == 'Inconclusive':
            has_sjid = False

        if ucid not in the_map:
            if has_sjid:
                the_map[ucid] = {"Good": [(final_ent, cleaned_ent, SJID)],"Inconclusive": []}
            else:
                the_map[ucid] = {"Good": [],"Inconclusive": [(final_ent, cleaned_ent, SJID)]}
        else:
            if has_sjid:
                the_map[ucid]["Good"].append((final_ent, cleaned_ent, SJID))
            else:
                the_map[ucid]["Inconclusive"].append((final_ent, cleaned_ent, SJID))

    # the entities to review will be any entity list from a ucid that is marked as inconclusive
    review = {}
    for ucid, ents in the_map.items():
        if ents['Inconclusive']:
            # if the ucid had inconclusive entities, keep the set of good and inconclusive ones in the reviewer dict
            review[ucid] = {k:set(v) for k,v in ents.items()}

    # updater will track which inconclusive entities we were able to update and point towards an SJID
    updater = []
    for ucid, ents in tqdm.tqdm(review.items()):
        for each in ents['Inconclusive']:
            badname = each[0] # the entity we will compare
            original = each[1] # what the entity originally looked like
            m_count = [] # match counter
            for good in ents['Good']:
                goodname = good[0] # name of entity with sjid to compare
                g_original = good[1] # original form of the entity

                # if the pointer entities match or the original entities match, add it as a match
                if fuzz.partial_ratio(badname,goodname)>=90 or fuzz.partial_ratio(original,g_original)>=90:
                    m_count.append(good)

            # if there is only one good match from the JEL entities
            # and the matched entity is a substring of the known judge, match it
            # this effectively matches ambiguous single token names on a docket
            if len(m_count)==1 and len([i for i in ents['Good'] if badname in i[0]])==1:
                good = m_count[0]
                updater.append({"ucid": ucid,
                                "Final_Pointer": each[0],
                                "New_Point": good[0],
                                "New_SJID": good[2],
                                "Absorb": True
                               })
    # build the DF for the updated mappings
    RECAST = pd.DataFrame(updater)            
    
    # log what we updated
    for each in updater:
        JU.log_message(f"Final Crosscheck | {each['ucid']:25} |{each['Final_Pointer']:25} --> {each['New_Point']}")

    print("Completing final SEL merge")
    # merge them together
    FMDF = Fin_Matched.merge(RECAST, how='left', on = ['ucid','Final_Pointer'])
    
    # any of the update entities are marked with "Absorb" meaning we will overwrite their SJID (previously inconclusive) and their Final Parent entity (final pointer)
    FMDF.loc[FMDF.Absorb==True, 'SJID'] = FMDF.New_SJID
    FMDF.loc[FMDF.Absorb==True, 'Final_Pointer'] = FMDF.New_Point
    
    # drop misc columns created
    FMDF.drop(['New_Point','New_SJID','Absorb'], axis=1, inplace=True)
    
    return FMDF

###################################
## Specialty Crosswalk Functions ##
###################################

def Abrams_Patch(NODES):
    """We cannot perfectly match all names algorithmically. Notably: special nicknames or marital name changes require
    a human knowledge level patch to account for in our code. This function is the shoehorn for those. It was originally
    created to account for Judge Leslie Abrams, but has since been updated with more names

    Args:
        NODES (list): Freematch objects in middle of disambiguation

    Returns:
        list: same list of freematch objects that entered the function, but now a few point to each other
    """
    print("\nPipe: Free Matching; Marital Name Changes or Other Caveats")

    # marital name change
    abrams = ['leslie abrams gardner','leslie joyce abrams']
    # some of the courts omit the accented a in roman due to computer error. Quite a lot of romn exists
    romans = ['nelson s romn','nelson stephen roman']
    # middle name is nicknamed and used as first name
    biery = ["fred biery","samuel frederick biery jr"]
    # first name nicknamed, middle initials unclear
    sickle = ['fred van sickle', 'frederick l van sickle']
    # middle name nickname used as first name
    kinkeade = ['ed kinkeade', 'james e kinkeade']
    # another J E Carnes exists, so checking EE Carnes fails
    carnes = ['ed carnes', 'edward earl carnes']

    specials = [abrams, romans, biery, sickle, kinkeade, carnes]

    # for each of our special pool of names, we assume the list in the loop all maps to each other
    # i.e. ed kinkeade will always collapse onto james e kinkeade
    # we also believe these are the final 2 iterations of the names and no other possible name exists (although more could be added)
    for specialty in specials:
        # grab all of the nodes that match up for this pool of specialty name
        Anodes = [n for n in NODES if n.name in specialty and n.eligible]
        # while the pool of eligible nodes remains larger than 1, reduce them onto each other using our existing "winner" decision criteria
        # TODO: choosing the most recent marital name as the "winning name"
        check = [o for o in Anodes if o.eligible]
        failsafe = 0
        while len(check)>1 and failsafe<50:
            failsafe+=1
            Anodes[0].choose_winner(Anodes[1], "Abrams Patch")
            check = [o for o in check if o.eligible]
    
    return NODES

def PIPE_Free_Van_Sweep(NODES):
    """Pipe for the Free-Matching phase of disambiguation.
    This pipe focuses on natural language bias fixes for germanic entities whose names are like Van Kloet

    Args:
        NODES (list): Freematch objects in middle of disambiguation

    Returns:
        list: same list of freematch objects that entered the function, but now a few point to each other
    """
    print("\nPipe: Free Matching; 'Van' Name Sweeps")
    
    # grab only the nodes where van is a token in the name
    vans = []
    for each in [o for o in NODES if re.search(r'^van | van',o.name) and o.eligible]:
        # if the first token is not van we will attempt disambiguation for it in the van search
        # not too sure why I did this??
        if "van"!= each.name.split()[0]:
            vans.append(each)

    new_vans = []
    old_vans = []
    for each in [o for o in vans]:
        toks = each.name.split()
        # we will try constructing names with the van not separated (Van Heusen --> VanHeusen)
        if "van" in toks:
            vin = toks.index("van")
            new_vans.append((each,f"{' '.join(toks[0:vin])} {''.join(toks[vin:vin+2])} {' '.join(toks[vin+2:])}"))
        else:
            old_vans.append(each)

    # now, using our newly constructed van names, see if they match up to the old ones
    # basically this leveled the field and compared them all as fuzed names VanHeusen, VanWeld, etc.
    for obj, newname in new_vans:
        if obj.suffix:
            anchor = newname.split()[-2]
        else:
            anchor = newname.split()[-1]

        for old in old_vans:
            old_anchor = old.anchor

            if fuzz.ratio(anchor, old_anchor)>=90:
                obj.choose_winner(old, "Van Sweep")
    return NODES


#######################################
## Anchor and Single Token Functions ##
#######################################

def PIPE_Anchor_Reduction_UCID(entity_map):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. The exceptions contain a few common typos that I solve for as well

    Args:
        entity_map (dict): key = ucid, values = list of nodes in a ucid we are trying to reduce

    Returns:
        dict: the same dict that entered, but the child objects in the lists may be updated and disambiguated
    """
    print("\nPipe: Anchor Reduction within UCIDs")

    # exception entities that caused some trouble in identifying their anchors (surnames)
    exceptions = {
        'otazo reyess':'alicia m otazo reyes',
        'der yeghiyan': 'samuel der yeghiayan',
        'der yeghiayans': 'samuel der yeghiayan',
        'der yerghiayan': 'samuel der yeghiayan',
        'emy st eve':'amy j st eve',
        'st eves': 'amy j st eve',
        'ann nolan': 'nan r nolan',
        'mc giverin': 'bruce j mcgiverin',
        'chip campbell': 'william l campbell jr'
    }

    for key, objs in tqdm.tqdm(entity_map.items()):
        # key = ucid
        # objs = entities on the docket
        start = 0
        # start with all entities
        it_list = objs[start:]
        # while we are still comparing all entities
        while it_list:
            # pick current judge
            this = objs[start]
            # pick all other judges to compare to from the case, that havent been compared to it yet
            those = objs[start+1:]
            
            # iterate our counters up, and the next list to iterate after we complete here.
            # the while loop exists when we go beyond the index length of the list
            start+=1
            it_list = objs[start:]

            # judges we will try to compare to
            search = [o for o in those if o.eligible]
            # if this judge is eligible to be mapped
            if search and this.eligible:
                for that in search: # for other judges on the case
                    # if either judge does not have an anchor, move on (if the entity surname was somehow not identifiable)
                    if not this.anchor or not that.anchor:
                        continue

                    # single letters are bad for this exercise, if the final anchor token was a single letter, move on
                    if len(this.anchor)==1 or len(that.anchor)==1:
                        continue

                    # checkwork begins, if the surname anchors are similar, or partially similar
                    if fuzz.ratio(this.anchor, that.anchor)>=92 or fuzz.partial_ratio(this.anchor, that.anchor)>=92:
                        # if we're doing a special check on single token names, make sure we're not getting coles in colemans
                        # TODO: there will be more names like this to account for
                        if len(this.tokens_wo_suff)==1 or len(that.tokens_wo_suff)==1:
                            if (this.anchor in ['cole','coles'] and that.anchor in ['coleman','colemans']) or (
                                that.anchor in ['cole','coles'] and this.anchor in ['coleman','colemans']):
                                # bad match
                                continue
                            # presumably good
                            #otherwise they match
                            this.choose_winner_ucids(that, f'Anchors-ucid-I', key)
                            continue

                        # o for o connor
                        # basically if they matched in fuzzy, and one name was "o connor" see if the other name is oconnor
                        # same deal with J Mathison and Mathison where the J stands for Judge 
                        if len(this.base_tokens)==2 and this.base_tokens[0] in ['jude','j','o']:
                            # attempt the remainder of the name after the botched prefix
                            thisname = " ".join(this.base_tokens[1:])
                            if fuzz.partial_ratio(thisname, that.name)>=92:
                                this.choose_winner_ucids(that, f'Anchors-ucid-I', key)
                                continue
                        # flip logic on the other judge
                        if len(that.base_tokens)==2 and that.base_tokens[0] in ['jude','j','o']:
                            thatname = " ".join(that.base_tokens[1:])
                            if fuzz.partial_ratio(this.name, thatname)>=92:
                                this.choose_winner_ucids(that, f'Anchors-ucid-I', key)
                                continue

                        # if there is a mismatch in the first tokens confirm that it's not a partial name
                        # i.e. Amy j st eve mismatches st ever
                        if len(this.base_tokens[0])>1 and \
                            len(that.base_tokens[0])>1 and \
                            this.base_tokens[0]!=that.base_tokens[0]:

                            if fuzz.partial_ratio(this.name, that.name)<98:
                                compy = this.name
                                campy = that.name
                                if this.name in exceptions:
                                    compy = exceptions[this.name]
                                if that.name in exceptions:
                                    campy = exceptions[that.name]
                                if fuzz.partial_ratio(compy, campy)>=98:
                                    # presumably good
                                    this.choose_winner_ucids(that, f'Anchors-ucid-I', key)
                                    continue
                            else:
                                # presumably good (their similarity was above 98%)                                
                                this.choose_winner_ucids(that, f'Anchors-ucid-I', key)
                                continue
    
    return entity_map

def PIPE_Anchor_Reduction_II_UCID(entity_map):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. This is a secondary implementation of disambiguation, stacked on top of the first layer
    of anchor disambiguation

    Args:
        entity_map (dict): key = ucid, values = list of nodes in a ucid we are trying to reduce

    Returns:
        dict: the same dict that entered, but the child objects in the lists may be updated and disambiguated
    """
    print("\nPipe: Anchor Reduction II within UCIDs")

    for key, objs in tqdm.tqdm(entity_map.items()):
        # key = ucid
        # objs = entities on the docket
        start = 0
        # start with all entities
        it_list = objs[start:]
        # while we are still comparing all entities
        while it_list:
            this = objs[start] # current judge
            those = objs[start+1:] # other judges to compare to

            start+=1 # update iterables
            it_list = objs[start:] # update iterables

            # eligible search for disambiguation
            search = [o for o in those if o.eligible]
            if search and this.eligible:
                for that in search: # for other judges on the ucid
                    # if the surnames match at 90% or more
                    if fuzz.ratio(this.anchor, that.anchor)>=90:
                        # anchors >90% and tokens all above 98%
                        if fuzz.token_set_ratio(this.base_tokens, that.base_tokens) >=98:
                            this.choose_winner_ucids(that,f"Anchors-ucid-II", key)
                            continue
                        # one of the entities was just a surname
                        if this.token_length==1 and that.token_length>1 or this.token_length>1 and that.token_length==1:
                            this.choose_winner_ucids(that,f"Anchors-ucid-II", key)
                            continue
                    # if one is a surname and the other is 2 tokens
                    if len(this.base_tokens[0])==1 and this.token_length==2:
                        # try fuzing the 2-token name into one and see if there was a misc. letter
                        this_alt_anchor = "".join(this.base_tokens)
                        if fuzz.ratio(this_alt_anchor, that.anchor)>=95:
                            this.choose_winner_ucids(that,f"Anchors-ucid-II", key)                            
                            continue
                    # if both multitoken
                    if this.token_length>=2 and that.token_length>=2:
                        # if they are long names and the last tokens first letter doesnt match, fail out
                        if this.token_length>=3 and that.token_length>=3:
                             if this.tokens_wo_suff[1][0] != that.tokens_wo_suff[1][0]:
                                continue
                        # try comparing the first and last tokens individually in the names
                        if fuzz.ratio(this.tokens_wo_suff[0],that.tokens_wo_suff[0])>=90 and \
                            fuzz.ratio(this.tokens_wo_suff[-1],that.tokens_wo_suff[-1])>=90:
                            this.choose_winner_ucids(that,f"Anchors-ucid-II", key)                            
                            continue
                    # compare if the longer entity had dual last names and the short entity matched one of them
                    # basically: this = Smith Washington | that = Smith Washington Jones
                    if this.token_length==2 and that.token_length>2:
                        if fuzz.ratio(this.tokens_wo_suff[0],that.tokens_wo_suff[-2])>=90 and \
                            fuzz.ratio(this.tokens_wo_suff[1],that.tokens_wo_suff[-1])>=90:
                            this.choose_winner_ucids(that,f"Anchors-ucid-II", key)
                            continue
    return entity_map

def PIPE_Anchor_Reduction_III_UCID(entity_map):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. This is a third implementation of disambiguation, stacked on top of the first and
    second layer of anchor disambiguation

    Args:
        entity_map (dict): key = ucid, values = list of nodes in a ucid we are trying to reduce

    Returns:
        dict: the same dict that entered, but the child objects in the lists may be updated and disambiguated
    """
    for key, objs in tqdm.tqdm(entity_map.items()):
        start = 0
        it_list = objs[start:]
        while it_list:
            this = objs[start] # object for comparison
            those = objs[start+1:] # list of other objects for comparison

            start+=1 # update iterables
            it_list = objs[start:] # update iterables

            # eligible to be matched
            search = [o for o in those if o.eligible]
            if search and this.eligible:
                for that in search: # for all other judge names on the ucid we are comparing to
                    # running only for longer names
                    if this.token_length>=2:
                        # against longer names
                        if that.token_length>=2:
                            # if the suffixless names match and the anchors (surnames match)
                            if fuzz.ratio(this.tokens_wo_suff[0], that.tokens_wo_suff[0])>=90 and fuzz.ratio(this.tokens_wo_suff[-1], that.tokens_wo_suff[-1])>=90:
                                # GOOD MATCH
                                this.choose_winner_ucids(that, f"Anchors-ucid-III", key)                                
                                continue
                            # if a mashed string form matches strongly, then it's probably misspelling or misc. letters junking up the match and its good
                            if fuzz.ratio("".join(this.base_tokens), "".join(that.base_tokens))>=95:
                                this.choose_winner_ucids(that, f"Anchors-ucid-III", key)                                
                                continue
                        
                        # if the first token is a letter and it's "j" assume it stands for judge, or jude stands for judge
                        if (len(this.base_tokens[0])==1 and this.base_tokens[0]=='j') or this.base_tokens[0]=='jude':
                            # assume this entity is a single surname then and attempt matching without the j or jude
                            this_alt_anchor = this.base_tokens[1]
                            if fuzz.ratio(this_alt_anchor, that.anchor)>=92:
                                this.choose_winner_ucids(that, f"Anchors-ucid-III", key)                                
                                continue
                        
                        # mashed string forms of the entity names
                        thisjoin = ".".join(this.base_tokens)
                        thatjoin = ".".join(that.base_tokens)
                        # if the smashed forms have a decent match
                        if thisjoin in thatjoin or thatjoin in thisjoin or fuzz.ratio(thisjoin, thatjoin)>=92:
                            # they need to be longer than 3 letters (initials not considered here like CJR)
                            if len("".join(this.base_tokens))<=3 or len("".join(that.base_tokens))<=3:
                                # bad match
                                continue
                            # single token names from headers are usually just initials and cannot confidently be handled here
                            if (this.token_length ==1 and this.was_header) or (that.token_length ==1 and that.was_header):
                                #bad match
                                continue
                            
                            # if we didnt fail out, then the mashed names worked and we say they match
                            this.choose_winner_ucids(that, f"Anchors-ucid-III", key)
                            
    
    return entity_map

def PIPE_Anchor_Reduction_Court(court_map_long, court_map_single):
    """Special function to work anchor matching (matching by surname only) into the court level disambiguation

    Args:
        court_map_long (dict): key = court, value = list of entity objects whose names are multi-tokened
        court_map_single (dict): key = court, value = list of entity objects whose names are single-tokened

    Returns:
        dict: key = court, value = list of entity objects reduced in the court
    """
    print("\nPipe: Court Anchor Reduction")

    # wonk spelling errors to try and solve for
    # if 2 entities match, but any are in this exceptions list, bypass them. We cannot be confident oliver is misspelled toliver, they could be distinct
    exceptions = {
        'case':'casey',
        'ann':'mann',
        'leven':'leen',
        'stevens':'stevenson',
        'toliver':'oliver'
    }

    # for each court
    for court, long_objs in court_map_long.items():
        # grab the uni-token entities as well corresponding to this court
        single_objs = court_map_single[court]

        # only check what is eligible at this point
        longs = [o for o in long_objs if o.eligible]
        singles = [o for o in single_objs if o.eligible]

        # for every uni-token entity
        for each in singles:
            # init matching nothing
            matches = []
            # compare to every long name
            for l in longs:
                # if the surnames (anchors) match confidently, add it to a running list of matches
                if fuzz.ratio(each.anchor, l.anchor)>=85:
                    matches.append(l)
            # if only one multi-token name matches this single token name
            if len(matches) == 1:
                # as long as it is not an exception
                if each.name in exceptions and exceptions[each.name] == matches[0]:
                    continue
                # reduce the small one onto the long one
                each.choose_winner_ucids(matches[0], f"Court-Anchors", court)
            
            # else if multiple multi-token entities match a single-token surname
            elif len(matches)>1:
                # determine if any of them are actual exact matches by surname
                exact_ms = [m for m in matches if each.anchor == m.anchor]
                # if only one is an exact match
                if len(exact_ms)==1:
                    # and it is not an exception
                    if each.name in exceptions and exceptions[each.name] == exact_ms[0]:
                        continue
                    
                    # reduce it
                    each.choose_winner_ucids(exact_ms[0], f"Court-Anchors", court)
    
    # group the 2 dicts into one now
    final_map = {}
    for court, objects in court_map_long.items():
        final_map[court] = objects + court_map_single[court]

    return final_map

##############################
## Fuzzy Matching Functions ##
##############################

#--------------#
# Unrestricted #
#--------------#
def PIPE_Free_Fuzzy(NODES):
    """Given a list of nodes, during free-matching disambiguation see if any fuzzy match to each other

    Args:
        NODES (list): list of objects to be free matched using fuzzy matching

    Returns:
        list: list of same objects that entered the function, but with some now disambiguated to each other
    """
    print("\nPipe: Free Matching Large Scale Fuzzy")
    # start with only those remaining eligible to be mapped
    objs = [N for N in NODES if N.eligible]
    
    start = 0 # iterables
    it_list = objs[start:] # iterables
    while it_list:
        this = objs[start] # entity to compare
        those = objs[start+1:] # other entities to test
        start+=1 # iterables
        it_list = objs[start:] # iterables

        # only the eligible ones to compare (if an entity got mapped during an earlier iterated entity, we don't want to map to it)
        search = [o for o in those if o.eligible]
        if search and this.eligible:
            # this function does not handle single token entities
            if this.token_length==1:
                continue
            for that in search:
                # if at some point in this loop at actually became ineligible, then move on
                # if the possible match is a single token, move on
                if not this.eligible or that.token_length==1:
                    continue
                # fuzzy match bound
                bound = 93
                # the full entity name needs to fuzzy match above this bound
                if fuzz.ratio(this.name, that.name) >=bound:
                    this.choose_winner(that, "FreeFuzzy")
    return NODES

#------------#
# UCID/COURT #
#------------#

def PIPE_Fuzzy_Matching(entity_map):
    """fuzzy matching to be used for intra-ucid or intra-court disambiguation

    Args:
        entity_map (dict): key = ucid or court, values = lists of objects per ucid or court that will be reduced onto each other

    Returns:
        dict: same dict that entered the function, but the child objects will be updated in some cases after reduction
    """
    print("\nPipe: Fuzzy Matching")
    # for each ucid/court and the corresponding entity objects
    for key, objs in tqdm.tqdm(entity_map.items()):
        start = 0 # iterables
        it_list = objs[start:] # iterables
        # comparison is bidirectional such that A compared to B is equivalent to B compared to A
        # when we enumerate out the list of comparisons it is effectively like: [A, B, C, D]
        # --> AB, AC, AD, BC, BD, CD
        while it_list: # go until we reach the end of the list
            this = objs[start] # entity to compare
            those = objs[start+1:] # other entities we compare to
            start+=1 # iterables
            it_list = objs[start:] # the next loop

            # only want eligible entities. Eligible means another entity can point to it and be disambiguated to it and this entity does not point elsewhere
            search = [o for o in those if o.eligible]
            if search: # if there are eligible ones
                for that in search:
                    # if each entity appeared on more than 20 ucids OR
                    # one of the entities appeared much more frequently than the other
                    # loosen the bound a bit, more common occurrences == more leeway for typos
                    if (this.n_ucids/that.n_ucids)>20 or (that.n_ucids/this.n_ucids)>20:
                        bound = 90
                    else:
                        bound = 93

                    # if they fuzzy matched, hooray
                    if fuzz.ratio(this.name, that.name) >=bound:
                        # object routine to reduce the objects with each other
                        this.choose_winner_ucids(that, "UCIDFuzzy", key)

    return entity_map

#######################
## Simple Token Sort ##
#######################

def PIPE_Free_Token_Sort(NODES):
    """Pipe used in Free-matching disambiguation to compare 2 entities tokens in a token-sort fuzzy match

    Args:
        NODES (list): list of objects being reduced in disambiguation

    Returns:
        list: same list that entered the function, but now some objects will point to each other
    """
    print("\nPipe: Free Matching; Token Sort Matching")

    # only eligible ones desired
    objs = [N for N in NODES if N.eligible]

    start = 0 # iterables
    it_list = objs[start:] # iterables
    while it_list:
        this = objs[start] # object to be compared
        those = objs[start+1:] # other objects to compare to
        start+=1 # iterables
        it_list = objs[start:] # iterables

        # again only eligible ones
        search = [o for o in those if o.eligible]
        if search and this.eligible:
            # function not equipped to handle single-token entities
            if this.token_length==1:
                continue
            # for every other entity, check it
            for that in search:
                if not this.eligible or that.token_length==1:
                    continue
                
                # fuzzy matching bound
                bound = 93

                # if their token sort ratios are strong matches, hooray
                if fuzz.token_sort_ratio(this.base_tokens, that.base_tokens) >=bound:
                    this.choose_winner(that, "Free Token Sort")
    return NODES

#########################################
## Initial and Middle Vacuum Functions ##
#########################################
def PIPE_Free_Vacuum(NODES):
    """Pipe to be used in free-matching disambiguation. This "vacuums out" middle names and just compares first and last names.
    Example: Christian John Rozolis and Chris J. Rozolis are compared as Christian Rozolis vs. Chris Rozolis

    Args:
        NODES (list): list of objects to be reduced in free matching

    Returns:
        list: same list that entered the function, but now a few of the objects point to each other
    """
    print("\nPipe: Free Matching; Vacuum middle initials")
    # only want eligible ones
    objs = [N for N in NODES if N.eligible]

    start = 0 # iterables
    it_list = objs[start:] # iterables
    while it_list:
        this = objs[start] # object to compare
        those = objs[start+1:] # objects to be compared to
        start+=1 # iterables
        it_list = objs[start:] # iterables

        search = [o for o in those if o.eligible]
        if search and this.eligible:
            # function not equipped to handle single token entities
            if this.token_length==1:
                continue
            
            # create the "vacuumed" name
            this_vacuumed = [this.tokens_wo_suff[0], this.tokens_wo_suff[-1]]
            for that in search:
                if not this.eligible or that.token_length==1:
                    continue
                # for every other entity, build the vacuumed name
                that_vacuumed = [that.tokens_wo_suff[0], that.tokens_wo_suff[-1]]
                # if the token comparison ratio of the vacuumed names is trong
                if fuzz.ratio(this_vacuumed[0], that_vacuumed[0])>=90 and fuzz.ratio(this_vacuumed[1], that_vacuumed[1])>=90:
                    # if the names are both long
                    if this.token_length>=3 and that.token_length>=3:
                        # and the last names first letters dont match, VETO
                        if this.tokens_wo_suff[1][0]!= that.tokens_wo_suff[1][0]:
                            continue
                        # or there is a suffix mismatch, VETO
                        if this.suffix and that.suffix and this.suffix!= that.suffix:
                            continue
                    # otherwise, hooray
                    this.choose_winner(that, "FreeVacuum")
    return NODES

def PIPE_Free_Initialisms(NODES):
    """Pipe to be used in Free-matching disambiguation. This pipe works on names that frequently occur as initialisms and
    dont have a strong match to their formal names
    example: P K Holmes needs to eventually find its way to Patrick Kinloch Holmes III

    Args:
        NODES (list): list of objects to be reduced in free matching

    Returns:
        list: same list that entered the function, but now a few of the objects point to each other
    """
    print("\nPipe: Free Matching; Initialisms")
    # empty list of possible nodes to match
    matchy = []
    # check only those eligible
    for each in [o for o in NODES if o.eligible]:
        # if the name is 3 tokens long, doesnt have a suffix, the surname is one letter, the first name is multiple, and the middle initial is one letter
        # then consider it for matching
        if each.token_length == 3 and\
            not each.suffix and \
            len(each.anchor)==1 and \
            len(each.base_tokens[0])>2 and \
            len(each.base_tokens[1])==1:
            matchy.append(each)
    # for all eligible to be matched
    for m in matchy:
        # compare against all long names
        for n in [o for o in NODES if o.eligible and o!=m and o.token_length>=3]:
            # if the first letters match across the board and the first names match
            if n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[1][0] == m.base_tokens[1][0] and\
                n.base_tokens[2][0] == m.base_tokens[2][0]:
                m.choose_winner(n, "Initialisms")
            # if the first names match and the offset tokens from a longer name match
            # i.e. Chris John Rozolis Stevens and Chris Rozolis Stevens
            if len(n.base_tokens)>3 and n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[2][0] == m.base_tokens[1][0] and\
                n.base_tokens[3][0] == m.base_tokens[2][0]:
                m.choose_winner(n, "Initialisms")
    return NODES

def PIPE_Free_Single_Letters(NODES):
    """Free matching pipe function for any entities whose first token is a single letter

    Args:
        NODES (list): list of objects to be reduced in free matching

    Returns:
        list: same list that entered the function, but now a few of the objects point to each other
    """
    print("\nPipe: Free Match; Single Letter Starters")

    # if the entity is multi-tokened
    for each in [o for o in NODES if o.eligible and o.token_length>=2]:
        # and the first token is a single letter
        if len(each.base_tokens[0])==1:
            # compare against all other multi-tokened names
            for check in [o for o in NODES if o.eligible and o!=each and o.token_length>=2]:
                # if they have a decent token sort ratio AND the second token is an exact match, then they're good
                # i.e. J A Adande and A Adandu
                if fuzz.token_sort_ratio(each.name,check.name)>80 and each.base_tokens[1]==check.base_tokens[1]:
                    each.choose_winner(check, "Single Letters")

    return NODES

##################################
## Party/Counsel Fuzzy Matching ##
##################################

def UCID_PIPE_Drop_Parties(ucid_map, parties_df, counsels_df):
    """Function used in the first round of large-scale disambiguation. We use this to omit any party or counsel names that
    the spacy model mistook to be judges. We do this using the known party and counsel names from header metadata.

    Args:
        ucid_map (dict): key = ucid, values = list of entities on the ucid
        parties_df (pandas.DataFrame): consists of the parties per case
        counsels_df (pandas.DataFrame): consists of the counsels per case
    Return:
        dict, dict: one dictionary is the one containing the entities we want to advance to disambiguation, the other is a log of which entities we threw out
    """
    
    def _check_if_party(entity_obj, party_names):
        """helper function to compare if an entity object is in a list of party names by fuzzy matching

        Args:
            entity_obj (obj): IntraMatch UCID Object
            party_names (list): list of party name strings on the case

        Returns:
            bool: is it a match to a party name or not
        """
        # check against every party in the case
        for party in party_names:
            # if the object matches strongly, return True
            if fuzz.ratio(str(entity_obj.name), str(party))>95:
                return True
        # if we got through every party and none of the entities flagged, return false and proceed to disambiguation with this one
        return False
    
    print("\nPipe: Party and Counsel Dropping Running")
    print(">> Cleaning party names")

    # drop null or blank string entities
    parties_df = parties_df[~parties_df.Entity.isna()].copy()
    # build unique list
    parties_unique = list(parties_df.Entity.unique())

    # get them into a cleaned form as parties
    pmap = {party: JCF.stacked_cleaning(str(party)) for party in parties_unique}
    # if the cleaning resulted in a string longer than nothing, we will map it back to the original party name
    pmap = {k:v for k,v in pmap.items() if v}
    # map it
    parties_df['CLEANED_ENT'] = parties_df.Entity.map(pmap)

    # now convert the parties df into a dict, keyed by ucid
    party_maps = defaultdict(list)
    for ucid, party in tqdm.tqdm([tuple(x) for x in parties_df[['ucid','CLEANED_ENT']].to_numpy()]):
        party_maps[ucid].append(party)

    print(">> Cleaning counsel names")
    # drop null counsels
    counsels_df = counsels_df[~counsels_df.Entity.isna()].copy()
    # take the unique ones
    counsels_unique = list(counsels_df.Entity.unique())
    # clean their names
    cmap = {counsel: str(JCF.stacked_cleaning(str(counsel))) for counsel in counsels_unique}
    # if the cleaned name is not an empty string, we will map it
    cmap = {k:v for k,v in cmap.items() if v}
    # map it back to the counsels df
    counsels_df['CLEANED_ENT'] = counsels_df.Entity.map(cmap)

    # now convert the counsels df into a dict, keyed by ucid
    counsel_maps = defaultdict(list)
    for ucid, counsel in tqdm.tqdm([tuple(x) for x in counsels_df[['ucid','CLEANED_ENT']].to_numpy()]):
        counsel_maps[ucid].append(counsel)
    
    # now combine both parties and counsels into one "parties" dict keyed by ucid
    parties = {ucid: party_maps[ucid] + counsel_maps[ucid] for ucid in party_maps.keys()}
    print(">> Now dropping parties and counsels")

    # now run thru each ucid and drop any entities that match parties for the case
    new_map = {}
    toss_map = {}
    for ucid, entities in tqdm.tqdm(ucid_map.items()):
        # if we didnt have parties for the case, that's shocking but move along I guess
        if ucid not in parties:
            continue
        # grab the parties (parties and counsels)
        case_parties = parties[ucid]
        
        # we compare the entities that appeared only on this ucid (that is a single occurence in our dataset)
        # (efficiency choice -- seems obvious if an entity appears on 100 ucids, it's not a party 99% of the time)
        compy = [e for e in entities if e.n_ucids==1]
        # anybody that fails the party check is a tosser
        tossers = [each for each in compy if _check_if_party(each, case_parties)]
        # keepers arent tossers
        keepers = [e for e in entities if e not in tossers]
        # map them
        new_map[ucid] = keepers
        toss_map[ucid] = tossers
    
    # we should log the tossers in case we want to do a post mortem on the spacy model and understand why it thought these entities were judges
    for ucid, tossed in toss_map.items():
        for t in tossed:
            JU.log_message(f"{ucid:25} -- Tossed out Party or Counsel -- {t.name}")
    return new_map, toss_map

############################################
## Tokens in Tokens Application Functions ##
############################################
def pool_runner(this_pool: list, that_pool: list):
    """Helper function that takes in 2 lists of tokens (pools) and compares them against each other using 
    my custom tokens in tokens subroutine
    example: Name Christian John Michael Rozolis and Chris John M Rozolis
    - A: [[Christian J M Rozolis],[Christian John M Rozolis],[Christian J Michael Rozolis]]
    - B: [[Chris John M Rozolis],[Chris J M Rozolis]]
    compare the combinations of A lists to B lists

    Args:
        this_pool (list): lists of token variations of entity name a
        that_pool (list): lists of token variations of entity name b

    Returns:
        bool: is one of these token sublists a subset of the other
    """
    # for each sublist of tokens in the pools
    for tokey_a in this_pool:
        for tokey_b in that_pool:
            # if one of the sublists is a single token and the other is multi
            if len(tokey_a)==1 and len(tokey_b)>1:
                # anchor a (surname)
                ta = tokey_a[0]
                # anchor b (surname)
                tbL = tokey_b[-2] if tokey_b[-1] in JG.suffixes_titles else tokey_b[-1]     
                # if the surnames dont match, move on fast
                if ta!=tbL:
                    continue  
            # vice versa
            if len(tokey_b)==1 and len(tokey_a)>1:
                tb = tokey_b[0]
                taL = tokey_a[-2] if tokey_a[-1] in JG.suffixes_titles else tokey_a[-1]
                if tb!=taL:
                    continue   
            # if you made it here, call the sub function
            if tokens_in_tokens_sub_function_caller(tokey_a, tokey_b):
                return True
    return False
    
def pool_creator(this, that, abbreviated_first, abbreviated_middle, style):
    """Given 2 entities, and abbreviation and style arguments, create the pools of tokens necessary for a tokens in tokens check to be run.
    Examples of the pools for a middle initial = True
    - A: "Christian John Michael Rozolis" --> [[Christian J M Rozolis],[Christian John M Rozolis],[Christian J Michael Rozolis]]
    - B: "Chris John M Rozolis" -->[[Chris John M Rozolis],[Chris J M Rozolis]]

    Args:
        this (obj): IntraMatch child object that will have certain attributes related to its name
        that (obj): IntraMatch child object that will have certain attributes related to its name
        abbreviated_first (bool): should we abbreviate the first token
        abbreviated_middle (bool): should we abbreviate the middle token[s]
        style (str): plaintext argument for type of entity names to be used (normal, nicknames, or universal spellings)

    Returns:
        list, list: returns lists of lists for each pool containing the appropriate abbreviation, and style settings
    """
    if style =='Plain':
        this_pool = this.inferred_tokens[abbreviated_first][abbreviated_middle]
        that_pool = that.inferred_tokens[abbreviated_first][abbreviated_middle]
    elif style == 'Unified':
        this_pool = this.unified_names_tokens[abbreviated_first][abbreviated_middle]
        that_pool = that.unified_names_tokens[abbreviated_first][abbreviated_middle]
    elif style == 'Nicknames':
        # this_pool = this.nicknames_tokens[abbreviated_first][abbreviated_middle]
        # we need to compare base tokens to nicknames and vice versa, so both get included in the pools
        this_pool = this.inferred_tokens[abbreviated_first][abbreviated_middle] + this.nicknames_tokens[abbreviated_first][abbreviated_middle]
        that_pool = that.nicknames_tokens[abbreviated_first][abbreviated_middle] + that.inferred_tokens[abbreviated_first][abbreviated_middle]
    else:
        # you gave a bad argument, sorry
        return [],[]
    
    return this_pool, that_pool

#------------#
# UCID/COURT #
#------------#

def PIPE_Tokens_in_Tokens(entity_map: dict,
                            abbreviated_first: bool = False, abbreviated_middle: bool = False,
                            style: str = 'Plain'):
    """High volume function called in UCID and COURT level disambiguation. This is used to determine by ucid
    if any of the entities are tokenized subsets of the other entities tokens
    Example: St Eve is a tokenized subset of Amy Joan St Eve
    Amy J St Eve is an abbreviated tokenied subset of A J St Eve

    Args:
        entity_map (dict): key = ucid/court, values = list of entity objects in that key-based group
        abbreviated_first (bool, optional): should we abbreviate the first token in this run. Defaults to False.
        abbreviated_middle (bool, optional): should we abbreviate the middle token[s] in this run. Defaults to False.
        style (str, optional): How should we build and compare entitiy names (as is plain, nicknames, universal spellings). Defaults to 'Plain'.

    Returns:
        dict: key = ucid/court, values = list of entity objects in that key-based group that have been reduced
    """
    print(f"\nPipe: Tokens in Tokens -- {style} -- AF={int(abbreviated_first)} AM={int(abbreviated_middle)}")

    # for every ucid or court
    for key, objs in tqdm.tqdm(entity_map.items()):
        start = 0 # iterables
        it_list = objs[start:] # iterables
        while it_list:
            this = objs[start] # object to compare
            those = objs[start+1:] # other objects to compare to
            
            start+=1 # iterables
            it_list = objs[start:] # iterables

            # only want eligible objects
            search = [o for o in those if o.eligible]
            if search:
                # for every object eligible to be compared
                for that in search:
                    # create the pools of tokens
                    this_pool, that_pool = pool_creator(this, that, abbreviated_first, abbreviated_middle, style)
                    # run the pools through the tokens in tokens check
                    if pool_runner(this_pool, that_pool):
                        # if they match, reduce them
                        this.choose_winner_ucids(that, f'TIT-{int(abbreviated_first)}{int(abbreviated_middle)}-{style}', key)

    return entity_map

#--------------#
# Unrestricted #
#--------------#

def PIPE_Free_Tokens_in_Tokens(NODES: list,
                            abbreviated_first: bool = False, abbreviated_middle: bool = False,
                            style: str = 'Plain'):
    """Pipe function to be used in Free-matching disambiguation that checks if 2 entities tokenized names are subsets of each other

    Args:
        NODES (list): list of objects to be reduced in free matching
        abbreviated_first (bool, optional): should we abbreviate the first token in this run. Defaults to False.
        abbreviated_middle (bool, optional): should we abbreviate the middle token[s] in this run. Defaults to False.
        style (str, optional): How should we build and compare entitiy names (as is plain, nicknames, universal spellings). Defaults to 'Plain'.

    Returns:
        list: the same list that entered the function, but now some of the objects point to each other
    """
    print(f"\nPipe: Tokens in Tokens -- {style} -- AF={int(abbreviated_first)} AM={int(abbreviated_middle)}")
    # only want eligible ones
    objs = [N for N in NODES if N.eligible]
    
    start = 0 # iterables
    it_list = objs[start:] # iterables
    while it_list:
        this = objs[start] # object to compare
        those = objs[start+1:] # objects to be compared

        start+=1 # iterables
        it_list = objs[start:] # iterables

        # function not equipped to handle single token entities.
        # We DONT want to match Brown to Brown in freematching, since we dont know they are truly the same across courts
        if this.token_length==1:
            continue
        # eligible entities
        search = [o for o in those if o.eligible]
        if search and this.eligible:
            for that in search:
                # again VETO single-token entities
                if that.token_length==1:
                    continue
                # create a pool of tokens to use in comparison
                this_pool, that_pool = pool_creator(this, that, abbreviated_first, abbreviated_middle, style)

                # run the pools against each other
                if pool_runner(this_pool, that_pool):
                    # if this entity is 2 tokens and one of the tokens is a single letter, VETO, dont want to consider in free-matching
                    if this.token_length == 2 and min(len(tok) for tok in this.base_tokens)==1:
                        continue
                    # same concept, vice versa
                    if that.token_length == 2 and min(len(tok) for tok in that.base_tokens)==1:
                        continue
                    # if not, if the pools matched, reduce them, hooray
                    this.choose_winner(that, f'Free TIT-{int(abbreviated_first)}{int(abbreviated_middle)}-{style}')

    return NODES

#########################################################
# Helper that calls the tokens in tokens check in both 
# directions for lists (list a in b or list b in a)
#########################################################
def tokens_in_tokens_sub_function_caller(tokens_a, tokens_b):
    """helper function to run through the tokens in tokens checker

    Args:
        tokens_a (list): list of name a's tokens
        tokens_b (list): list of name b's tokens

    Returns:
        bool: are a's tokens wholly in list b or are b's tokens wholly in list a
    """
    if tokens_a ==[] or tokens_b == []:
        return False

    # if a in b, great return
    if tokens_in_tokens_sub_function(tokens_a, tokens_b):
        
        return True
    # else, try the reverse
    elif tokens_in_tokens_sub_function(tokens_b, tokens_a):
        return True
    # else, false
    else:
        return False


def tokens_in_tokens_sub_function(tokens_1, tokens_2):
    """ This is the sub-function that will token-wise compare 2 lists and determine if list_1
    is wholly present in list_2. Note sets() and Counters() cannot be compared since we need to account for
    dual names appearing i.e. [jo, jo, smith] or [george, h, george] and account for their order of appearance

    Args:
        tokens_1 (list): pre-split name into list of token strings for one name
        tokens_2 (list): pre-split name into list of token strings to check if list_1 is present

    Returns:
        bool: a bool indicating if one of these token lists is wholly present in the other
    """
    
    # determine if every token in the first list appears in the second lists elements              
    if all(token in tokens_2 for token in tokens_1):
        # SPECIAL CASE 1.
        # the george h. george catch -- if a token appears twice in the list_1, the check above doesnt
        # confirm it appears twice in the second list, just that it appeared in the second list.
        # Thus both george tokens in george h george do both technically appear in george h washington
        # we must confirm via counts that there is in fact a double george in the second name
        # or else we say "actually -- no match"
        # init a tracker
        tracker = []
        # for every token in list 1 that appears more than once (counter comprehension here)
        # if there are no double tokens, we move right along
        count_1 = Counter(tokens_1)
        count_2 = Counter(tokens_2)
        for tok, count in {k:v for k,v in count_1.items() if v>1}.items():
            # if the count of that token in list 2 is less than the count in list 1
            # then we know this can't be right (2 georges compared to 1)
            if count_2[tok] < count:
                # this token fails so note it as false
                tracker.append(False)
                break # fail early
            else:
                # else note this token as true, check next tokens
                tracker.append(True)

        # if any of the tokens returned false, this mismatches and kicks out a failure on the sub-function
        # the failure meaning list_1 is not wholly present in list_2
        if sum(tracker) != len(tracker):
            return False
        
        # else we move right along
        # SPECIAL CASE 2.
        # the Lewis A. and Wilma A. Lewis check. Lewis A is wholly present in the second name,
        # however the tokens appear out of order, so we should not match them
        # if we passed the double george check, we now do this ordered check for the tokens
        else:
            # find the indices of the tokens from the first list in the second list
            # these should in theory be ascending values as that means they appeared in the proper order
            
            # this is all of list 2's indices in order they appear.
            # for example, "Wilma A Lewis" should looks like {Wilma: [0], A:[1], Lewis:[2]}
            t2 = defaultdict(list)
            for i,j in enumerate(tokens_2):
                t2[j].append(i)
            
            # All tokens in Lewis A matched into Wilma A Lewis, but now we can catch the ordering error
            #  (Lewis, 0) (A, 1) doesn't match the correct (Wilma, 0) (A, 1) (Lewis, 2)

            # this list tracks where in list 2 this token appears. Each element location is the element location
            # in list one, and the value is the location in list 2
            # [Lewis, A] looks like [2, 1] (third index - value 2 of the second list for Lewis,
            #  then the second index, value 1 for A)
            tok2_inds = []
            for token in tokens_1:
                # grab the front index from the t2 Counter dict. This is the earliest appearance of
                # the token in list 2
                tok2_ind = t2[token][0]
                # now reset the t2 to remove that front index since we are calling it a match, we want to pop it
                # in a George H George scenario, if we matched the first George, the counter goes
                # from {George: [0,2], H:[1]}
                # to {George: [2], H:[1]}
                t2[token] = t2[token][1:]
                # now we append that index to our tracking list, this is a list of the order our tokens appear
                tok2_inds.append(tok2_ind)
            
            # now that I have the order in which my tokens appear in the second list
            # we will cycle and check that no greater value appears before a lesser value
            # in the above examples [2,1] for Lewis A the order in Wilma A Lewis is out of order and we know its bad
            for ind,tok_ind in enumerate(tok2_inds):
                # comprehension - bools for if current element is always less than all following elements
                checker = [tok_ind < i for i in tok2_inds[ind+1:]]
                # if any of them are are False, the sum wont be the length
                if sum(checker) == len(checker):
                    continue
                else:
                    # mismatched, break (no point continuing we know it's bad)
                    return False
            
            # if any of the indices fails the test for values to the right, then we know it's a failure
            # if we made it to here, we successfully passed
            return True
    
    # else means not all tokens appeared in list 2 at the start, kick out a failure
    else:
        return False

def tokens_in_string_algo(tokens, string_check):
    """Helper function used to determine if a list of tokens is wholly present in a string, in the proper order
    For example A A Milne is wholly in Robert A Milne  with just substrings, but this function confirms that 
    2 A's needed to appear in Robert A Milne as standalone tokens to qualify as "in the string"

    Args:
        tokens (list): list of string which are the tokenized form of an entity name
        string_check (str): the entity string we are determining if those tokens are in

    Returns:
        bool: yes or no, the tokens are in the string, in the correct order, as tokens
    """
    in_string = string_check

    match_count = 0
    # for every token in the judge A token grouping
    for token in tokens:
        # if the token appears as a substring in name B (that's great!)
        if token in string_check:
            ### CHECK SINGLE LENGTH TOKENS
            # if the token was 1 letter (a middle initial) let's confirm it matches a single letter token
            # and not just a random letter in a word of name B
            if len(token) == 1:
                # build string b tokens
                sb_split = string_check.split()
                # if the token is in the split for B that means a single letter token exists as
                # an element in name b
                if token in sb_split:
                    # up our match count
                    match_count+=1
                    # now we update what string_check looks like so that if there are double tokens in a name,
                    # we can confirm there are double occurrences in B
                    # Ex. J J Smith --> Matched a J, now remaining tokens will compare to J Smith, 
                    # if there's another J token, repeat and match remaining tokens to Smith
                    tokind = sb_split.index(token)
                    string_check = " ".join(sb_split[0:tokind]+sb_split[tokind+1:]).strip()
                # if it's not in the element list, then we falsely matched a subcharacter, sad
                else:
                    return False
            # if it's a longer token we assume it's a genuine match
            else:
                # don't catch liam in william, list in allister or ott in mcdermott, hon in anthony, etc.
                internal_oddities = ['liam','list','ott', 'lau','hon']
                if token in internal_oddities and token not in string_check.split():
                    return False
                # up the count, cut the string
                match_count+=1
                tokind = string_check.index(token)
                # cut out this matched token so a duplicate token from A cannot match the same substring
                # this ensures the substring has to appear twice if the token appears twice
                # George. H. George matching to George H Washington would mean
                # George matched George, Now H. George checks against H Washington -- H's match
                # now the first George from Washington is no longer there to falsely match the "last name" george from the first one
                string_check = string_check[0:tokind] + string_check[tokind+len(token):]
        
        # if the token doesn't match, we know not all of the tokens will be in the string, fail early
        else:
            return False
    
    #### Determine if there is a qualifying match
    # if our match count is the length of tokens, that means every token passed the substring test
    if match_count == len(tokens):
        order_check = []
        next_string = in_string
        for tok in tokens:
            if len(tok) == 1:
                init_ind = next_string.split().index(tok)
                init_ind = len(' '.join(next_string.split()[0:init_ind]).strip())
                next_string = next_string[0:init_ind] + next_string[init_ind+len(tok):]
                order_check.append(init_ind)
            else:
                next_ind = next_string.index(tok)
                next_string = next_string[0:next_ind] + next_string[next_ind+len(tok):]
                order_check.append(next_ind)

        for j in range(len(order_check[0:-1])):
            if order_check[j] > order_check[j+1]:
                return False
        return True
    else:
        return False
