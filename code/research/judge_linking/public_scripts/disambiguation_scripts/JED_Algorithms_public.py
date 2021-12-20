import tqdm
from collections import defaultdict, Counter
import re

from fuzzywuzzy import fuzz
import pandas as pd

import JED_Globals_public as JG
import JED_Cleaning_Functions_public as JCF
import JED_Classes_public as JCL
import JED_Utilities_public as JU


##################################
## Final Intra-UCID Crosschecks ##
##################################

def FINAL_CLEANUP(PCID_Mappings: pd.DataFrame):
    """Given the final disambiguation results, double check any remaining inconclusive entities cannot
    be tagged to a known entity

    Args:
        PCID_Mappings (pandas.DataFrame): final output DF the contains span columns, entity columns, point to entity columns, and an SJID tag

    Returns:
        pandas.DataFrame: DF of the same input shape, with 0 to many updates on inconclusive entities
    """
    # I only need the unique list of entities by ucid to run through the final check
    mapper_frame = PCID_Mappings[['ucid','extracted_entity','Points_To', 'SJID']].drop_duplicates()

    # I will be building a map by ucid of all entities marked as good (known judge with SJID) or "inconclusive"
    the_map = {}
    # cleaned ent was the original extraction, final ent is the pointer or parent entity
    for ucid, cleaned_ent, final_ent, SJID in [tuple(x) for x in mapper_frame.to_numpy()]:
        has_sjid = True
        if SJID == 'Inconclusive':
            has_sjid = False

        # note the ordering of the tuples flips them from when they come in
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
        # for every ucid, and each inconclusive entity
        for each in ents['Inconclusive']:
            badname = each[0] # the entity we will compare
            original = each[1] # what the entity originally looked like
            m_count = [] # match counter
            # compare against the names of all the known SJID entities
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
                                "Points_To": each[0],
                                "New_Point": good[0],
                                "New_SJID": good[2],
                                "Absorb": True
                            })
    # build the DF for the updated mappings
    RECAST = pd.DataFrame(updater)  
    RECAST.drop_duplicates(inplace=True)
    
    # if there were remappings, we will need to merge them back onto the original DF
    if not RECAST.empty:
        # log what we updated
        for each in updater:
            JU.log_message(f"Final Crosscheck | {each['ucid']:25} |{each['Points_To']:25} --> {each['New_Point']}")

        print("Completing final SEL merge")
        # merge them together
        FMDF = PCID_Mappings.merge(RECAST, how='left', on = ['ucid','Points_To'])

        # any of the update entities are marked with "Absorb" meaning we will overwrite their SJID (previously inconclusive) and their Final Parent entity (final pointer)
        FMDF.loc[FMDF.Absorb==True, 'SJID'] = FMDF.New_SJID
        FMDF.loc[FMDF.Absorb==True, 'Points_To'] = FMDF.New_Point

        # drop misc columns created
        FMDF.drop(['New_Point','New_SJID','Absorb'], axis=1, inplace=True)
    else:
        FMDF = PCID_Mappings
    
    return FMDF

##############################
## Free Matching Algorithms ##
##############################

def PIPE_Free_Exact_Beginning(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match exactly to each other
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """
    
    # we will only consider the nodes that remain eligible to be mapped to each other
    eligible_nodes = [N for N in nodes if N.eligible]
    
    # we begin iteration at the beginning of the list
    start = 0
    # the iterables are the whole list
    it_list = eligible_nodes[start:]

    # while there remains entities in the list we haven't compared, keep going
    while it_list:
        # the beginning node we will compare too
        this = eligible_nodes[start]
        # the remaining peers to compare to it
        those = eligible_nodes[start+1:]

        # advance the parameters of our while loop for the next iteration
        start+=1
        it_list = eligible_nodes[start:]

        # the search checks again for those that remain eligible
        # it is possible one of the nodes was originally eligible upon creation of the it_list, but has since been mapped to another node
        # and is now ineligible
        search = [o for o in those if o.eligible]
        # default to having no matched nodes
        matches = []

        # if there is a remaining list to search after eligibility checks, and this node remains eligible
        if search and this.eligible:
            # begin the search
            for that in search:
                # criterion: strings match exactly for their names
                if that.name == this.name:
                    # if satisfied, add it to the list of possible matches
                    matches.append(that)

        # if names were considered a match
        if matches:
            # run the ambiguity reduction method for the objects: this will either choose a winner or deem them ambiguous
            this.assess_ambiguity(matches, 'Instant Exact Reduction','Free [1]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches,  'Instant Exact Reduction','Free [1]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
    
    return nodes

def PIPE_Free_Fuzzy_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by fuzzy standards
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """

    # we will only consider the nodes that remain eligible to be mapped to each other
    eligible_nodes = [N for N in nodes if N.eligible]
    
    # we begin iteration at the beginning of the list
    start = 0
    # the iterables are the whole list
    it_list = eligible_nodes[start:]

    # while there remains entities in the list we haven't compared, keep going
    while it_list:
        this = eligible_nodes[start] # entity to compare
        those = eligible_nodes[start+1:] # other entities to test
        start+=1 # iterables
        it_list = eligible_nodes[start:] # iterables

        # only the eligible ones to compare (if an entity got mapped during an earlier iterated entity, we don't want to map to it)
        search = [o for o in those if o.eligible]
        matches = []

        if search and this.eligible:
            # this function does not handle single token entities, fuzzy matching on single tokens is bad
            if this.token_length==1:
                continue
            for that in search:
                # if the possible match is a single token, move on
                if that.token_length==1:
                    continue
                # fuzzy match bound
                bound = 95
                # the full entity name needs to fuzzy match above this bound
                if fuzz.ratio(this.name, that.name) >=bound:
                    # consider it as a possible match
                    matches.append(that)
        # if possible matches were found
        if matches:
            # run the ambiguity reduction method for the objects: this will either choose a winner or deem them ambiguous
            this.assess_ambiguity(matches, 'Free Fuzzy', 'Free [2]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches, 'Free Fuzzy', 'Free [2]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
    
    return nodes

def PIPE_Free_Tokens_in_Tokens_Pool_Based(nodes: list, abbreviated_first: bool, abbreviated_middle: bool, style: str):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by token overlap standards
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes
        abbreviated_first (bool): should the first token be abbreviated
        abbreviated_middle (bool): should the middle tokens be abbreviated
        style (str): run the function on the plain string, nicknames of the strings, or unified spellings
    """

    # we will only consider the nodes that remain eligible to be mapped to each other
    eligible_nodes = [N for N in nodes if N.eligible]
    
    # we begin iteration at the beginning of the list
    start = 0
    # the iterables are the whole list
    it_list = eligible_nodes[start:]
    while it_list:
        this = eligible_nodes[start] # object to compare
        those = eligible_nodes[start+1:] # objects to be compared

        start+=1 # iterables
        it_list = eligible_nodes[start:] # iterables

        # function not equipped to handle single token entities.
        # We DONT want to match Brown to Brown in freematching, since we dont know they are truly the same across courts
        if this.token_length==1:
            continue
        
         # only the eligible ones to compare (if an entity got mapped during an earlier iterated entity, we don't want to map to it)
        search = [o for o in those if o.eligible]
        matches = []

        if search and this.eligible:
            for that in search:
                # again VETO single-token entities
                if that.token_length==1:
                    continue

                # create a pool of tokens to use in comparison
                # the pool is based on the abbreviation and string standardization style args
                this_pool, that_pool = pool_creator(this, that, abbreviated_first, abbreviated_middle, style)

                # run the pools against each other to determine if one pools tokens are entirely in the other
                if pool_runner(this_pool, that_pool):
                    # the pool runner is a bool for "Yes the tokens of one are in the other" or "No"
                    # richard b should not match richard b jones -- too ambiguous
                    # if the final regular token is one character, we will not consider the name variant
                    if this.token_length==2: 
                        if len(this.tokens_wo_suff[-1])==1:
                            # important to consider the suffixless form because "James Smith I" could improperly trigger this
                            continue
                        # this is 2 tokens, that is 3+ tokens
                        if that.token_length>2 and this.base_tokens[1] == that.base_tokens[1]:
                            # make sure the last name is not the middle of another 
                            # john thomas should not match john thomas coepenhaver
                            continue
                    # same logic but mirrored for the "that" name variant
                    if that.token_length==2:
                        if len(that.tokens_wo_suff[-1])==1:
                            continue
                        if this.token_length>2 and that.base_tokens[1] == this.base_tokens[1]:
                            continue
                    # if we did not continue above in exclusions, consider it a possible match
                    matches.append(that)

        # if there were presumed matches confirm there are no ambiguities
        if matches:
            # if the name variant we are considering matches for had a suffix, then reduce the matches to only names that had suffixes too
            # note if James Smith III and James Smith are actually the same (and the latter is colloquially written sans suffix)... then
            # ... during the James Smith III run, the match is voided, but it is reconsidered when the list is checking James Smith
            if len(matches)>1 and this.suffix:
                if any(m.suffix for m in matches):
                    matches =[m for m in matches if m.suffix]
            
            this.assess_ambiguity(matches,  f'Free TIT-{int(abbreviated_first)}{int(abbreviated_middle)}-{style}', 'Free [3]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches,  f'Free TIT-{int(abbreviated_first)}{int(abbreviated_middle)}-{style}', 'Free [3]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
    return nodes
        
def PIPE_Free_Vacuum_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by "vacuumed out middle tokens" standards
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """
    # we will only consider the nodes that remain eligible to be mapped to each other
    eligible_nodes = [N for N in nodes if N.eligible]
    
    # we begin iteration at the beginning of the list
    start = 0
    # the iterables are the whole list
    it_list = eligible_nodes[start:]

    while it_list:
        this = eligible_nodes[start] # object to compare
        those = eligible_nodes[start+1:] # objects to be compared to
        start+=1 # iterables
        it_list = eligible_nodes[start:] # iterables

        # only the eligible ones to compare (if an entity got mapped during an earlier iterated entity, we don't want to map to it)
        search = [o for o in those if o.eligible]
        matches = []

        if search and this.eligible:
            # function not equipped to handle single token entities
            if this.token_length==1:
                continue

            # create the "vacuumed" name
            # Christian John Rozolis becomes Christian Rozolis
            this_vacuumed = [this.tokens_wo_suff[0], this.tokens_wo_suff[-1]]

            for that in search:
                # don't handle single token entities, make sure this entity is still eligible
                if not this.eligible or that.token_length==1:
                    continue

                # for the other entity to compare, build the vacuumed name
                that_vacuumed = [that.tokens_wo_suff[0], that.tokens_wo_suff[-1]]

                # if the vacuumed names are similar
                if fuzz.ratio(this_vacuumed[0], that_vacuumed[0])>=85 and fuzz.ratio(this_vacuumed[1], that_vacuumed[1])>=85:
                    # if the names are both long
                    if this.token_length>=3 and that.token_length>=3:
                        # and the second tokens first letters dont match, VETO
                        # Christian Smith Vanhausen vs. Christian Mith Vanheurson
                        if this.tokens_wo_suff[1][0]!= that.tokens_wo_suff[1][0]:
                            continue
                        # or there is a suffix mismatch, VETO
                        if this.suffix and that.suffix and this.suffix!= that.suffix:
                            continue
                    # otherwise, consider as possible match
                    matches.append(that)

        # if possible matches were found                    
        if matches:
            this.assess_ambiguity(matches,  'Vacuum Search', 'Free [4]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches,  'Vacuum Search', 'Free [4]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))

    return nodes

def PIPE_Free_Token_Sort_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by token sort fuzzy standards
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """
    # we will only consider the nodes that remain eligible to be mapped to each other
    eligible_nodes = [N for N in nodes if N.eligible]
    
    # we begin iteration at the beginning of the list
    start = 0
    # the iterables are the whole list
    it_list = eligible_nodes[start:]
    while it_list:
        this = eligible_nodes[start] # object to be compared
        those = eligible_nodes[start+1:] # other objects to compare to
        start+=1 # iterables
        it_list = eligible_nodes[start:] # iterables

        # only the eligible ones to compare (if an entity got mapped during an earlier iterated entity, we don't want to map to it)
        search = [o for o in those if o.eligible]
        matches = []

        # if a search space remains, and this entity is eligible
        if search and this.eligible:
            # function not equipped to handle single-token entities, and we err on the side of caution and only consider
            # entities with 3 or more tokens
            if this.token_length<=2:
                continue
            # for every other entity, check it
            for that in search:
                # will only compare to eligible 3+ token entities
                # confirm in a prior loop that this entity was not mapped elsewhere, and that it remains eligible
                if not this.eligible or that.token_length<=2:
                    continue

                # fuzzy matching bound
                bound = 98

                # if their token sort ratios are strong matches, hooray
                if fuzz.token_sort_ratio(this.base_tokens, that.base_tokens) >=bound:

                    if this.tokens_wo_suff[0] == that.tokens_wo_suff[-1]+"s" or \
                        that.tokens_wo_suff[0] == this.tokens_wo_suff[-1]+"s":
                        # mark a roberts is not robert a marks (rolls eyes)
                        # john roberts, robert johns, etc.
                        # I cannot believe this problem exists
                        continue
                    # if they are both equally long, had a high match rate, had single character middle initials
                    # but their middle initials don't match, be cautious and dont match
                    # James M Smith != James T Smith
                    if this.token_length ==3 and that.token_length == 3:
                        if len(this.base_tokens[1])==1 and len(that.base_tokens[1])==1:
                            if this.base_tokens[1] != that.base_tokens[1]:
                                continue

                    # if the match was successful and not excluded via continuation above, consider it
                    matches.append(that)

        # if possible matches were found        
        if matches:
            this.assess_ambiguity(matches,  'Token Sorts', 'Free [5]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches,  'Token Sorts', 'Free [5]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
                    
    return nodes

def PIPE_Free_Van_Sweeps_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by the Germanic "Van" pattern standards
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """

    # build the list of known entities that qualify as "Van" based names
    vans = []
    # the entity may begin with the token van, or van as part of a longer name if the written form
    # did not maintain spacing
    # i.e. Van Kloet could have been written Vankloet. This pattern grabs both
    for each in [o for o in nodes if re.search(r'^van | van',o.name) and o.eligible]:
        # if the first token is not van we will attempt disambiguation for it in the van search
        # not too sure why I did this?? -- this function is meant for full names with Van in them
        # -- not for surname representations starting with van
        # i.e. YES to Gregory F Van Tatenhoven NO to Van Gundy
        # if "van"!= each.name.split()[0]:
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
    for this, newname in new_vans:
        if this.suffix:
            anchor = newname.split()[-2]
        else:
            anchor = newname.split()[-1]

        matches = []
        for old in old_vans:
            old_anchor = old.anchor
            # now comparing fuzed VAN names as the anchors
            if fuzz.ratio(anchor, old_anchor)>=90:
                matches.append(old)
        
        if matches:
            this.assess_ambiguity(matches,  'Van Names', 'Free [6]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not this.assess_ambiguity(matches,  'Van Names', 'Free [6]'):
            #     print('failure')
            #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
            #     for m in matches:
            #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))

    return nodes

def PIPE_Free_Initialisms_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by abstract initials standards
    i.e. Irene Patricia Murphy Kelly was frequently written as Irene M K
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """
    # develop a list of nodes that qualify to be considered in this style of matching
    matchy = []
    # check only those eligible
    for each in [o for o in nodes if o.eligible]:
        # if the name is:
        #    3 tokens long,
        #    doesnt have a suffix,
        #    the surname is one letter,
        #    the first name is multiple characters,
        #    and the middle initial is one letter
        # i.e. Irene Patricia Murphy Kelly was frequently written as Irene M K
        # then consider it for matching
        if each.token_length == 3 and\
            not each.suffix and \
            len(each.anchor)==1 and \
            len(each.base_tokens[0])>2 and \
            len(each.base_tokens[1])==1:
            matchy.append(each)

    # for all eligible to be matched
    for m in matchy:
        if not m.eligible:
            continue
        # compare against all long names
        matches = []

        for n in [o for o in nodes if o.eligible and o!=m and o.token_length>=3]:
            # if the first letters match across the board and the first names match
            if n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[1][0] == m.base_tokens[1][0] and\
                n.base_tokens[2][0] == m.base_tokens[2][0]:
                matches.append(n)
    
            # if the first names match and the offset tokens from a longer name match
            # i.e. Chris John Rozolis Stevens and Chris Rozolis Stevens
            if len(n.base_tokens)>3 and n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[2][0] == m.base_tokens[1][0] and\
                n.base_tokens[3][0] == m.base_tokens[2][0]:
                matches.append(n)

        # if we had names qualify as possible matches, assess ambiguity
        if matches:
            m.assess_ambiguity(matches,  'initialisms', 'Free [7]')
            ## BLOCK BELOW IS DEV TESTING BLOCK
            # if not m.assess_ambiguity(matches,  'initialisms', 'Free [7]'):
            #     print('failure')
            #     print((m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
            #     for oth in matches:
            #         print("--",(oth.name, oth.courts, oth.eligible, oth.NID, oth.BA_MAG_ID, oth.serial_id, oth.POINTS_TO_SID))
    
    return nodes

def PIPE_Free_Single_Letter_Names_Pool_Based(nodes: list):
    """Given a group of IntraMatch/FreeMatch objects, reduce them if their names match by single letter name standards
    i.e. a wallace tashima and atsushi wallace tashima are the same entity
    This function handles ambiguous entities: if there are multiple ground truth names across districts, reduction
    does not happen. (i.e. Patricia Sullivan in ORD is different from Patricia Sullivan in RID, an appearance of 
    Patricia Sullivan in ILND cannot be reduced to one or the other and remains ambiguous)

    Args:
        nodes (list): list of FreeMatch custom objects from JED_Classes

    Returns:
        list: list of the same objects that entered the function, with their parent/child connections updated
    """

    # only going to compare to long names and eligible names
    for this in [o for o in nodes if o.eligible and o.token_length>=2]:
        if not this.eligible:
            continue

        # specifically care if the first token is a single letter
        if len(this.base_tokens[0])==1:

            # compare against all other multi-tokened names
            matches = []
            for check in [o for o in nodes if o.eligible and o!=this and o.token_length>=2]:
                # if they have a decent token sort ratio AND the second token is an exact match, then they're good
                # i.e. a wallace tashima and atsushi wallace tashima
                if fuzz.token_sort_ratio(this.name,check.name)>80 and this.base_tokens[1]==check.base_tokens[1]:
                    matches.append(check)
                # if their dual abbreviation forms are a strong match
                # paul kinlock holmes iii and pk holmes iii match here
                elif fuzz.ratio(this.init_init_sur_suff, check.init_init_sur_suff)>=92 and this.anchor==check.anchor:
                    # if the second token in the words are both not abbreviated and dont equal each other, void the match
                    if len(this.base_tokens[1])>1 and len(check.base_tokens[1])>1 and this.base_tokens[1] != check.base_tokens[1]:
                        continue
                    matches.append(check)
            
            # assess the matches for ambiguities
            if matches:
                this.assess_ambiguity(matches,  'Single Letters', 'Free [8]')
                ## BLOCK BELOW IS DEV TESTING BLOCK
                # if not this.assess_ambiguity(matches,  'Single Letters', 'Free [8]'):
                #     print('failure')
                #     print((this.name, this.courts, this.eligible, this.NID, this.BA_MAG_ID, this.serial_id, this.POINTS_TO_SID))
                #     for m in matches:
                #         print("--",(m.name, m.courts, m.eligible, m.NID, m.BA_MAG_ID, m.serial_id, m.POINTS_TO_SID))
    
    return nodes
    

#######################################
## Anchor and Single Token Functions ##
#######################################

def PIPE_Anchor_Reduction_UCID(entity_list: list, pipeline_locale: str):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. The exceptions contain a few common typos that I solve for as well

    Args:
        entity_map (list): list of nodes in a ucid we are trying to reduce
        pipeline_locale (str): where in the disambiguation pipeline this is being called (for logging)

    Returns:
        list: the same list that entered, but the child objects in the lists may be updated and disambiguated
    """
    # print("\nPipe: Anchor Reduction within UCIDs")

    eligible_list = [o for o in entity_list if o.eligible]

    # objs = entities on the docket
    start = 0
    # start with all entities
    it_list = eligible_list[start:]
    # while we are still comparing all entities
    while it_list:
        # pick current judge
        this = eligible_list[start]
        # pick all other judges to compare to from the case, that havent been compared to it yet
        those = eligible_list[start+1:]
        
        # iterate our counters up, and the next list to iterate after we complete here.
        # the while loop exists when we go beyond the index length of the list
        start+=1
        it_list = eligible_list[start:]

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
                        # if it is just surnames, ensure they are close enough in size that this is a typo
                        # i.e. do not capture Cole in Coleman or Roberts in Robertson
                        diff = len(this.anchor)-len(that.anchor)
                        if diff >2 or diff <-2:
                            continue
                        # presumably good
                        #otherwise they match
                        this.choose_winner(that, f'Anchors-ucid-I [CB1]', pipeline_locale)
                        continue

                    # o for o connor
                    # basically if they matched in fuzzy, and one name was "o connor" see if the other name is oconnor
                    # same deal with J Mathison and Mathison where the J stands for Judge 
                    # Mc Donald, Van Geulen
                    possibilities=['jude','j','o', 'mc','van']
                    if len(this.base_tokens)==2 and this.base_tokens[0] in possibilities:
                        # attempt the remainder of the name after the botched prefix
                        thisname = " ".join(this.base_tokens[1:])
                        if fuzz.partial_ratio(thisname, that.name)>=92:
                            this.choose_winner(that, f'Anchors-ucid-I [CB2]', pipeline_locale)
                            continue
                    # flip logic on the other judge
                    if len(that.base_tokens)==2 and that.base_tokens[0] in possibilities:
                        thatname = " ".join(that.base_tokens[1:])
                        if fuzz.partial_ratio(this.name, thatname)>=92:
                            this.choose_winner(that, f'Anchors-ucid-I [CB3]', pipeline_locale)
                            continue

                    # if there is a mismatch in the first tokens confirm that it's not a partial name
                    # i.e. Amy j st eve matches st eves
                    if this.token_length >=2 and that.token_length >=2 and \
                        len(this.base_tokens[0])>1 and \
                        len(that.base_tokens[0])>1 and \
                        this.base_tokens[0]!=that.base_tokens[0]:

                        if fuzz.partial_ratio(this.name, that.name)>=98:
                            # presumably good (their similarity was above 98%)                                
                            this.choose_winner(that, f'Anchors-ucid-I [CB4]', pipeline_locale)
                            continue
    
    return entity_list

def PIPE_Anchor_Reduction_II_UCID(entity_list: list, pipeline_locale: str):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. This is a secondary implementation of disambiguation, stacked on top of the first layer
    of anchor disambiguation

    Args:
        entity_map (list): list of nodes in a ucid we are trying to reduce
        pipeline_locale (str): where in the disambiguation pipeline this is being called (for logging)

    Returns:
        list: the same list that entered, but the child objects in the lists may be updated and disambiguated
    """
    # print("\nPipe: Anchor Reduction II within UCIDs")
    eligible_list = [o for o in entity_list if o.eligible]

    start = 0
    # start with all entities
    it_list = eligible_list[start:]
    # while we are still comparing all entities
    while it_list:
        this = eligible_list[start] # current judge
        those = eligible_list[start+1:] # other judges to compare to

        start+=1 # update iterables
        it_list = eligible_list[start:] # update iterables

        # eligible search for disambiguation
        search = [o for o in those if o.eligible]
        if search and this.eligible:
            for that in search: # for other judges on the ucid
                # if the surnames match at 90% or more
                if fuzz.ratio(this.anchor, that.anchor)>=90:
                    # anchors >90% and tokens all above 98%
                    if fuzz.token_set_ratio(this.base_tokens, that.base_tokens) >=98:
                        this.choose_winner(that,f"Anchors-ucid-II [CB1]", pipeline_locale)
                        continue
                    # one of the entities was just a surname
                    if this.token_length==1 and that.token_length>1 or this.token_length>1 and that.token_length==1:
                        this.choose_winner(that,f"Anchors-ucid-II [CB2]", pipeline_locale)
                        continue
                # if one is a surname and the other is 2 tokens
                if this.token_length==2:
                    if len(this.base_tokens[0])==1 or len(this.base_tokens[-1])==1:
                        # try fuzing the 2-token name into one and see if there was a misc. letter
                        # i.e. "Gelpi" vs. "Gelp i"
                        this_alt_anchor = "".join(this.base_tokens)
                        if fuzz.ratio(this_alt_anchor, that.anchor)>=95:
                            this.choose_winner(that,f"Anchors-ucid-II [CB3]", pipeline_locale)                            
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
                        # now ensure no disjointed middle initial
                        # Karen J Williams and Karen M Williams
                        if len(this.tokens_wo_suff)==3 and len(that.tokens_wo_suff)==3 \
                            and this.tokens_wo_suff[1]!= that.tokens_wo_suff[1]:
                            continue
                        else:
                            this.choose_winner(that,f"Anchors-ucid-II [CB4]", pipeline_locale)                            
                            continue
                # compare if the longer entity had dual last names and the short entity matched one of them
                # basically: this = Smith Washington | that = Smith Washington Jones
                if this.token_length==2 and that.token_length>2:
                    if fuzz.ratio(this.tokens_wo_suff[0],that.tokens_wo_suff[-2])>=90 and \
                        fuzz.ratio(this.tokens_wo_suff[1],that.tokens_wo_suff[-1])>=90:
                        this.choose_winner(that,f"Anchors-ucid-II [CB5]", pipeline_locale)
                        continue
    return entity_list

def PIPE_Anchor_Reduction_III_UCID(entity_list: list, pipeline_locale: str):
    """Specialty disambiguation function that uses exceptions + anchors to map some entities to each other
    primarily by surname. This is a third implementation of disambiguation, stacked on top of the first and
    second layer of anchor disambiguation

    Args:
        entity_map (list): list of nodes in a ucid we are trying to reduce
        pipeline_locale (str): where in the disambiguation pipeline this is being called (for logging)

    Returns:
        list: the same list that entered, but the child objects in the lists may be updated and disambiguated
    """
    eligible_list = [o for o in entity_list if o.eligible]

    start = 0
    it_list = eligible_list[start:]
    while it_list:
        this = eligible_list[start] # object for comparison
        those = eligible_list[start+1:] # list of other objects for comparison

        start+=1 # update iterables
        it_list = eligible_list[start:] # update iterables

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
                            this.choose_winner(that, f"Anchors-ucid-III [CB1]", pipeline_locale)                                
                            continue
                        # if a mashed string form matches strongly, then it's probably misspelling or misc. letters junking up the match and its good
                        if fuzz.ratio("".join(this.base_tokens), "".join(that.base_tokens))>=95:
                            this.choose_winner(that, f"Anchors-ucid-III [CB2]", pipeline_locale)                                
                            continue
                    
                    # if the first token is a letter and it's "j" assume it stands for judge, or jude stands for judge
                    if (len(this.base_tokens[0])==1 and this.base_tokens[0]=='j') or this.base_tokens[0]=='jude':
                        # assume this entity is a single surname then and attempt matching without the j or jude
                        this_alt_anchor = this.base_tokens[1]
                        if fuzz.ratio(this_alt_anchor, that.anchor)>=92:
                            this.choose_winner(that, f"Anchors-ucid-III [CB3]", pipeline_locale)                                
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
                        this.choose_winner(that, f"Anchors-ucid-III [CB4]", pipeline_locale)
                            
    return entity_list

def PIPE_Anchor_Reduction_Court(court_long: list, court_short: list, court: str):
    """Special function to work anchor matching (matching by surname only) into the court level disambiguation

    Args:
        court_map_long (list): list of entity objects whose names are multi-tokened
        court_map_single (list): list of entity objects whose names are single-tokened
        court (str): which court in the disambiguation pipeline this is being called (for logging)

    Returns:
        list: list of entity objects reduced in the court
    """

    # only check what is eligible at this point
    longs = [o for o in court_long if o.eligible]
    singles = [o for o in court_short if o.eligible]

    # for every uni-token entity
    for each in singles:
        # init matching nothing
        matches = []
        # compare to every long name
        for l in longs:
            # possessive quickcheck:
            if each.anchor == f"{l.anchor}s" or l.anchor == f"{each.anchor}s":
                matches.append(l)
        
            # if the characters are slightly typoed, this catches them
            elif fuzz.ratio(each.name, l.anchor)>=80 and Counter(each.name) == Counter(l.anchor):
                matches.append(l)
        
        # if matches were found, confirm there were no ambiguous surname mappings
        if matches:
            each.assess_ambiguity(matches, "court_layer_crossover",court)

    # pool the entity lists into one large list    
    final_map = court_long+court_short

    return final_map

##############################
## Fuzzy Matching Functions ##
##############################

#------------#
# UCID/COURT #
#------------#

def PIPE_Fuzzy_Matching(entity_list: list, pipeline_locale: str):
    """fuzzy matching to be used for intra-ucid or intra-court disambiguation

    Args:
        entity_map (list): list of objects per ucid or court that will be reduced onto each other
        pipeline_locale (str): where in the disambiguation pipeline this is being called (for logging)

    Returns:
        list: same list that entered the function, but the child objects will be updated in some cases after reduction
    """
    # print("\nPipe: Fuzzy Matching")
    
    eligible_list = [o for o in entity_list if o.eligible]

    start = 0 # iterables
    it_list = eligible_list[start:] # iterables
    # comparison is bidirectional such that A compared to B is equivalent to B compared to A
    # when we enumerate out the list of comparisons it is effectively like: [A, B, C, D]
    # --> AB, AC, AD, BC, BD, CD
    while it_list: # go until we reach the end of the list
        this = eligible_list[start] # entity to compare
        those = eligible_list[start+1:] # other entities we compare to
        start+=1 # iterables
        it_list = eligible_list[start:] # the next loop

        # only want eligible entities. Eligible means another entity can point to it and be disambiguated to it and this entity does not point elsewhere
        search = [o for o in those if o.eligible]
        if search: # if there are eligible ones
            for that in search:
                # if each entity appeared on more than 20 ucids OR
                # one of the entities appeared much more frequently than the other
                # loosen the bound a bit, more common occurrences == more leeway for typos
                bound = 93
                if this.n_ucids and that.n_ucids:
                    if (this.n_ucids/that.n_ucids)>20 or (that.n_ucids/this.n_ucids)>20:
                        bound = 90  

                # if they fuzzy matched, hooray
                if fuzz.ratio(this.name, that.name) >=bound:
                    # object routine to reduce the objects with each other
                    this.choose_winner(that, "UCIDFuzzy", pipeline_locale)

    return entity_list


##################################
## Party/Counsel Fuzzy Matching ##
##################################

def UCID_PIPE_Drop_Parties(ucid_map: dict, parties_df: pd.DataFrame, counsels_df: pd.DataFrame):
    """Function used in the first round of large-scale disambiguation. We use this to omit any party or counsel names that
    the spacy model mistook to be judges. We do this using the known party and counsel names from header metadata.

    Args:
        ucid_map (dict): key = ucid, values = list of entities on the ucid
        parties_df (pandas.DataFrame): consists of the parties per case
        counsels_df (pandas.DataFrame): consists of the counsels per case
    Return:
        dict, dict: one dictionary is the one containing the entities we want to advance to disambiguation, the other is a log of which entities we threw out
    """
    
    def _check_if_party(entity_obj: JCL.UCIDMatch, party_names: list):
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
            new_map[ucid] = entities
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
                # note these are strings and not the actual objects themselves
                tbL = tokey_b[-2] if tokey_b[-1] in JG.suffixes_titles else tokey_b[-1]     
                # if the surnames dont match, move on fast
                if ta!=tbL:
                    continue  
                else:
                    # if the surnames match, its good
                    return True
            # vice versa
            if len(tokey_b)==1 and len(tokey_a)>1:
                tb = tokey_b[0]
                taL = tokey_a[-2] if tokey_a[-1] in JG.suffixes_titles else tokey_a[-1]
                if tb!=taL:
                    continue   
                else:
                    # if the surnames match, its good
                    return True
            # if you made it here, call the sub function
            if tokens_in_tokens_sub_function_caller(tokey_a, tokey_b):
                return True
    return False
    
def pool_creator(this: JCL.IntraMatch, that: JCL.IntraMatch, abbreviated_first: bool, abbreviated_middle: bool, style: str):
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

def PIPE_Tokens_in_Tokens(entity_list: list, pipeline_locale: str,
                            abbreviated_first: bool = False, 
                            abbreviated_middle: bool = False,
                            style: str = 'Plain'):
    """High volume function called in UCID and COURT level disambiguation. This is used to determine by ucid
    if any of the entities are tokenized subsets of the other entities tokens
    Example: St Eve is a tokenized subset of Amy Joan St Eve
    Amy J St Eve is an abbreviated tokenized subset of A J St Eve

    Args:
        entity_map (list): list of entity objects in that key-based group
        pipeline_locale (str): where in the disambiguation pipeline this is being called (for logging)
        abbreviated_first (bool, optional): should we abbreviate the first token in this run. Defaults to False.
        abbreviated_middle (bool, optional): should we abbreviate the middle token[s] in this run. Defaults to False.
        style (str, optional): How should we build and compare entitiy names (as is plain, nicknames, universal spellings). Defaults to 'Plain'.

    Returns:
        list: list of entity objects in that key-based group that have been reduced
    """

    # for every ucid or court
    eligible_list = [o for o in entity_list if o.eligible]

    start = 0 # iterables
    it_list = eligible_list[start:] # iterables
    while it_list:
        this = eligible_list[start] # object to compare
        those = eligible_list[start+1:] # other objects to compare to
        
        start+=1 # iterables
        it_list = eligible_list[start:] # iterables

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
                    this.choose_winner(that, f'TIT-{int(abbreviated_first)}{int(abbreviated_middle)}-{style}', pipeline_locale)

    return entity_list

def PIPE_UCID_COURT_INITIALISMS(entity_list_long: list, court: str):
    """Given a list of multi-token names in a court, determine if they match to each other
    by initials based matching patterns. No ambiguity is presumed at this step

    Args:
        entity_list_long (list): list of IntraMatch objects in a ucid/court we are trying to reduce
        court (str): used for logging to indicate where in the pipeline the matching is happening

    Returns:
        list: same list that entered the function, but the object attributes are updated to point to each other
    """

    # now inside of a court
    # empty list of possible nodes to match
    matchy = []
    # check only those eligible
    for each in [o for o in entity_list_long if o.eligible]:
        # if the name is:
        #    3 tokens long,
        #    doesnt have a suffix,
        #    the surname is one letter,
        #    the first name is multiple characters,
        #    and the middle initial is one letter
        # i.e. Irene Patricia Murphy Kelly was frequently written as Irene M K
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
        for n in [o for o in entity_list_long if o.eligible and o!=m and o.token_length>=3]:
            # if the first letters match across the board and the first names match
            if n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[1][0] == m.base_tokens[1][0] and\
                n.base_tokens[2][0] == m.base_tokens[2][0]:
                m.choose_winner(n, "Initialisms Styling, new rules", court)
                # print(f"NEW STYLE: {m.name} -- {n.name} {court}") # dev messaging
            # if the first names match and the offset tokens from a longer name match
            # i.e. Chris John Rozolis Stevens and Chris Rozolis Stevens
            elif len(n.base_tokens)>3 and n.base_tokens[0] == m.base_tokens[0] and \
                n.base_tokens[2][0] == m.base_tokens[1][0] and\
                n.base_tokens[3][0] == m.base_tokens[2][0]:
                m.choose_winner(n, "Initialisms", court)
                # print(f"NEW STYLE: {m.name} -- {n.name} {court}") # dev messaging

    return entity_list_long

def PIPE_COURT_Anchors_Self_Reduction(entity_list_short: list, court: str):
    """Given a list of single tokens within a court, reduce them among each other for synonymous
    entities. This works for possessive vs. singular entities (i.e. Smith and Smiths match). It
    is okay if they are different Smiths because we can never allocate them to the correct Smith, and would
    all be mapped as ambiguous through the same core entity denoting them as "Smith"

    Args:
        entity_list_short (list): list of IntraMatch objects in a ucid/court we are trying to reduce
        court (str): used for logging to indicate where in the pipeline the matching is happening

    Returns:
        list: same list that entered the function, but the object attributes are updated to point to each other
    """
    # only consider those entities that remained eligible upon entering this function
    eligible_list = [o for o in entity_list_short if o.eligible]
    
    start = 0 # iterables
    it_list = eligible_list[start:] # iterables
    while it_list:
        this = eligible_list[start] # object to compare
        those = eligible_list[start+1:] # other objects to compare to
        
        start+=1 # iterables
        it_list = eligible_list[start:] # iterables

        # only want eligible objects
        search = [o for o in those if o.eligible]
        if search and this.eligible:
            for that in search:
                # possessive quickcheck:
                if this.anchor == f"{that.anchor}s" or that.anchor == f"{this.anchor}s":
                    this.choose_winner(that, f'Anchor Self-Reduction in Court Matching', court)
                
                # surnames have lower threshold as they are single tokens
                if fuzz.ratio(this.name, that.name)>80 and Counter(this.name) == Counter(that.name):
                    this.choose_winner(that, f'Anchor Self-Reduction in Court Matching', court)
        
    return entity_list_short



#########################################################
# Helper that calls the tokens in tokens check in both 
# directions for lists (list a in b or list b in a)
#########################################################
def tokens_in_tokens_sub_function_caller(tokens_a: list, tokens_b: list):
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


def tokens_in_tokens_sub_function(tokens_1: list, tokens_2: list):
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

############################
#### DEPRECATED ALGORITHM ##
############################
def tokens_in_string_algo(tokens: list, string_check: str):
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
