import JED_Globals_public as JG

def build_inferred_tokens(tokens_raw: list):
    """ Function to pre-build various tokenizations of a string name. The tokenizations are abbreviations and suffix dropping

    Args:
        tokens_raw (list): list of strings, the originating name split on spaces

    Returns:
        dict: returns a dictionary with 2 layers of Bools: abbreviate the first initial, abbreviate the middle initial args
    """

    # -- 2 T/F for abbreviating the first initial
    # -- 2 T/F for abbreviating the middle initials of a name. In the instance a name has 2 middle names, we can abbreviate one or both in tandem
    
    return {
            # first layer is should we abbreviate the first initial
            # second layer is should we abbreviate the second initial
            True: {# abbreviate first initial
                True: build_entity_tokens(tokens_raw, abbreviated_first = True, abbreviated_middle= True, keep_suffix = True),
                False: build_entity_tokens(tokens_raw, abbreviated_first = True, abbreviated_middle= False, keep_suffix = True)
                },
            False: {# don't abbreviate first initial
                True: build_entity_tokens(tokens_raw, abbreviated_first = False, abbreviated_middle= True, keep_suffix = True),
                False: build_entity_tokens(tokens_raw, abbreviated_first = False, abbreviated_middle= False, keep_suffix = True)
                } 
        }

############################
############################
## Token Inference
############################
############################

def build_entity_tokens(token_list: list, abbreviated_first: bool = False, abbreviated_middle: bool = False, keep_suffix: bool = False):
    """ Inference routine used to build a "last name" and "middle initial" when name mapping

    Args:
        token_list (list): list of tokens (entity string split on " ")
        abbreviated_first (bool, optional): if I should return a first initial or first token. Defaults to False.
        abbreviated_middle (bool, optional): if I should return a middle initial or middle token. Defaults to False.
        keep_suffix (bool, optional): if I should keep any suffixes like sr, jr, iii in the name, or drop them. Defaults to False

    Returns:
        list: list of lists containing "tokens" if routine is successful
    """

    # init my empty set
    token_set = []
    # every entity has at least one first token
    first = token_list[0]
    # set the abbreviated form in case I need it
    abbr_f = first[0]


    # if the final token is in fact a suffix, let's identify it
    if token_list[-1] in JG.suffixes_titles:
        # found it
        the_suffix = [token_list[-1]]
        # yes this token list has a special suffix
        has_suffix = True
    else:
        # else, it doesn't exist
        the_suffix = []
        has_suffix = False
    
    # if the function is called with keep_suffix as false, regardless if we found it, ditch it
    if not keep_suffix: # do not keep
        # even if we found one, we wouldnt keep it so we end up appending this empy list
        the_suffix = []

    # this is going to be the list of token lists we will iterate through and build our abbreviated/edited tokens from
    all_token_lists =[token_list]
    # most of the time, this list above is just the original base tokens, however the hyphen control below will add to it...

    # if there is a hyphen in the string for this unedited name, we want to add a de-hyphenated form of the tokens as well as a backup check
    # John Henry-Adams would have been added above as the baseline, then we also add John Henry Adams too
    if "-" in " ".join(token_list):
        de_hyphenated = " ".join(token_list)
        de_hyphenated = de_hyphenated.replace("-"," ").replace("  "," ").strip().split(" ")
        de_hyphenated = [tok for tok in de_hyphenated if tok!= '']
        all_token_lists.append(de_hyphenated)
    
    # for every token option (hyphenated + non) we want to add possible token sets that this entity could look like when abbreviated or not
    for token_list in all_token_lists:
        # if it's 1 token, all we will do is just add the token and move on.
        # even if this function was called with abbreviation on the first token, making Adams --> A is useless in matching. "A" is going to appear everywhere in other names
        if len(token_list) == 1:
            token_set.append([first])

        # if it is a last name-suffix token like Adams Jr.
        elif len(token_list) == 2 and has_suffix:
            # Adams Jr.
            if keep_suffix:
                token_set.append([first] + the_suffix)
            else:
                # just Adams
                token_set.append([first])

        # normal 2-token name like John Adams
        elif len(token_list) == 2:
            #there is no suffix
            # John Adams
            if abbreviated_first:
                # j adams
                token_set.append([abbr_f, token_list[1]])
            else:
                # original john adams
                token_set.append(token_list)

        elif len(token_list) == 3 and has_suffix:
            # pretty much assuming we won't get John [St Adams Jr]
            # John Adams Jr.
            if keep_suffix and abbreviated_first:
                # j adams jr or j adams
                token_set.append([abbr_f, token_list[1]] + the_suffix)
            else:
                # john adams jr or john adams
                token_set.append([first, token_list[1]] + the_suffix)
        
        elif len(token_list) == 3:
            # there is no suffix, we know this will botch Amy St Eve (assumes st is a middle name here)
            # John Michael Adams
            mid = token_list[1]
            abbr_m = mid[0]
            last = token_list[2]
 
            if abbreviated_first and abbreviated_middle:
                # j m adams
                token_set.append([abbr_f, abbr_m, last])
            elif abbreviated_middle:
                # john m adams
                token_set.append([first, abbr_m, last])
            elif abbreviated_first:
                # j michael adams
                token_set.append([abbr_f, mid, last])
            else:
                # john michael adams
                token_set.append([first, mid, last])

        elif len(token_list) == 4 and has_suffix:
            # john michael adams jr
            mid = token_list[1]
            abbr_m = mid[0]
            last = token_list[2]
            if abbreviated_middle and abbreviated_first:
                # j m adams [jr]
                token_set.append([abbr_f, abbr_m, last] + the_suffix)
            elif abbreviated_middle:
                # john m adams [jr]
                token_set.append([first, abbr_m, last] + the_suffix)
            elif abbreviated_first:
                # j michael adams [jr]
                token_set.append([abbr_f, mid, last] + the_suffix)
            else:
                # john michael adams [jr]
                token_set.append([first, mid, last] + the_suffix)

        elif len(token_list) >= 4:
            if has_suffix:
                last_ind = -1
            else:
                last_ind = 999

            # either 4 names or 5 names or 4names+suffix
            # john michael james adams
            # john michael edward james adams
            # john michael james adams jr
            mid_1 = token_list[1]
            mid_2 = token_list[2]
            abbr_m1 = mid_1[0]
            abbr_m2 = mid_2[0]
            last = token_list[3:last_ind]

            if type(last) is not list:
                last = [last]

            if abbreviated_first and abbreviated_middle:
                # j m j adams
                # j michael j adams
                # j m james adams
                token_set.append([abbr_f, abbr_m1, abbr_m2] + last + the_suffix)
                token_set.append([abbr_f, mid_1, abbr_m2] + last + the_suffix)
                token_set.append([abbr_f, abbr_m1, mid_2] + last + the_suffix)

            elif abbreviated_middle:
                # john m j adams
                # john michael j adams
                # john m james adams
                token_set.append([first, abbr_m1, abbr_m2]+ last + the_suffix)
                token_set.append([first, mid_1, abbr_m2]+ last + the_suffix)
                token_set.append([first, abbr_m1, mid_2]+ last + the_suffix)
            
            elif abbreviated_first:
                # j michael adams
                token_set.append([abbr_f, mid_1, mid_2]+ last + the_suffix)
            else:
                # john michael adams
                token_set.append([first, mid_1, mid_2]+ last + the_suffix)

    return token_set


def build_nicknames_and_unified(tokens_set: list):
    """ Helper Function that takes a list of lists of tokens and converts that list into a nickname or unified name last
    Example: [[Catherine, Samantha, Zeta-Jones]] --> [[Cathy, Sam, Zeta-Jones], [Catherine, Sam, Zeta-Jones], [Cathy, Samantha, Zeta-Jones]]

    Args:
        tokens_set (list): list of lists, the sub-lists are tokenizations of a string name

    Returns:
        list: list of sub-lists converted for JG.nicknames or unified names, if none exist, it's the base token lists returned
    """
    # init runners for what we will return
    nickys = []
    unis = []
    # for each token list
    for each in tokens_set:
        # there should be at least one token in the list, identify the nickname for it if it exists, else just the normal name
        # this is a list of JG.nicknames
        # if the token set is 2 tokens
        if len(each) == 2:            
            known_nicks_0 = JG.nicknames[each[0]] if each[0] in JG.nicknames else []
            # for every known nickname for the first token, add the nickname + last token as tokens
            for nn in known_nicks_0:
                nickys.append([nn, each[1]])
        # if the length is 3
        elif len(each) == 3:
            # try and grab JG.nicknames for the "middle name" token            
            known_nicks_0 = JG.nicknames[each[0]] if each[0] in JG.nicknames else []
            known_nicks_1 = JG.nicknames[each[1]] if each[1] in JG.nicknames else []
            # add all combinations of JG.nicknames

            if len(known_nicks_0) == 0 and len(known_nicks_1) == 0:
                continue
            elif len(known_nicks_0) > 0 and len(known_nicks_1) == 0:
                known_nicks_1 = [each[1]]
                for nn in known_nicks_0:
                    nickys.append([nn, each[1],each[2]])

            elif len(known_nicks_0) == 0 and len(known_nicks_1) > 0:
                for nn1 in known_nicks_1:
                    nickys.append([each[0], nn1, each[2]])
            else: # both have JG.nicknames
                for nn in known_nicks_0:
                    nickys.append([nn, each[1],each[2]])
                    for nn1 in known_nicks_1:
                        nickys.append([each[0],nn1,each[2]])
                        nickys.append([nn,nn1,each[2]])                   
      
        # if there are 4+ tokens,
        elif len(each) > 3:
            # get additional JG.nicknames in case there are "2 middle names"
            known_nicks_0 = JG.nicknames[each[0]] if each[0] in JG.nicknames else []
            known_nicks_1 = JG.nicknames[each[1]] if each[1] in JG.nicknames else []
            known_nicks_2 = JG.nicknames[each[2]] if each[2] in JG.nicknames else []
            # need the rest of the tokens to be a list, if they are not, convert them
            if type(each[3:]) is not list:
                fin = list(each[3:])
            else:
                fin = each[3:]

            # create every combination of different JG.nicknames -- this is a 2x2x2 control block that needs all specifications

            if len(known_nicks_0) == 0 and len(known_nicks_1) == 0 and len(known_nicks_2) == 0:
                continue
            elif len(known_nicks_0) == 0 and len(known_nicks_1) == 0 and len(known_nicks_2) > 0:
                for nn2 in known_nicks_2:
                    nickys.append([each[0], each[1], nn2] + fin)
            elif len(known_nicks_0) == 0 and len(known_nicks_1) > 0 and len(known_nicks_2) == 0:
                for nn1 in known_nicks_1:
                    nickys.append([each[0], nn1, each[2]] + fin)
            elif len(known_nicks_0) == 0 and len(known_nicks_1) > 0 and len(known_nicks_2) > 0:
                for nn1 in known_nicks_1:
                    nickys.append([each[0], nn1 ,each[2]] + fin)
                    for nn2 in known_nicks_2:
                        nickys.append([each[0], each[1], nn2] + fin)
                        nickys.append([each[0], nn1, nn2] + fin)
            elif len(known_nicks_0) > 0 and len(known_nicks_1) == 0 and len(known_nicks_2) == 0:
                for nn in known_nicks_0:
                    nickys.append([nn, each[1], each[2]]+fin)
            elif len(known_nicks_0) > 0 and len(known_nicks_1) > 0 and len(known_nicks_2) == 0:
                for nn in known_nicks_0:
                    nickys.append([nn, each[1],each[2]] + fin)
                    for nn1 in known_nicks_1:
                        nickys.append([each[0],nn1,each[2]] + fin)
                        nickys.append([nn,nn1,each[2]] + fin)   
            elif len(known_nicks_0) > 0 and len(known_nicks_1) == 0 and len(known_nicks_2) > 0:
                for nn in known_nicks_0:
                    nickys.append([nn, each[1],each[2]] + fin)
                    for nn2 in known_nicks_2:
                        nickys.append([each[0],each[1],nn2] + fin)
                        nickys.append([nn,each[1],nn2] + fin)   
            else: # everyone has JG.nicknames (thats wild)
                # James Michael Henry Adams --> Jim Mike Hank Adams + Jimmy Mike Hank Adams + Jim Mikey Hank Adams + Jimmy Mikey Hank Adams
                for nn in known_nicks_0:
                    nickys.append([nn,each[1],each[2]] + fin)
                    for nn1 in known_nicks_1:
                        nickys.append([each[0],nn1,each[2]] + fin)
                        nickys.append([nn,nn1,each[2]] + fin)
                        
                        for nn2 in known_nicks_2:
                            nickys.append([each[0],each[1],nn2] + fin)
                            nickys.append([each[0],nn1,nn2] + fin)
                            nickys.append([nn,each[1],nn2] + fin)

                            nickys.append([nn,nn1,nn2] + fin)

        # we just try and unify every token if possible for the unifier
        # make all Catharines, Catherines, Katherines, Katharines --> Catherine, etc.
        if any(tok in JG.name_unifier for tok in each):
            unis.append([JG.name_unifier[tok] if tok in JG.name_unifier else tok for tok in each])
    if len(nickys) == 0:
        nickys.append([])
    if len(unis) ==0:
        unis.append([])
    return nickys, unis