
import JED_Utilities_public as JU
import JED_Helpers_public as JH
import JED_Globals_public as JG

class IntraMatch(object):
    """Parent class used for disambiguation. The class represents an entity string with meta-information related to it

    Args:
        object (obj): Python object representation to be used in disambiguation node pools
    """
    def __init__(self,  cleaned_name, n_ucids):
        """Initialize the object

        Args:
            cleaned_name (str): entity string to be used in disambiguation
            n_ucids (int): number of unique ucids per grouping factor (in a court, or overall) that the entity appears on
        """
        # grab the logging function in the global space, should be instantiated by the main function
        self.log = JU.log_message #logging.getLogger()

        # cleaned name string
        self.name = cleaned_name
        # number of ucids the entity appeared on
        self.n_ucids = n_ucids

        # split the name on simple whitespace
        self.base_tokens = cleaned_name.split()
        # specialty function that builds initialed forms of the name (i.e. John Robert Smith --> J R Smith, John R Smith, J Robert Smith)
        self.inferred_tokens = JH.build_inferred_tokens(self.base_tokens)

        # init blank dicts for nicknames and universal spellings
        self.nicknames_tokens =  {True:{}, False:{}}
        self.unified_names_tokens = {True:{}, False:{}}
        for fi in [True, False]:
            for mi in [True, False]:
                # if the base tokens can be cast to nicknames or universal spellings, make them both as plain and also abbreviated forms
                self.nicknames_tokens[fi][mi],self.unified_names_tokens[fi][mi] = JH.build_nicknames_and_unified(self.inferred_tokens[fi][mi])

        # all nodes start as eligible to be mapped to, pointing to themselevs, and have no children
        self.eligible = True
        self.POINTS_TO = '>>SELF<<'
        self.children = []

        # helpful attribute constantly checked
        self.token_length = len(self.base_tokens)

        # determine if this is a "jr", "sr",etc. name, if so find the suffix
        # also build the initials of the judge entity
        if self.base_tokens[-1] in JG.suffixes_titles:
            self.suffix = self.base_tokens[-1]
            if self.token_length==1:
                self.anchor = None
            else:
                self.anchor = self.base_tokens[-2]
            self.initials_wo_suff = [tok[0] for tok in self.base_tokens[0:-1]]
            self.tokens_wo_suff = [tok for tok in self.base_tokens[0:-1]]
        else:
            self.suffix=None
            self.anchor = self.base_tokens[-1]
            self.initials_wo_suff = [tok[0] for tok in self.base_tokens]
            self.tokens_wo_suff = self.base_tokens
        
        
        self.initials_w_suff = [tok[0] for tok in self.base_tokens]

    def adopt_children(self, other, method, where):
        """method used to assign another entity node to this node as the parent entity

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
            method (str): description of the matching method that led to this disambiguation
            where (str): ucid or court in which the disambiguation occurred
        """

        # log the disambiguation
        self.log(f"{where:25} | {method:22} |{other.name:25} --> {self.name:25}")#.info(f"{where:25} | {method:22} |{other.name:25} --> {self.name:25}")

        # add the child node to a list of children
        self.children.append(other)
        # if the other node already had children disambiguated to it, take those as well
        self.children+=other.children

        # for each child now belonging to this node, make sure it points to this entity
        for each in self.children:
            each.points_to(self)

    def choose_winner_ucids(self, other, method, where):
        """when given this object and another that matches to it. Determine which should be the "parent" based on ucid and entity length

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
            method (str): description of the matching method that led to this disambiguation
            where (str): ucid or court in which the disambiguation occurred
        """
        # if one of the entities is a single token and the other is not, choose the multi-token entity
        # when a winner is determined, call the "Adopt_children" method for the winner, passing the loser as the argument
        if self.token_length ==1 and other.token_length >1:
            winner = other
            loser = self                
            winner.adopt_children(loser, method, where)

        elif self.token_length >1 and other.token_length ==1:
            winner = self
            loser = other                
            winner.adopt_children(loser, method, where)

        # if both are multi-token entities, determine winner by number of unique ucid appearances for the entity
        # rationale: the more frequently a name is written, the more likely it is to be the "true" spelling
        else:       
            # get the ucid counts to determine the max     
            U_comp = set([self.n_ucids, other.n_ucids])
            # if the counts are different, choose the higher count
            if len(U_comp)>1:
                UMax = max(U_comp)
                winner = [o for o in [self, other] if o.n_ucids == UMax][0]
                loser = [o for o in [self, other] if o != winner][0]                   
                winner.adopt_children(loser, method, where)
            else:
                # equal number of ucids
                # choose by token length, then by character length
                # if they tie, we take the 0th index anyways
                sorty = sorted([self,other], key=lambda obj: (len(obj.base_tokens), len(obj.name)), reverse = True)
                winner = sorty[0]
                loser = sorty[1]
                winner.adopt_children(loser, method, where)
        return

    def points_to(self,other):
        """update this object to point to another object (meaning this object is the child of the other object)

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
        """
        # if pointing to another object, this one is no longer eligible to be another nodes parent
        self.POINTS_TO = other.name
        self.eligible = False
        # if it matched onto another one, it's children would have been transferred before this function is called
        # therefore setting this to empty is safe and won't be tossing data
        self.children = []
        
    def print_results(self, other, method):
        """ helper method to print to console when one node absorbs another

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
            method (str): description of the matching method that led to this disambiguation
        """
        print(f"{self.name} -- absorbed the following with {method}")
        
        print(f">>{other.name} -- and the following priors")
        for each in other.children:
            print(f">>\t--{each.name}")

class UCIDMatch(IntraMatch):
    """Derivative objects used in intra-ucid disambiguation

    Args:
        IntraMatch (obj): parent class
    """
    def __init__(self,  name, ucid, n_ucids, was_header):
        """init method to build the nodes

        Args:
            name (str): entity name
            ucid (str): ucid the disambiguation is happening on
            n_ucids (int): number of unique ucids this particular entity string appeared on in its court
            was_header (bool): bool for the source of the entity -- docket entry or header
        """
        self.ucid = ucid
        self.was_header= was_header
        super().__init__(name, n_ucids)

class CourtMatch(IntraMatch):
    """Derivative objects used in intra-court disambiguation

    Args:
        IntraMatch (obj): parent class
    """
    def __init__(self,  name, court, n_ucids):
        """init method to build nodes

        Args:
            name (str): entity name
            court (str): court the disambiguation is happening in
            n_ucids (int): number of unique ucids this particular entity string appeared on in its court
        """
        self.court = court
        super().__init__(name, n_ucids)

class FreeMatch(IntraMatch):
    """Derivative objects used in free-form disambiguation

    Args:
        IntraMatch (obj): parent class
    """
    def __init__(self,  name, n_ucids, courts =[], FJC_Info = None, SJID = "Inconclusive"):
        """init method for freeform disambiguation nodes

        Args:
            name (str): entity name
            n_ucids (int): number of unique ucids this particular entity string appeared on in total
            courts (list, optional): courts the exact spelling of the entity appeared in. Defaults to [].
            FJC_Info (dict, optional): if this node is being instantiated using FJC_Info, it is in this dict and should be unpacked to object attributes. Defaults to None.
            SJID (str, optional): if this is a second run of disambiguation and this node already had an SJID, use it when creating the object, otherwise leave as inconclusive
        """
        # unpack the FJC Data if it exists for this entity
        if FJC_Info:
            self.is_FJC = True
            self.Full_Name = FJC_Info["Full_Name"]
            self.NID = FJC_Info["NID"]
            self.Earliest_Commission = FJC_Info["Earliest_Commission"]
            self.Latest_Termination = FJC_Info["Latest_Termination"]
            self.courts = []
        else:
            self.is_FJC = False
            self.Full_Name = None
            self.NID = None
            self.Earliest_Commission = None
            self.Latest_Termination = None
            self.courts = courts
        
        if SJID == "Inconclusive":
            self.has_SJID = False
            self.SJID = None
        else:
            self.has_SJID = True
            self.SJID = SJID
        
        super().__init__(name, n_ucids)
        
    def set_SJID(self, new_id):
        """Internal setter method for the SJID, assumes this will be a known SJID and not "inconclusive"

        Args:
            new_id (str): The SJID to assign to this object
        """
        # change both attributes
        self.has_SJID = True
        self.SJID= new_id
        return
    
    def set_NID(self, new_id):
        """Internal setter method for the NID if the judge is an FJC judge. Assumed the NID is correct and that the entity has already been checked as an FJC Article III Judge

        Args:
            new_id (str): the FJC known NID for the judge entity
        """
        self.NID = new_id


    def choose_winner(self, other, method):
        """method used to compare 2 entities that matched to each other. Comparison determines which entity should be the "parent"

        Args:
            other (obj): other freematch node that matched this one
            method (str): where in disambiguation did these two nodes match each other
        """

        # if 2 entities each have an SJID, they cannot match to each other. From a disambiguation routine process -- this could happen if they are the same name, but different people, OR if they are jr/sr OR extremely close names
        if self.has_SJID and other.has_SJID:
            # this is bad and they should not match. RIP
            return
        # if one has an SJID and the other does not
        elif self.has_SJID and not other.has_SJID:
            # if both are FJC judges, this shouldnt happen
            if self.is_FJC and other.is_FJC:
                # this seems bad, shouldnt happen
                return
            # if one is an FJC judge and the other isn't, we want the FJC judge to win (i.e. was an SJID on a magistrate, but now turned FJC judge --> same entity, defer to FJC information node)
            elif self.is_FJC and not other.is_FJC:
                winner = self
                loser = other
                winner.free_adopt_children(loser, method, "Free")
            # vice versa
            elif not self.is_FJC and other.is_FJC:
                winner = other
                loser = self
                winner.free_adopt_children(loser, method, "Free")
            else:
                winner = self
                loser = other
                winner.free_adopt_children(loser, method, "Free")
        elif not self.has_SJID and other.has_SJID:
            if self.is_FJC and other.is_FJC:
                # this seems bad, shouldnt happen
                return
            elif self.is_FJC and not other.is_FJC:
                winner = self
                loser = other
                winner.free_adopt_children(loser, method, "Free")
            elif not self.is_FJC and other.is_FJC:
                winner = other
                loser = self
                winner.free_adopt_children(loser, method, "Free")
            else:
                winner = other
                loser = self
                winner.free_adopt_children(loser, method, "Free")
        elif self.is_FJC and other.is_FJC:
            # VERY BAD
            # 2 distinct fjc nodes should not be able to map to each other, if they do just return and don't map them
            # this could happen if a father maps to a son (john smith jr is similar to john smith sr)
            if method=='Abrams Patch':
                winner = self
                loser = other
                winner.free_adopt_children(loser, method, "Free")
            else:
                self.log(f"WARNING: Distinct Entities will not be merged [{method}]: {self.Full_Name} | {other.Full_Name}")
                # print("oh no!")
            return
        # if one entity is an FJC known judge and the other is not, the FJC judge wins
        elif self.is_FJC and not other.is_FJC:
            winner = self
            loser = other
            winner.free_adopt_children(loser, method, "Free")
        elif not self.is_FJC and other.is_FJC:
            winner = other
            loser = self
            winner.free_adopt_children(loser, method, "Free")
        # if neither is an FJC judge, use the ucid/token length method from the parent class
        else:
            self.free_choose_winner_ucids(other, method, "Free")

    def free_adopt_children(self, other, method, where):
        """method used to assign another entity node to this node as the parent entity

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
            method (str): description of the matching method that led to this disambiguation
            where (str): ucid or court in which the disambiguation occurred
        """

        # log the disambiguation
        self.log(f"{where:25} | {method:22} |{other.name:25} --> {self.name:25}")#.info(f"{where:25} | {method:22} |{other.name:25} --> {self.name:25}")

        if self.has_SJID and other.has_SJID:
            print("FAILURE DETECTED")
        elif not self.has_SJID and other.has_SJID:   
            # if the other node already had children disambiguated to it, take those as well
            self.set_SJID(other.SJID)
            self.set_NID(other.NID)
        elif self.has_SJID and not other.has_SJID:
            other.set_SJID(self.SJID)
            other.set_NID(self.NID)
            for each in other.children:
                each.set_SJID(self.SJID)
                each.set_NID(self.NID)
        
        # add the child node to a list of children
        self.children.append(other)
        self.children+=other.children

        # for each child now belonging to this node, make sure it points to this entity
        for each in self.children:
            each.points_to(self)

    def free_choose_winner_ucids(self, other, method, where):
        """when given this object and another that matches to it. Determine which should be the "parent" based on ucid and entity length

        Args:
            other (obj): another IntraMatch derivative object that gets mapped onto this one
            method (str): description of the matching method that led to this disambiguation
            where (str): ucid or court in which the disambiguation occurred
        """
        # if one of the entities is a single token and the other is not, choose the multi-token entity
        # when a winner is determined, call the "Adopt_children" method for the winner, passing the loser as the argument
        if self.token_length ==1 and other.token_length >1:
            winner = other
            loser = self                
            winner.free_adopt_children(loser, method, where)

        elif self.token_length >1 and other.token_length ==1:
            winner = self
            loser = other                
            winner.free_adopt_children(loser, method, where)

        # if both are multi-token entities, determine winner by number of unique ucid appearances for the entity
        # rationale: the more frequently a name is written, the more likely it is to be the "true" spelling
        else:       
            # get the ucid counts to determine the max     
            U_comp = set([self.n_ucids, other.n_ucids])
            # if the counts are different, choose the higher count
            if len(U_comp)>1:
                UMax = max(U_comp)
                winner = [o for o in [self, other] if o.n_ucids == UMax][0]
                loser = [o for o in [self, other] if o != winner][0]                   
                winner.free_adopt_children(loser, method, where)
            else:
                # equal number of ucids
                # choose by token length, then by character length
                # if they tie, we take the 0th index anyways
                sorty = sorted([self,other], key=lambda obj: (len(obj.base_tokens), len(obj.name)), reverse = True)
                winner = sorty[0]
                loser = sorty[1]
                winner.free_adopt_children(loser, method, where)
        return


class Algorithmic_Mapping(object):
    """object class used for the final entities post-disambiguation. The primary purpose of this class is to algorithmically label the entity

    Args:
        object (obj): class method to estimate an entity label
    """
    def __init__(self, name, is_FJC, FJC_Info, Prefixes, Head_UCIDs, Tot_UCIDs, Prior_SJID = None):
        """init method for the class

        Args:
            name (str): final entity name
            is_FJC (bool): does this entity have an nid and FJC appointments
            FJC_Info (dict): the FJC data if it exists, else empty dict
            Prefixes (dict): dict of prefix label counts keyed by mutually exclusive bucket i.e. {"Bankruptcy_Judge": 100 ucids, "Magistrated_Judge":2 ucids}
            Head_UCIDs (int): number of total headers in ucids the entity appeared on
            Tot_UCIDs (int): number of total ucids the entity appeared on
            Prior_SJID (str, optional): if the entity previously had an SJID, take that and keep it assigned, otherwise default to None
        """
        self.name = name
        self.is_FJC = is_FJC
        self.FJC_Info = FJC_Info
        self.Prefixes = Prefixes
        self.Head_UCIDs = Head_UCIDs
        self.Tot_UCIDs = Tot_UCIDs
        
        # identify the name tokens
        self.base_tokens = self.name.split()
        if self.base_tokens[-1] in JG.suffixes_titles:
            self.suffix = True
            self.tokens_wo_suff = self.base_tokens[0:-1]
        else:
            self.suffix = False
            self.tokens_wo_suff = self.base_tokens
        if self.Head_UCIDs>0:
            self.was_header=True
        else:
            self.was_header = False

        self.Prior_SJID = Prior_SJID
            
    def compute_weights(self):
        """using the attributes from init, build a weights attribute that weighs prefix categories and ucid counts for the entity.
        this weighting will be used in labelling
        """
        # an FJC judge does not need predictive labelling, so we return early and don't build proportions
        if self.is_FJC or (self.Prior_SJID and not self.Prefixes):
            self.relative_proportions = {}
            self.judgey_proportion = None
            return
        
        # convert counts to percentages
        relative_proportions = {k:100*(v/sum(self.Prefixes.values())) for k,v in self.Prefixes.items()}

        # determine what percentage of the time of all entity appearances, that the pretext had "judgey-like" terms
        judgey = ['Bankruptcy_Judge', 'Circuit_Appeals', 'District_Judge', 'Magistrate_Judge','Nondescript_Judge']
        judgey_proportion = 100*sum([v for k,v in self.Prefixes.items() if k in judgey])/sum(self.Prefixes.values())
        
        # assign to self
        self.relative_proportions = relative_proportions
        for k in judgey:
            if k not in self.relative_proportions:
                self.relative_proportions[k] = 0
        self.judgey_proportion = judgey_proportion
        return
    
    def set_guess(self, g):
        """setter function for the entity label

        Args:
            g (str): guessed entity label
        """
        self.SCALES_Guess = g
        
        
    def Label_Algorithm(self):
        """Method used to estimate an entity label using the ucid counts and other attributes of the entity
        """

        # prep the self attributes data
        self.compute_weights()

        # FJC judges are just labeled as such
        if self.is_FJC:
            self.set_guess("FJC Judge")
            return

        # low frequency we do not consider, fail early
        if self.Tot_UCIDs <=3:
            self.set_guess("deny - low occurence")
            return

        # if 100% of the time the entity was never prefaced with judgey terms, reject it
        if self.relative_proportions['No_Keywords']==100:
            self.set_guess('deny - Junk')
            return

        # if the entity is in 1 of the categories of labels, 100% of the time
        if 100 in self.relative_proportions.values():
            winner = {k:v for k,v in self.relative_proportions.items() if v==100}
            sg = list(winner.keys())[0]
            # if it's JA, it is likely a clerk or mediator 
            # do not consider as a judge if no header ucids and low frequency
            if sg == 'Judicial_Actor' and self.Tot_UCIDs<=3 and self.Head_UCIDs == 0:
                self.set_guess('deny - Junk - Actor')
                return
            # same deal if it's only ever nondescript
            if sg == 'Nondescript_Judge' and self.Tot_UCIDs<=3 and self.Head_UCIDs == 0:
                self.set_guess('deny - Junk - Nondescript')
                return
            
            self.set_guess(sg)
            return

        # find all labels that appear for this entity
        over_0_keys = [k for k,v in self.relative_proportions.items() if v>0]
        # if only ever nondescript or magistrate
        if all([p in ['Nondescript_Judge','Magistrate_Judge', 'No_Keywords','Judicial_Actor'] for p in over_0_keys]):
            self.set_guess('Magistrate_Judge')
            return
        # if only ever nondescript or district
        if all([p in ['Nondescript_Judge','District_Judge', 'No_Keywords','Judicial_Actor'] for p in over_0_keys]) and self.Tot_UCIDs>3:
            self.set_guess('District_Judge')
            return      
        # if only ever nondescript or bankruptcy
        if all([p in ['Nondescript_Judge','Bankruptcy_Judge', 'No_Keywords','Judicial_Actor'] for p in over_0_keys]):
            self.set_guess('Bankruptcy_Judge')
            return      

        if self.relative_proportions['Magistrate_Judge']>=50:
            self.set_guess("Magistrate_Judge")
            return
        if self.relative_proportions['District_Judge']>=50 and self.Tot_UCIDs>3:
            self.set_guess("District_Judge")
            return
        if self.relative_proportions['Bankruptcy_Judge']>=50:
            self.set_guess("Bankruptcy_Judge")
            return

        if self.relative_proportions['Magistrate_Judge'] >= 25 and self.relative_proportions['District_Judge'] < 25 and self.relative_proportions['Bankruptcy_Judge'] < 25:
            self.set_guess("Magistrate_Judge")
            return

        if self.relative_proportions['Magistrate_Judge'] < 25 and self.relative_proportions['District_Judge'] >= 25 and self.relative_proportions['Bankruptcy_Judge'] < 25 and self.Tot_UCIDs>3:
            self.set_guess("District_Judge")
            return

        if self.relative_proportions['Magistrate_Judge'] < 25 and self.relative_proportions['District_Judge'] < 25 and self.relative_proportions['Bankruptcy_Judge'] >= 25:
            self.set_guess("Bankruptcy_Judge")
            return

        if self.relative_proportions['District_Judge'] > 5 and self.was_header:
            self.set_guess("District_Judge**")
            return
        if self.relative_proportions['Magistrate_Judge'] > 5 and self.was_header:
            self.set_guess("Magistrate_Judge")
            return
        if self.relative_proportions['Bankruptcy_Judge'] > 5 and self.was_header:
            self.set_guess("Bankruptcy_Judge**")
            return


        if self.Tot_UCIDs >= 25 and max(self.relative_proportions, key = self.relative_proportions.get) =='Nondescript_Judge':
            if self.Head_UCIDs>=10 and self.relative_proportions['District_Judge']>5:
                self.set_guess("District_Judge")
                return
            if self.Head_UCIDs>=10 and self.relative_proportions['Magistrate_Judge']>5:
                self.set_guess("Magistrate_Judge--")
                return
            
            others = {k:v for k,v in self.relative_proportions.items() if k!='Nondescript_Judge'}
            nextmax_val = max(others, key = others.get)
            if self.relative_proportions[nextmax_val]>10:
                self.set_guess(nextmax_val)
                return
            else:
                self.set_guess("Nondescript_Judge")
                return

        if max(self.relative_proportions,key=self.relative_proportions.get) == 'No_Keywords' and self.relative_proportions['No_Keywords']>=90:
            self.set_guess('--- deny --- insufficient data')
            return

        if (self.relative_proportions['No_Keywords'] + self.relative_proportions['Judicial_Actor']) > 60:
            self.set_guess('--- deny --- clerk or attorney')
            return
        
        if self.judgey_proportion>50 and self.Tot_UCIDs >=10:
            self.set_guess('Nondescript_Judge')
            return

        # if we failed all the way through, I guess it's a generic, nondescript judge
        self.set_guess('Nondescript_Judge')
        
    def build_row(self, update = False):
        """take the entity attributes and return them as a row of data

        Returns:
            dict: entity attributes
        """
        out =  {"name": self.name, 
                "SCALES_Guess": self.SCALES_Guess, 
                "is_FJC": self.is_FJC, 
                "Head_UCIDs": self.Head_UCIDs, 
                "Tot_UCIDs": self.Tot_UCIDs, 
                "was_header": self.was_header, 
                "judgey_proportion": self.judgey_proportion,
               **self.relative_proportions}

        if update:
            out['Prior_SJID'] = self.Prior_SJID

        return out

    
    def prettify_name(self):
        """custom method that transforms an entity string into a "pretty" form (all words have first letter capitalized)
        """
        upper = [f"{tok[0].upper()}{tok[1:]}" for tok in self.base_tokens]
        if self.suffix:
            if upper[-1][0].lower()=="i":
                upper[-1] = upper[-1].upper()
        pretty = " ".join(upper)

        self.pretty_name = pretty
        
    def set_SCALES_JID(self, SJID):
        """setter method for a unique Scales Judge ID

        Args:
            SJID (str): SJXXXXXX id for the entity
        """
        self.SJID = SJID
        # once an ID is set, prettify the name
        self.prettify_name()
    
    def JEL_row(self):
        """return a row of data to be used in creating the JEL

        Returns:
            dict: selected entity attributes for the JEL
        """
        return {"name": self.name,
                "Presentable_Name": self.pretty_name,
                "SJID": self.SJID,
                "SCALES_Guess": self.SCALES_Guess, 
                "is_FJC": self.is_FJC, 
                "Head_UCIDs": self.Head_UCIDs, 
                "Tot_UCIDs": self.Tot_UCIDs, 
               **self.FJC_Info}


class UPDATER_NODE(object):
    """Special class object to be used when updating a new case to tag judge entities on it

    Args:
        object (obj): custom object for updating a case
    """
    def __init__(self,  cleaned_name):
        """init method

        Args:
            cleaned_name (str): entity name extracted either from header metadata or docket entries
        """

        # self.log = logging.getLogger()

        self.name = cleaned_name
        self.eligible = True

        self.base_tokens = cleaned_name.split()
        self.inferred_tokens = JH.build_inferred_tokens(self.base_tokens)
        self.nicknames_tokens =  {True:{}, False:{}}
        self.unified_names_tokens = {True:{}, False:{}}

        for fi in [True, False]:
            for mi in [True, False]:
                self.nicknames_tokens[fi][mi],self.unified_names_tokens[fi][mi] = JH.build_nicknames_and_unified(self.inferred_tokens[fi][mi])

        self.eligible = True
        self.token_length = len(self.base_tokens)

        if self.base_tokens[-1] in JG.suffixes_titles:
            self.suffix = self.base_tokens[-1]
            if self.token_length==1:
                self.anchor = None
            else:
                self.anchor = self.base_tokens[-2]
            self.initials_wo_suff = [tok[0] for tok in self.base_tokens[0:-1]]
            self.tokens_wo_suff = [tok for tok in self.base_tokens[0:-1]]
        else:
            self.suffix=None
            self.anchor = self.base_tokens[-1]
            self.initials_wo_suff = [tok[0] for tok in self.base_tokens]
            self.tokens_wo_suff = self.base_tokens
        
        
        self.initials_w_suff = [tok[0] for tok in self.base_tokens]
        
    def assign_SJID(self, sjid):
        """setter function that sets this entity to a known SJID, and then flags the entity as ineligible (complete)

        Args:
            sjid (str): sjid of the parent entity this node was mapped to
        """
        self.SJID = sjid
        self.eligible = False

class JEL_NODE(UPDATER_NODE):
    """Node creator to be used to make the JEL table entities into nodes to be used in comparison

    Args:
        UPDATER_NODE (obj): parent class
    """
    def __init__(self,  name, sjid):
        """init function for a JEL object, a known disambiguated judge entity (to be used in disambiguation updating)

        Args:
            name (str): lowercase string form of the judge name
            sjid (str): the disambiguation ID from the JEL table corresponding to this judge
        """
        self.SJID= sjid
        super().__init__(name)