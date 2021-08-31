# **JED Data Models**

## **Inputs**
The following sections are data inputs to the disambiguation pipeline. Internal SCALES models create data that is output in these formats that the disambiguation pipeline then ingests.

### **Extracted Entry Entities**
Our pool of entities is identified from docket entries using an in-house trained Spacy NER model. This model is fed docket entries and tags them with honorific and judge spans.

Input files that correspond to Extracted Entry Entities and the following data model:
- `inputs/input_extractions/public_sample_*.csv`
- `inputs/input_new_tags/new_entries.csv`
- `inputs/input_new_disambiguation/extracted_entities/public_sample_update_*.csv`

#### Columns:
- **ucid:** unique case identifier
- **court:** abbreviated district court of docket
- **year:** integer filing date year
- **cid:** court specific case id
- **docket_index:** pythonic index of docket entry in which the entity was located (index 0 is the first docket entry, ordered chronologically oldest to newest)
- **entry_date:** string form of docket entry date (if exists)
- **original_text:** substring of the docket entry text containing the extracted entity; this is not the full docket entry, just the extracted entity +- 5-7 word tokens before and after
- **full_span_start:** character span starting point of the full text sample (original_text) with respect to character 0 of the entry
- **full_span_end:** character span ending point of the full text sample (original_text) with respect to character 0 of the entry
- **extracted_pre_5:** 5 word tokens preceding our extracted entity structured as one string as they appeared on the docket (meaning multi-spaces, punctuation, underscores, etc. may be present)
- **extracted_entity:** n word tokens corresponding to the extracted entity (structured verbatim as a string as it appears, punctuation included)
- **extracted_post_5:** 5 word tokens following our extracted entity structured as one string as they appeared on the docket (meaning multi-spaces, punctuation, underscores, etc. may be present)
- **Ent_span_start:** character span starting point of the extracted entity with respect to character 0 of the entry
- **Ent_span_end:** character span ending point of the extracted entity with respect to character 0 of the entry
- **docket_source:** in this structure, the value is a line_entry meaning this data came from a docket line and not case header metadata
- **Entity_Extraction_Method:** SPACY_JNLP2 means this data was extracted using a Spacy NER model specifically trained for federal court dockets


### **Extracted Header Entities**
Our pool of header entities is identified from docket header data using regular expressions and brute force pattern recognition on the court docket sheet. Colloquially this is the *Assigned to* or *Referred to* judge field

Input files that correspond to Extracted Header Entities and the following data model:
- `inputs/input_extractions/_public_sample_headers.csv`
- `inputs/input_new_tags/new_headers.csv`
- `inputs/input_new_disambiguation/input_extractions/_public_sample_update_headers.csv`

#### Columns:
- **ucid:** unique case identifier
- **court:** abbreviated district court of docket
- **cid:** court specific case id
- **year:** integer filing date year
- **filing_date:** filing date for this case
- **original_text:** substring of the docket entry text containing the extracted entity; this is not the full docket entry, just the extracted entity +- 5-7 word tokens before and after
- **extracted_pretext:** any text preceding our extracted entity structured as one string as they appeared on the docket (meaning multi-spaces, punctuation, underscores, etc. may be present)
- **extracted_entity:** n word tokens corresponding to the extracted entity (structured verbatim as a string as it appears, punctuation included)
- **Ent_span_start:** character span starting point of the extracted entity with respect to character 0 of the relevant header field
- **Ent_span_end:** character span ending point of the extracted entity with respect to character 0 of the relevant header field
- **Pre_span_start:** character span starting point of any text that preceded the entity string
- **Pre_span_end:** character span ending point of any text that preceded the entity string
- **docket_source:** case_party or case_header will be listed in this data. Criminal cases have defendant-specific judge listings (case_party); civil cases have generalized judge assignments (case_header) that we extract the entities from
- **Entity_Extraction_Method:** in header metadata the extracted judge can either come from the assigned_to or referred_to key on the docket
- **judge_enum:** if multiple judges were listed under a key, this is an enumeration making a distinction ordinally of judge 0, judge 1, etc. listed
- **party_enum:** in criminal cases, this is the enumerated party order in which the judges were extracted. Judge 0 corresponds to party 0, 1 to 1, etc. This will be null for non-criminal cases
- **pacer_id:** PACER places an intra-case id on these judges for criminal dockets, this will also be null for non-criminal cases
- **docket_index:** null for header metadata, only filled for entry level extractions


### **Extracted Party and Counsel Entities**
Our pool of header party and counsel entities is identified from docket header data using regular expressions and brute force pattern recognition on the court docket sheet.

Input files that correspond to Extracted Party/Counsel Entities and the following data model:
- `inputs/input_header_meta/*.csv`
- `inputs/input_new_tags/new_parties.csv`
- `inputs/input_new_tags/new_counsels.csv`
- `inputs/input_new_disambiguation/meta_headers/*.csv`

#### Columns:
- **ucid:** unique case identifier
- **court:** abbreviated district court of docket
- **cid:** court specific case id
- **year:** integer filing date year
- **filing_date:** filing date for this case
- **Role:** general case role (plaintiff or defendant)
- **Entity:** string listed name from docket metadata, uncleaned

## **Outputs**
### **JEL: Judicial Entity Lookup**
The JEL is a structured dataset of unique judges derived from the data that disambiguation receives. It combines ground truth Article III judge information from the FJC (using nid) with algorithmically identified Article I and other Article III judges missing from the biographical dictionary.

Each row of this dataset is a unique entity and can be connected to tagged docket information using the SJID (Scales Judicial ID)

#### Columns
- **name:** all lowercased entity string name for the final representation of the unique entity
- **Presentable_Name:** cleaned mixed case representation of the entity
- **SJID:** string based unique identifier for an entity
- **SCALES_Judge_Label:** the FJC data label or SCALES-generated algorithmic label for the judge (FJC Judge, Magistrate, Bankruptcy, etc.)
- **Head_UCIDs:** total number of unique docket headers this entity and its various representations appeared on when it was identified in disambiguation
- **Tot_UCIDs:** total number of unique cases this entity and its various representations appeared on when it was identified in disambiguation
- **Full_Name:** if this is an FJC judge, this is the concatenation of FJC name columns
- **NID:** if this is an FJC judge, this is the corresponding nid for the entity

### **SEL: Spacy Entity Lookup**
The SEL structure is named as such because most of our extracted entities were identified using Spacy. The SEL data is structurally the same whether it is in a bulk `.jsonl` file or if it exists in individual case files. This data structure is meant to represent a single occurrence in a single span of a singular entity - that is "on case x, at entry y, at character span z: entity Alpha identified"

#### Columns
- **Entity_Extraction_Method:** how was the entity originally extracted, a NER model or pattern recognition on header meta-data
- **docket_source:** header or entry source
- **judge_enum:** if it was header data, which ordinal judge was it 
- **party_enum:** if it was header data, which ordinal party was the judge listed for
- **pacer_id:** if it was header data, what intra-case PACER id was this judge given 
- **docket_index:** if this was entry data, what integer index was the entry (pythonic starting at 0)
- **ucid:** SCALES unique case identifier
- **cid:** court specific case identifier
- **court:** abbreviated district court
- **year:** year of filing date for the case
- **original_text:** string of text the entity was extracted from within
- **Extracted_Entity:** original text string of the entity 
- **Prefix_Categories:** what type of label did we categorize the preceding text with (i.e. magistrate honorifics, nondescript honorifics, bankruptcy, judicial actor, etc.)
- **Transferred_Flag:** did any sort of transfer terminology appear around this entity (transferred to or from X)
- **full_span_start:** character span starting point of the full text sample (original_text) with respect to character 0 of the originating text entry/header
- **full_span_end:** character span ending point of the full text sample (original_text) with respect to character 0 of the originating text entry/header
- **Ent_Span_Start:** character span starting point of the extracted entity with respect to character 0 of the originating text entry/header
- **Ent_Span_End:** character span ending point of the extracted entity with respect to character 0 of the originating text entry/header
- **Parent_Entity:** disambiguated, general name that this entity corresponds to (i.e. often a formal name)
- **SJID:** the identified SCALES Judicial Identifier key that can be tied to the JEL and subsequently tied to an NID if one exists