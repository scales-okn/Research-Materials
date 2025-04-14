from pathlib import Path
from spacy.util import registry
from spacy.tokenizer import Tokenizer

MODEL_PATH = Path(__file__).resolve().parents[1] / 'model'
if not MODEL_PATH.exists(): # we're in the wheel version of the model directory
	MODEL_PATH = Path(__file__).resolve().parents[0] / 'en_pipeline-0.0.0' # sorry for hardcoded version number
TOK_PATH = MODEL_PATH / 'tokenizer'

@registry.callbacks("custom_tok")
def get_custom():
    def load_it(nlp):
        tokenizer = Tokenizer(nlp.vocab)
        tokenizer.from_disk(TOK_PATH)
        return tokenizer    
    return load_it