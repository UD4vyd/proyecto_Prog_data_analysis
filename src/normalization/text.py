"""Text cleaning for the free-text fields (alert descriptions, storm narratives)."""
import re

# Common English words that carry no meaning for a keyword search. A short fixed list is enough here:
# the texts are official weather bulletins written in plain English.
STOP_WORDS = frozenset("""
a an the and or but if then than so of to in on at by for from with without into over under until up down out
is are was were be been being am do does did has have had will would can could may might should must shall
this that these those it its he she they them their we our you your i me my who whom which what when where why how
as not no nor only also very too more most some any each all both few other such own same about above below after
before again further here there during through between against off once
""".split())

# Workshop 3: words found in the real bulletins that say nothing about the hazard. Time zones and clock words
# ("cst", "pm") were among the 15 most frequent alert tokens, and links like http://www.weather.gov/safety/flood
# left "http", "www" and "gov" as tokens.
NOISE_WORDS = frozenset("""
cst cdt mst mdt est edt utc am pm noon midnight
monday tuesday wednesday thursday friday saturday sunday tonight today tomorrow
http https www gov com org html php
""".split())

_URL = re.compile(r"(https?://|www\.)\S+")
_EMAIL = re.compile(r"\S+@\S+")
_NOT_LETTERS = re.compile(r"[^a-z0-9\s]")
_SPACES = re.compile(r"\s+")


def clean_text(text):
    """Lowercase, remove links, punctuation, line breaks and repeated spaces."""
    if not isinstance(text, str):
        return ""
    lowered = text.lower()
    lowered = _EMAIL.sub(" ", _URL.sub(" ", lowered))
    # NWS bulletins mark sections as "* WHAT...Heavy rain". The dots are separators, not words.
    lowered = lowered.replace("...", " ")
    return _SPACES.sub(" ", _NOT_LETTERS.sub(" ", lowered)).strip()


def tokenize(clean):
    """Split a cleaned text into words, without stop words, noise words, numbers and one-letter tokens."""
    return [word for word in clean.split()
            if len(word) > 1 and not word.isdigit() and word not in STOP_WORDS and word not in NOISE_WORDS]
