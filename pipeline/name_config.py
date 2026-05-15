"""
HGEM MVP Dashboard — Name & Sentiment Configuration

SUMMARY
-------
Single source of truth for every name-handling rule. Every entry here was
added because something went wrong without it.

Sections:
  1. NAME_GROUPS        — merge variant spellings into one canonical name
  2. NEVER_SPLIT        — names the auto-splitter should NOT split by venue
  3. FORCE_SPLIT        — names the auto-splitter MUST split by venue
  4. PRIMARY_VENUE_OVERRIDE — pin an employee's venue tag (for staff moves)
  5. MISATTRIBUTED_MENTIONS — scrub specific (visit_id, name) combinations
  6. NON_NAMES          — exclusion list of false-positive "names"
                          (also reads common_english_words.txt at import)
  7. SENTIMENT lexicons — positive/negative/complaint words
"""
from pathlib import Path

# 1. NAME_GROUPS — variant -> canonical
NAME_GROUPS = {
    "jaz":      "Jas",      # Oxford
    "jazz":     "Jas",
    "jasmine":  "Jas",
    "jasmin":   "Jas",
    "conner":   "Connor",   # Oxford
    "isiah":    "Isaiah",   # Oxford
    "holly":    "Hollie",   # Cardiff
    "megan":    "Meg",      # Cheltenham
    "tegan":    "Teagan",   # Cardiff
    "lucas":    "Lukas",    # Oxford
    "forrest":  "Forest",   # Cardiff
    "tristen":  "Tristan",  # Cheltenham/Bristol/Reading
}

# 2. NEVER_SPLIT — auto-splitter must keep these as a single person
NEVER_SPLIT = {"Tristan"}

# 3. FORCE_SPLIT — always split these by venue
FORCE_SPLIT = {"Emily"}

# 4. PRIMARY_VENUE_OVERRIDE — pin venue tag (for staff moves)
PRIMARY_VENUE_OVERRIDE = {
    "Tristan": "FC Reading",
}

# 5. MISATTRIBUTED_MENTIONS — scrub (visit_id, name) pairs
MISATTRIBUTED_MENTIONS = {
    ("21049442", "Zoe"),    # Customer named Zoe Marie, not staff
}

# 6. NON_NAMES — words that look like names but aren't
_NON_NAMES_INLINE = {
    # Flight Club venue/city names
    "Birmingham", "Bristol", "Cardiff", "Cheltenham", "Oxford", "Reading",
    # Other UK cities (pre-emptive)
    "London", "Manchester", "Liverpool", "Leeds", "Sheffield", "Edinburgh",
    "Glasgow", "Newcastle", "Nottingham", "Brighton", "Bath", "Norwich",
    "Plymouth", "Southampton", "York", "Belfast", "Dublin", "Ireland",
    # Locations / landmarks
    "Westgate", "Lorry", "Summer",
    # Praise adjectives
    "Absolute", "Absolutely", "Amazing", "Awesome", "Beautiful", "Best",
    "Better", "Brill", "Brilliant", "Cracking", "Delicious", "Epic",
    "Especially", "Excellent", "Fab", "Fabulous", "Fantastic", "Great",
    "Incredible", "Incredibly", "Lovely", "Outstanding", "Perfect",
    "Phenomenal", "Polite", "Quality", "Smooth", "Solid", "Special",
    "Stunning", "Superb", "Tasty", "Top", "Totally", "Wonderful", "Wow",
    # Praise nouns / slang
    "Champ", "Class", "Funny", "Gem", "Goat", "Hero", "Legend",
    "Legendary", "MVP", "Shoutout", "Shout", "Star", "Smash", "Smiley",
    # Service roles
    "Service", "Server", "Bartender", "Waiter", "Waitress", "Hostess",
    "Host", "Manager", "Bouncer", "Doorman", "Doormen", "Staff",
    "Customer", "Guest",
    # Game/venue
    "Tap", "Pin", "Dart", "Darts", "Throw", "Throws", "Board", "Boards",
    "Bullseye", "Oche", "Score", "Match",
    # Verbs / connectives
    "Additionally", "Already", "Booked", "Bring", "Brought", "Constant",
    "Explained", "Genuinely", "Having", "Hope", "Keep", "Kept", "Kind",
    "Look", "Make", "Makes", "None", "Once", "Passing", "Played",
    "Plenty", "Plus", "Popped", "Promote", "Provided", "Saw", "See",
    "Seen", "Sorry", "Took", "Through", "Thoroughly", "Two", "Unknown",
    "Upon", "Used", "Visited", "Walk", "Welcomed", "Welcoming", "Whilst",
    "Wish", "Word", "Whether", "Based",
    # Food / drink / brands
    "Bit", "Brunch", "Chicken", "Coke", "Deals", "Diet", "Feast",
    "Guinness", "Korean", "Lemonade", "Moretti",
    "Nduja", "Parmesan", "Peri", "Prosecco", "Signature", "Lively",
    "Lots", "Lower", "House", "Live", "Local", "Paid", "Playing",
    "Shortly", "Soooo", "Unsure", "Wicked",
    # Tech / generic
    "Allergen", "Attention", "Google", "Helpful", "Helpfull", "Matrix",
    "Stuff", "Over", "Thankyou",
    # Surname-only
    "Morris", "Bantosh",
}

# Load big common-English-words list from a sibling text file
_words_path = Path(__file__).parent / "common_english_words.txt"
if _words_path.exists():
    _common = {w.strip() for w in _words_path.read_text(encoding="utf-8").splitlines()
               if w.strip()}
else:
    _common = set()

NON_NAMES = _NON_NAMES_INLINE | _common


# 7. SENTIMENT WORD LISTS
POSITIVE_WORDS = {
    "amazing", "awesome", "best", "brilliant", "excellent", "fab", "fabulous",
    "fantastic", "friendly", "good", "great", "incredible", "lovely", "nice",
    "outstanding", "perfect", "phenomenal", "polite", "professional", "quality",
    "smooth", "solid", "special", "superb", "thank", "thanks", "thankyou",
    "top", "wonderful", "wow", "shoutout", "shout-out", "shout", "credit",
    "praise", "kudos", "star", "legend", "hero", "champion", "champ", "mvp",
    "goat", "gem", "smiley", "smile", "helpful", "attentive", "engaging",
    "charming", "delight", "delightful", "happy", "love", "loved", "loving",
    "enjoy", "enjoyed", "appreciate", "appreciated", "fast", "quick",
    "efficient", "knowledgeable", "welcoming", "warm", "kind", "patient",
    "fun", "funny", "hilarious", "entertaining", "exceptional", "memorable",
    "recommend", "recommended", "first-class", "first class", "ace",
    "class", "cracking", "epic", "tasty", "beautiful", "stunning", "absolute",
    "absolutely", "totally", "incredibly", "genuinely", "really", "very",
    "especially", "particularly", "definitely", "must", "highly",
}

NEGATIVE_WORDS = {
    "rude", "slow", "awful", "terrible", "horrible", "bad", "poor",
    "disappointed", "disappointing", "disappointment", "unhelpful", "unfriendly",
    "ignored", "ignoring", "ignorant", "wait", "waited", "waiting",
    "complaint", "complain", "complained", "complaining", "issue", "issues",
    "problem", "problems", "wrong", "mistake", "error", "failure", "failed",
    "fail", "miserable", "appalling", "unacceptable", "shocking", "shocked",
    "annoyed", "annoying", "frustrated", "frustrating", "uncomfortable",
    "let down", "let-down", "letdown",
}

COMPLAINT_SIGNALS = {
    "didn't", "didnt", "did not", "despite", "unfortunately", "complaint",
    "disappointed", "let down", "let-down", "poor", "issue", "wait",
    "slow", "rude", "supposed to", "should have", "should've", "wasn't",
    "wasnt", "was not",
}

NEGATION_WORDS = {"not", "no", "never", "nothing", "neither", "nor", "n't",
                  "barely", "hardly", "scarcely", "isn't", "wasn't", "weren't",
                  "didn't", "doesn't", "don't"}
