"""
Team-name → league mapping and league detection utilities.

Detection strategy (in priority order):
  1. Full team name substring match — "Pittsburgh Penguins" only matches NHL,
     never MLB, regardless of shared city names like "Pittsburgh Pirates".
  2. Nickname-only fallback — used when no full name is found (e.g. abbreviated
     event titles).  Ambiguous nicknames (Rangers, Cardinals, Giants) are skipped
     in the fallback to avoid false positives.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical team names per league
# ---------------------------------------------------------------------------

MLB_TEAMS = [
    "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles",
    "Boston Red Sox", "Chicago Cubs", "Chicago White Sox",
    "Cincinnati Reds", "Cleveland Guardians", "Colorado Rockies",
    "Detroit Tigers", "Houston Astros", "Kansas City Royals",
    "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins",
    "Milwaukee Brewers", "Minnesota Twins", "New York Mets",
    "New York Yankees", "Athletics", "Philadelphia Phillies",
    "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
    "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays",
    "Texas Rangers", "Toronto Blue Jays", "Washington Nationals",
]

NHL_TEAMS = [
    "Anaheim Ducks", "Boston Bruins", "Buffalo Sabres",
    "Calgary Flames", "Carolina Hurricanes", "Chicago Blackhawks",
    "Colorado Avalanche", "Columbus Blue Jackets", "Dallas Stars",
    "Detroit Red Wings", "Edmonton Oilers", "Florida Panthers",
    "Los Angeles Kings", "Minnesota Wild", "Montreal Canadiens",
    "Nashville Predators", "New Jersey Devils", "New York Islanders",
    "New York Rangers", "Ottawa Senators", "Philadelphia Flyers",
    "Pittsburgh Penguins", "San Jose Sharks", "Seattle Kraken",
    "St. Louis Blues", "Tampa Bay Lightning", "Toronto Maple Leafs",
    "Utah Hockey Club", "Vancouver Canucks", "Vegas Golden Knights",
    "Washington Capitals", "Winnipeg Jets",
]

NBA_TEAMS = [
    "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets",
    "Charlotte Hornets", "Chicago Bulls", "Cleveland Cavaliers",
    "Dallas Mavericks", "Denver Nuggets", "Detroit Pistons",
    "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
    "Los Angeles Clippers", "Los Angeles Lakers", "Memphis Grizzlies",
    "Miami Heat", "Milwaukee Bucks", "Minnesota Timberwolves",
    "New Orleans Pelicans", "New York Knicks", "Oklahoma City Thunder",
    "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
    "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs",
    "Toronto Raptors", "Utah Jazz", "Washington Wizards",
]

NFL_TEAMS = [
    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens",
    "Buffalo Bills", "Carolina Panthers", "Chicago Bears",
    "Cincinnati Bengals", "Cleveland Browns", "Dallas Cowboys",
    "Denver Broncos", "Detroit Lions", "Green Bay Packers",
    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars",
    "Kansas City Chiefs", "Las Vegas Raiders", "Los Angeles Chargers",
    "Los Angeles Rams", "Miami Dolphins", "Minnesota Vikings",
    "New England Patriots", "New Orleans Saints", "New York Giants",
    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers",
    "San Francisco 49ers", "Seattle Seahawks", "Tampa Bay Buccaneers",
    "Tennessee Titans", "Washington Commanders",
]

SUPPORTED_LEAGUES: list[str] = ["MLB", "NHL", "NBA", "NFL"]

# ---------------------------------------------------------------------------
# Pre-built lookup structures
# ---------------------------------------------------------------------------

# Full lowercased team names per league  →  used for primary detection
_TEAM_NAMES: dict[str, list[str]] = {
    "MLB": [t.lower() for t in MLB_TEAMS],
    "NHL": [t.lower() for t in NHL_TEAMS],
    "NBA": [t.lower() for t in NBA_TEAMS],
    "NFL": [t.lower() for t in NFL_TEAMS],
}

# Nicknames that appear in MORE THAN ONE league — excluded from fallback
# to prevent cross-contamination (Rangers=MLB+NHL, Cardinals=MLB+NFL, etc.)
def _find_ambiguous_nicknames() -> set[str]:
    seen: dict[str, str] = {}
    ambiguous: set[str] = set()
    for league, teams in _TEAM_NAMES.items():
        for team in teams:
            nickname = team.split()[-1]
            if nickname in seen and seen[nickname] != league:
                ambiguous.add(nickname)
            else:
                seen[nickname] = league
    return ambiguous

_AMBIGUOUS_NICKNAMES: set[str] = _find_ambiguous_nicknames()

# Unambiguous nickname  →  league  (fallback only)
_NICKNAME_INDEX: dict[str, str] = {}
for _league, _teams in _TEAM_NAMES.items():
    for _team in _teams:
        _nick = _team.split()[-1]
        if len(_nick) >= 4 and _nick not in _AMBIGUOUS_NICKNAMES:
            _NICKNAME_INDEX.setdefault(_nick, _league)


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def detect_league(text: str) -> str:
    """
    Return the league (MLB / NHL / NBA / NFL) for an event, or
    'CONCERT' / 'UNKNOWN' when no sports team is matched.

    Uses full team name matching first to prevent city-name collisions
    (e.g. Pittsburgh Pirates vs Pittsburgh Penguins).
    """
    text_lower = text.lower()
    votes: dict[str, int] = {}

    # ── Primary: full team name substring match ──────────────────────────
    for league, names in _TEAM_NAMES.items():
        for name in names:
            if name in text_lower:
                votes[league] = votes.get(league, 0) + 1

    if votes:
        return max(votes, key=lambda k: votes[k])

    # ── Fallback: unambiguous nickname match ─────────────────────────────
    for word in text_lower.split():
        clean = word.strip(".,\"'()-")
        if len(clean) >= 4 and clean in _NICKNAME_INDEX:
            league = _NICKNAME_INDEX[clean]
            votes[league] = votes.get(league, 0) + 1

    if not votes:
        return "CONCERT"
    return max(votes, key=lambda k: votes[k])


def team_in_text(team_name: str, text: str, min_word_len: int = 4) -> bool:
    """
    Return True if *team_name* (from the official schedule) can be matched
    against *text* (from inventory).

    Tries full name first, then word-by-word for abbreviated event titles.
    """
    text_lower = text.lower()

    # Full name match (most reliable)
    if team_name.lower() in text_lower:
        return True

    # Word-by-word fallback
    for word in team_name.lower().split():
        if len(word) >= min_word_len and word.strip(".,") in text_lower:
            return True

    return False
