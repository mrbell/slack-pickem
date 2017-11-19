'''
Pick em slack commands
'''

import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import logging
import os
import requests
from datetime import datetime
import math

from urlparse import parse_qs

"""
Resources
"""

help_text = "Use this command to manage your pick'em selections."
help_attachment_text = (
    "Use `/pickem [subcommand]` with one of the following:\n"
    "Either `pick` to check your pick for the week, `pick [team]` " +
    "to make a new pick, `record` to check your record, " +
    "`who` to see who has picked this week, or `standings` to check " +
    "standings, e.g. `/pickem pick pats`."
)

# Mapping of normalized team locations to normalized team nicknames
locs_to_teams = {
    'arizona': 'cardinals',
    'atlanta': 'falcons',
    'baltimore': 'ravens',
    'buffalo': 'bills',
    'carolina': 'panthers',
    'chicago': 'bears',
    'cincinnati': 'bengals',
    'cleveland': 'browns',
    'dallas': 'cowboys',
    'denver': 'broncos',
    'detroit': 'lions',
    'green': 'packers',
    'houston': 'texans',
    'indianapolis': 'colts',
    'jacksonville': 'jaguars',
    'kansas': 'chiefs',
    'miami': 'dolphins',
    'minnesota': 'vikings',
    'england': 'patriots',
    'orleans': 'saints',
    'oakland': 'raiders',
    'philadelphia': 'eagles',
    'pittsburgh': 'steelers',
    'francisco': '49ers',
    'seattle': 'seahawks',
    'tampa': 'buccaneers',
    'tennessee': 'titans',
    'washington': 'redskins'
}

# Mapping from NFL scoreboard abbreviations to normalized team names
scoreboard_to_team = {
    'ari': 'cardinals',
    'atl': 'falcons',
    'bal': 'ravens',
    'buf': 'bills',
    'car': 'panthers',
    'chi': 'bears',
    'cin': 'bengals',
    'cle': 'browns',
    'dal': 'cowboys',
    'den': 'broncos',
    'det': 'lions',
    'gb': 'packers',
    'hou': 'texans',
    'ind': 'colts',
    'jax': 'jaguars',
    'mia': 'dolphins',
    'min': 'vikings',
    'no': 'saints',
    'oak': 'raiders',
    'phi': 'eagles',
    'pit': 'steelers',
    'sea': 'seahawks',
    'tb': 'buccaneers',
    'ten': 'titans',
    'was': 'redskins',
    'lac': 'chargers',
    'lar': 'rams',
    'nyg': 'giants',
    'nyj': 'jets'
}

# Compile a list of teams
teams = ['jets', 'giants', 'rams', 'chargers']
for k in locs_to_teams:
    if isinstance(locs_to_teams[k], basestring):
        teams.append(locs_to_teams[k])
    else:
        teams.extend(locs_to_teams[k])
teams = set(teams)

# Mapping from some common location nicknames to normalized locations
loc_aliases = {
    'ne': 'england',
    'philly': 'philadelphia',
    'sf': 'francisco',
    'pitt': 'pittsburgh',
    'nola': 'orleans',
    'indy': 'indianapolis',
    'cinci': 'cincinnati',
    'kc': 'kansas'
}

# Mapping from some common alternate team nicknames to normalized team nicknames
team_aliases = {
    'cards': 'cardinals',
    'jags': 'jaguars',
    'pats': 'patriots',
    'niners': '49ers',
    'skins': 'redskins',
    'bucs': 'buccaneers'
}

# Various tokens that we will need
slack_token = os.environ['slackAppToken']
sr_token = os.environ['sportRadarToken']
webhook_url = os.environ['slackWebHookURL']
sns_arn = os.environ['snsARN']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb')

week_1_start = datetime(2017, 9, 5)  # Obviously only works for 2017 season

"""
Custom exceptions
"""

class NoTeamGiven(Exception):
    pass


class UnknownTeam(Exception):
    pass

"""
Helper functions
"""

def get_current_week(custom_date=None):
    """
    Get the number of the current week as an integer.
    Weeks start on Tuesday during the season.
    Returns 1 before the season has started (say over the summer).
    """
    if custom_date is None:
        test = datetime.today()
    else:
        test = custom_date

    return max([
        1,
        int(
            1 + math.floor(
                (test - week_1_start).total_seconds() / 3600.0 / 24.0 / 7.0
            )
        )
    ])


def get_team(user_entry):
    """
    Given a USER_ENTRY team name, return a normalized team name if one is
    recognized, else raise an UnknownTeam exception or a NoTeamGiven exception
    if USER_ENTRY is blank.
    """

    if len(user_entry) == 0:
        raise NoTeamGiven()

    tmp = user_entry.strip().split()

    team_choice = (
        " ".join(tmp)
        .lower()
        .replace('.', '')
        .replace('new', '')
        .replace('bay', '')
        .replace('los', '')
        .replace('city', '')
        .replace('san', '')
    )
    tokens = team_choice.split()

    team = None

    for token in tokens:
        if token in teams:
            team = token
        elif token in team_aliases:
            team = team_aliases[token]
        elif token in locs_to_teams:
            team = locs_to_teams[token]
        elif token in loc_aliases:
            team = locs_to_teams[loc_aliases[token]]
        elif token in scoreboard_to_team:
            team = scoreboard_to_team[token]
        if team is not None:
            break

    if team is None:
        raise UnknownTeam()

    return team


def get_user_record(user_id, week_num):
    """
    Return the set of picks and results from previous weeks. Returns a list of
    previous selections, sorted in ascending week number. Each selection is a
    dictionary with items `weekNumber`, `selectedTeam`, `userId`,
    and `teamWon` (1 if the selected team won that week).
    """
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.query(
        KeyConditionExpression=(
            Key('userId').eq(user_id) &
            Key('weekNumber').lt(week_num)
        )
    )
    record = response['Items']

    return sorted(record, key=lambda x: x['weekNumber'])


def get_current_pick(user_id, week_num):
    """
    Get the pick for the given user USER_ID and week number WEEK_NUM. Returns
    None if no pick has been made.
    """
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.get_item(
        Key={'userId': user_id, 'weekNumber': week_num}
    )

    if 'Item' not in response:
        return None
    else:
        return response['Item']['selectedTeam']


def get_standings():
    """
    Get the standings (number of wins to date) for all players.
    Returns a sorted (descending) list of dictionaries with keys
    'userName' and `wins`.
    """
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.scan()
    all_picks = response['Items']

    standings = {}
    for row in all_picks:
        if row['userId'] not in standings:
            standings[row['userId']] = {
                'wins': 0,
                'name': row['userName']
            }

        if 'teamWon' in row and row['teamWon'] > 0:
            standings[row['userId']]['wins'] += 1

    standings = sorted(
        [standings[k] for k in standings],
        key=lambda x: x['wins'], reverse=True
    )

    return standings


def get_who_picked(week_num):
    """
    Returns a list of user names that have made picks for the current week.
    """
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.query(
        IndexName='weekNumber-index',
        KeyConditionExpression=Key('weekNumber').eq(week_num)
    )
    all_picks = response['Items']

    this_week = sorted([pick['userName'] for pick in all_picks])

    return this_week


def get_open_picks():
    """
    Return a list of pick entries where a result has not been recorded.
    """
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.scan()
    all_picks = response['Items']

    return [pick for pick in all_picks if 'teamWon' not in pick]


def submit_pick(user_id, week_num, team, user_name, sr_game_id):
    """
    Log a pick to the database for the given
        USER_ID: Slack user ID,
        WEEK_NUM: The week number for the pick,
        TEAM: The normalized team name from `get_team`,
        USER_NAME: The Slack user name,
        SR_GAME_ID: The sports radar game identifier
    """
    pick_table = dynamo.Table('pickem-picks')
    pick_table.put_item(
        Item={
            'userId': user_id,
            'weekNumber': week_num,
            'selectedTeam': team,
            'userName': user_name,
            'selectionTime': str(datetime.now()),
            'sportRadarGameID': sr_game_id
        }
    )


def get_schedule(week_num):
    """
    Get the scheduled games for the given WEEK_NUM. Returns a list of
    games as dicts, each having a key `scheduled` indicating when the game
    is scheduled to start as a datetime string of format
    '%Y-%m-%dT%H:%M:%S+00:00', and a 'home' and 'away' team listing,
    each dicts with a key 'name' that gives the names of the home and away
    teams.
    """
    ws_url = (
        'https://api.sportradar.us/' +
        'nfl-ot2/games/{:}/REG/' +
        '{:}/schedule.json?api_key={:}'
    ).format(2017, week_num, sr_token)
    ws_response = requests.get(ws_url)
    ws = json.loads(ws_response.text)

    return ws['week']['games']


def update_result(row, outcome):
    """
    For a given pick entry ROW, set the `teamWon` field based on the boolean
    OUTCOME, which is True if the selected team won. Write results to the
    database.
    """

    new_row = dict(row)
    new_row['teamWon'] = 1 if outcome else 0

    pick_table = dynamo.Table('pickem-picks')
    pick_table.put_item(Item=new_row)


def parse_subcommand(command_text):
    """
    Parse the subcommand from the given COMMAND_TEXT, which is everything that
    follows `/pickem`.  The subcommand is the option passed to the command, e.g.
    'pick' in the case of `/pickem pick`.
    """
    return command_text.strip().split()[0].lower()


def parse_options(command_text):
    """
    Parse options passed into the command, e.g. returns 'cards' from the
    command `/pickem pick cards`, where `pickem` is the command, `pick` is the
    subcommand, and cards is the option passed to the subcommand.
    """
    sc = parse_subcommand(command_text)
    return command_text.replace(sc, '').strip()


def receptionist_handler(event, context):

    # WHY ISN'T THIS WORKING!?

    params = parse_qs(event['body'])
    token = params['token'][0]
    if token != slack_token:
        logger.error("Request token (%s) does not match expected", token)
        return respond('Invalid request token', is_error=True)

    command_text = params['text'][0]

    subcommand = parse_subcommand(command_text)

    if subcommand == 'help':
        """Return a help message."""
        return respond(help_text, help_attachment_text)

    elif (subcommand == 'standings' or subcommand == 'record' or
          subcommand == 'pick' or subcommand == 'who'):

        sns = boto3.client('sns')
        sns.publish(
            TopicArn=sns_arn,
            Message=json.dumps({'default': json.dumps(params)}),
            MessageStructure='json'
        )

        return respond("")

    else:
        return respond(
            ":persevere: Invalid command! " + help_text, help_attachment_text
        )


def respond(response_text=None, attachment_text=None,
            in_channel=False, response_url=None, is_error=False):

    if response_text is None and not is_error:
        response_text = ""
    elif response_text is None and is_error:
        response_text = "An unspecified error has occurred!"

    body = {
        'response_type': 'in_channel' if in_channel else 'ephemeral',
        'text': response_text
    }

    if attachment_text:
        body['attachments'] = [{'text': attachment_text, 'mrkdwn_in': ['text']}]

    if response_url is not None:
        requests.post(
            response_url, json=body,
            headers={'Content-Type': 'application/json'}
        )
    else:
        to_return = {
            'statusCode': '400' if is_error else '200',
            'body': response_text if is_error else json.dumps(body)
        }

        if not is_error:
            to_return['headers'] = {
                'Content-Type': 'application/json',
            }

        return to_return


def pickem_handler(event, context):
    """
    Handles the requests from the `/pickem` command to the lambda function
    that is responsible for handling API requests.
    """
    ## Uncomment this when I trigger from SNS
    params = parse_qs(event['body'])

    token = params['token'][0]
    if token != slack_token:
        logger.error("Request token (%s) does not match expected", token)
        return respond('Invalid request token', is_error=True)

    user_name = params['user_name'][0]
    user_id = params['user_id'][0]
    command = params['command'][0]
    channel = params['channel_name'][0]
    command_text = params['text'][0]
    response_url = params['response_url'][0]

    subcommand = parse_subcommand(command_text)
    options = parse_options(command_text)

    week_num = get_current_week()

    if subcommand == 'help':
        """Return a help message."""
        return respond(help_text, help_attachment_text)

    elif subcommand == 'standings':
        """Returns standings in channel for everyone to see."""
        standings = get_standings()

        standings_string = '`{:<10} {:>5}`\n'.format('Name', 'Wins')
        standings_string += '`' + "-"*16 + '`'
        for row in standings:
            standings_string += '\n`{:<10} {:>5}`'.format(
                row['name'], row['wins']
            )

        return respond(
            'Standings as of week {:}'.format(week_num),
            standings_string,
            in_channel=True
        )

    elif subcommand == 'record':
        record = get_user_record(user_id, week_num)

        wins = sum(r['teamWon'] for r in record if 'teamWon' in r)
        # We occassionally gift wins, which are added at negative week number
        actual_wins = sum(
            r['teamWon'] for r in record
            if 'teamWon' in r and r['weekNumber'] > 0
        )
        losses = week_num - 1 - actual_wins

        record_string = "`{:<10} {:<16} {:<10}`\n".format(
            'Week', 'Team', 'Result'
            )
        record_string += "`" + "-"*38 + "`"
        for r in record:
            record_string += "\n`{:<10} {:<16} {:<10}`".format(
                r['weekNumber'], r['selectedTeam'].capitalize(),
                'Win' if 'teamWon' in r and r['teamWon'] > 0 else 'Loss'
            )

        return respond(
            "Your record: {:} wins, {:} losses".format(wins, losses),
            record_string
        )

    elif subcommand == 'pick':

        if week_num > 17:
            return respond(
                "The 2017 season has ended. Thanks for playing!"
            )

        # In case the user has already made a pick this week
        standing_team = get_current_pick(user_id, week_num)

        try:
            team = get_team(options)
        except UnknownTeam:
            return respond(
                ":confused: Sorry, I don't know that team. Try again."
            )
        except NoTeamGiven:
            # Just report the current pick if there is one
            if standing_team is None:
                return respond(
                    ":persevere: You haven't picked a team this week. " +
                    "Try `/pickem pick [team name]`."
                )
            else:
                return respond(
                    "You've picked {:} for this week. Good luck!".format(
                        standing_team.capitalize()
                    )
                )

        record = get_user_record(user_id, week_num)
        team_previously_chosen = False
        previous_week = None
        for r in record:
            if team == r['selectedTeam']:
                team_previously_chosen = True
                previous_week = r['weekNumber']
                break

        if team_previously_chosen:
            return respond(
                ":no_good: You already picked {:} in week {:}. " +
                "Try again.".format(team.capitalize(), previous_week)
            )
        else:
            games = get_schedule(week_num)

            team_playing = False
            standing_team_game_started = False
            game_started = False
            current_time = datetime.utcnow()
            sr_game_id = None
            for game in games:
                away_team = game['away']['name'].split()[-1].lower()
                home_team = game['home']['name'].split()[-1].lower()
                game_time = datetime.strptime(
                    game['scheduled'],
                    '%Y-%m-%dT%H:%M:%S+00:00'
                )

                if away_team == team or home_team == team:
                    team_playing = True
                    if current_time >= game_time:
                        game_started = True
                    sr_game_id = game['id']

                if (
                    standing_team is not None and
                    (
                        away_team == standing_team or
                        home_team == standing_team
                    ) and
                    current_time >= game_time
                ):
                    standing_team_game_started = True

            if standing_team_game_started:
                return respond(
                    ":thumbsdown: The {:} game has started. " +
                    "You can't change your pick now, cheater!".format(
                        standing_team.capitalize()
                    )
                )
            elif not team_playing:
                return respond(
                    ":no_good: The {:} aren't playing this week. " +
                    "Try again.".format(team.capitalize())
                )
            elif game_started:
                return respond(
                    ":thumbsdown: The {:} game has started. " +
                    "You can't pick them now, cheater!".format(
                        team.capitalize()
                    )
                )
            else:
                submit_pick(user_id, week_num, team, user_name, sr_game_id)
                return respond(
                    ":ok_hand: {:} has picked the {:} for week {:}".format(
                        user_name, team.capitalize(), week_num
                    )
                )

    elif subcommand == 'who':
        users = get_who_picked(week_num)

        return respond(
            'Here are the people that have picked so far this week.',
            attachment_text="\n".join(users)
        )

    else:
        return respond(
            ":persevere: Invalid command! " +
            help_text, help_attachment_text
        )


def worker_handler(event, context):
    """
    Handles the requests from the `/pickem` command to the lambda function
    that is responsible for handling API requests.
    """
    ## Uncomment this when I trigger from SNS
    params = json.loads(event['Records'][0]['Sns']['Message'])

    token = params['token'][0]
    if token != slack_token:
        logger.error("Request token (%s) does not match expected", token)
        raise Exception('Invalid request token!')

    user_name = params['user_name'][0]
    user_id = params['user_id'][0]
    command = params['command'][0]
    channel = params['channel_name'][0]
    command_text = params['text'][0]
    response_url = params['response_url'][0]

    subcommand = parse_subcommand(command_text)
    options = parse_options(command_text)

    week_num = get_current_week()

    if subcommand == 'help':
        """Return a help message."""
        return respond(help_text, help_attachment_text,
                       response_url=response_url)

    elif subcommand == 'standings':
        """Returns standings in channel for everyone to see."""
        standings = get_standings()

        standings_string = '`{:<10} {:>5}`\n'.format('Name', 'Wins')
        standings_string += '`' + "-"*16 + '`'
        for row in standings:
            standings_string += '\n`{:<10} {:>5}`'.format(
                row['name'], row['wins']
            )

        return respond(
            'Standings as of week {:}'.format(week_num),
            standings_string,
            in_channel=True, response_url=response_url
        )

    elif subcommand == 'record':
        record = get_user_record(user_id, week_num)

        wins = sum(r['teamWon'] for r in record if 'teamWon' in r)
        # We occassionally gift wins, which are added at negative week number
        actual_wins = sum(
            r['teamWon'] for r in record
            if 'teamWon' in r and r['weekNumber'] > 0
        )
        losses = week_num - 1 - actual_wins

        record_string = "`{:<10} {:<16} {:<10}`\n".format(
            'Week', 'Team', 'Result'
            )
        record_string += "`" + "-"*38 + "`"
        for r in record:
            record_string += "\n`{:<10} {:<16} {:<10}`".format(
                r['weekNumber'], r['selectedTeam'].capitalize(),
                'Win' if 'teamWon' in r and r['teamWon'] > 0 else 'Loss'
            )

        return respond(
            "Your record: {:} wins, {:} losses".format(wins, losses),
            record_string, response_url=response_url
        )

    elif subcommand == 'pick':

        if week_num > 17:
            return respond(
                "The 2017 season has ended. Thanks for playing!",
                response_url=response_url
            )

        # In case the user has already made a pick this week
        standing_team = get_current_pick(user_id, week_num)

        try:
            team = get_team(options)
        except UnknownTeam:
            return respond(
                ":confused: Sorry, I don't know that team. Try again.",
                response_url=response_url
            )
        except NoTeamGiven:
            # Just report the current pick if there is one
            if standing_team is None:
                return respond(
                    ":persevere: You haven't picked a team this week. " +
                    "Try `/pickem pick [team name]`.",
                    response_url=response_url
                )
            else:
                return respond(
                    "You've picked {:} for this week. Good luck!".format(
                        standing_team.capitalize()
                    ),
                    response_url=response_url
                )

        record = get_user_record(user_id, week_num)
        team_previously_chosen = False
        previous_week = None
        for r in record:
            if team == r['selectedTeam']:
                team_previously_chosen = True
                previous_week = r['weekNumber']
                break

        if team_previously_chosen:
            return respond(
                ":no_good: You already picked {:} in week {:}. " +
                "Try again.".format(team.capitalize(), previous_week),
                response_url=response_url
            )
        else:
            games = get_schedule(week_num)

            team_playing = False
            standing_team_game_started = False
            game_started = False
            current_time = datetime.utcnow()
            sr_game_id = None
            for game in games:
                away_team = game['away']['name'].split()[-1].lower()
                home_team = game['home']['name'].split()[-1].lower()
                game_time = datetime.strptime(
                    game['scheduled'],
                    '%Y-%m-%dT%H:%M:%S+00:00'
                )

                if away_team == team or home_team == team:
                    team_playing = True
                    if current_time >= game_time:
                        game_started = True
                    sr_game_id = game['id']

                if (
                    standing_team is not None and
                    (
                        away_team == standing_team or
                        home_team == standing_team
                    ) and
                    current_time >= game_time
                ):
                    standing_team_game_started = True

            if standing_team_game_started:
                return respond(
                    ":thumbsdown: The {:} game has started. " +
                    "You can't change your pick now, cheater!".format(
                        standing_team.capitalize()
                    ),
                    response_url=response_url
                )
            elif not team_playing:
                return respond(
                    ":no_good: The {:} aren't playing this week. " +
                    "Try again.".format(team.capitalize()),
                    response_url=response_url
                )
            elif game_started:
                return respond(
                    ":thumbsdown: The {:} game has started. " +
                    "You can't pick them now, cheater!".format(
                        team.capitalize()
                    ),
                    response_url=response_url
                )
            else:
                submit_pick(user_id, week_num, team, user_name, sr_game_id)
                return respond(
                    ":ok_hand: {:} has picked the {:} for week {:}".format(
                        user_name, team.capitalize(), week_num
                    ),
                    response_url=response_url
                )

    elif subcommand == 'who':
        users = get_who_picked(week_num)

        return respond(
            'Here are the people that have picked so far this week.',
            attachment_text="\n".join(users),
            response_url=response_url
        )

    else:
        return respond(
            ":persevere: Invalid command! " +
            help_text, help_attachment_text, response_url=response_url
        )


def results_update_handler(event, context):
    """
    Run on a schedule to update pick results based on scores from the previous
    week.
    """
    week_num = get_current_week()

    if week_num > 1:
        games = get_schedule(week_num - 1)

        picks = get_open_picks()

        for pick in picks:
            team_won = None

            if 'sportRadarGameID' not in pick:
                continue

            for game in games:

                home_team = game['home']['name'].split()[-1].lower()

                if pick['sportRadarGameID'] == game['id']:
                    if not game['status'] == 'closed':
                        break

                    team_side = 'away'
                    other_side = 'home'
                    if pick['selectedTeam'] == home_team:
                        team_side = 'home'
                        other_side = 'away'

                    if (
                        game['scoring']['{:}_points'.format(team_side)] >
                        game['scoring']['{:}_points'.format(other_side)]
                    ):
                        team_won = True
                    else:
                        team_won = False

            if team_won is not None:
                update_result(pick, team_won)


def send_reminder_handler(event, context):
    """
    Reminds players to make a pick.
    """
    return respond(
        "It's that time! " +
        "Don't forget to make your pick for the week! :football:",
        in_channel=True
    )
