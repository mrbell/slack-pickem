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


class NoTeamGiven(Exception):
    pass


class UnknownTeam(Exception):
    pass


slack_token = os.environ['slackAppToken']
sr_token = os.environ['sportRadarToken']

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamo = boto3.resource('dynamodb')

help_text = (
    "Use this command to manage your pick'em selections.  "
    "Either `pick [team]` to make a pick, `record` to check your record, " +
    "or `standings` to check standings."
)

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

teams = ['jets', 'giants', 'rams', 'chargers']
for k in locs_to_teams:
    if isinstance(locs_to_teams[k], basestring):
        teams.append(locs_to_teams[k])
    else:
        teams.extend(locs_to_teams[k])
teams = set(teams)

loc_aliases = {
    'ne': 'england',
    'philly': 'philadelphia',
    'sf': 'francisco',
    'tampa': 'tampa',
    'pitt': 'pittsburgh',
    'nola': 'orleans',
    'indy': 'indianapolis',
    'cinci': 'cincinnati',
    'kc': 'kansas'
}

team_aliases = {
    'cards': 'cardinals',
    'jags': 'jaguars',
    'pats': 'patriots',
    'niners': '49ers',
    'skins': 'redskins',
    'bucs': 'buccaneers'
}

week_1_start = datetime(2017, 9, 5)


def get_current_week():
    test = datetime.today()
    return max([
        1,
        int(math.floor((test - week_1_start).total_seconds() / 3600.0 / 24.0 / 7.0) + 1)
    ])


def get_team(command_text):
    tmp = command_text.strip().split()
    if len(tmp) == 1:
        raise NoTeamGiven()

    team_choice = (
        " ".join(tmp[1:])
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
        if team is not None:
            break

    if team is None:
        raise UnknownTeam()

    return team


def get_user_record(user_id, week_num):
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.query(
        KeyConditionExpression=Key('userId').eq(user_id) & Key('weekNumber').lt(week_num)
    )
    record = response['Items']

    return sorted(record, key=lambda x: x['weekNumber'])


def get_current_pick(user_id, week_num):
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.get_item(
        Key={'userId': user_id, 'weekNumber': week_num}
    )

    if 'Item' not in response:
        return None
    else:
        return response['Item']['selectedTeam']


def get_standings():
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

    standings = sorted([standings[k] for k in standings], key=lambda x: x['wins'], reverse=True)

    return standings


def get_open_picks():
    pick_table = dynamo.Table('pickem-picks')
    response = pick_table.scan()
    all_picks = response['Items']

    return [pick for pick in all_picks if 'teamWon' not in pick]


def submit_pick(user_id, week_num, team, user_name, sr_game_id):
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


def respond(err, res=None, attachment_text=None, in_channel=False):

    body = {
        'response_type': 'in_channel' if in_channel else 'ephemeral',
        'text': res
    }

    if attachment_text:
        body['attachments'] = [{'text': attachment_text, 'mrkdwn_in': ['text']}]

    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(body),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


def get_schedule(week_num):
    ws_url = (
        'https://api.sportradar.us/' +
        'nfl-ot2/games/{:}/REG/' +
        '{:}/schedule.json?api_key={:}'
    ).format(2017, week_num, sr_token)
    ws_response = requests.get(ws_url)
    ws = json.loads(ws_response.text)

    return ws['week']['games']


def update_result(row, outcome):

    new_row = dict(row)
    new_row['teamWon'] = 1 if outcome else 0

    pick_table = dynamo.Table('pickem-picks')
    pick_table.put_item(Item=new_row)


def pickem_handler(event, context):
    params = parse_qs(event['body'])
    token = params['token'][0]
    if token != slack_token:
        logger.error("Request token (%s) does not match expected", token)
        return respond(Exception('Invalid request token'))

    user_name = params['user_name'][0]
    user_id = params['user_id'][0]
    command = params['command'][0]
    channel = params['channel_name'][0]
    command_text = params['text'][0]

    subcommand = command_text.strip().split()[0].lower()

    week_num = get_current_week()

    if subcommand == 'help':
        """Return a help message."""
        return respond(None, help_text)

    elif subcommand == 'standings':
        """Returns standings in channel for everyone to see."""
        standings = get_standings()

        standings_string = '`{:<10} {:>5}`\n'.format('Name', 'Wins')
        standings_string += '`' + "-"*16 + '`'
        for row in standings:
            standings_string += '\n`{:<10} {:>5}`'.format(row['name'], row['wins'])

        return respond(
            None,
            'Standings as of week {:}'.format(week_num),
            standings_string,
            True
        )

    elif subcommand == 'record':
        record = get_user_record(user_id, week_num)

        wins = sum(r['teamWon'] for r in record if 'teamWon' in r)
        losses = week_num - 1 - wins

        record_string = "`{:<10} {:<16} {:<10}`\n".format('Week', 'Team', 'Result')
        record_string += "`" + "-"*38 + "`"
        for r in record:
            record_string += "\n`{:<10} {:<16} {:<10}`".format(
                r['weekNumber'], r['selectedTeam'].capitalize(),
                'Win' if 'teamWon' in r and r['teamWon'] > 0 else 'Loss'
            )

        return respond(
            None,
            "Your record: {:} wins, {:} losses".format(wins, losses),
            record_string
        )

    elif subcommand == 'pick':

        if week_num > 17:
            return respond(None, "The 2017 season has ended. Thanks for playing!")

        # In case the user has already made a pick this week
        standing_team = get_current_pick(user_id, week_num)

        try:
            team = get_team(command_text)
        except UnknownTeam:
            return respond(None, ":confused: Sorry, I don't know that team. Try again.")
        except NoTeamGiven:
            # Just report the current pick if there is one
            if standing_team is None:
                return respond(
                    None,
                    ":persevere: You haven't picked a team this week. Try `/pickem pick [team name]`."
                )
            else:
                return respond(
                    None,
                    "You've picked {:} for this week. Good luck!".format(standing_team.capitalize())
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
                None,
                ":no_good: You already picked {:} in week {:}. Try again.".format(team.capitalize(), previous_week)
            )
        else:
            games = get_schedule(week_num)

            team_playing = False
            standing_team_game_started = False
            game_started = False
            current_time = datetime(2017, 9, 8, 1)  # datetime.utcnow()
            sr_game_id = None
            for game in games:
                away_team = game['away']['name'].split()[-1].lower()
                home_team = game['home']['name'].split()[-1].lower()
                game_time = datetime.strptime(game['scheduled'], '%Y-%m-%dT%H:%M:%S+00:00')

                if away_team == team or home_team == team:
                    team_playing = True
                    if current_time >= game_time:
                        game_started = True
                    sr_game_id = game['id']

                if (
                    standing_team is not None and
                    (away_team == standing_team or home_team == standing_team) and
                    current_time >= game_time
                ):
                    standing_team_game_started = True

            if standing_team_game_started:
                return respond(
                    None,
                    ":thumbsdown: The {:} game has started. You can't change your pick now, cheater!".format(
                        standing_team.capitalize()
                    )
                )
            elif not team_playing:
                return respond(
                    None,
                    ":no_good: The {:} aren't playing this week. Try again.".format(team.capitalize())
                )
            elif game_started:
                return respond(
                    None,
                    ":thumbsdown: The {:} game has started. You pick them now, cheater!".format(team.capitalize())
                )
            else:
                submit_pick(user_id, week_num, team, user_name, sr_game_id)
                return respond(
                    None,
                    ":ok_hand: {:} has picked the {:} for week {:}".format(
                        user_name, team.capitalize(), week_num
                    ),
                    in_channel=True
                )

    else:
        return respond(None, ":persevere: Invalid command! " + help_text)


def results_update_handler(event, context):
    week_num = get_current_week()

    if week_num > 1:
        games = get_schedule(week_num - 1)

        picks = get_open_picks()

        for pick in picks:
            team_won = None

            if 'sportRadarGameID' not in pick:
                continue

            for game in games:
                if pick['sportRadarGameID'] == game['id']:
                    if not game['status'] == 'closed':
                        break

                    team_side = 'away'
                    other_side = 'home'
                    if pick['selectedTeam'] == game['home']['name'].split()[-1].lower():
                        team_side = 'home'
                        other_side = 'away'

                    if game['scoring']['{:}_points'.format(team_side)] > game['scoring']['{:}_points'.format(other_side)]:
                        team_won = True
                    else:
                        team_won = False

            if team_won is not None:
                update_result(pick, team_won)
