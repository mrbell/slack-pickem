'''
Pick em slack commands
'''

import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import logging
import os
# import requests
from datetime import datetime
import math

from urlparse import parse_qs


class NoTeamGiven(Exception):
    pass


class UnknownTeam(Exception):
    pass


slack_token = os.environ['slackAppToken']
stattleship_token = os.environ['stattleshipToken']

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
    return int(math.floor((test - week_1_start).total_seconds() / 3600.0 / 24.0 / 7.0) + 1)


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

    return record


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


def submit_pick(user_id, week_num, team, user_name):
    pick_table = dynamo.Table('pickem-picks')
    pick_table.put_item(
        Item={
            'userId': user_id,
            'weekNumber': week_num,
            'selectedTeam': team,
            'userName': user_name,
            'selectionTime': str(datetime.now())
        }
    )


def respond(err, res=None):
    return {
        'statusCode': '400' if err else '200',
        'body': err.message if err else json.dumps(res),
        'headers': {
            'Content-Type': 'application/json',
        },
    }


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
        return respond(None, help_text)

    elif subcommand == 'standings':
        standings = get_standings()

        standings_string = '`{:<10} {:>5}`\n`'.format('Name', 'Wins')
        standings_string += "-"*16 + '`'
        for row in standings:
            standings_string += '\n`{:<10} {:>5}`'.format(row['name'], row['wins'])

        return respond(
            None, {
                'response_type': 'in_channel',
                'text': 'Standings as of week {:} are:\n\n{:}'.format(week_num, standings_string)
            }
        )

    elif subcommand == 'record':
        return respond(None, "No record yet!")

    elif subcommand == 'pick':
        try:
            team = get_team(command_text)
        except UnknownTeam:
            return respond(None, ":confused: I don't know that team. Try again.")
        except NoTeamGiven:
            return respond(
                None,
                ":persevere: You didn't tell me which team you want to pick. Try `/pickem pick [team name]`."
            )

        # TODO: If it's a negative week, or above week 17, return an appropriate response

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
                ":no_good: You already picked {:} in week {:}.".format(team.capitalize(), previous_week)
            )
        else:
            # TODO: Make sure the team is actually playing this week!
            # TODO: Make sure the game for the current selection or new selection hasn't started yet!
            submit_pick(user_id, week_num, team, user_name)
            return respond(
                None,
                ":ok_hand: OK, you've picked the {:} for week {:}".format(
                    team.capitalize(), week_num
                )
            )

    else:
        return respond(None, ":persevere: Invalid command! " + help_text)
