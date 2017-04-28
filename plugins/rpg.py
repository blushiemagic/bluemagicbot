import random
import re
import operator

from time import time
from collections import defaultdict
from sqlalchemy import Table, Column, String, Integer, PrimaryKeyConstraint, desc
from sqlalchemy.sql import select
from cloudbot import hook
from cloudbot.event import EventType
from cloudbot.util import database

table = Table(
    'rpg',
    database.metadata,
    Column('network', String),
    Column('chan', String),
    Column('name', String),
    Column('level', Integer),
    Column('exp', Integer),
    Column('hp_lost', Integer),
    Column('deaths', Integer),
    PrimaryKeyConstraint('name', 'chan', 'network')
    )

captures_table = Table(
    'rpgcaptures',
    database.metadata,
    Column('network', String),
    Column('chan', String),
    Column('name', String),
    Column('monster', Integer),
    Column('captures', Integer),
    PrimaryKeyConstraint('name', 'chan', 'network', 'monster')
    )

optout = Table(
    'nohunt',
    database.metadata,
    Column('network', String),
    Column('chan', String),
    PrimaryKeyConstraint('chan', 'network')
    )



"""
game_status structure 
{ 
    'network':{
        '#chan1':{
            'monster_status':STATUS_WAITING|STATUS_ACTIVE|STATUS_DONE, 
            'monster1':'integer',
            'monster2':'integer',
            'monster3':'integer',
            'max_level':'integer',
            'next_monster_time':'integer',
            'next_monster_idle_time': 'integer',
            'game_on':0|1,
            'disabled':0|1,
            'monster_time': 'float', 
            'idle_monster': 0|1,
            'attack_time': 'float',
            'messages': integer,
            'masks' : list,
            'longmode': 0|1,
            'failures': integer
        }
    }
}
"""

STATUS_WAITING = 0
STATUS_ACTIVE = 1
STATUS_DONE = 2

MIN_WAIT_TIME = 600
MAX_WAIT_TIME = 3600

MIN_IDLE_TIME = 21600
MAX_IDLE_TIME = 43200

MSG_DELAY = 20
MASK_REQ = 3

MAX_FAILURES = 5

scripters = defaultdict(int)
freeze = 1
game_status = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

monsters = ['Green Slime', 'Bat', 'Spider', 'Bee Swarm', 'Bear',\
        'Living Tree', 'Goblin', 'Blue Slime', 'Robot', 'r/nosleep Trope',\
        'Earth Spirit', 'Giant Bat', 'Skeleton', 'Wolf', 'Golem',\
        'Red Slime', 'Ghost', 'Gray Goblin', 'Giant Rat', 'Not Slime',\
        'Ice Bat', 'Frost Spider', 'Ice Slime', 'Polar Bear', 'Snow Spirit',\
        'Frozen Golem', 'White Goblin', 'Frost Ghoul', 'White Wolf', 'Yeti']
monsters_short = ['Grn Slm', 'Bat', 'Spider', 'Bees', 'Bear',\
        'LiveTree', 'Goblin', 'Blu Slm', 'Robot', 'NS Trope',\
        'LandSprt', 'Big Bat', 'Skeleton', 'Wolf', 'Golem',\
        'Red Slm', 'Ghost', 'GrayGbln', 'Big Rat', 'Not Slm',\
        'Ice Bat', 'FrstSpdr', 'Ice Slm', 'Plr Bear', 'SnowSprt',\
        'Frzn Glm', 'Wht Gbln', 'SnwGhoul', 'Wht Wolf', 'Yeti']
faces = ['(☼Д☼)', 'Ψ(☆ｗ☆)Ψ']

monster_info = [\
        '', \
        '', \
        '', \
        '', \
        '', \
        '', \
        '', \
        '', \
        '', \
        '' \
]

miss_msg = ['but you tripped and left yourself defenseless',\
        'but it was just too strong',\
        'but forgot monsters can attack you too']
capture_miss_msg = ['but you tripped and left yourself defenseless',\
        'but it was just too string',\
        'but forgot monsters won\'t just sit there calmly']
too_fast = 'You were rushing so fast that you tripped and were mauled to death by all 3 monsters. Then a meteor fell from the sky and landed on your corpse.'

def get_monster_level(monster_index):
    return 2 * monster_index + 1

def get_monster_index(monster_level):
    return (monster_level - 1) // 2


@hook.on_start()
def load_optout(db):
    """load a list of channels duckhunt should be off in. Right now I am being lazy and not
    differentiating between networks this should be cleaned up later."""
    global opt_out
    opt_out = []
    chans = db.execute(select([optout.c.chan]))
    if chans:
        for row in chans:
            chan = row["chan"]
            opt_out.append(chan)

@hook.event([EventType.message, EventType.action], singlethread=True)
def incrementMsgCounter(event, conn, chan, nick, db):
    """Increment the number of messages said in an active game channel. Also keep track of the unique masks that are speaking."""
    global game_status
    if freeze > 0:
        return
    if event.chan in opt_out:
        return
    if nick == 'gonzobot' or nick.lower() == 'subwatch':
        return
    if game_status[conn.name][chan]['disabled'] == 0 and game_status[conn.name][chan]['monster_status'] == STATUS_WAITING:
        game_status[conn.name][chan]['messages'] += 1
        level = get_player_level(db, conn.name, chan, nick)
        if level and level > game_status[conn.name][chan]['max_level']:
            game_status[conn.name][chan]['max_level'] = level
        if event.host not in game_status[conn.name][chan]['masks']:
            game_status[conn.name][chan]['masks'].append(event.host)

@hook.command("start", autohelp=False)
def start_game(bot, chan, message, conn):
    """This command starts an RPG game in your channel, to stop the hunt use ,stop"""
    global game_status
    if freeze > 0:
        return
    if chan in opt_out:
        return
    elif not chan.startswith("#"):
        return "No playing by yourself, that isn't safe."
    check = game_status[conn.name][chan]['game_on']
    if check:
        return "There is already a game running in {}.".format(chan)
    else:
        game_status[conn.name][chan]['game_on'] = 1
        game_status[conn.name][chan]['disabled'] = 0
    set_monster_time(chan, conn)
    longmode = game_status[conn.name][chan]['longmode']
    if longmode:
        message("Monsters have been spotted nearby. See how much EXP you can earn. Type \",instructions\" for more info.", chan)
    else:
        message("Game started! Type \",instructions\" for help.", chan)

@hook.command("longmode", autohelp=False)
def longmode(text, bot, chan, message, conn):
    """This command enables or disables long-text mode."""
    global game_status
    if chan in opt_out:
        return
    elif not chan.startswith("#"):
        return "No playing by yourself, that isn't safe."
    check = not game_status[conn.name][chan]['disabled']
    if not check:
        return "There is no game running in {}.".format(chan)
    check = game_status[conn.name][chan]['longmode']
    if not text:
        return "Long-text mode is currently {}".format("enabled" if check else "disabled")
    elif text == "enable":
        if check:
            return "Long-text mode is already enabled"
        else:
            game_status[conn.name][chan]['longmode'] = 1
            return "Enabled long-text mode"
    elif text == "disable":
        if check:
            game_status[conn.name][chan]['longmode'] = 0
            return "Disabled long-text mode"
        else:
            return "Long-text mode is already disabled"

def set_monster_time(chan, conn):
    global game_status
    game_status[conn.name][chan]['next_monster_time'] = random.randint(int(time()) + MIN_WAIT_TIME, int(time()) + MAX_WAIT_TIME)
    game_status[conn.name][chan]['next_monster_idle_time'] = int(time()) + random.randint(MIN_IDLE_TIME, MAX_IDLE_TIME)
    #game_status[conn.name][chan]['flyaway'] = game_status[conn.name][chan]['next_monster_time'] + 600
    game_status[conn.name][chan]['max_level'] = 1
    game_status[conn.name][chan]['monster_status'] = STATUS_WAITING
    # let's also reset the number of messages said and the list of masks that have spoken.
    game_status[conn.name][chan]['messages'] = 0
    game_status[conn.name][chan]['masks'] = []
    game_status[conn.name][chan]['failures'] = 0
    return

@hook.command("stop", autohelp=False)
def stop_game(chan, conn):
    """This command stops the RPG game in your channel. Scores will be preserved"""
    global game_status
    if chan in opt_out:
        return
    if not game_status[conn.name][chan]['disabled']:
        game_status[conn.name][chan]['game_on'] = 0
        game_status[conn.name][chan]['disabled'] = 1
        return "The game has been stopped."
    else:
        return "There is no game running in {}.".format(chan)

def generate_monsters(conn, chan):
    """Try and randomize the duck message so people can't highlight on it/script against it."""
    global game_status
    monster1 = 0
    max_level = game_status[conn.name][chan]['max_level'] + 1
    monster3 = get_monster_index(random.randint(1, max_level))
    if monster3 >= len(monsters):
        monster3 = len(monsters) - 1
    monster2 = (monster1 + monster3) // 2
    game_status[conn.name][chan]['monster1'] = monster1
    game_status[conn.name][chan]['monster2'] = monster2
    game_status[conn.name][chan]['monster3'] = monster3
    longmode = game_status[conn.name][chan]['longmode']
    message = "{}  {}, {}, and {} have appeared!".format(\
        random.choice(faces), \
        monster_name(monster1, longmode), \
        monster_name(monster2, longmode), \
        monster_name(monster3, longmode))
    conn.message(chan, anti_cheat(message))

def do_nothing(msg, chan):
    pass

@hook.periodic(11, initial_interval=11)
def deploy_monsters(message, bot):
    global game_status, freeze
    if freeze > 0:
        freeze -= 1
        if freeze == 0:
            for network in bot.connections:
                conn = bot.connections[network]
                if not conn.ready:
                    continue
                for chan in conn.channels:
                    start_game(bot, chan.lower(), do_nothing, conn)
        return
    for network in game_status:
        if network not in bot.connections:
            continue
        conn = bot.connections[network]
        if not conn.ready:
            continue
        for chan in game_status[network]:
            if game_status[network][chan]['disabled'] == 0 and game_status[network][chan]['game_on'] == 0:
                start_game(bot, chan, do_nothing, conn)
            active = not game_status[network][chan]['disabled']
            monster_status = game_status[network][chan]['monster_status']
            next_monster = game_status[network][chan]['next_monster_time']
            next_idle = game_status[network][chan]['next_monster_idle_time']
            chan_messages = game_status[network][chan]['messages']
            chan_masks = game_status[network][chan]['masks']
            if active == 1 and monster_status == STATUS_WAITING and next_monster <= time() and chan_messages >= MSG_DELAY and len(chan_masks) >= MASK_REQ:
                #deploy a duck to channel
                game_status[network][chan]['monster_status'] = STATUS_ACTIVE
                game_status[network][chan]['monster_time'] = time()
                game_status[network][chan]['idle_monster'] = 0
                generate_monsters(conn, chan)
            elif active == 1 and monster_status == STATUS_WAITING and next_idle <= time():
                game_status[network][chan]['monster_status'] = STATUS_ACTIVE
                game_status[network][chan]['monster_time'] = time()
                game_status[network][chan]['idle_monster'] = 1
                generate_monsters(conn, chan)
            if active == 1 and monster_status == STATUS_ACTIVE:
                monster_time = game_status[network][chan]['monster_time']
                attack_time = game_status[network][chan]['attack_time']
                dismiss_time = 300
                if game_status[network][chan]['idle_monster'] == 1:
                    dismiss_time = 3600
                if time() > max(monster_time, attack_time) + dismiss_time:
                    dismiss_monsters(conn, chan, "The monsters got bored and left.")
            continue
        continue

def dismiss_monsters(conn, chan, message):
    global game_status
    conn.message(chan, message)
    game_status[conn.name][chan]['monster_status'] = STATUS_DONE
    set_monster_time(chan, conn)


def hit_or_miss(db, network, chan, name, deploy, attack, monster_level, level):
    """This function calculates if the attack will be successful."""
    if attack - deploy < 1:
        return -1
    elif attack - deploy < 7:
        out = 0.7
    else:
        out = 0.9
    if monster_level > 20:
        monster_level += 1
    captures = get_player_captures(db, network, chan, name, get_monster_index(monster_level))
    if captures > 0:
        level += captures
    if monster_level < level:
        out = 1 - out
        out *= 0.7 ** (level - monster_level)
        out = 1 - out
    elif monster_level > level:
        out *= 0.7 ** (monster_level - level)
    return out

def capture_or_miss(deploy, attack, monster_level, level):
    """This function calculates if the capture will be successful."""
    if attack - deploy < 1:
        return -1
    elif attack - deploy < 7:
        out = 0.7
    else:
        out = 0.9
    if monster_level < level:
        out = 1 - out
        out *= 0.7 ** (level - monster_level)
        out = 1 - out
    elif monster_level > level:
        out *= 0.7 ** (monster_level - level)
    return out

def dbadd_entry(nick, chan, db, conn, level, exp, hp_lost, deaths):
    """Takes care of adding a new row to the database."""
    query = table.insert().values(
        network = conn.name,
        chan = chan.lower(),
        name = nick.lower(),
        level = level,
        exp = exp,
        hp_lost = hp_lost,
        deaths = deaths)
    db.execute(query)
    db.commit()

def dbupdate_progress(nick, chan, db, conn, level, exp):
    """update a db row"""
    query = table.update() \
        .where(table.c.network == conn.name) \
        .where(table.c.chan == chan.lower()) \
        .where(table.c.name == nick.lower()) \
        .values(level = level) \
        .values(exp = exp)
    db.execute(query)
    db.commit()

def dbupdate_loss(nick, chan, db, conn, hp_lost, deaths):
    """update a db row"""
    query = table.update() \
        .where(table.c.network == conn.name) \
        .where(table.c.chan == chan.lower()) \
        .where(table.c.name == nick.lower()) \
        .values(hp_lost = hp_lost) \
        .values(deaths = deaths)
    db.execute(query)
    db.commit()

def dbadd_capture(nick, chan, db, conn, monster, captures):
    query = captures_table.insert().values(
        network = conn.name,
        chan = chan.lower(),
        name = nick.lower(),
        monster = monster,
        captures = captures)
    db.execute(query)
    db.commit()

def dbupdate_captures(nick, chan, db, conn, monster, captures):
    """update a db row"""
    query = captures_table.update() \
        .where(captures_table.c.network == conn.name) \
        .where(captures_table.c.chan == chan.lower()) \
        .where(captures_table.c.name == nick.lower()) \
        .where(captures_table.c.monster == monster) \
        .values(captures = captures)
    db.execute(query)
    db.commit()

@hook.command("attack", "atk", autohelp=False)
def attack(text, nick, chan, message, db, conn, notice):
    """When there are monsters on the loose use ,attack or ,atk to attack one. By default, this command will attack the highest-level monster that is not stronger than you. To attack a specific monster, use ',attack 1', ',attack 2', or ,attack 3'."""
    global game_status, scripters
    if freeze > 0:
        return
    if chan in opt_out:
        return
    network = conn.name
    score = ""
    out = ""
    if game_status[network][chan]['disabled']:
        return "There is no active game right now. Use ,start to start a game."
    elif game_status[network][chan]['monster_status'] != STATUS_ACTIVE:
        return "There are no monsters. You attacked thin air like a madman."
    else:
        game_status[network][chan]['attack_time'] = time()
        deploy = game_status[network][chan]['monster_time']
        attack = game_status[network][chan]['attack_time']
        if nick.lower() in scripters:
            if scripters[nick.lower()] > attack:
                notice("You are in a cool down period, you can try again in {} seconds.".format(str(scripters[nick.lower()] - attack)))
                return
        stats = get_player_info(db, network, chan, nick)
        if stats:
            level = stats['level']
        else:
            level = 1
        monster = choose_monster(network, chan, level, text)
        chance = hit_or_miss(db, network, chan, nick, deploy, attack, get_monster_level(monster), level)
        if chance >= 0 and not random.random() < chance:
            scripters[nick.lower()] = attack
            out = miss(db, conn, chan, nick, stats, monster)
            game_status[network][chan]['failures'] += 1
            if game_status[network][chan]['failures'] >= MAX_FAILURES:
                dismiss_monsters(conn, chan, "The monsters left triumphantly.")
            return out
        if chance == -1:
            scripters[nick.lower()] = attack
            out = too_fast + " " + kill(db, conn, chan, nick, stats)
            return out
        game_status[network][chan]['monster_status'] = STATUS_DONE
        out = hit(db, conn, chan, nick, stats, monster, attack - deploy)
        set_monster_time(chan, conn)
        return out

@hook.command("capture", "cap", autohelp=False)
def capture(text, nick, chan, message, db, conn, notice):
    """When there are monsters on the loose use ,capture or ,cap to capture one. By default, this command will capture the highest-level monster that is not stronger than you. To capture a specific monster, use ',capture 1', ',capture 2', or ,capture 3'. Capturing has a low success rate."""
    global game_status, scripters
    if freeze > 0:
        return
    if chan in opt_out:
        return
    network = conn.name
    score = ""
    out = ""
    if game_status[network][chan]['disabled']:
        return "There is no active game right now. Use ,start to start a game."
    elif game_status[network][chan]['monster_status'] != STATUS_ACTIVE:
        return "There are no monsters. What are you even trying to capture?"
    else:
        game_status[network][chan]['attack_time'] = time()
        deploy = game_status[network][chan]['monster_time']
        attack = game_status[network][chan]['attack_time']
        if nick.lower() in scripters:
            if scripters[nick.lower()] > attack:
                notice("You are in a cool down period, you can try again in {} seconds.".format(str(scripters[nick.lower()] - attack)))
                return
        stats = get_player_info(db, network, chan, nick)
        if stats:
            level = stats['level']
        else:
            level = 1
        monster = choose_monster(network, chan, level, text)
        chance = capture_or_miss(deploy, attack, get_monster_level(monster), level)
        if chance >= 0 and not random.random() < chance:
            scripters[nick.lower()] = attack
            out = capture_miss(db, conn, chan, nick, stats, monster)
            game_status[network][chan]['failures'] += 1
            if game_status[network][chan]['failures'] >= MAX_FAILURES:
                dismiss_monsters(conn, chan, "The monsters left triumphantly.")
            return out
        if chance == -1:
            scripters[nick.lower()] = attack
            out = too_fast + " " + kill(db, conn, chan, nick, stats)
            return out
        game_status[network][chan]['monster_status'] = STATUS_DONE
        message(capture_success(db, conn, chan, nick, monster, attack - deploy))
        set_monster_time(chan, conn)

def choose_monster(network, chan, level, text):
    if text:
        text = text.split()[0]
    if text == '1':
        return game_status[network][chan]['monster1']
    elif text == '2':
        return game_status[network][chan]['monster2']
    elif text == '3':
        return game_status[network][chan]['monster3']
    elif get_monster_level(game_status[network][chan]['monster3']) <= level:
        return game_status[network][chan]['monster3']
    elif get_monster_level(game_status[network][chan]['monster2']) <= level:
        return game_status[network][chan]['monster2']
    else:
        return game_status[network][chan]['monster1']

def miss(db, conn, chan, nick, stats, monster):
    global scripters
    monster_level = get_monster_level(monster)
    if stats:
        hp_lost = stats['hp_lost'] + get_monster_atk(monster_level)
        max_hp = get_max_hp(stats['level'])
    else:
        hp_lost = get_monster_atk(monster_level)
        max_hp = get_max_hp(1)
    longmode = game_status[conn.name][chan]['longmode']
    if longmode:
        out = "You attacked {}, {}. You took {} damage!".format(monster_name(monster, True), random.choice(miss_msg), monster_level)
    else:
        out = "You attacked {}, but took {} damage!".format(monster_name(monster, False), monster_level)
    if hp_lost >= max_hp:
        return out + " " + kill(db, conn, chan, nick, stats)
    else:
        if stats:
            dbupdate_loss(nick, chan, db, conn, hp_lost, stats['deaths'])
        else:
            dbadd_entry(nick, chan, db, conn, 1, 0, hp_lost, 0)
        scripters[nick.lower()] += 7
        if longmode:
            out += " You have {}/{} HP left. You can try again in 7 seconds.".format(max_hp - hp_lost, max_hp)
        else:
            out += " Cooldown: 7 seconds"
        return out

def capture_miss(db, conn, chan, nick, stats, monster):
    global scripters
    monster_level = get_monster_level(monster)
    if stats:
        hp_lost = stats['hp_lost'] + get_monster_atk(monster_level)
        max_hp = get_max_hp(stats['level'])
    else:
        hp_lost = get_monster_atk(monster_level)
        max_hp = get_max_hp(1)
    longmode = game_status[conn.name][chan]['longmode']
    if longmode:
        out = "You tried to capture {}, {}. You took {} damage!".format(monster_name(monster, True), random.choice(capture_miss_msg), monster_level)
    else:
        out = "You tried to capture {}, but took {} damage!".format(monster_name(monster, False), monster_level)
    if hp_lost >= max_hp:
        return out + " " + kill(db, conn, chan, nick, stats)
    else:
        if stats:
            dbupdate_loss(nick, chan, db, conn, hp_lost, stats['deaths'])
        else:
            dbadd_entry(nick, chan, db, conn, 1, 0, hp_lost, 0)
        scripters[nick.lower()] += 7
        if longmode:
            out += " You have {}/{} HP left. You can try again in 7 seconds.".format(max_hp - hp_lost, max_hp)
        else:
            out += " Cooldown: 7 seconds"
        return out

def kill(db, conn, chan, nick, stats):
    global scripters
    if stats:
        dbupdate_loss(nick, chan, db, conn, 0, stats['deaths'] + 1)
    else:
        dbadd_entry(nick, chan, db, conn, 1, 0, 0, 1)
    scripters[nick.lower()] += 7200
    longmode = game_status[conn][chan]['longmode']
    if longmode:
        return 'You died and must wait 2 hours to revive.'
    else:
        return 'You died! Cooldown: 2 hours'

def hit(db, conn, chan, nick, stats, monster, time):
    monster_level = get_monster_level(monster)
    if stats:
        exp = stats['exp'] + monster_level
        level = stats['level']
    else:
        exp = monster_level
        level = 1
    max_exp = get_max_exp(level)
    level_up = False
    while exp >= max_exp:
        level += 1
        exp -= max_exp
        max_exp = get_max_exp(level)
        level_up = True
    longmode = game_status[conn.name][chan]['longmode']
    if longmode:
        out = "You slew {} in {:.3f} seconds!".format(monster_name(monster, True), time)
    else:
        out = "Slew {} in {:.3f} seconds!".format(monster_name(monster, False), time)
    if longmode:
        out += " You gained {} EXP.".format(monster_level)
    if level_up:
        if longmode:
            out += " You leveled up to {}!".format(level)
        else:
            out += " Leveled up to {}!".format(level)
    if longmode:
        out += " You now have {}/{} EXP.".format(exp, max_exp)
    else:
        out += " You have {}/{} EXP.".format(exp, max_exp)
    if longmode:
        out += " The rest of the monsters fled."
    else:
        out += " The other monsters fled."
    if stats:
        dbupdate_progress(nick, chan, db, conn, level, exp)
    else:
        dbadd_entry(nick, chan, db, conn, level, exp, 0, 0)
    return out

def capture_success(db, conn, chan, nick, monster, time):
    captures = get_player_captures(db, conn.name, chan, nick, monster) + 1
    longmode = game_status[conn.name][chan]['longmode']
    if longmode:
        out = "You captured {} in {:.3f} seconds!".format(monster_name(monster, True), time)
        out += " You now have {} of {}.".format(captures, monster_name(monster, True))
        out += " The rest of the monsters fled."
    else:
        out = "Captured {} in {:.3f} seconds!".format(monster_name(monster, False), time)
        out += " You have {}.".format(captures)
        out += " The other monsters fled."
    if captures > 1:
        dbupdate_captures(nick, chan, db, conn, monster, captures)
    else:
        dbadd_capture(nick, chan, db, conn, monster, captures)
    return out

def anti_cheat(content):
    words = content.split(' ')
    result = ""
    for word in words:
        formatted = word
        if len(formatted) > 1:
            index = random.randint(1, len(formatted) - 1)
            formatted = formatted[:index] + u'\u200b' + formatted[index:]
        result += formatted + " "
    if len(words) > 0:
        result = result[:-1]
    return result

def smart_truncate(content, length=320, suffix='...'):
    if len(content) <= length:
        return content
    else:
        return content[:length].rsplit(' • ', 1)[0]+suffix


@hook.command("leaderboard", "leader", "leaders", "lead", "leads", autohelp=False)
def leaderboard(text, chan, conn, db):
    """Prints a list of the top duck killers in the channel."""
    if freeze > 0:
        return
    if chan in opt_out:
        return
    players = defaultdict(tuple)
    out = "Leaderboard for {}: ".format(chan)
    scores = db.execute(select([table.c.name, table.c.level, table.c.exp]) \
        .where(table.c.network == conn.name) \
        .where(table.c.chan == chan.lower()) \
        .order_by(desc(table.c.level), desc(table.c.exp)))
    if scores:
        for row in scores:
            if row[1] == 1 and row[2] == 0:
                continue
            players[row[0]] = (row[1], row[2])
    if len(players) == 0:
        return "It appears no one has slayed any monster yet."

    topkillers = sorted(players.items(), key=operator.itemgetter(1), reverse = True)
    out += ' • '.join(["{}: LVL {} ({} EXP)".format('\x02' + k[:1] + u'\u200b' + k[1:] + '\x02', str(v[0]), str(v[1]))  for k, v in topkillers])
    out = smart_truncate(out)
    return out

@hook.command("deaths", autohelp=False)
def deaths_leaderboard(text, chan, conn, db):
    """Prints a list of the top duck killers in the channel."""
    if freeze > 0:
        return
    if chan in opt_out:
        return
    players = defaultdict(tuple)
    out = "Top deaths for {}: ".format(chan)
    scores = db.execute(select([table.c.name, table.c.deaths]) \
        .where(table.c.network == conn.name) \
        .where(table.c.chan == chan.lower()) \
        .order_by(desc(table.c.deaths)))
    if scores:
        for row in scores:
            if row[1] == 0:
                continue
            players[row[0]] = row[1]
    if len(players) == 0:
        return "It appears no one has died yet."

    topkillers = sorted(players.items(), key=operator.itemgetter(1), reverse = True)
    out += ' • '.join(["{}: {}".format('\x02' + k[:1] + u'\u200b' + k[1:] + '\x02', str(v))  for k, v in topkillers])
    out = smart_truncate(out)
    return out

@hook.command("forgive", permissions=["op", "ignore"])
def forgive(text):
    """Allows people to be removed from the mandatory cooldown period."""
    global scripters
    if text.lower() in scripters and scripters[text.lower()] > time():
        scripters[text.lower()] = 0
        return "{} has been removed from the mandatory cooldown period.".format(text)
    else:
        return "I couldn't find anyone banned from the hunt by that nick"

@hook.command("opt_out", permissions=["op", "ignore"], autohelp=False)
def game_opt_out(text, chan, db, conn):
    """Running this command without any arguments displays the status of the current channel. hunt_opt_out add #channel will disable all duck hunt commands in the specified channel. hunt_opt_out remove #channel will re-enable the game for the specified channel."""
    if not text:
        if chan in opt_out:
            return "RPG game is disabled in {}. To re-enable it run ,opt_out remove #channel".format(chan)
        else:
            return "RPG game is enabled in {}. To disable it run ,opt_out add #channel".format(chan)
    if text == "list":
        return ", ".join(opt_out)
    if len(text.split(' ')) < 2:
        return "please specify add or remove and a valid channel name"
    command = text.split()[0]
    channel = text.split()[1]
    if not channel.startswith('#'):
        return "Please specify a valid channel."
    if command.lower() == "add":
        if channel in opt_out:
            return "RPG game has already been disabled in {}.".format(channel)
        query = optout.insert().values(
            network = conn.name,
            chan = channel.lower())
        db.execute(query)
        db.commit()
        load_optout(db)
        return "The RPG game has been successfully disabled in {}.".format(channel)
    if command.lower() == "remove":
        #if not channel in opt_out:
        #    return "RPG game is already enabled in {}.".format(channel)
        delete = optout.delete(optout.c.chan == channel.lower())
        db.execute(delete)
        db.commit()
        load_optout(db)

@hook.command("merge", permissions=["botcontrol"])
def merge_score(text, conn, db, message):
    """Moves the rpg scores from one nick to another nick. Accepts two nicks as input the first will have their duck scores removed the second will have the first score added. Warning this cannot be undone."""
    if freeze > 0:
        return
    oldnick, newnick = text.lower().split()
    if not oldnick or not newnick:
        return "Please specify two nicks for this command."
    has_change = False
    if merge_statistics(db, conn, oldnick, newnick):
        has_change = True
    if merge_captures(db, conn, oldnick, newnick):
        has_change = True
    if has_change:
        message("Success")
    else:
        return "There are no stats or captures to migrate from {}".format(oldnick)

def merge_statistics(db, conn, oldnick, newnick):
    oldnickscore = db.execute(select([table.c.chan, table.c.level, table.c.exp, table.c.hp_lost, table.c.deaths])
        .where(table.c.network == conn.name)
        .where(table.c.name == oldnick)).fetchall()
    newnickscore = db.execute(select([table.c.chan, table.c.level, table.c.exp, table.c.hp_lost, table.c.deaths])
        .where(table.c.network == conn.name)
        .where(table.c.name == newnick)).fetchall()
    merge = defaultdict(lambda: defaultdict(int))
    channelkey = {"update":[], "insert":[]}
    if oldnickscore:
        if newnickscore:
            for row in newnickscore:
                chan = row["chan"]
                merge[chan]["exp"] = total_exp(row["level"], row["exp"])
                merge[chan]["hp_lost"] = row["hp_lost"]
                merge[chan]["deaths"] = row["deaths"]
        for row in oldnickscore:
            chan = row["chan"]
            if chan in merge:
                merge[chan]["exp"] += total_exp(row["level"], row["exp"])
                merge[chan]["hp_lost"] += row["hp_lost"]
                merge[chan]["deaths"] += row["deaths"]
                channelkey["update"].append(chan)
            else:
                merge[chan]["exp"] = total_exp(row["level"], row["exp"])
                merge[chan]["hp_lost"] = row["hp_lost"]
                merge[chan]["deaths"] = row["deaths"]
                channelkey["insert"].append(chan)
       # TODO: Call dbupdate() and db_add_entry for the items in merge
        for chan in channelkey["insert"]:
            level, exp = from_total_exp(merge[chan]["exp"])
            hp_loss, deaths = fix_hp_loss(level, merge[chan]["hp_lost"], merge[chan]["deaths"])
            dbadd_entry(newnick, chan, db, conn, level, exp, hp_loss, deaths)
        for chan in channelkey["update"]:
            level, exp = from_total_exp(merge[chan]["exp"])
            hp_loss, deaths = fix_hp_loss(level, merge[chan]["hp_lost"], merge[chan]["deaths"])
            dbupdate_progress(newnick, chan, db, conn, level, exp)
            dbupdate_loss(newnick, chan, db, conn, hp_loss, deaths)
        query = table.delete() \
            .where(table.c.network == conn.name) \
            .where(table.c.name == oldnick)
        db.execute(query)
        db.commit()
        return True
    else:
        return False

def merge_captures(db, conn, oldnick, newnick):
    oldnickscore = db.execute(select([captures_table.c.chan, captures_table.c.monster, captures_table.c.captures])
        .where(captures_table.c.network == conn.name)
        .where(captures_table.c.name == oldnick)).fetchall()
    newnickscore = db.execute(select([captures_table.c.chan, captures_table.c.monster, captures_table.c.captures])
        .where(captures_table.c.network == conn.name)
        .where(captures_table.c.name == newnick)).fetchall()
    merge = defaultdict(lambda: defaultdict(int))
    chan_monster_key = {"update":[], "insert":[]}
    if oldnickscore:
        if newnickscore:
            for row in newnickscore:
                chan = row["chan"]
                monster = row["monster"]
                merge[chan][monster] = row["captures"]
        for row in oldnickscore:
            chan = row["chan"]
            monster = row["monster"]
            if chan in merge and monster in merge[chan]:
                merge[chan][monster] += row["captures"]
                channelkey["update"].append((chan, monster))
            else:
                merge[chan][monster] = row["captures"]
                channelkey["insert"].append((chan, monster))
       # TODO: Call dbupdate() and db_add_entry for the items in merge
        for chan, monster in channelkey["insert"]:
            dbadd_capture(newnick, chan, db, conn, monster, merge[chan][monster]["captures"])
        for chan, monster in channelkey["update"]:
            dbupdate_captures(newnick, chan, db, conn, monster, merge[chan][monster]["captures"])
        query = captures_table.delete() \
            .where(captures_table.c.network == conn.name) \
            .where(captures_table.c.name == oldnick)
        db.execute(query)
        db.commit()
        return True
    else:
        return False

def total_exp(level, exp):
    total = 0
    for i in range(1, level):
        total += get_max_exp(i)
    return total + exp

def from_total_exp(total_exp):
    level = 1
    while total_exp >= get_max_exp(level):
        total_exp -= get_max_exp(level)
        level += 1
    return level, total_exp

def fix_hp_loss(level, hp_loss, deaths):
    max_hp = get_max_hp(level)
    while hp_loss >= max_hp:
        hp_loss -= max_hp
        deaths += 1
    return hp_loss, deaths

@hook.command("stats", autohelp=False)
def stats_user(text, nick, chan, conn, db, message):
    """Prints a user's stats. If no nick is input it will check the calling username."""
    if freeze > 0:
        return
    name = nick.lower()
    if text:
        name = text.split()[0].lower()
    stats = get_player_info(db, conn.name, chan, name)
    if text:
        name = text.split()[0]
    else:
        name = nick
    if stats:
        level = stats["level"]
        exp = stats["exp"]
        max_exp = get_max_exp(level)
        max_hp = get_max_hp(level)
        hp = max_hp - stats["hp_lost"]
        deaths = stats["deaths"]
        message("Stats for {} in {}.   LVL: {}.   EXP: {}/{}.   HP: {}/{}.   Deaths: {}."\
            .format(name, chan, level, exp, max_exp, hp, max_hp, deaths))
    else:
        return "It appears {} has not participated in the RPG game in {}.".format(name, chan)

@hook.command("captures", autohelp=False)
def captures_user(text, nick, chan, conn, db, message):
    """Prints a user's captures. If no nick is input it will check the calling username."""
    if freeze > 0:
        return
    name = nick.lower()
    if text:
        name = text.split()[0].lower()
    captures = db.execute(select([captures_table.c.monster, captures_table.c.captures]) \
        .where(captures_table.c.network == conn.name) \
        .where(captures_table.c.chan == chan.lower()) \
        .where(captures_table.c.name == name)).fetchall()
    if text:
        name = text.split()[0]
    else:
        name = nick
    if captures:
        capture_list = defaultdict(int)
        for capture in captures:
            if capture[1] == 0:
                continue
            longmode = game_status[conn.name][chan]['longmode']
            capture_list[monster_name(capture[0], longmode)] = capture[1]
        sorted_captures = sorted(capture_list.items(), key=operator.itemgetter(0), reverse = False)
        out = "Captures for {} in {}. • ".format(name, chan)
        out += ' • '.join(["{}: {}".format('\x02' + k[:1] + u'\u200b' + k[1:] + '\x02', str(v))  for k, v in sorted_captures])
        out = smart_truncate(out)
        return out
    else:
        return "It appears {} has not captured anything in {}.".format(name, chan)

@hook.command("captureleads", autohelp=False)
def capture_leads(text, chan, conn, db, message):
    """Prints top captures. If no monster is given, totals captures across all monsters."""
    if freeze > 0:
        return
    if text:
        if text in monsters:
            monster_index = monsters.index(text)
        elif text in monsters_short:
            monster_index = monsters_short.index(text)
        else:
            return "The monster {} does not exist".format(text)
        captures = db.execute(select([captures_table.c.name, captures_table.c.captures]) \
            .where(captures_table.c.network == conn.name) \
            .where(captures_table.c.chan == chan.lower()) \
            .where(captures_table.c.monster == monster_index)).fetchall()
        if captures:
            capture_list = defaultdict(int)
            for capture in captures:
                if capture[1] == 0:
                    continue
                capture_list[capture[0]] = capture[1]
            sorted_captures = sorted(capture_list.items(), key=operator.itemgetter(1), reverse = True)
            out = "Captures of {} in {}. • ".format(text, chan)
            out += ' • '.join(["{}: {}".format('\x02' + k[:1] + u'\u200b' + k[1:] + '\x02', str(v))  for k, v in sorted_captures])
            out = smart_truncate(out)
            return out
        else:
            return "It appears no {} has been captured in {}.".format(text, chan)
    else:
        captures = db.execute(select([captures_table.c.name, captures_table.c.captures]) \
            .where(captures_table.c.network == conn.name) \
            .where(captures_table.c.chan == chan.lower())).fetchall()
        if captures:
            capture_list = defaultdict(int)
            for capture in captures:
                if capture[1] == 0:
                    continue
                capture_list[capture[0]] += capture[1]
            sorted_captures = sorted(capture_list.items(), key=operator.itemgetter(1), reverse = True)
            out = "Captures in {}. • ".format(chan)
            out += ' • '.join(["{}: {}".format('\x02' + k[:1] + u'\u200b' + k[1:] + '\x02', str(v))  for k, v in sorted_captures])
            out = smart_truncate(out)
            return out
        else:
            return "It appears no monsters have been captured in {}.".format(chan)

@hook.command("instructions", autohelp=False)
def instructions(nick, chan, conn, notice):
    """Messages you the instructions to the RPG game."""
    if freeze > 0:
        return
    out = "Use the ,attack or ,atk command to attack a monster."
    out += " By default, this will attack the highest-level monster that is not stronger than you."
    out += " To control which monster you attack, you can add a number after the command."
    out += " For example, ',atk 1' or ',atk 3'."
    out += " The ,capture or ,cap command works the same way, except it captures instead of attacks."
    out += " Capturing has a lower success rate, but permanently increases the chance of killing the same type of monster."
    out += " Killing a monster rewards you with EXP. Collect enough EXP to level up."
    out += " Higher levels increase your chances of killing and capturing monsters."
    out += " NOTE: Monsters appear as a function of time and channel activity."
    notice(out)

def try_get_int(arg):
    try:
        return int(arg)
    except ValueError:
        return None

@hook.command("setstats", autohelp=False, permissions=["botcontrol"])
def set_stats(text, nick, chan, conn, db):
    if freeze > 0:
        return
    if not text:
        return "Please specify user"
    args = text.split()
    if len(args) < 2:
        return "Please specify level"
    level = try_get_int(args[1])
    if level is None:
        return "Please input number for level"
    if len(args) < 3:
        return "Please specify exp"
    exp = try_get_int(args[2])
    if exp is None:
        return "Please input number for exp"
    if len(args) < 4:
        return "Please specify hp lost"
    hp_lost = try_get_int(args[3])
    if hp_lost is None:
        return "Please specify number for hp lost"
    if len(args) < 3:
        return "Please specify deaths"
    deaths = try_get_int(args[4])
    if deaths is None:
        return "Please specify number for deaths"
    dbupdate_progress(args[0], chan, db, conn, level, exp)
    dbupdate_loss(args[0], chan, db, conn, hp_lost, deaths)

@hook.command("givemonster", autohelp=False, permissions=["botcontrol"])
def give_monster(text, nick, chan, conn, db):
    if freeze > 0:
        return
    if not text:
        return "Please specify user"
    args = text.split()
    if len(args) < 2:
        return "Please specify monster"
    monster = try_get_int(args[1])
    if monster is None:
        return "Please input number for monster"
    if len(args) < 3:
        return "Please specify amount"
    amount = try_get_int(args[2])
    if amount is None:
        return "Please input number for amount"
    captures = get_player_captures(db, conn.name, chan, args[0], monster)
    is_new = (captures == 0)
    captures += amount
    if is_new:
        dbadd_capture(args[0], chan, db, conn, monster, captures)
    else:
        dbupdate_captures(args[0], chan, db, conn, monster, captures)
    return out


def get_max_exp(level):
    if level <= 20:
        return level * 5
    else:
        return 100 + (level - 20) * 6

def get_max_hp(level):
    return level * 5

def get_monster_atk(level):
    if level <= 20:
        return level
    else:
        return int(20 + (level - 20) * 1.25)

def get_monster_heal(level):
    heal = get_monster_index(level) + 1
    return heal

def monster_name(monster_index, longmode):
    if longmode:
        name_list = monsters
        result = "{} (LVL {})"
    else:
        name_list = monsters_short
        result = "{} (lv {})"
    return result.format(name_list[monster_index], get_monster_level(monster_index))

def get_player_level(db, network, chan, name):
    val = db.execute(select([table.c.level])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()
    if val:
        return val[0]
    else:
        return None

def get_player_exp(db, network, chan, name):
    val = db.execute(select([table.c.exp])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()
    if val:
        return val[0]
    else:
        return None

def get_player_progress(db, network, chan, name):
    return db.execute(select([table.c.level, table.c.exp])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()

def get_player_hp_lost(db, network, chan, name):
    val = db.execute(select([table.c.hp_lost])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()
    if val:
        return val[0]
    else:
        return None

def get_player_deaths(db, network, chan, name):
    val = db.execute(select([table.c.deaths])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()
    if val:
        return val[0]
    else:
        return None

def get_player_loss(db, network, chan, name):
    return db.execute(select([table.c.hp_lost, table.c.deaths])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()

def get_player_info(db, network, chan, name):
    return db.execute(select([table.c.name, table.c.level, table.c.exp, table.c.hp_lost, table.c.deaths])
        .where(table.c.network == network)
        .where(table.c.chan == chan.lower())
        .where(table.c.name == name.lower())).fetchone()

def get_player_captures(db, network, chan, name, monster):
    val = db.execute(select([captures_table.c.captures])
        .where(captures_table.c.network == network)
        .where(captures_table.c.chan == chan.lower())
        .where(captures_table.c.name == name.lower())
        .where(captures_table.c.monster == monster)).fetchone()
    if val:
        return val[0]
    else:
        return 0