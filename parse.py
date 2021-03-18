import discord
from discord.ext import commands
import sqlite3
import datetime
import asyncio

intents = discord.Intents.all()

client = commands.Bot(command_prefix="-", intents=intents)
client.remove_command("help")

connection = sqlite3.connect('server.db')
cursor = connection.cursor()


@client.event
async def on_ready():
    print('ready')

    cursor.execute("""CREATE TABLE IF NOT EXISTS guild_stats(
        nickname VARCHAR,
        id INT,
        max_warn SMALLINT,
        shop_channel_id INT,
        shop_message_id INT,
        moder_roles TEXT,
        create_voice_id INT
        )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS lvls(
        lvl INT,
        exp INT
        )""")
    connection.commit()

    for guild in client.guilds:
        user_guild = str(guild.id) + '_users'
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS '{user_guild}'(
                nickname VARCHAR,
                id INT,
                cash FLOAT,
                rep_rate FLOAT,
                cash_rate FLOAT,
                reputation INT,
                warn SMALLINT,
                lvl INT,
                exp INT,
                channel_owner INT
                )""")

        role_guild = str(guild.id) + '_roles'
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS '{role_guild}'(
                nickname VARCHAR,
                id INT,
                cash_rate FLOAT,
                rep_rate FLOAT,
                lvl_role INT,
                cost INT,
                emoji CHARACTER,
                serial_number SMALLINT
                )""")

        cursor.execute(f"INSERT INTO guild_stats VALUES ('{guild.name}', {guild.id}, 3, 0, 0, 0, 0)")
        connection.commit()

    await check_time()


@client.event
async def on_guild_join(guild):
    print('on_guild_join')

    user_guild = str(guild.id)+'_users'
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS '{user_guild}'(
        nickname VARCHAR,
        id INT,
        cash FLOAT,
        rep_rate FLOAT,
        cash_rate FLOAT,
        reputation INT,
        warn SMALLINT,
        lvl INT,
        exp INT
        )""")

    role_guild = str(guild.id) + '_roles'
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS '{role_guild}'(
        nickname VARCHAR,
        id INT,
        cash_rate FLOAT,
        rep_rate FLOAT,
        lvl_role INT,
        cost INT,
        emoji_id INT,
        serial_number SMALLINT
        )""")

    cursor.execute(f"INSERT INTO guild_stats VALUES ('{guild.name}', {guild.id}, 3, 0, 0, 0, 0)")
    connection.commit()

    await fill_db(guild=guild)


@client.event
async def on_raw_reaction_add(payload):
    if cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE shop_message_id = {payload.message_id}").fetchone() is not None:
        print('on_raw_reaction_add')

        rest = cursor.execute(f"SELECT cash FROM '{str(payload.guild_id) + '_users'}' WHERE id = {payload.member.id}").fetchone()[0] - cursor.execute(f"SELECT cost FROM '{str(payload.guild_id) + '_roles'}' WHERE emoji = '{payload.emoji}'").fetchone()[0]
        if rest >= 0:
            cursor.execute(f"UPDATE '{str(payload.guild_id) + '_users'}' SET cash = {rest} WHERE id = {payload.member.id}")
            connection.commit()
            await payload.member.add_roles(payload.member.guild.get_role(cursor.execute(f"SELECT id FROM '{str(payload.guild_id) + '_roles'}' WHERE emoji = '{payload.emoji}'").fetchone()[0]))
        else:
            msg = await client.get_channel(payload.channel_id).fetch_message(payload.message_id)
            await msg.remove_reaction(payload.emoji, payload.member)


@client.event
async def on_raw_reaction_remove(payload):
    if cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE shop_message_id = {payload.message_id}").fetchone() is not None:
        print('on_raw_reaction_remove')

        rest = cursor.execute(f"SELECT cash FROM '{str(payload.guild_id) + '_users'}' WHERE id = {payload.user_id}").fetchone()[0] + 0.3 * cursor.execute(f"SELECT cost FROM '{str(payload.guild_id) + '_roles'}' WHERE emoji = '{payload.emoji}'").fetchone()[0]
        cursor.execute(f"UPDATE '{str(payload.guild_id) + '_users'}' SET cash = {rest} WHERE id = {payload.user_id}")
        connection.commit()
        guild = discord.utils.get(client.guilds, id=payload.guild_id)
        await discord.utils.get(guild.members, id=payload.user_id).remove_roles(discord.utils.get(guild.roles, id=cursor.execute(f"SELECT id FROM '{str(payload.guild_id) + '_roles'}' WHERE emoji = '{payload.emoji}'").fetchone()[0]))


@client.event
async def on_voice_state_update(member, before, after):
    if after.channel is not None:
        if after.channel.id == cursor.execute(f"SELECT create_voice_id FROM guild_stats WHERE id = {member.guild.id}").fetchone()[0]:
            print('create_voice')

            channel = await after.channel.category.create_voice_channel(name=f"Приват {member.name}", user_limit=2)
            await channel.set_permissions(member, connect=True, manage_channels=True)
            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET channel_owner = {after.channel.id} WHERE id = {member.id}")
            connection.commit()
            await member.move_to(channel)

            def check(x, y, z):
                return len(channel.members) == 0
            await client.wait_for('voice_state_update', check=check)

            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET channel_owner = 0 WHERE id = {member.id}")
            connection.commit()
            await channel.delete()


@client.command()
async def flush(ctx, table: str = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('flush')
        table_users = None
        table_roles = None
        guild = ctx.guild
        if table == 'users':
            table_users = str(guild.id) + '_users'
            cursor.execute("""DELETE FROM '{table_users}'""")
            connection.commit()
        elif table == 'roles':
            table_roles = str(guild.id) + '_roles'
            cursor.execute("""DELETE FROM '{table_roles}'""")
            connection.commit()
        else:
            table_users = str(guild.id) + '_users'
            table_roles = str(guild.id) + '_roles'
            cursor.execute("""DELETE FROM '{table_users}'""")
            cursor.execute("""DELETE FROM '{table_roles}'""")
            connection.commit()

        await fill_db(table_users, table_roles, guild)

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def role_edit(ctx, role: discord.Role = None, cash_rate: float = 1, rep_rate: float = None, lvl_role: int = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('role_edit')

        cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET cash_rate = {cash_rate} WHERE id = {role.id}")
        if rep_rate is not None:
            cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET rep_rate = {rep_rate} WHERE id = {role.id}")
        if lvl_role is not None:
            cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET lvl_role = {lvl_role} WHERE id = {role.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def add_role_to_shop(ctx, role: discord.Role = None, cost: int = 100, emj: str = None, serial_number: int = 50):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('add_role_to_shop')

        if emj is not None and role is not None:
            if len(emj) == 1 and cursor.execute(f"SELECT emoji FROM '{str(ctx.guild.id) + '_roles'}' WHERE emoji = '{emj}'").fetchone() is None:
                cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET cost = {cost} WHERE id = {role.id}")
                cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET emoji = '{emj}' WHERE id = {role.id}")
                cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET serial_number = {serial_number} WHERE id = {role.id}")
                connection.commit()

                shop_message = cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()
                if shop_message is not None:
                    shop_message = shop_message[0]
                    if shop_message != 0:
                        msg = await ctx.guild.get_channel(cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0]).fetch_message(shop_message)
                        await msg.delete()
                        cursor.execute(f"UPDATE guild_stats SET shop_message_id = 0 WHERE id = {ctx.guild.id}")
                        connection.commit()

        await update_shop(role)

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def remove_role_from_shop(ctx, role: discord.Role = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('remove_role_from_shop')

        if role != None:
            cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET cost = 0 WHERE id = {role.id}")
            cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET emoji = '0' WHERE id = {role.id}")
            cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET serial_number = 0 WHERE id = {role.id}")
            connection.commit()

            shop_message = cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()
            if shop_message is not None:
                shop_message = shop_message[0]
                if shop_channel != 0:
                    ctx.guild.get_channel(cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0]).fetch_message(shop_message).delete()
                    cursor.execute(f"UPDATE guild_stats SET shop_message_id = 0 WHERE id = {ctx.guild.id}")

            await update_shop(role)

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def max_warns(ctx, max_warn: int = 3):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('max_warns')

        cursor.execute(f"UPDATE guild_stats SET max_warn = {max_warn} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def shop_channel(ctx, channel: discord.TextChannel = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('shop_channel')

        if channel is None:
            channel = ctx.channel
        shop_message = cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()
        if shop_message is not None:
            if shop_message[0] != 0:
                ctx.guild.get_channel(cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0]).fetch_message(shop_message).delete()
        cursor.execute(f"UPDATE guild_stats SET shop_channel_id = {channel.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await update_shop(channel=channel)

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def create_voice_creator(ctx, name: str = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('create_voice_creator')

        if name is None:
            name = 'Создать-голосовой'

        voice = await ctx.channel.category.create_voice_channel(name=name, user_limit=1)
        cursor.execute(f"UPDATE guild_stats SET create_voice_id = {voice.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def add_moder_role(ctx, role: discord.Role = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('add_moder_role')

        cursor.execute(f"UPDATE guild_stats SET moder_roles = moder_roles + '_' + {role.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def remove_moder_role(ctx, role: discord.Role = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('remove_moder_role')

        moder_roles = cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].replace(f"_{role.id}", "")
        cursor.execute(f"UPDATE guild_stats SET moder_roles = {moder_roles} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def stats(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    embed = discord.Embed(title=f"📊Статистика {member.name}")
    embed.add_field(name="**Высшая роль**", value=member.top_role, inline=True)
    embed.add_field(name="**Баланс**", value=str(int(round(cursor.execute(f"SELECT cash FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0],0))) + " 💵", inline=True)
    embed.add_field(name="**Репутация**", value=str(int(round(cursor.execute(f"SELECT reputation FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]))) + "", inline=True)
    embed.add_field(name="**Уровень**", value=str(cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]), inline=True)
    if cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] == 50:
        embed.colour = discord.Colour.gold()
        embed.add_field(name="**Опыт**", value="**1000/1000**", inline=True)
    else:
        level = cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
        embed.add_field(name="**Опыт**", value=str(cursor.execute(f"SELECT exp FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]) + "/" + str(cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}").fetchone()[0]), inline=True)
    embed.add_field(name="**Варны**", value="{}/3".format(cursor.execute(f"SELECT warn FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]),inline=True)

    await ctx.message.add_reaction('✅')
    msg = await ctx.send(embed=embed)
    await ctx.message.delete(delay=20)
    await msg.delete(delay=20)


@client.command()
async def cash(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    embed = discord.Embed(description=f"**Баланс** {member.name}: " + str(int(round(cursor.execute(f"SELECT cash FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0],0))) + " 💵")

    await ctx.message.add_reaction('✅')
    msg = await ctx.send(embed=embed)
    await ctx.message.delete(delay=20)
    await msg.delete(delay=20)


@client.command()
async def give(ctx, member: discord.Member = None, count: int = None):
    if member is not None and count is not None:
        print('give')

        rest = cursor.execute(f"SELECT cash FROM '{str(ctx.author.guild.id) + '_users'}' WHERE id = {ctx.author.id}").fetchone()[0] - count

        if rest >= 0:
            cursor.execute(f"UPDATE '{str(ctx.author.guild.id) + '_users'}' SET cash = {rest} WHERE id = {ctx.author.id}")
            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET cash = cash + {count} WHERE id = {member.id}")
            connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def take(ctx, member: discord.Member = None, count: int = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        if member is not None and count is not None:
            print('take')

            rest = cursor.execute(f"SELECT cash FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] - count

            if rest >= 0:
                cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET cash = cash -{count} WHERE id = {member.id}")
                connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def award(ctx, member: discord.Member = None, count: int = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        if member is not None and count is not None:
            print('award')

            rest = cursor.execute(f"SELECT cash FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] - count

            if rest >= 0:
                cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET cash = cash + {count} WHERE id = {member.id}")
                connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def clean(ctx, amount: int = 10):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        print('clean')

        if amount > 100:
            amount = 100
        await ctx.channel.purge(limit=amount + 1)
        message = await ctx.send(embed=discord.Embed(description=f"Удалено {amount} сообщений"))
        await message.delete(delay=5)


@client.command()
async def warn(ctx, member: discord.Member = None, reason: str = None):
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}").fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator:
        if member is not None:
            print('warn')

            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET warn = warn + 1 WHERE id = {member.id}")
            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET reputation = reputation - 40 WHERE id = {member.id}")

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


async def check_time():
    await asyncio.sleep(1)
    while True:
        await fill_db()

        print('check_time')

        if str(datetime.datetime.now()).split(' ')[1].split(':')[1] in ['00', '01', '02', '03', '04']:
            for member in client.get_all_members():
                if member.bot == 0 and cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone() is not None:
                    rep_rate = 1
                    cash_rate = 1
                    bad_role = 0
                    good_role = 0
                    for role in member.roles:
                        if cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone() is not None:
                            rep_rate += cursor.execute(f"SELECT rep_rate FROM '{str(member.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone()[0]
                            cash_rate += cursor.execute(f"SELECT cash_rate FROM '{str(member.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone()[0]

                            if cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -2").fetchone() is not None:
                                if cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -2").fetchone()[0] == role.id:
                                    bad_role = cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -2").fetchone()[0]
                            elif cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -3").fetchone() is not None:
                                if cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -3").fetchone()[0]:
                                    good_role = cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -3").fetchone()[0]

                    rep_rate = round(rep_rate, 2)
                    cash_rate = round(cash_rate, 2)
                    cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET rep_rate = {rep_rate} WHERE id = {member.id}")
                    cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET cash_rate = {cash_rate} WHERE id = {member.id}")
                    connection.commit()

                    max_warn = cursor.execute(f"SELECT max_warn FROM guild_stats WHERE id = {member.guild.id}").fetchone()[0]

                    if bad_role == 0:
                        bad_role = cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -2").fetchone()

                        if (cursor.execute(f"SELECT warn FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] >= max_warn or cursor.execute(f"SELECT reputation FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] <= max_warn * -40) and bad_role is not None:
                            await member.add_roles(member.guild.get_role(bad_role[0]))
                    elif good_role == 0:
                        good_role = cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = -3").fetchone()

                        if (cursor.execute(f"SELECT warn FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] < max_warn and cursor.execute(f"SELECT reputation FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] >= max_warn * 120) and good_role is not None:
                            await member.add_roles(member.guild.get_role(good_role[0]))

                    if bad_role != 0:
                        if cursor.execute(f"SELECT reputation FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] >= 0:
                            await member.remove_roles(member.guild.get_role(bad_role[0]))
                    elif good_role != 0:
                        if cursor.execute(f"SELECT reputation FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] <= 120:
                            await member.remove_roles(member.guild.get_role(good_role[0]))

        for member in client.get_all_members():
            if member.bot == 0:
                if cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] != 50:
                    level = cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
                    if member.bot == 0 and cursor.execute(f"SELECT exp FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] >= cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}").fetchone()[0]:
                        cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET lvl = lvl + 1 WHERE id = {member.id}")
                        connection.commit()

            if member.bot == 0 and cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone() is not None and member.voice is not None:
                if not member.voice.afk:
                    cash_rate = cursor.execute(f"SELECT cash_rate FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
                    cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET cash = cash + {cash_rate} WHERE id = {member.id}")
                    level = cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
                    if level != 50:
                        cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET exp = exp + 1 WHERE id = {member.id}")
                        if cursor.execute(f"SELECT exp FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] >= cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}").fetchone()[0]:
                            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET lvl = lvl + 1 WHERE id = {member.id}")
                            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET exp = 0 WHERE id = {member.id}")
                            level = cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
                            role = cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_roles'}' WHERE lvl_role = {level}").fetchone()
                            if role is not None:
                                role = member.guild.get_role(role_id=role[0])
                                if role not in member.roles:
                                    await member.add_roles(role)
                    connection.commit()
        await asyncio.sleep(299)


async def fill_db(table_users: str = None, table_roles: str = None, guild: discord.Guild = None):
    print('fill_db')

    if table_users is not None and table_roles is None:
        for member in guild.members:
            await fill_user(member)
    elif table_users is None and table_roles is not None:
        for role in guild.roles:
            await fill_role(role)
    else:
        for member in client.get_all_members():
            await fill_user(member)

        for guild in client.guilds:
            for role in guild.roles:
                await fill_role(role)


async def fill_user(member: discord.Member):
    if member.bot == 0:
        if cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone() is None:
            cursor.execute(f"INSERT INTO '{str(member.guild.id) + '_users'}' VALUES ('{member.name}', {member.id}, 0, 1, 1, 0, 0, 0, 0, 0)")
            connection.commit()

        elif cursor.execute(f"SELECT id FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone() is not None and cursor.execute(f"SELECT nickname FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0] != member.name:
            cursor.execute(f"UPDATE '{str(member.guild.id) + '_users'}' SET nickname = '{member.name}' WHERE id = {member.id}")
            connection.commit()

        level = cursor.execute(f"SELECT lvl FROM '{str(member.guild.id) + '_users'}' WHERE id = {member.id}").fetchone()[0]
        if cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}").fetchone() is None:
            cursor.execute("""DELETE FROM lvls""")

            for i in range(50):
                if i < 5:
                    a = 10 + (10 * i)
                elif i < 10:
                    a = 50 + (20 * (i - 4))
                elif i < 15:
                    a = 150 + (30 * (i - 9))
                elif i < 20:
                    a = 300 + (40 * (i - 14))
                elif i < 25:
                    a = 500 + (50 * (i - 19))
                elif i < 30:
                    a = 750 + (60 * (i - 24))
                elif i < 35:
                    a = 1050 + (70 * (i - 29))
                elif i < 40:
                    a = 1400 + (80 * (i - 34))
                elif i < 45:
                    a = 1800 + (90 * (i - 39))
                else:
                    a = 2250 + (100 * (i - 44))
                cursor.execute(f"INSERT INTO lvls VALUES ({i}, {a})")
                connection.commit()


async def fill_role(role: discord.Role):
    if cursor.execute(f"SELECT id FROM '{str(role.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone() is None:
        cursor.execute(f"INSERT INTO '{str(role.guild.id) + '_roles'}' VALUES ('{role.name}', {role.id}, 0, 0, -1, 0, '0', 0)")
        connection.commit()

    elif cursor.execute(f"SELECT id FROM '{str(role.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone() is not None and cursor.execute(f"SELECT nickname FROM '{str(role.guild.id) + '_roles'}' WHERE id = {role.id}").fetchone()[0] != role.name:
        cursor.execute(f"UPDATE '{str(role.guild.id) + '_roles'}' SET nickname = '{role.name}' WHERE id = {role.id}")
        connection.commit()


async def update_shop(role: discord.Role = None, channel: discord.TextChannel = None):
    print('update_shop')

    if role is not None:
        channel = cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {role.guild.id}").fetchone()
        if channel is not None:
            channel = role.guild.get_channel(channel[0])

    if channel is not None:
        role_list = []
        for rol in channel.guild.roles:
            ser_num = cursor.execute(f"SELECT serial_number FROM '{str(channel.guild.id) + '_roles'}' WHERE id = {rol.id}").fetchone()[0]
            if ser_num > 0:
                role_list.append(str(ser_num) + '_' + str(rol.id))
        role_list.sort()
        if role_list:
            embed = discord.Embed(title="**Магазин ролей**")
            for rol in role_list:
                rol = channel.guild.get_role(int(rol.split('_')[1]))
                rol_cost = cursor.execute(f"SELECT cost FROM '{str(channel.guild.id) + '_roles'}' WHERE id = {rol.id}").fetchone()[0]
                embed.add_field(name=rol + ' - ' + cursor.execute(f"SELECT emoji FROM '{str(channel.guild.id) + '_roles'}' WHERE id = {rol.id}").fetchone()[0], value=f"Цена: {rol_cost} 💲", inline=True)
            msg = await channel.send(embed=embed)
            cursor.execute(f"UPDATE guild_stats SET shop_message_id = {msg.id} WHERE id = {channel.guild.id}")
            connection.commit()
            for rol in role_list:
                rol = channel.guild.get_role(int(rol.split('_')[1]))
                rol_emoji = cursor.execute(f"SELECT emoji FROM '{str(role.guild.id) + '_roles'}' WHERE id = {rol.id}").fetchone()[0]
                await msg.add_reaction(rol_emoji)


client.run('NzgzNzUxMzAxNzYyNTE0OTU0.X8fTRw.aNj9f3Vjl6uH10nIdQg_5i9r4JU')
