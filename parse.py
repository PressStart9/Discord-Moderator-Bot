import discord
from discord.ext import commands
import datetime
import asyncio
import os
import psycopg2
import requests
from bs4 import BeautifulSoup
import os
import asyncio
import random

intents = discord.Intents.all()

client = commands.Bot(command_prefix="-", intents=intents)
client.remove_command("help")

connection = psycopg2.connect(database="d2lr2a0d09qdjp", user="iievftgbqcbnzw", password="c4b3d10d51eb4bc2b998adec462f32445a14753c663cdb9d7eb5cdca6b527943", host="ec2-54-217-195-234.eu-west-1.compute.amazonaws.com", port="5432")
cursor = connection.cursor()

website = 'https://freesteam.ru/'
website_news = 'https://stopgame.ru/news'
headers = {'UserAgent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36'}


@client.command()
async def help(ctx):
    emb = discord.Embed(title="Привет, я - CISBOT", description="Я бот для модерирования и развития сервера. Вот, что я могу:")
    emb.add_field(name="-flush <название-таблицы>", value="М_Очищает таблицу users или roles")
    emb.add_field(name="-role_edit <*роль> <множитель-кэша> <множитель-опыта> <уровень-при-котором-выдаётся>", value="М-Изменяет параметря роли")
    emb.add_field(name="-add_role_to_shop <*роль> <цена> <*эмоджи> <положение_в_магазине>", value="М-Добавляет роль в магазин")
    emb.add_field(name="-remove_role_from_shop <*роль>", value="М-Удаляет роль из магазина")
    emb.add_field(name="-max_warns <число-предупреждений>", value="М-Устанавливает максимальное число предупреждений")
    emb.add_field(name="-shop_channel <канал>", value="М-Устанавливает канал с сообщением для покупки ролей")
    emb.add_field(name="-create_voice_creator <название-канала>", value="М-Создаёт канал, при заходе в который создаётся приват для участника")
    emb.add_field(name="-distribution_channel <канал>", value="М-Устанавливает канал, в который будут присылаться раздачи игр")
    emb.add_field(name="-add_moder_role <*роль>", value="М-Позволяет роли использовать модераторские команды")
    emb.add_field(name="-remove_moder_role <*роль>", value="М-Запрещает роли использовать модераторские команды")
    emb.add_field(name="-price <*название-игры>", value="Выводит самые низкие цены на игру в разных магазинах")
    emb.add_field(name="-stats <участник>", value="Выводит статистику участника")
    emb.add_field(name="-cash <участник>", value="Выводит баланс участника")
    emb.add_field(name="-give <*участник> <*сумма>", value="Передаёт деньги другому участнику")
    emb.add_field(name="-take <*участник> <*сумма>", value="М-Забирает сумму у игрока")
    emb.add_field(name="-award <*участник> <*сумма>", value="М-Даёт сумму игроку")
    emb.add_field(name="-clean <количество-сообщений>", value="Удаляет последние сообщения в канале")
    emb.add_field(name="-warn <*участник> <причина>", value="Выдаёт игроку предупреждение")
    msg = await ctx.channel.send(embed=emb)
    msg.delete(delay=100)


@client.event
async def on_ready():
    print('ready')

    cursor.execute("""CREATE TABLE IF NOT EXISTS guild_stats(
        nickname VARCHAR,
        id BIGINT,
        max_warn SMALLINT,
        shop_channel_id BIGINT,
        shop_message_id BIGINT,
        moder_roles TEXT,
        create_voice_id BIGINT,
        distribution_channel_id BIGINT,
        last_game_time BIGINT
        )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS lvls(
        lvl INT,
        exp BIGINT
        )""")
    connection.commit()

    for guild in client.guilds:
        user_guild = 'users_' + str(guild.id)
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {user_guild}(
                nickname VARCHAR,
                id BIGINT,
                cash FLOAT,
                rep_rate FLOAT,
                cash_rate FLOAT,
                reputation BIGINT,
                warn SMALLINT,
                lvl INT,
                exp BIGINT,
                channel_owner BIGINT
                )""")

        role_guild = 'roles_' + str(guild.id)
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {role_guild}(
                nickname VARCHAR,
                id BIGINT,
                cash_rate FLOAT,
                rep_rate FLOAT,
                lvl_role INT,
                cost BIGINT,
                emoji CHARACTER,
                serial_number SMALLINT
                )""")

        cursor.execute(f"SELECT id FROM guild_stats WHERE id = {guild.id}")
        if cursor.fetchone() is None:
            cursor.execute(f"INSERT INTO guild_stats (nickname, id, max_warn, shop_channel_id, shop_message_id, moder_roles, create_voice_id, distribution_channel_id, last_game_time) VALUES ('{guild.name}', {guild.id}, 3, 0, 0, ' ', 0, 0, 0)")
            connection.commit()

    await check_time()


@client.event
async def on_guild_join(guild):
    print('on_guild_join')

    user_guild = 'users_' + str(guild.id)
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {user_guild}(
        nickname VARCHAR,
        id BIGINT,
        cash FLOAT,
        rep_rate FLOAT,
        cash_rate FLOAT,
        reputation BIGINT,
        warn SMALLINT,
        lvl INT,
        exp BIGINT,
        channel_owner BIGINT
        )""")

    role_guild = 'roles_' + str(guild.id)
    cursor.execute(f"""CREATE TABLE IF NOT EXISTS {role_guild}(
        nickname VARCHAR,
        id BIGINT,
        cash_rate FLOAT,
        rep_rate FLOAT,
        lvl_role INT,
        cost BIGINT,
        emoji CHARACTER,
        serial_number SMALLINT
        )""")

    cursor.execute(f"SELECT id FROM guild_stats WHERE id = {guild.id}")
    if cursor.fetchone() is None:
        cursor.execute(f"INSERT INTO guild_stats (nickname, id, max_warn, shop_channel_id, shop_message_id, moder_roles, create_voice_id, distribution_channel_id, last_game_time) VALUES ('{guild.name}', {guild.id}, 3, 0, 0, ' ', 0, 0, 0)")
        connection.commit()

    await fill_db(guild=guild)


@client.event
async def on_raw_reaction_add(payload):
    cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE shop_message_id = {payload.message_id}")
    if cursor.fetchone() is not None:
        print('on_raw_reaction_add')
        
        cursor.execute(f"SELECT cash FROM {'users_' + str(payload.guild_id)} WHERE id = {payload.member.id}")
        rest = cursor.fetchone()[0]
        cursor.execute(f"SELECT cost FROM {'roles_' + str(payload.guild_id)} WHERE emoji = '{payload.emoji}'")
        rest -= cursor.fetchone()[0]
        if rest >= 0:
            cursor.execute(f"UPDATE {'users_' + str(payload.guild_id)} SET cash = {rest} WHERE id = {payload.member.id}")
            connection.commit()
            cursor.execute(f"SELECT id FROM {'roles_' + str(payload.guild_id)} WHERE emoji = '{payload.emoji}'")
            await payload.member.add_roles(payload.member.guild.get_role(cursor.fetchone()[0]))
        else:
            msg = await client.get_channel(payload.channel_id).fetch_message(payload.message_id)
            await msg.remove_reaction(payload.emoji, payload.member)


@client.event
async def on_raw_reaction_remove(payload):
    cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE shop_message_id = {payload.message_id}")
    if cursor.fetchone() is not None:
        print('on_raw_reaction_remove')

        cursor.execute(f"SELECT cash FROM {'users_' + str(payload.guild_id)} WHERE id = {payload.user_id}")
        rest = cursor.fetchone()[0]
        cursor.execute(f"SELECT cost FROM {'roles_' + str(payload.guild_id)} WHERE emoji = '{payload.emoji}'")
        rest += 0.3 * cursor.fetchone()[0]
        cursor.execute(f"UPDATE {'users_' + str(payload.guild_id)} SET cash = {rest} WHERE id = {payload.user_id}")
        connection.commit()
        guild = discord.utils.get(client.guilds, id=payload.guild_id)
        cursor.execute(f"SELECT id FROM {'roles_' + str(payload.guild_id)} WHERE emoji = '{payload.emoji}'")
        await discord.utils.get(guild.members, id=payload.user_id).remove_roles(discord.utils.get(guild.roles, id=cursor.fetchone()[0]))


@client.event
async def on_member_join(member):
    print('on_member_join')

    fill_user(member)


@client.event
async def on_member_remove(member):
    cursor.execute(f"SELECT id FROM {'users_' + member.guild.id} WHERE id = {member.id}")
    if cursor.fetchone() is not None:
        print('on_member_remove')

        cursor.execute(f"""DELETE FROM {'users_' + member.guild.id} WHERE id = {member.id}""")
        connection.commit()


@client.event
async def on_voice_state_update(member, before, after):
    cursor.execute(f"SELECT create_voice_id FROM guild_stats WHERE id = {member.guild.id}")
    if after.channel is not None and cursor.fetchone() is not None:
        cursor.execute(f"SELECT create_voice_id FROM guild_stats WHERE id = {member.guild.id}")
        if after.channel.id == cursor.fetchone()[0]:
            print('create_voice')

            channel = await after.channel.category.create_voice_channel(name=f"Приват {member.name}", user_limit=2)
            await channel.set_permissions(member, connect=True, manage_channels=True)
            cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET channel_owner = {after.channel.id} WHERE id = {member.id}")
            connection.commit()
            await member.move_to(channel)

            def check(x, y, z):
                return len(channel.members) == 0
            await client.wait_for('voice_state_update', check=check)

            cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET channel_owner = 0 WHERE id = {member.id}")
            connection.commit()
            await channel.delete()


@client.command()
async def flush(ctx, table: str = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('flush')
        table_users = None
        table_roles = None
        guild = ctx.guild
        if table == 'users':
            table_users = 'users_' + str(guild.id)
            cursor.execute(f"""DELETE FROM {table_users}""")
            connection.commit()
        elif table == 'roles_':
            table_roles = 'roles_' + str(guild.id)
            cursor.execute(f"""DELETE FROM {table_roles}""")
            connection.commit()
        else:
            table_users = 'users_' + str(guild.id)
            table_roles = 'roles_' + str(guild.id)
            cursor.execute(f"""DELETE FROM {table_roles}""")
            cursor.execute(f"""DELETE FROM {table_users}""")
            connection.commit()

        await fill_db(table_users, table_roles, guild)

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def role_edit(ctx, role: discord.Role = None, cash_rate: float = 1, rep_rate: float = None, lvl_role: int = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('role_edit')

        cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET cash_rate = {cash_rate} WHERE id = {role.id}")
        if rep_rate is not None:
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET rep_rate = {rep_rate} WHERE id = {role.id}")
        if lvl_role is not None:
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET lvl_role = {lvl_role} WHERE id = {role.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def add_role_to_shop(ctx, role: discord.Role = None, cost: int = 100, emj: str = None, serial_number: int = 50):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('add_role_to_shop')

        if emj is not None and role is not None:
            if len(emj) == 1 and cursor.execute(f"SELECT emoji FROM {'roles_' + str(role.guild.id)} WHERE emoji = '{emj}'") is None:
                cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET cost = {cost} WHERE id = {role.id}")
                cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET emoji = '{emj}' WHERE id = {role.id}")
                cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET serial_number = {serial_number} WHERE id = {role.id}")
                connection.commit()

                cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}")
                shop_message = cursor.fetchone()
                if shop_message is not None:
                    shop_message = shop_message[0]
                    if shop_message != 0:
                        cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}")
                        msg = await ctx.guild.get_channel(cursor.fetchone()[0]).fetch_message(shop_message)
                        await msg.delete()
                        cursor.execute(f"UPDATE guild_stats SET shop_message_id = 0 WHERE id = {ctx.guild.id}")
                        connection.commit()

                await update_shop(role)

                await ctx.message.add_reaction('✅')
                await ctx.message.delete(delay=5)


@client.command()
async def remove_role_from_shop(ctx, role: discord.Role = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('remove_role_from_shop')

        if role is not None:
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET cost = 0 WHERE id = {role.id}")
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET emoji = '0' WHERE id = {role.id}")
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET serial_number = 0 WHERE id = {role.id}")
            connection.commit()

            cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}")
            shop_message = cursor.fetchone()
            if shop_message is not None:
                shop_message = shop_message[0]
                if shop_channel != 0:
                    cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}")
                    ctx.guild.get_channel(cursor.fetchone()[0]).fetch_message(shop_message).delete()
                    cursor.execute(f"UPDATE guild_stats SET shop_message_id = 0 WHERE id = {ctx.guild.id}")

            await update_shop(role)

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def max_warns(ctx, max_warn: int = 3):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('max_warns')

        cursor.execute(f"UPDATE guild_stats SET max_warn = {max_warn} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def shop_channel(ctx, channel: discord.TextChannel = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('shop_channel')

        if channel is None:
            channel = ctx.channel
        cursor.execute(f"SELECT shop_message_id FROM guild_stats WHERE id = {ctx.guild.id}")
        shop_message = cursor.fetchone()
        if shop_message is not None:
            if shop_message[0] != 0:
                cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {ctx.guild.id}")
                msg = await ctx.guild.get_channel(cursor.fetchone()[0]).fetch_message(shop_message)
                msg.delete()
        cursor.execute(f"UPDATE guild_stats SET shop_channel_id = {channel.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await update_shop(channel=channel)

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def create_voice_creator(ctx, name: str = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    print([crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')])
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('create_voice_creator')

        if name is None:
            name = 'Создать-голосовой'

        voice = await ctx.channel.category.create_voice_channel(name=name, user_limit=1)
        cursor.execute(f"UPDATE guild_stats SET create_voice_id = {voice.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def distribution_channel(ctx, channel: discord.TextChannel = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('distribution_channel')

        if channel is None:
            channel = ctx.channel

        cursor.execute(f"UPDATE guild_stats SET distribution_channel_id = {channel.id} WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def add_moder_role(ctx, role: discord.Role = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('add_moder_role')

        cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
        roles = str(cursor.fetchone()[0])  + '_' + str(role.id)
        cursor.execute(f"UPDATE guild_stats SET moder_roles = '{roles}' WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def remove_moder_role(ctx, role: discord.Role = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('remove_moder_role')

        cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
        moder_roles = cursor.fetchone()[0].replace(f"_{role.id}", "")
        cursor.execute(f"UPDATE guild_stats SET moder_roles = '{moder_roles}' WHERE id = {ctx.guild.id}")
        connection.commit()

        await ctx.message.add_reaction('✅')
        await ctx.message.delete(delay=5)


@client.command()
async def price(ctx, game):
    try:
        page_game_name = 'https://hot-game.info/q=' + game
        game_name_full_page = requests.get(page_game_name, headers=headers)
        game_sou = BeautifulSoup(game_name_full_page.content, 'html.parser')
        game_link = 'https://hot-game.info' + game_sou.find_all('div', "game-preview hg-block")[0].find_all("a")[0].attrs['href']
        game_full_page = requests.get(game_link, headers=headers)
        game_soup = BeautifulSoup(game_full_page.content, 'html.parser')
        game_link_buy = game_soup.find_all("div", "price-block", "digital")
        messy = await ctx.message.channel.send(game_soup.find("div", "game-price-title red").h1.text)
        await messy.delete(delay=30)
        for i in range(len(game_link_buy)):
            if game_link_buy[i].span.text != 'нет в наличии' and i < 7:
                embed = discord.Embed(title='Цена: ' + game_link_buy[i].span.span.text, url=game_link_buy[i].div.attrs['data-href'])
                messy = await ctx.message.channel.send(embed=embed.set_image(url=game_link_buy[i].img.attrs['src']))
                await messy.delete(delay=30)
                await asyncio.sleep(0.1)
    except:
        my_msg = await ctx.message.channel.send('Укажите более точное название игры, но возможно её нет в моих магазинах')
        my_msg.delete(delay=10)


@client.command()
async def stats(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    embed = discord.Embed(title=f"📊Статистика {member.name}")
    embed.add_field(name="**Высшая роль**", value=member.top_role, inline=True)
    cursor.execute(f"SELECT cash FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    embed.add_field(name="**Баланс**", value=str(int(round(cursor.fetchone()[0],0))) + " 💵", inline=True)
    cursor.execute(f"SELECT reputation FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    embed.add_field(name="**Репутация**", value=str(int(round(cursor.fetchone()[0]))) + "", inline=True)
    cursor.execute(f"SELECT lvl FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    embed.add_field(name="**Уровень**", value=str(cursor.fetchone()[0]), inline=True)
    cursor.execute(f"SELECT lvl FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    if cursor.fetchone()[0] == 50:
        embed.colour = discord.Colour.gold()
        embed.add_field(name="**Опыт**", value="**1000/1000**", inline=True)
    else:
        cursor.execute(f"SELECT lvl FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
        level = cursor.fetchone()[0]
        cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}")
        maxl = cursor.fetchone()[0]
        cursor.execute(f"SELECT exp FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
        embed.add_field(name="**Опыт**", value=str(cursor.fetchone()[0]) + "/" + str(maxl), inline=True)
    cursor.execute(f"SELECT max_warn FROM guild_stats WHERE id = {member.guild.id}")
    maxw = cursor.fetchone()[0]
    cursor.execute(f"SELECT warn FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    embed.add_field(name="**Варны**", value="{}/{}".format(cursor.fetchone()[0], maxw), inline=True)

    await ctx.message.add_reaction('✅')
    msg = await ctx.send(embed=embed)
    await ctx.message.delete(delay=20)
    await msg.delete(delay=20)


@client.command()
async def cash(ctx, member: discord.Member = None):
    if member is None:
        member = ctx.author
    cursor.execute(f"SELECT cash FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
    embed = discord.Embed(description=f"**Баланс** {member.name}: " + str(int(round(cursor.fetchone()[0],0))) + " 💵")

    await ctx.message.add_reaction('✅')
    msg = await ctx.send(embed=embed)
    await ctx.message.delete(delay=20)
    await msg.delete(delay=20)


@client.command()
async def give(ctx, member: discord.Member = None, count: int = None):
    if member is not None and count is not None:
        print('give')

        cursor.execute(f"SELECT cash FROM {'users_' + str(ctx.guild.id)} WHERE id = {ctx.author.id}")
        rest = cursor.fetchone()[0] - count

        if rest >= 0:
            cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET cash = {rest} WHERE id = {ctx.author.id}")
            cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET cash = cash + {count} WHERE id = {member.id}")
            connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def take(ctx, member: discord.Member = None, count: int = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        if member is not None and count is not None:
            print('take')

            cursor.execute(f"SELECT cash FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
            rest = cursor.fetchone()[0] - count

            if rest >= 0:
                cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET cash = cash -{count} WHERE id = {member.id}")
                connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def award(ctx, member: discord.Member = None, count: int = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        if member is not None and count is not None:
            print('award')

            cursor.execute(f"SELECT cash FROM {'users_' + str(ctx.guild.id)} WHERE id = {member.id}")
            rest = cursor.fetchone()[0] + count

            if rest >= 0:
                cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET cash = cash + {count} WHERE id = {member.id}")
                connection.commit()

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


@client.command()
async def clean(ctx, amount: int = 10):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        print('clean')

        if amount > 100:
            amount = 100
        await ctx.channel.purge(limit=amount + 1)
        message = await ctx.send(embed=discord.Embed(description=f"Удалено {amount} сообщений"))
        await message.delete(delay=5)


@client.command()
async def warn(ctx, member: discord.Member = None, reason: str = None):
    cursor.execute(f"SELECT moder_roles FROM guild_stats WHERE id = {ctx.guild.id}")
    if [crossing for crossing in ctx.author.roles if crossing.id in cursor.fetchone()[0].split('_')] or ctx.author.guild_permissions.administrator or ctx.author.id == 533651610249986048:
        if member is not None:
            print('warn')

            cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET warn = warn + 1 WHERE id = {member.id}")
            cursor.execute(f"UPDATE {'users_' + str(ctx.guild.id)} SET reputation = reputation - 40 WHERE id = {member.id}")

            await ctx.message.add_reaction('✅')
            await ctx.message.delete(delay=5)


async def check_time():
    await asyncio.sleep(1)
    while True:
        await fill_db()

        print('check_time')

        if str(datetime.datetime.now()).split(' ')[1].split(':')[1] in ['00', '01', '02', '03', '04']:
            for member in client.get_all_members():
                if member.bot == 0 and cursor.execute(f"SELECT id FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}") is not None:
                    rep_rate = 1
                    cash_rate = 1
                    bad_role = 0
                    good_role = 0
                    for role in member.roles:
                        if cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE id = {role.id}") is not None:
                            cursor.execute(f"SELECT rep_rate FROM {'roles_' + str(role.guild.id)} WHERE id = {role.id}")
                            rep_rate += cursor.fetchone()[0]
                            cursor.execute(f"SELECT cash_rate FROM {'roles_' + str(role.guild.id)} WHERE id = {role.id}")
                            cash_rate += cursor.fetchone()[0]

                            cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -3")
                            rol3 = cursor.fetchone()
                            cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -2")
                            if cursor.fetchone() is not None:
                                cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -2")
                                if cursor.fetchone()[0] == role.id:
                                    cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -2")
                                    bad_role = cursor.fetchone()[0]
                            elif rol3 is not None:
                                cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -3")
                                if cursor.fetchone()[0]:
                                    cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -3")
                                    good_role = cursor.fetchone()[0]

                    rep_rate = round(rep_rate, 2)
                    cash_rate = round(cash_rate, 2)
                    cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET rep_rate = {rep_rate} WHERE id = {member.id}")
                    cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET cash_rate = {cash_rate} WHERE id = {member.id}")
                    connection.commit()

                    cursor.execute(f"SELECT max_warn FROM guild_stats WHERE id = {member.guild.id}")
                    max_warn = cursor.fetchone()[0]

                    if bad_role == 0 and bad_role is not None:
                        cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -2")
                        bad_role = cursor.fetchone()

                        cursor.execute(f"SELECT reputation FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        warn4 = cursor.fetchone()[0]
                        cursor.execute(f"SELECT warn FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        if (cursor.fetchone()[0] >= max_warn or warn4 <= max_warn * -40) and bad_role is not None:
                            await member.add_roles(member.guild.get_role(bad_role[0]))
                    elif good_role == 0 and bad_role is not None:
                        cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE lvl_role = -3")
                        good_role = cursor.fetchone()

                        cursor.execute(f"SELECT reputation FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        max4 = cursor.fetchone()[0]
                        cursor.execute(f"SELECT warn FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        if (cursor.fetchone()[0] < max_warn and max4 >= max_warn * 120) and good_role is not None:
                            await member.add_roles(member.guild.get_role(good_role[0]))

                    if bad_role != 0 and bad_role is not None:
                        cursor.execute(f"SELECT reputation FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        if cursor.fetchone()[0] >= 0:
                            await member.remove_roles(member.guild.get_role(bad_role[0]))
                    elif good_role != 0 and bad_role is not None:
                        cursor.execute(f"SELECT reputation FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        if cursor.fetchone()[0] <= 120:
                            await member.remove_roles(member.guild.get_role(good_role[0]))
        
        full_page = requests.get(website, headers=headers)
        soup = BeautifulSoup(full_page.content, 'html.parser')
        elements_game = soup.find_all("div", "col-lg-4 col-md-4 three-columns post-box")
        elements_game.reverse()

        for num in range(9):
            url = elements_game[num].find("div", "entry-content").p.text
            if url == url.replace('http', '') or url == url.replace('Страница раздачи: ', ''):
                embed = discord.Embed(title=elements_game[num].find("h2", "entry-title").a.text)
            else:
                embed = discord.Embed(title=elements_game[num].find("h2", "entry-title").a.text, url=url.split('Страница раздачи: ')[1].split()[0])
            img = elements_game[num].find('img', 'attachment-banner-small-image size-banner-small-image wp-post-image lazyloaded')
            if img is None:
                img = elements_game[num].find('img', 'attachment-banner-small-image size-banner-small-image wp-post-image lazyload')
                
            cursor.execute("SELECT distribution_channel_id, id FROM guild_stats")
            for guild in cursor.fetchall():
                if guild[0] != 0:
                    channel = client.get_channel(guild[0])
                    cursor.execute(f"SELECT last_game_time FROM guild_stats WHERE id = {guild[1]}")
                    if (datetime.datetime.strptime(elements_game[num].find('time', 'entry-date published').attrs['datetime'], '%Y-%m-%dT%H:%M:%S+03:00') - datetime.datetime(1970, 1, 1)).total_seconds() > cursor.fetchone()[0] and elements_game[num].find("span", "entry-cats").find_all("a")[1].text == 'Активная':
                        await channel.send(embed=embed.set_image(url=img.attrs['data-src']))
                        last_game = int((datetime.datetime.strptime(elements_game[num].find('time', 'entry-date published').attrs['datetime'], '%Y-%m-%dT%H:%M:%S+03:00') - datetime.datetime(1970, 1, 1)).total_seconds())
                        cursor.execute(f"UPDATE guild_stats SET last_game_time = '{last_game}' WHERE id = {guild[1]}")
                        connection.commit()
                await asyncio.sleep(0.1)

        for member in client.get_all_members():
            if member.bot == 0:
                cursor.execute(f"SELECT lvl FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                if cursor.fetchone()[0] != 50:
                    cursor.execute(f"SELECT lvl FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                    level = cursor.fetchone()[0]
                    cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}")
                    minl = cursor.fetchone()[0]
                    cursor.execute(f"SELECT exp FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                    if member.bot == 0 and cursor.fetchone()[0] >= minl:
                        cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET lvl = lvl + 1 WHERE id = {member.id}")
                        connection.commit()

            if member.bot == 0 and cursor.execute(f"SELECT id FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}") is not None and member.voice is not None:
                if not member.voice.afk:
                    cursor.execute(f"SELECT cash_rate FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                    cash_rate = cursor.fetchone()[0]
                    cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET cash = cash + {cash_rate} WHERE id = {member.id}")
                    cursor.execute(f"SELECT lvl FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                    level = cursor.fetchone()[0]
                    if level != 50:
                        cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET exp = exp + 1 WHERE id = {member.id}")
                        cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}")
                        exl = cursor.fetchone()[0]
                        cursor.execute(f"SELECT exp FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                        if cursor.fetchone()[0] >= exl:
                            cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET lvl = lvl + 1 WHERE id = {member.id}")
                            cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET exp = 0 WHERE id = {member.id}")
                            cursor.execute(f"SELECT lvl FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
                            level = cursor.fetchone()[0]
                            cursor.execute(f"SELECT id FROM {'roles_' + str(member.guild.id)} WHERE lvl_role = {level}")
                            role = cursor.fetchone()
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
        cursor.execute(f"SELECT id FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
        if cursor.fetchone() is None:
            cursor.execute(f"INSERT INTO {'users_' + str(member.guild.id)} (nickname, id, cash, rep_rate, cash_rate, reputation, warn, lvl, exp, channel_owner) VALUES ('{member.name}', {member.id}, 0, 1, 1, 0, 0, 0, 0, 0)")
            connection.commit()
        else:
            cursor.execute(f"SELECT nickname FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
            if cursor.fetchone()[0] != member.name:
                cursor.execute(f"UPDATE {'users_' + str(member.guild.id)} SET nickname = '{member.name}' WHERE id = {member.id}")
                connection.commit()

        cursor.execute(f"SELECT lvl FROM {'users_' + str(member.guild.id)} WHERE id = {member.id}")
        level = cursor.fetchone()[0]
        if cursor.execute(f"SELECT exp FROM lvls WHERE lvl = {level}") is None:
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
                cursor.execute(f"INSERT INTO lvls (lvl, exp) VALUES ({i}, {a})")
                connection.commit()


async def fill_role(role: discord.Role):
    cursor.execute(f"SELECT id FROM {'roles_' + str(role.guild.id)} WHERE id = {role.id}")
    if cursor.fetchone() is None:
        cursor.execute(f"INSERT INTO {'roles_' + str(role.guild.id)} (nickname, id, cash_rate, rep_rate, lvl_role, cost, emoji, serial_number) VALUES ('{role.name}', {role.id}, 0, 0, -1, 0, '0', 0)")
        connection.commit()

    else:
        cursor.execute(f"SELECT nickname FROM {'roles_' + str(role.guild.id)} WHERE id = {role.id}")
        if cursor.fetchone()[0] != role.name:
            cursor.execute(f"UPDATE {'roles_' + str(role.guild.id)} SET nickname = '{role.name}' WHERE id = {role.id}")
            connection.commit()


async def update_shop(role: discord.Role = None, channel: discord.TextChannel = None):
    print('update_shop')

    if role is not None:
        cursor.execute(f"SELECT shop_channel_id FROM guild_stats WHERE id = {role.guild.id}")
        channel = cursor.fetchone()
        if channel is not None:
            channel = role.guild.get_channel(channel[0])

    if channel is not None:
        role_list = []
        for rol in channel.guild.roles:
            cursor.execute(f"SELECT serial_number FROM {'roles_' + str(rol.guild.id)} WHERE id = {rol.id}")
            ser_num = cursor.fetchone()[0]
            if ser_num > 0:
                role_list.append(str(ser_num) + '_' + str(rol.id))
        role_list.sort()
        if role_list:
            embed = discord.Embed(title="**Магазин ролей**")
            for rol in role_list:
                rol = channel.guild.get_role(int(rol.split('_')[1]))
                cursor.execute(f"SELECT cost FROM {'roles_' + str(rol.guild.id)} WHERE id = {rol.id}")
                rol_cost = cursor.fetchone()[0]
                cursor.execute(f"SELECT emoji FROM {'roles_' + str(rol.guild.id)} WHERE id = {rol.id}")
                embed.add_field(name=rol.name + ' - ' + cursor.fetchone()[0], value=f"Цена: {rol_cost} 💲", inline=True)
            msg = await channel.send(embed=embed)
            cursor.execute(f"UPDATE guild_stats SET shop_message_id = {msg.id} WHERE id = {channel.guild.id}")
            connection.commit()
            for rol in role_list:
                rol = channel.guild.get_role(int(rol.split('_')[1]))
                cursor.execute(f"SELECT emoji FROM {'roles_' + str(rol.guild.id)} WHERE id = {rol.id}")
                rol_emoji = cursor.fetchone()[0]
                await msg.add_reaction(rol_emoji)


token = os.environ.get('BOT_TOKEN')
client.run(str(token))
