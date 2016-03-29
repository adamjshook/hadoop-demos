
part = LOAD '$input' USING AvroStorage();

--part: {
-- uuid: chararray,
-- matchId: int,
-- championId: int,
-- teamId: int,
-- winner: boolean,
-- summonerId: int,
-- summonerName: chararray,
-- firstInhibitor: boolean,
-- firstBlood: boolean,
-- firstTower: boolean
-- }

champ_groups = GROUP part BY championId;
--champ_groups: {group: int,part:
-- {
--  (uuid: chararray,
--  matchId: int,
--  championId: int,
--  teamId: int,
--  winner: boolean,
--  summonerId: int,
--  summonerName: chararray,
--  firstInhibitor: boolean,
--  firstBlood: boolean,
--  firstTower: boolean)
-- }}

count_groups = FOREACH champ_groups GENERATE
    group AS id,
    COUNT(part) AS cnt;

champs = LOAD 'champs.txt' AS (id: int,
    name: chararray,
    title: chararray);

count_group_champs = JOIN count_groups BY id,
                            champs BY id;
--count_group_champs: {
-- count_groups::id: int,
-- count_groups::cnt: long,
-- champs::id: int,
-- champs::name: chararray,
-- champs::title: chararray}

enriched = FOREACH count_group_champs GENERATE champs::id, name, title, cnt;
order_groups = ORDER enriched BY cnt DESC;
lmt_champ_groups = LIMIT order_groups 10;

STORE lmt_champ_groups INTO '$output';
