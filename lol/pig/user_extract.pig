
part = LOAD '$input' USING AvroStorage();
DESCRIBE part;
--part: {
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

user_info = FOREACH part GENERATE
    summonerId,
    summonerName,
    championId;

DESCRIBE user_info;

STORE user_info INTO '$output';