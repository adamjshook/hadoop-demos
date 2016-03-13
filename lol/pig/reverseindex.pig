
match = LOAD '$input' USING AvroStorage();

DESCRIBE match;
--match: {
-- mapId: int,
-- matchCreation: int,
-- matchDuration: int,
-- matchId: int,
-- matchMode: chararray,
-- winningTeam: int,
-- participants: {(int)}}

part_index = FOREACH match GENERATE
    FLATTEN(participants) AS participant,
    matchId;

STORE grp_index INTO '$output';