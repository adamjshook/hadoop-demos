tweets = LOAD '$input' USING AvroStorage();

A = GROUP tweets BY user_id;
B = FOREACH A GENERATE group AS user_id, COUNT(tweets) AS ctn;
C = JOIN tweets BY user_id, B by user_id;
D = FOREACH C GENERATE B::user_id, screen_name, ctn;
E = DISTINCT D;
F = ORDER E BY ctn DESC;
G = LIMIT F 100;

STORE G INTO '$output';
