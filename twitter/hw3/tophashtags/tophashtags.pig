tweets = LOAD '$input' USING AvroStorage();

A = FOREACH tweets GENERATE id, FLATTEN(hashtags) AS ht;
B = GROUP A BY ht;
C = FOREACH B GENERATE group AS ht, COUNT(A) AS cnt;
D = ORDER C BY cnt DESC;
E = LIMIT D 100;
D = JOIN A BY ht, E BY ht USING 'replicated';
F = FOREACH D GENERATE id, A::ht, cnt;

STORE F INTO '$output';
