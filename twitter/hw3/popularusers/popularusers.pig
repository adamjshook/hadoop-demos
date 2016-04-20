tweets = LOAD '$input' USING AvroStorage();

A = GROUP tweets BY user_id;
B = FOREACH A {
	result = ORDER tweets BY followers_count;
    result = TOP(1, 0, tweets);
    GENERATE FLATTEN(result);
};
C = FOREACH B GENERATE user_id, screen_name, followers_count, statuses_count;
D = ORDER C BY followers_count DESC;
E = LIMIT D 100;

STORE E INTO '$output';

