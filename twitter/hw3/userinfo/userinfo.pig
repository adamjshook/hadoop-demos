tweets = LOAD '$input' USING AvroStorage();

tweets = FOREACH tweets GENERATE ToDate(created_at, 'EEE MMM dd HH:mm:ss Z yyyy') AS created_at, id, user_id, screen_name, location, description, followers_count, statuses_count, geo_enabled, lang;
DESCRIBE tweets;
 
A = GROUP tweets BY user_id;
B = FOREACH A {
	result = ORDER tweets BY created_at DESC;
    result = TOP(1, 0, tweets);
    GENERATE FLATTEN(result);
};
C = FOREACH B GENERATE user_id, screen_name, location, description, followers_count, statuses_count, geo_enabled, lang;

STORE C INTO '$output';
