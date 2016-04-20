tweets = LOAD '$input' USING AvroStorage();

A = FOREACH tweets GENERATE FLATTEN(TOKENIZE(text)), id;

STORE A INTO '$output';
