package com.adamjshook.demo.accumulo;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter.Tweet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import static java.lang.String.format;

public class Main
        extends Configured
        implements Tool
{
    private static Logger LOG = LoggerFactory.getLogger(Main.class);

    private static final char INSTANCE_OPT = 'i';
    private static final char ZOOKEEPER_OPT = 'z';
    private static final char USER_OPT = 'u';
    private static final char PASSWORD_OPT = 'p';
    private static final char TWEET_DIR = 't';
    private static final char INDEX_DIR = 'n';
    private static final char HASHTAGS_DIR = 'h';
    private static final char POPULAR_USERS_DIR = 'o';

    private String instance;
    private String zooKeepers;
    private String user;
    private String password;
    private Path tweetDir;
    private Path indexDir;
    private Path hashtagDir;
    private Path popUsersDir;

    public static final String TWEET_TABLE = "tweets";
    public static final String TWEET_INDEX_TABLE = "tweet_index";
    public static final String HASHTAGS_TABLE = "hashtags";
    public static final String TMP_HASHTAGS_TABLE = "hashtags_tmp";
    public static final String POPULAR_USERS_TABLE = "popular_users";
    public static final String TMP_POPULAR_USERS_TABLE = "popular_users_tmp";

    public static final String TWEET_CF = "tweet";
    public static final String CREATED_AT_CQ = "created";
    public static final String TEXT_CQ = "text";
    public static final String SOURCE_CQ = "source";
    public static final String RETWEET_COUNT_CQ = "retweet_count";
    public static final String FAVORITE_COUNT_CQ = "favorite_count";

    public static final String USER_CF = "user";
    public static final String USER_ID_CQ = "user_id";
    public static final String SCREEN_NAME_CQ = "screen_name";
    public static final String LOCATION_CQ = "location";
    public static final String DESCRIPTION_CQ = "description";
    public static final String FOLLOWERS_COUNT_CQ = "followers_count";
    public static final String STATUSES_COUNT_CQ = "statuses_count";
    public static final String GEO_ENABLED_CQ = "geo_enabled";
    public static final String LANG_CQ = "lang";
    public static final String LATITUDE_CQ = "lat";
    public static final String LONGITUDE_CQ = "long";

    public static final String HASHTAGS_CF = "ht";
    public static final String URLS_CF = "url";
    public static final String MENTIONS_CF = "mention";
    public static final String INDEX_CF = "idx";
    public static final String EMPTY_STRING = "";

    private Connector conn;
    private FileSystem fs;

    @Override
    public int run(String[] args)
            throws Exception
    {
        if (!parseCommandLine(args)) {
            return 1;
        }

        Instance inst = new ZooKeeperInstance(getInstance(), getZooKeepers());
        conn = inst.getConnector(getUser(),
                new PasswordToken(getPassword()));

        fs = FileSystem.get(getConf());

        ingestTweets();
        ingestIndex();
        ingestHashTags();
        ingestPopularUsers();

        return 0;
    }

    private void ingestTweets()
    {
        try {
            if (!fs.exists(getTweetDir()) || fs.isFile(getTweetDir())) {
                LOG.warn(format("%s does not exist or is a file",
                        getTweetDir()));
                return;
            }

            validateTable(TWEET_TABLE);

            BatchWriter wrtr = conn.createBatchWriter(TWEET_TABLE, new BatchWriterConfig());

            FileStatus[] files = fs.listStatus(getTweetDir(), new HiddenFileFilter());

            int numMutations = 0;
            for (FileStatus stat : files) {
                if (fs.isDirectory(stat.getPath())) {
                    continue;
                }

                LOG.info(format("Ingesting %s", stat.getPath()));

                DataFileStream<Tweet> data = new DataFileStream<>(fs.open(stat.getPath()), new SpecificDatumReader<Tweet>(Tweet.class));
                Tweet t = new Tweet();

                while (data.hasNext()) {
                    t = data.next(t);

                    Mutation m = new Mutation(t.getId().toString());

                    m.put(TWEET_CF, CREATED_AT_CQ, t.getCreatedAt().toString());
                    m.put(TWEET_CF, TEXT_CQ, t.getText().toString());
                    m.put(TWEET_CF, SOURCE_CQ, t.getSource().toString());
                    m.put(TWEET_CF, RETWEET_COUNT_CQ, t.getRetweetCount().toString());
                    m.put(TWEET_CF, FAVORITE_COUNT_CQ, t.getFavoriteCount().toString());

                    m.put(USER_CF, USER_ID_CQ, t.getUserId().toString());
                    m.put(USER_CF, SCREEN_NAME_CQ, t.getScreenName().toString());

                    if (t.getLocation() != null) {
                        m.put(USER_CF, LOCATION_CQ, t.getLocation().toString());
                    }

                    if (t.getDescription() != null) {
                        m.put(USER_CF, DESCRIPTION_CQ, t.getDescription().toString());
                    }

                    m.put(USER_CF, FOLLOWERS_COUNT_CQ, t.getFollowersCount().toString());
                    m.put(USER_CF, STATUSES_COUNT_CQ, t.getStatusesCount().toString());
                    m.put(USER_CF, GEO_ENABLED_CQ, t.getGeoEnabled().toString());
                    m.put(USER_CF, LANG_CQ, t.getLang().toString());

                    if (t.getLatitude() != null) {
                        m.put(USER_CF, LATITUDE_CQ, t.getLatitude().toString());
                    }

                    if (t.getLongitude() != null) {
                        m.put(USER_CF, LONGITUDE_CQ, t.getLongitude().toString());
                    }

                    for (CharSequence s : t.getHashtags()) {
                        m.put(HASHTAGS_CF, s, EMPTY_STRING);
                    }

                    for (CharSequence s : t.getUrls()) {
                        m.put(URLS_CF, s, EMPTY_STRING);
                    }

                    for (CharSequence s : t.getMentions()) {
                        m.put(MENTIONS_CF, s, EMPTY_STRING);
                    }

                    wrtr.addMutation(m);
                    ++numMutations;
                }
            }

            wrtr.close();
            LOG.info(format("Wrote %d mutations to %s", numMutations, TWEET_TABLE));
        }
        catch (IOException | TableNotFoundException | MutationsRejectedException e) {
            LOG.error("Failed to ingest tweets data set", e);
        }
    }

    private void ingestIndex()
    {
        try {
            if (!fs.exists(getIndexDir()) || fs.isFile(getIndexDir())) {
                LOG.warn(format("%s does not exist or is a file",
                        getIndexDir()));
                return;
            }

            validateTable(TWEET_INDEX_TABLE);

            BatchWriter wrtr = conn.createBatchWriter(TWEET_INDEX_TABLE, new BatchWriterConfig());
            FileStatus[] files = fs.listStatus(getIndexDir(), new HiddenFileFilter());

            int badLines = 0;
            int numMutations = 0;
            for (FileStatus stat : files) {
                if (fs.isDirectory(stat.getPath())) {
                    continue;
                }

                LOG.info(format("Ingesting %s", stat.getPath()));

                BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));

                String line;
                while ((line = rdr.readLine()) != null) {
                    String[] tokens = line.split("\t");

                    if (tokens.length != 2) {
                        ++badLines;
                        continue;
                    }

                    // word as row id
                    Mutation m = new Mutation(tokens[0]);

                    // tweet ID as CQ
                    m.put(INDEX_CF, tokens[1], EMPTY_STRING);

                    wrtr.addMutation(m);
                    ++numMutations;
                }
            }

            wrtr.close();
            LOG.info(format("Wrote %d mutations to %s, %s bad lines", numMutations, TWEET_INDEX_TABLE, badLines));
        }
        catch (IOException | TableNotFoundException | MutationsRejectedException e) {
            LOG.error("Failed to ingest tweets data set", e);
        }
    }

    private void ingestHashTags()
    {
        try {
            if (!fs.exists(getHashtagDir()) || fs.isFile(getHashtagDir())) {
                LOG.warn(format("%s does not exist or is a file",
                        getHashtagDir()));
                return;
            }

            deleteTable(TMP_HASHTAGS_TABLE);
            validateTable(TMP_HASHTAGS_TABLE);

            BatchWriter wrtr = conn.createBatchWriter(TMP_HASHTAGS_TABLE, new BatchWriterConfig());

            FileStatus[] files = fs.listStatus(getHashtagDir(), new HiddenFileFilter());

            int badLines = 0;
            int numMutations = 0;
            for (FileStatus stat : files) {
                if (fs.isDirectory(stat.getPath())) {
                    continue;
                }

                LOG.info(format("Ingesting %s", stat.getPath()));

                BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));

                String line;
                while ((line = rdr.readLine()) != null) {
                    String[] tokens = line.split("\t");

                    if (tokens.length != 3) {
                        ++badLines;
                        continue;
                    }

                    // count as row ID
                    Mutation m = new Mutation(tokens[2]);
                    m.put(HASHTAGS_CF, tokens[1], EMPTY_STRING);

                    wrtr.addMutation(m);
                    ++numMutations;
                }
            }

            wrtr.close();
            LOG.info(format("Wrote %d mutations to %s, %s bad lines", numMutations, TMP_HASHTAGS_TABLE, badLines));

            deleteTable(HASHTAGS_TABLE);
            renameTable(TMP_HASHTAGS_TABLE, HASHTAGS_TABLE);
        }
        catch (IOException | TableNotFoundException | MutationsRejectedException e) {
            LOG.error("Failed to ingest tweets data set", e);
        }
    }

    private void ingestPopularUsers()
    {
        try {
            if (!fs.exists(getPopUsersDir()) || fs.isFile(getPopUsersDir())) {
                LOG.warn(format("%s does not exist or is a file",
                        getPopUsersDir()));
                return;
            }

            deleteTable(TMP_POPULAR_USERS_TABLE);
            validateTable(TMP_POPULAR_USERS_TABLE);

            BatchWriter wrtr = conn.createBatchWriter(TMP_POPULAR_USERS_TABLE, new BatchWriterConfig());

            FileStatus[] files = fs.listStatus(getPopUsersDir(), new HiddenFileFilter());

            int badLines = 0;
            int numMutations = 0;
            for (FileStatus stat : files) {
                if (fs.isDirectory(stat.getPath())) {
                    continue;
                }
                LOG.info(format("Ingesting %s", stat.getPath()));

                BufferedReader rdr = new BufferedReader(new InputStreamReader(fs.open(stat.getPath())));

                String line;
                while ((line = rdr.readLine()) != null) {
                    String[] tokens = line.split("\t");

                    if (tokens.length != 4) {
                        ++badLines;
                        continue;
                    }

                    // follower count as row ID
                    Mutation m = new Mutation(tokens[2]);

                    m.put(USER_CF, SCREEN_NAME_CQ, tokens[1]);
                    m.put(USER_CF, USER_ID_CQ, tokens[0]);
                    m.put(USER_CF, STATUSES_COUNT_CQ, tokens[3]);

                    wrtr.addMutation(m);
                    ++numMutations;
                }
            }

            wrtr.close();
            LOG.info(format("Wrote %d mutations to %s, %s bad lines", numMutations, TMP_POPULAR_USERS_TABLE, badLines));

            deleteTable(POPULAR_USERS_TABLE);
            renameTable(TMP_POPULAR_USERS_TABLE, POPULAR_USERS_TABLE);
        }
        catch (IOException | TableNotFoundException | MutationsRejectedException e) {
            LOG.error("Failed to ingest tweets data set", e);
        }
    }

    private void validateTable(String table)
    {
        try {
            if (!conn.tableOperations().exists(table)) {
                conn.tableOperations().create(table);
                LOG.info(format("Created %s", table));
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void renameTable(String oldName, String newName)
    {
        try {
            conn.tableOperations().rename(oldName, newName);
            LOG.info(format("Renamed %s to %s", oldName, newName));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTable(String table)
    {
        if (conn.tableOperations().exists(table)) {
            try {
                conn.tableOperations().delete(table);
                LOG.info(format("Deleted %s", table));
            }
            catch (Exception e) {
                // suppress
            }
        }
    }

    private boolean parseCommandLine(String[] args)
    {
        // If no arguments, print help
        if (args.length == 0) {
            printHelp();
            return false;
        }

        // Parse command line
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(getOptions(), args);
        }
        catch (ParseException e) {
            System.err.println(e.getMessage());
            printHelp();
            return false;
        }

        // Print help if the option is set
        if (cmd.hasOption("help")) {
            printHelp();
            return false;
        }
        else {
            this.setInstance(cmd.getOptionValue(INSTANCE_OPT));
            this.setZooKeepers(cmd.getOptionValue(ZOOKEEPER_OPT));
            this.setUser(cmd.getOptionValue(USER_OPT));
            this.setPassword(cmd.getOptionValue(PASSWORD_OPT));
            this.setTweetDir(new Path(cmd.getOptionValue(TWEET_DIR)));
            this.setIndexDir(new Path(cmd.getOptionValue(INDEX_DIR)));
            this.setHashtagDir(new Path(cmd.getOptionValue(HASHTAGS_DIR)));
            this.setPopUsersDir(new Path(cmd.getOptionValue(POPULAR_USERS_DIR)));
        }
        return true;
    }

    private void printHelp()
    {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("usage: java -jar <jarfile> [args]", getOptions());
    }

    @SuppressWarnings("static-access")
    private Options getOptions()
    {
        Options opts = new Options();
        opts.addOption(
                OptionBuilder.isRequired().hasArg().withLongOpt("instance")
                        .withDescription("Accumulo instance name")
                        .create(INSTANCE_OPT));
        opts.addOption(
                OptionBuilder
                        .withDescription("Print this help")
                        .create("help"));
        opts.addOption(
                OptionBuilder.isRequired().hasArg().withLongOpt("zookeepers")
                        .withDescription("ZooKeeper connect string")
                        .create(ZOOKEEPER_OPT));
        opts.addOption(OptionBuilder.isRequired().hasArg().withLongOpt("user")
                .withDescription("Accumulo user name").create(USER_OPT));
        opts.addOption(OptionBuilder.isRequired().hasArg()
                .withLongOpt("password").withDescription("Accumulo password")
                .create(PASSWORD_OPT));
        opts.addOption(OptionBuilder.isRequired().hasArg()
                .withLongOpt("tweet-dir")
                .withDescription(
                        "Directory containing twitter data as Avro files")
                .create(TWEET_DIR));
        opts.addOption(OptionBuilder.isRequired().hasArg()
                .withLongOpt("index-dir")
                .withDescription("Directory containing twitter index pig output")
                .create(INDEX_DIR));
        opts.addOption(OptionBuilder.isRequired().hasArg()
                .withLongOpt("hashtags-dir")
                .withDescription("Directory containing hashtags pig output")
                .create(HASHTAGS_DIR));
        opts.addOption(OptionBuilder.isRequired().hasArg()
                .withLongOpt("pop-users-dir")
                .withDescription(
                        "Directory containing popular users pig output")
                .create(POPULAR_USERS_DIR));
        return opts;
    }

    public static void main(String[] args)
            throws Exception
    {
        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    public String getInstance()
    {
        return instance;
    }

    public void setInstance(String instance)
    {
        this.instance = instance;
    }

    public String getZooKeepers()
    {
        return zooKeepers;
    }

    public void setZooKeepers(String zooKeepers)
    {
        this.zooKeepers = zooKeepers;
    }

    public String getUser()
    {
        return user;
    }

    public void setUser(String user)
    {
        this.user = user;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public Path getTweetDir()
    {
        return tweetDir;
    }

    public void setTweetDir(Path tweetDir)
    {
        this.tweetDir = tweetDir;
    }

    public Path getIndexDir()
    {
        return indexDir;
    }

    public void setIndexDir(Path indexDir)
    {
        this.indexDir = indexDir;
    }

    public Path getHashtagDir()
    {
        return hashtagDir;
    }

    public void setHashtagDir(Path hashtagDir)
    {
        this.hashtagDir = hashtagDir;
    }

    public Path getPopUsersDir()
    {
        return popUsersDir;
    }

    public void setPopUsersDir(Path popUsersDir)
    {
        this.popUsersDir = popUsersDir;
    }

    public class HiddenFileFilter
            implements PathFilter
    {
        @Override
        public boolean accept(Path path)
        {
            char c = path.getName().charAt(0);
            return c != '_' && c != '.';
        }
    }
}
