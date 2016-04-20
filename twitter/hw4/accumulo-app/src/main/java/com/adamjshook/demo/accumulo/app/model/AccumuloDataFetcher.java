package com.adamjshook.demo.accumulo.app.model;

import com.adamjshook.demo.accumulo.Main;
import com.google.common.collect.ImmutableList;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import static java.lang.String.format;

public class AccumuloDataFetcher
        extends DataFetcher
{
    private static final Logger LOG = LoggerFactory.getLogger(AccumuloDataFetcher.class);

    private SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

    private Connector conn;

    public AccumuloDataFetcher()
            throws AccumuloSecurityException, AccumuloException
    {
        conn = new ZooKeeperInstance("default", "localhost:2181").getConnector("root", new PasswordToken("secret"));
    }

    @Override
    public List<TweetBean> fetchTweetsFromIndex(String word)
            throws Exception
    {
        Scanner idxScanner = conn.createScanner(Main.TWEET_INDEX_TABLE,
                new Authorizations());
        idxScanner.setRange(new Range(word));

        Text cq = new Text();
        ImmutableList.Builder<Range> bldr = ImmutableList.builder();
        for (Entry<Key, Value> record : idxScanner) {
            record.getKey().getColumnQualifier(cq);
            bldr.add(new Range(cq.toString()));
        }
        idxScanner.close();
        List<Range> tweetIds = bldr.build();

        LOG.info("Retrieved {} IDs from index", tweetIds.size());

        ImmutableList.Builder<TweetBean> tbBldr = ImmutableList.builder();
        if (tweetIds.size() > 0) {
            BatchScanner dataScanner = conn.createBatchScanner(Main.TWEET_TABLE,
                    new Authorizations(), 10);
            dataScanner.setRanges(tweetIds);
            dataScanner.addScanIterator(new IteratorSetting(Integer.MAX_VALUE, WholeRowIterator.class));
            dataScanner.fetchColumn(new Text(Main.TWEET_CF), new Text(Main.TEXT_CQ));
            dataScanner.fetchColumn(new Text(Main.TWEET_CF), new Text(Main.CREATED_AT_CQ));
            dataScanner.fetchColumn(new Text(Main.USER_CF), new Text(Main.USER_ID_CQ));

            long id = 0;
            long userId = 0;
            String text = null;
            Date created = null;

            Text row = new Text();
            Text colQual = new Text();
            long numScanned = 0;
            for (Entry<Key, Value> entry : dataScanner) {
                ++numScanned;
                entry.getKey().getRow(row);

                id = Long.parseLong(row.toString());

                SortedMap<Key, Value> rowMap =
                        WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
                for (Entry<Key, Value> record : rowMap.entrySet()) {
                    record.getKey().getColumnQualifier(colQual);

                    switch (colQual.toString()) {
                        case Main.TEXT_CQ:
                            text = record.getValue().toString();
                            break;
                        case Main.CREATED_AT_CQ:
                            created = simpleDateFormat.parse(record.getValue().toString());
                            break;
                        case Main.USER_ID_CQ:
                            userId = Long.parseLong(record.getValue().toString());
                            break;
                    }
                }

                TweetBean tb = new TweetBean();
                tb.setCreated(created);
                tb.setId(id);
                tb.setTweet(text);
                tb.setUserId(userId);
                tbBldr.add(tb);

                if (numScanned == 20) {
                    break;
                }
            }
            LOG.info("Scanned {} entries", numScanned);
        }

        return tbBldr.build();
    }

    private class LongDescendingComparator
            implements Comparator<Long>
    {
        @Override
        public int compare(Long o1, Long o2)
        {
            return -o1.compareTo(o2);
        }
    }

    @Override
    public List<String> fetchTrendingHashtags()
            throws Exception
    {
        SortedMap<Long, String> topTags = new TreeMap<>(new LongDescendingComparator());
        Scanner scan = conn.createScanner(Main.HASHTAGS_TABLE,
                new Authorizations());

        Text row = new Text();
        Text cq = new Text();
        for (Entry<Key, Value> record : scan) {
            record.getKey().getRow(row);
            long count = Long.parseLong(row.toString());
            record.getKey().getColumnQualifier(cq);
            String hashtag = cq.toString();

            topTags.put(count, hashtag);

            if (topTags.size() > 10) {
                topTags.remove(topTags.lastKey());
            }
        }

        scan.close();

        LOG.info(format("Top tags are %s", topTags));

        return ImmutableList.copyOf(topTags.values());
    }

    @Override
    public List<String> fetchPopularUsers()
            throws Exception
    {
        SortedMap<Long, String> topUsers = new TreeMap<>(new LongDescendingComparator());
        Scanner scan = conn.createScanner(Main.POPULAR_USERS_TABLE,
                new Authorizations());
        scan.fetchColumn(new Text(Main.USER_CF), new Text(Main.SCREEN_NAME_CQ));

        Text row = new Text();
        for (Entry<Key, Value> record : scan) {
            record.getKey().getRow(row);
            long count = Long.parseLong(row.toString());
            String user = record.getValue().toString();

            topUsers.put(count, user);

            if (topUsers.size() > 10) {
                topUsers.remove(topUsers.firstKey());
            }
        }

        scan.close();

        LOG.info(format("Top users are %s", topUsers));

        return ImmutableList.copyOf(topUsers.values());
    }
}
