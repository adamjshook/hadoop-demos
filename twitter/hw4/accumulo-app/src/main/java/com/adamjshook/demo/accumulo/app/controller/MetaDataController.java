package com.adamjshook.demo.accumulo.app.controller;

import com.adamjshook.demo.accumulo.app.model.DataFetcher;
import com.adamjshook.demo.accumulo.app.model.TweetBean;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.SimpleDateFormat;
import java.util.List;

@Controller()
public class MetaDataController
{
    private static final Logger LOG = LoggerFactory.getLogger(MetaDataController.class);
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private DataFetcher fetcher = null;

    public MetaDataController()
    {
        try {
            fetcher = DataFetcher.getDefault();
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    @RequestMapping(value = "/lookup", method = RequestMethod.GET)
    @ResponseBody
    public List<TweetBean> lookup(@RequestParam(value = "word", defaultValue = "") String word)
            throws Exception
    {
        LOG.info("Looking up tweets containing {}", word);
        List<TweetBean> tweets = fetcher.fetchTweetsFromIndex(word);
        LOG.info("Returning {} tweets", tweets.size());
        return tweets;
    }

    @RequestMapping(value = "/hashtags", method = RequestMethod.GET)
    @ResponseBody
    public List<String> hashtags()
            throws Exception
    {
        LOG.info("Loading trending hashtags");
        List<String> hts = fetcher.fetchTrendingHashtags();
        LOG.info("Returning {} hashtags", hts.size());
        return hts;
    }

    @RequestMapping(value = "/users", method = RequestMethod.GET)
    @ResponseBody
    public List<String> users()
            throws Exception
    {
        LOG.info("Loading popular users");
        List<String> users = fetcher.fetchPopularUsers();
        LOG.info("Returning {} users", users.size());
        return users;
    }
}
