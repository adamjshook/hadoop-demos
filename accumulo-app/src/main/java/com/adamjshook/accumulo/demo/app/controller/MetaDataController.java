package com.adamjshook.accumulo.demo.app.controller;

import com.adamjshook.accumulo.demo.app.model.DataFetcher;
import com.adamjshook.accumulo.demo.app.model.TweetBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

@Controller()
public class MetaDataController
{
	private static final Logger LOG = LoggerFactory.getLogger(MetaDataController.class);
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	private DataFetcher fetcher = DataFetcher.getDefault();

	@RequestMapping(value = "/lookup", method = RequestMethod.GET)
	@ResponseBody
	public List<TweetBean> filterData(@RequestParam(value = "word", defaultValue = "") String word)
			throws ParseException
	{
		LOG.info("Looking up tweets containing {}", word);
		List<TweetBean> tweets = fetcher.fetchTweetsFromIndex(word);
		LOG.info("Returning {} tweets", tweets.size());
		return tweets;
	}
}
