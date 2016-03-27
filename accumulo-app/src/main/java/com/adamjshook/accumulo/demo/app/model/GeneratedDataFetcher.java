package com.adamjshook.accumulo.demo.app.model;

import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class GeneratedDataFetcher
		extends DataFetcher
{
	private static final Logger LOG = LoggerFactory.getLogger(GeneratedDataFetcher.class);
	private Random rndm = new SecureRandom();
	private Faker faker = new Faker();

	@Override
	public List<TweetBean> fetchTweetsFromIndex(String word)
	{
		int num = rndm.nextInt(9) + 1;
		LOG.info("Generating {} events", num);
		List<TweetBean> tb = new ArrayList<>();
		for (int i = 0; i < num; ++i) {
			TweetBean bean = new TweetBean();
			bean.setCreated(new Date(System.currentTimeMillis()));
			bean.setId(Math.abs(rndm.nextLong()));
			bean.setUserId(Math.abs(rndm.nextLong()));

			String tweet = faker.lorem().paragraph();
			bean.setTweet(tweet.substring(0, Math.min(tweet.length(), 140)));
			LOG.info("{}", bean);
			tb.add(bean);
		}

		return tb;
	}

	@Override
	public List<String> fetchTrendingHashtags()
	{
		return faker.lorem().words(10);
	}
}
