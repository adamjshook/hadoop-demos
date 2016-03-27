package com.adamjshook.accumulo.demo.app.model;

import java.util.List;

public abstract class DataFetcher
{
	/**
	 * Gets the default data fetcher
	 * 
	 * @return
	 */
	public static DataFetcher getDefault()
	{
		return new GeneratedDataFetcher();
	}

	/**
	 * Gets the tweets that contain the given word
	 *
	 * @param word
	 *            Word to search for
	 * @return A list of tweets containing the word, preferably a short one
	 */
	public abstract List<TweetBean> fetchTweetsFromIndex(String word);

	/**
	 * Gets the top ten trending hashtags
	 *
	 * @return
	 */
	public abstract List<String> fetchTrendingHashtags();

	/**
	 * Gets the top ten popular users
	 *
	 * @return
	 */
	public abstract List<String> fetchPopularUsers();
}
