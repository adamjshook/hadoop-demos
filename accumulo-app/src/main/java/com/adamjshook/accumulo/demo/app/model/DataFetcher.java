package com.adamjshook.accumulo.demo.app.model;

import java.util.List;

public abstract class DataFetcher
{
	public static DataFetcher getDefault()
	{
		return new GeneratedDataFetcher();
	}

	public abstract List<TweetBean> fetchTweetsFromIndex(String word);

	public abstract List<String> fetchTrendingHashtags();
}
