package com.adamjshook.accumulo.demo.app.model;

import java.util.Date;
import static com.google.common.base.MoreObjects.toStringHelper;

public class TweetBean
{
	private Date created;
	private Long id;
	private Long userId;
	private String tweet;

	public Date getCreated()
	{
		return created;
	}

	public void setCreated(Date created)
	{
		this.created = created;
	}

	public Long getId()
	{
		return id;
	}

	public void setId(Long id)
	{
		this.id = id;
	}

	public Long getUserId()
	{
		return userId;
	}

	public void setUserId(Long userId)
	{
		this.userId = userId;

	}

	public String getTweet()
	{
		return tweet;
	}

	public void setTweet(String tweet)
	{
		this.tweet = tweet;
	}

	@Override
	public String toString()
	{
		return toStringHelper(this).add("id", id).add("created", created).add("userId", userId)
				.add("tweet", tweet).toString();
	}
}
