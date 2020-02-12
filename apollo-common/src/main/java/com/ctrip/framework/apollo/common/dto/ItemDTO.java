package com.ctrip.framework.apollo.common.dto;

public class ItemDTO extends BaseDTO
{

	/**
	 * Item 编号
	 */
	private long id;

	/**
	 * Namespace 编号
	 */
	private long namespaceId;

	/**
	 * 键
	 */
	private String key;

	/**
	 * 值
	 */
	private String value;

	/**
	 * 备注
	 */
	private String comment;

	/**
	 * 行数
	 */
	private int lineNum;

	public ItemDTO()
	{

	}

	public ItemDTO(String key, String value, String comment, int lineNum)
	{
		this.key = key;
		this.value = value;
		this.comment = comment;
		this.lineNum = lineNum;
	}

	public long getId()
	{
		return id;
	}

	public void setId(long id)
	{
		this.id = id;
	}

	public String getComment()
	{
		return comment;
	}

	public String getKey()
	{
		return key;
	}

	public long getNamespaceId()
	{
		return namespaceId;
	}

	public String getValue()
	{
		return value;
	}

	public void setComment(String comment)
	{
		this.comment = comment;
	}

	public void setKey(String key)
	{
		this.key = key;
	}

	public void setNamespaceId(long namespaceId)
	{
		this.namespaceId = namespaceId;
	}

	public void setValue(String value)
	{
		this.value = value;
	}

	public int getLineNum()
	{
		return lineNum;
	}

	public void setLineNum(int lineNum)
	{
		this.lineNum = lineNum;
	}

}
