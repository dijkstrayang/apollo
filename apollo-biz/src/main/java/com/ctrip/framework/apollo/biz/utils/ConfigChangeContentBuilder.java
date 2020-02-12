package com.ctrip.framework.apollo.biz.utils;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.springframework.beans.BeanUtils;

import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * 配置变更内容构建器
 */
public class ConfigChangeContentBuilder
{

	private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();

	/**
	 * 创建 Item 集合
	 */
	private List<Item> createItems = new LinkedList<>();
	/**
	 * 更新 Item 集合
	 */
	private List<ItemPair> updateItems = new LinkedList<>();
	/**
	 * 删除 Item 集合
	 */
	private List<Item> deleteItems = new LinkedList<>();

	/**
	 * 创建 Item 集合
	 * @param item
	 * @return
	 */
	public ConfigChangeContentBuilder createItem(Item item)
	{
		if (!StringUtils.isEmpty(item.getKey()))
		{
			createItems.add(cloneItem(item));
		}
		return this;
	}

	/**
	 * 更新 Item 集合
	 * @param oldItem
	 * @param newItem
	 * @return
	 */
	public ConfigChangeContentBuilder updateItem(Item oldItem, Item newItem)
	{
		if (!oldItem.getValue().equals(newItem.getValue()))
		{
			ItemPair itemPair = new ItemPair(cloneItem(oldItem), cloneItem(newItem));
			updateItems.add(itemPair);
		}
		return this;
	}

	/**
	 * 删除 Item 集合
	 * @param item
	 * @return
	 */
	public ConfigChangeContentBuilder deleteItem(Item item)
	{
		if (!StringUtils.isEmpty(item.getKey()))
		{
			deleteItems.add(cloneItem(item));
		}
		return this;
	}

	/**
	 * 判断是否有变化。当且仅当有变化才记录 Commit
	 * @return
	 */
	public boolean hasContent()
	{
		return !createItems.isEmpty() || !updateItems.isEmpty() || !deleteItems.isEmpty();
	}

	public String build()
	{
		//因为事务第一段提交并没有更新时间,所以build时统一更新
		Date now = new Date();

		for (Item item : createItems)
		{
			item.setDataChangeLastModifiedTime(now);
		}

		for (ItemPair item : updateItems)
		{
			item.newItem.setDataChangeLastModifiedTime(now);
		}

		for (Item item : deleteItems)
		{
			item.setDataChangeLastModifiedTime(now);
		}
		return gson.toJson(this);
	}

	static class ItemPair
	{

		Item oldItem;
		Item newItem;

		public ItemPair(Item oldItem, Item newItem)
		{
			this.oldItem = oldItem;
			this.newItem = newItem;
		}
	}

	Item cloneItem(Item source)
	{
		Item target = new Item();

		BeanUtils.copyProperties(source, target);

		return target;
	}

}
