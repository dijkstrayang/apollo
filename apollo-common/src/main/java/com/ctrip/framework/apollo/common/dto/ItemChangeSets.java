package com.ctrip.framework.apollo.common.dto;

import java.util.LinkedList;
import java.util.List;

/**
 * storage cud result：Item 变更集合
 */
public class ItemChangeSets extends BaseDTO
{

	/**
	 * 新增 Item 集合
	 */
	private List<ItemDTO> createItems = new LinkedList<>();
	/**
	 * 修改 Item 集合
	 */
	private List<ItemDTO> updateItems = new LinkedList<>();
	/**
	 * 删除 Item 集合
	 */
	private List<ItemDTO> deleteItems = new LinkedList<>();

	public void addCreateItem(ItemDTO item)
	{
		createItems.add(item);
	}

	public void addUpdateItem(ItemDTO item)
	{
		updateItems.add(item);
	}

	public void addDeleteItem(ItemDTO item)
	{
		deleteItems.add(item);
	}

	public boolean isEmpty()
	{
		return createItems.isEmpty() && updateItems.isEmpty() && deleteItems.isEmpty();
	}

	public List<ItemDTO> getCreateItems()
	{
		return createItems;
	}

	public List<ItemDTO> getUpdateItems()
	{
		return updateItems;
	}

	public List<ItemDTO> getDeleteItems()
	{
		return deleteItems;
	}

	public void setCreateItems(List<ItemDTO> createItems)
	{
		this.createItems = createItems;
	}

	public void setUpdateItems(List<ItemDTO> updateItems)
	{
		this.updateItems = updateItems;
	}

	public void setDeleteItems(List<ItemDTO> deleteItems)
	{
		this.deleteItems = deleteItems;
	}

}
