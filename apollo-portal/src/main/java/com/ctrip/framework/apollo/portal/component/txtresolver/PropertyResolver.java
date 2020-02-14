package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;

import com.google.common.base.Strings;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 实现 ConfigTextResolver 接口，properties 配置解析器
 * normal property file resolver.
 * update comment and blank item implement by create new item and delete old item.通过创建新项和删除旧项来实现更新注释和空白项
 * update normal key/value item implement by update.通过更新来更新普通的键值项
 */
@Component("propertyResolver")
public class PropertyResolver implements ConfigTextResolver
{

	private static final String KV_SEPARATOR = "=";
	private static final String ITEM_SEPARATOR = "\n";

	@Override
	public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems)
	{

		// 创建 Item Map ，以 lineNum 为 键
		Map<Integer, ItemDTO> oldLineNumMapItem = BeanUtils.mapByKey("lineNum", baseItems);
		// 创建 Item Map ，以 key 为 键
		Map<String, ItemDTO> oldKeyMapItem = BeanUtils.mapByKey("key", baseItems);

		//remove comment and blank item map.
		oldKeyMapItem.remove("");

		// 拆分 Property 配置
		String[] newItems = configText.split(ITEM_SEPARATOR);

		// 校验是否存在重复配置 Key 。若是，抛出 BadRequestException 异常
		if (isHasRepeatKey(newItems))
		{
			throw new BadRequestException("config text has repeat key please check.");
		}

		// 创建 ItemChangeSets 对象，并解析配置文件到 ItemChangeSets 中
		ItemChangeSets changeSets = new ItemChangeSets();
		Map<Integer, String> newLineNumMapItem = new HashMap<>();//use for delete blank and comment item
		int lineCounter = 1;
		for (String newItem : newItems)
		{
			newItem = newItem.trim();
			newLineNumMapItem.put(lineCounter, newItem);
			// 使用行号，获得已存在的 ItemDTO
			ItemDTO oldItemByLine = oldLineNumMapItem.get(lineCounter);

			//comment item 注释 Item
			if (isCommentItem(newItem))
			{

				handleCommentLine(namespaceId, oldItemByLine, newItem, lineCounter, changeSets);
			}
			//blank item 空白 Item
			else if (isBlankItem(newItem))
			{

				handleBlankLine(namespaceId, oldItemByLine, lineCounter, changeSets);
			}
			//normal item 正常 Item
			else
			{
				handleNormalLine(namespaceId, oldKeyMapItem, newItem, lineCounter, changeSets);
			}
			// 行号计数 + 1
			lineCounter++;
		}
		// 删除注释和空行配置项
		deleteCommentAndBlankItem(oldLineNumMapItem, newLineNumMapItem, changeSets);
		// 删除普通配置项
		deleteNormalKVItem(oldKeyMapItem, changeSets);

		return changeSets;
	}

	/**
	 * 校验是否存在重复配置 Key 。若是，抛出 BadRequestException 异常。
	 * 基于 Set 做排重判断。通过比较配置项中key的数量 和 key集合大小
	 * @param newItems
	 * @return
	 */
	private boolean isHasRepeatKey(String[] newItems)
	{
		Set<String> keys = new HashSet<>();
		int lineCounter = 1;
		int keyCount = 0;
		for (String item : newItems)
		{
			// 排除注释和空行的配置项
			if (!isCommentItem(item) && !isBlankItem(item))
			{
				keyCount++;
				String[] kv = parseKeyValueFromItem(item);
				if (kv != null)
				{
					keys.add(kv[0].toLowerCase());
				}
				else
				{
					throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
				}
			}
			lineCounter++;
		}

		return keyCount > keys.size();
	}

	private String[] parseKeyValueFromItem(String item)
	{
		int kvSeparator = item.indexOf(KV_SEPARATOR);
		if (kvSeparator == -1)
		{
			return null;
		}

		String[] kv = new String[2];
		kv[0] = item.substring(0, kvSeparator).trim();
		kv[1] = item.substring(kvSeparator + 1, item.length()).trim();
		return kv;
	}

	/**
	 * 处理注释配置项
	 * @param namespaceId
	 * @param oldItemByLine
	 * @param newItem
	 * @param lineCounter
	 * @param changeSets
	 */
	private void handleCommentLine(Long namespaceId, ItemDTO oldItemByLine, String newItem, int lineCounter,
			ItemChangeSets changeSets)
	{
		String oldComment = oldItemByLine == null ? "" : oldItemByLine.getComment();
		//create comment. implement update comment by delete old comment and create new comment
		// 创建注释 ItemDTO 到 ItemChangeSets 的新增项，若老的配置项不是注释或者不相等。另外，更新注释配置，通过删除 + 添加的方式
		if (!(isCommentItem(oldItemByLine) && newItem.equals(oldComment)))
		{
			changeSets.addCreateItem(buildCommentItem(0l, namespaceId, newItem, lineCounter));
		}
	}

	/**
	 * 处理空配置项
	 * @param namespaceId
	 * @param oldItem
	 * @param lineCounter
	 * @param changeSets
	 */
	private void handleBlankLine(Long namespaceId, ItemDTO oldItem, int lineCounter, ItemChangeSets changeSets)
	{
		// 创建空行 ItemDTO 到 ItemChangeSets 的新增项，若老的不是空行。另外，更新空行配置，通过删除 + 添加的方式
		if (!isBlankItem(oldItem))
		{
			changeSets.addCreateItem(buildBlankItem(0l, namespaceId, lineCounter));
		}
	}

	/**
	 * 处理普通配置项
	 * @param namespaceId
	 * @param keyMapOldItem
	 * @param newItem
	 * @param lineCounter
	 * @param changeSets
	 */
	private void handleNormalLine(Long namespaceId, Map<String, ItemDTO> keyMapOldItem, String newItem, int lineCounter,
			ItemChangeSets changeSets)
	{
		// 解析一行，生成 [key, value]
		String[] kv = parseKeyValueFromItem(newItem);

		if (kv == null)
		{
			throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
		}

		String newKey = kv[0];
		String newValue = kv[1].replace("\\n", "\n"); //handle user input \n

		// 获得老的 ItemDTO 对象
		ItemDTO oldItem = keyMapOldItem.get(newKey);
		// 不存在，则创建 ItemDTO 到 ItemChangeSets 的添加项
		if (oldItem == null)
		{ //new item 
			changeSets.addCreateItem(buildNormalItem(0l, namespaceId, newKey, newValue, "", lineCounter));
		}
		// 如果值或者行号不相等，则创建 ItemDTO 到 ItemChangeSets 的修改项
		else if (!newValue.equals(oldItem.getValue()) || lineCounter != oldItem.getLineNum())
		{ //update item
			changeSets.addUpdateItem(
					buildNormalItem(oldItem.getId(), namespaceId, newKey, newValue, oldItem.getComment(), lineCounter));
		}
		// 移除老的 ItemDTO 对象
		keyMapOldItem.remove(newKey);
	}

	/**
	 * 备注 以#或者! 开头
	 * @param item
	 * @return
	 */
	private boolean isCommentItem(ItemDTO item)
	{
		return item != null && "".equals(item.getKey())
				&& (item.getComment().startsWith("#") || item.getComment().startsWith("!"));
	}

	/**
	 * 备注 以#或者! 开头
	 * @param line
	 * @return
	 */
	private boolean isCommentItem(String line)
	{
		return line != null && (line.startsWith("#") || line.startsWith("!"));
	}

	/**
	 *
	 * @param item
	 * @return
	 */
	private boolean isBlankItem(ItemDTO item)
	{
		return item != null && "".equals(item.getKey()) && "".equals(item.getComment());
	}

	private boolean isBlankItem(String line)
	{
		return Strings.nullToEmpty(line).trim().isEmpty();
	}

	/**
	 * 删除普通配置项
	 * @param baseKeyMapItem
	 * @param changeSets
	 */
	private void deleteNormalKVItem(Map<String, ItemDTO> baseKeyMapItem, ItemChangeSets changeSets)
	{
		//surplus item is to be deleted 将剩余的配置项，添加到 ItemChangeSets 的删除项
		for (Map.Entry<String, ItemDTO> entry : baseKeyMapItem.entrySet())
		{
			changeSets.addDeleteItem(entry.getValue());
		}
	}

	/**
	 * 删除注释和空行配置项
	 * @param oldLineNumMapItem
	 * @param newLineNumMapItem
	 * @param changeSets
	 */
	private void deleteCommentAndBlankItem(Map<Integer, ItemDTO> oldLineNumMapItem,
			Map<Integer, String> newLineNumMapItem, ItemChangeSets changeSets)
	{

		for (Map.Entry<Integer, ItemDTO> entry : oldLineNumMapItem.entrySet())
		{
			int lineNum = entry.getKey();
			ItemDTO oldItem = entry.getValue();
			String newItem = newLineNumMapItem.get(lineNum);

			//1. old is blank by now is not 老的是空行配置项，新的不是空行配置项
			//2.old is comment by now is not exist or modified 老的是注释配置项，新的不是或者不相等
			if ((isBlankItem(oldItem) && !isBlankItem(newItem))
					|| isCommentItem(oldItem) && (newItem == null || !newItem.equals(oldItem.getComment())))
			{
				changeSets.addDeleteItem(oldItem);
			}
		}
	}

	private ItemDTO buildCommentItem(Long id, Long namespaceId, String comment, int lineNum)
	{
		return buildNormalItem(id, namespaceId, "", "", comment, lineNum);
	}

	private ItemDTO buildBlankItem(Long id, Long namespaceId, int lineNum)
	{
		return buildNormalItem(id, namespaceId, "", "", "", lineNum);
	}

	private ItemDTO buildNormalItem(Long id, Long namespaceId, String key, String value, String comment, int lineNum)
	{
		ItemDTO item = new ItemDTO(key, value, comment, lineNum);
		item.setId(id);
		item.setNamespaceId(namespaceId);
		return item;
	}
}
