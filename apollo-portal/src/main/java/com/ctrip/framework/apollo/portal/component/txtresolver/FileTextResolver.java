package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.List;

/**
 * 实现 ConfigTextResolver 接口，文件配置文本解析器，适用于 yaml、yml、json、xml 格式
 */
@Component("fileTextResolver")
public class FileTextResolver implements ConfigTextResolver
{

	@Override
	public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems)
	{
		ItemChangeSets changeSets = new ItemChangeSets();
		// 配置为空
		if (CollectionUtils.isEmpty(baseItems) && StringUtils.isEmpty(configText))
		{
			return changeSets;
		}
		if (CollectionUtils.isEmpty(baseItems))
		{
			// 创建
			changeSets.addCreateItem(createItem(namespaceId, 0, configText));
		}
		else
		{
			ItemDTO beforeItem = baseItems.get(0);
			if (!configText.equals(beforeItem.getValue()))
			{
				//修改
				changeSets.addUpdateItem(createItem(namespaceId, beforeItem.getId(), configText));
			}
		}

		return changeSets;
	}

	private ItemDTO createItem(long namespaceId, long itemId, String value)
	{
		ItemDTO item = new ItemDTO();
		item.setId(itemId);
		item.setNamespaceId(namespaceId);
		item.setValue(value);
		item.setLineNum(1);
		item.setKey(ConfigConsts.CONFIG_FILE_CONTENT_KEY);
		return item;
	}
}
