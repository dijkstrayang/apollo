package com.ctrip.framework.apollo.portal.entity.model;

import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.core.utils.StringUtils;

public class NamespaceTextModel implements Verifiable
{

	/**
	 * app 编号
	 */
	private String appId;
	/**
	 * env名称
	 */
	private String env;
	/**
	 * 集群名称
	 */
	private String clusterName;
	/**
	 * ns 名称
	 */
	private String namespaceName;
	/**
	 * ns编号
	 */
	private long namespaceId;
	/**
	 * 格式
	 */
	private String format;
	/**
	 * 配置文本
	 */
	private String configText;

	@Override
	public boolean isInvalid()
	{
		return StringUtils.isContainEmpty(appId, env, clusterName, namespaceName) || namespaceId <= 0;
	}

	public String getAppId()
	{
		return appId;
	}

	public void setAppId(String appId)
	{
		this.appId = appId;
	}

	public Env getEnv()
	{
		return Env.fromString(env);
	}

	public void setEnv(String env)
	{
		this.env = env;
	}

	public String getClusterName()
	{
		return clusterName;
	}

	public void setClusterName(String clusterName)
	{
		this.clusterName = clusterName;
	}

	public String getNamespaceName()
	{
		return namespaceName;
	}

	public void setNamespaceName(String namespaceName)
	{
		this.namespaceName = namespaceName;
	}

	public long getNamespaceId()
	{
		return namespaceId;
	}

	public void setNamespaceId(long namespaceId)
	{
		this.namespaceId = namespaceId;
	}

	public String getConfigText()
	{
		return configText;
	}

	public void setConfigText(String configText)
	{
		this.configText = configText;
	}

	public ConfigFileFormat getFormat()
	{
		return ConfigFileFormat.fromString(this.format);
	}

	public void setFormat(String format)
	{
		this.format = format;
	}
}
