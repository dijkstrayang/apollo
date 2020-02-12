package com.ctrip.framework.apollo.portal.entity.model;

import com.ctrip.framework.apollo.common.dto.NamespaceDTO;

public class NamespaceCreationModel
{
	/**
	 * 环境
	 */
	private String env;

	/**
	 * Namespace 信息
	 */
	private NamespaceDTO namespace;

	public String getEnv()
	{
		return env;
	}

	public void setEnv(String env)
	{
		this.env = env;
	}

	public NamespaceDTO getNamespace()
	{
		return namespace;
	}

	public void setNamespace(NamespaceDTO namespace)
	{
		this.namespace = namespace;
	}
}
