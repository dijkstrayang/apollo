package com.ctrip.framework.apollo.portal.entity.model;

import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.core.utils.StringUtils;

public class NamespaceReleaseModel implements Verifiable
{

	/**
	 * app 编号
	 */
	private String appId;
	/**
	 * 环境
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
	 * 发布名称
	 */
	private String releaseTitle;
	/**
	 * 发布标题
	 */
	private String releaseComment;
	/**
	 * 发布者
	 */
	private String releasedBy;
	/**
	 * 是否是紧急发布
	 */
	private boolean isEmergencyPublish;

	@Override
	public boolean isInvalid()
	{
		return StringUtils.isContainEmpty(appId, env, clusterName, namespaceName, releaseTitle);
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
		return Env.valueOf(env);
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

	public String getReleaseTitle()
	{
		return releaseTitle;
	}

	public void setReleaseTitle(String releaseTitle)
	{
		this.releaseTitle = releaseTitle;
	}

	public String getReleaseComment()
	{
		return releaseComment;
	}

	public void setReleaseComment(String releaseComment)
	{
		this.releaseComment = releaseComment;
	}

	public String getReleasedBy()
	{
		return releasedBy;
	}

	public void setReleasedBy(String releasedBy)
	{
		this.releasedBy = releasedBy;
	}

	public boolean isEmergencyPublish()
	{
		return isEmergencyPublish;
	}

	public void setEmergencyPublish(boolean emergencyPublish)
	{
		isEmergencyPublish = emergencyPublish;
	}
}
