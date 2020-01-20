package com.ctrip.framework.apollo.biz.utils;

import com.ctrip.framework.apollo.core.ConfigConsts;
import com.google.common.base.Joiner;

/**
 * ReleaseMessage 消息内容( ReleaseMessage.message )生成器
 */
public class ReleaseMessageKeyGenerator
{

	private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);

	/**
	 * 将 appId + cluster + namespace 拼接，使用 ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR = "+" 作为间隔，例如："test+default+application"
	 * 因此，对于同一个 Namespace ，生成的消息内容是相同的。
	 * 通过这样的方式，我们可以使用最新的 ReleaseMessage 的 id 属性，作为 Namespace 是否发生变更的标识。
	 * 而 Apollo 确实是通过这样的方式实现，Client 通过不断使用获得到 ReleaseMessage 的 id 属性作为版本号，请求 Config Service 判断是否配置发生了变化
	 *
	 *
	 * ReleaseMessage这样设计的意图是作为配置发生变化的通知，对于同一个 Namespace ，仅需要保留其最新的 ReleaseMessage 记录即可。
	 * 所以，在 {@link com.ctrip.framework.apollo.biz.message.DatabaseMessageSender} 中，会看到，有后台任务不断清理旧的 ReleaseMessage 记录
	 * @param appId
	 * @param cluster
	 * @param namespace
	 * @return
	 */
	public static String generate(String appId, String cluster, String namespace)
	{
		return STRING_JOINER.join(appId, cluster, namespace);
	}
}
