package com.ctrip.framework.apollo.biz.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import org.hibernate.annotations.Where;

import com.ctrip.framework.apollo.common.entity.BaseEntity;

@Entity
@Table(name = "NamespaceLock")
@Where(clause = "isDeleted = 0")
public class NamespaceLock extends BaseEntity
{

	/**
	 * ns编号
	 * 该字段在数据库中是唯一索引，通过该锁定，保证并发写操作时，同一个 Namespace 有且仅有创建一条 NamespaceLock 记录。
	 */
	@Column(name = "NamespaceId")
	private long namespaceId;

	public long getNamespaceId()
	{
		return namespaceId;
	}

	public void setNamespaceId(long namespaceId)
	{
		this.namespaceId = namespaceId;
	}
}
