package com.ctrip.framework.apollo.biz.repository;

import org.springframework.data.repository.PagingAndSortingRepository;

import com.ctrip.framework.apollo.biz.entity.NamespaceLock;

/**
 * 提供 NamespaceLock 的数据访问 给 Admin Service 和 Config Service 
 */
public interface NamespaceLockRepository extends PagingAndSortingRepository<NamespaceLock, Long>
{

  NamespaceLock findByNamespaceId(Long namespaceId);

  Long deleteByNamespaceId(Long namespaceId);

}
