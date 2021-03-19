package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.repository.NamespaceLockRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 提供 NamespaceLock 的 Service 逻辑给 Admin Service 和 Config Service
 */
@Service
public class NamespaceLockService
{

	private final NamespaceLockRepository namespaceLockRepository;

	public NamespaceLockService(final NamespaceLockRepository namespaceLockRepository)
	{
		this.namespaceLockRepository = namespaceLockRepository;
	}

	public NamespaceLock findLock(Long namespaceId)
	{
		return namespaceLockRepository.findByNamespaceId(namespaceId);
	}

	@Transactional
	public NamespaceLock tryLock(NamespaceLock lock)
	{
		return namespaceLockRepository.save(lock);
	}

	@Transactional
	public void unlock(Long namespaceId)
	{
		namespaceLockRepository.deleteByNamespaceId(namespaceId);
	}
}
