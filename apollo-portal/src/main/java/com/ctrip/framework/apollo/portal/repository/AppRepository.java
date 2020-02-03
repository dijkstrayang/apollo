package com.ctrip.framework.apollo.portal.repository;

import java.util.List;
import java.util.Set;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import com.ctrip.framework.apollo.common.entity.App;

/**
 * 提供 App 的数据访问，即 DAO 
 */
public interface AppRepository extends PagingAndSortingRepository<App, Long>
{

	App findByAppId(String appId);

	List<App> findByOwnerName(String ownerName, Pageable page);

	List<App> findByAppIdIn(Set<String> appIds);

	List<App> findByAppIdIn(Set<String> appIds, Pageable pageable);

	Page<App> findByAppIdContainingOrNameContaining(String appId, String name, Pageable pageable);

	@Modifying
	@Query("UPDATE App SET IsDeleted=1,DataChange_LastModifiedBy = ?2 WHERE AppId=?1")
	int deleteApp(String appId, String operator);
}
