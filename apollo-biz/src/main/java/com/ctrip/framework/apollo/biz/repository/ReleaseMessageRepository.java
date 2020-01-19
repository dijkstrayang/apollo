package com.ctrip.framework.apollo.biz.repository;

import java.util.Collection;
import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 提供 ReleaseMessage 的数据访问 给 Admin Service 和 Config Service 。
 */
public interface ReleaseMessageRepository extends PagingAndSortingRepository<ReleaseMessage, Long>
{
	List<ReleaseMessage> findFirst500ByIdGreaterThanOrderByIdAsc(Long id);

	ReleaseMessage findTopByOrderByIdDesc();

	ReleaseMessage findTopByMessageInOrderByIdDesc(Collection<String> messages);

	List<ReleaseMessage> findFirst100ByMessageAndIdLessThanOrderByIdAsc(String message, Long id);

	@Query("select message, max(id) as id from ReleaseMessage where message in :messages group by message")
	List<Object[]> findLatestReleaseMessagesGroupByMessages(@Param("messages") Collection<String> messages);
}
