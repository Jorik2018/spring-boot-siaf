package org.isobit.siaf.repository;

import org.isobit.siaf.jpa.HCC;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;

public interface  HCCRepository extends CrudRepository<HCC, Integer> {

	@Query(value="select u from HCC u where u.anoEje=?1 AND u.month = ?2 AND u.number>0",countQuery="1")
	HCC findFirstByMonthOrderByNumberDesc(Integer year,Integer month);
	
}
