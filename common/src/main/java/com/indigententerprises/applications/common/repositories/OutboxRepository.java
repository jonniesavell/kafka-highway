package com.indigententerprises.applications.common.repositories;

import com.indigententerprises.applications.common.domain.OutboxRecord;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface OutboxRepository extends JpaRepository<OutboxRecord, Long> {}
