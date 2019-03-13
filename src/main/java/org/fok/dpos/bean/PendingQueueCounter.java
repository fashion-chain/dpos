package org.fok.dpos.bean;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class PendingQueueCounter implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public AtomicLong ptr_pending = new AtomicLong(0);
	public AtomicLong ptr_sending = new AtomicLong(0);
	public AtomicLong ptr_saved = new AtomicLong(1000);

	public PendingQueueCounter(AtomicLong ptr_pending, AtomicLong ptr_sending, AtomicLong ptr_saved) {
		this.ptr_pending = ptr_pending;
		this.ptr_sending = ptr_sending;
		this.ptr_saved = ptr_saved;
	}
}