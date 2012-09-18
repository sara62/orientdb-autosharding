package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 14.09.12
 */
public class ODHTKeyOwnerIsAbsentException extends RuntimeException  {
	static final long serialVersionUID = 1;

	private final long key;

	public ODHTKeyOwnerIsAbsentException(Throwable cause, long key) {
		super(cause);
		this.key = key;
	}

	public ODHTKeyOwnerIsAbsentException(String message, Throwable cause, long key) {
		super(message, cause);
		this.key = key;
	}

	public ODHTKeyOwnerIsAbsentException(String message, long key) {
		super(message);
		this.key = key;
	}

	public long getKey() {
		return key;
	}
}