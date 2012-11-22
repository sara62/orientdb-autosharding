package com.orientechnologies.orient.server.distributed.operations;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
public final class ORemoteNodeCallResult<T> {
  private boolean repeat;
  private T       data;

  public ORemoteNodeCallResult(boolean repeat, T data) {
    this.repeat = repeat;
    this.data = data;
  }

  public boolean isRepeat() {
    return repeat;
  }

  public T getData() {
    return data;
  }
}
