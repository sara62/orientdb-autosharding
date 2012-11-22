package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 21.11.12
 */
final class ORemoteNodeCallResult<T> {
  private boolean repeat;
  private T       data;

  ORemoteNodeCallResult(boolean repeat, T data) {
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
