package com.orientechnologies.orient.server.distributed;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
public final class ODHTRingInterval {
  private final long start;
  private final long end;

  public ODHTRingInterval(long start, long end) {
    this.start = start;
    this.end = end;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public boolean insideInterval(long value) {
    if (end >= start)
      return start <= value && end >= value;
    else
      return value <= end || value >= start;
  }

  public ODHTRingInterval intersection(ODHTRingInterval other) {
    if (end >= start && other.end >= other.start) {
      if (other.end < start)
        return null;

      if (end < other.start)
        return null;

      if (other.end >= start && other.start < start)
        return new ODHTRingInterval(start, other.end);

      if (end >= other.start && end < other.end)
        return new ODHTRingInterval(other.start, end);

      return new ODHTRingInterval(other.start, other.end);
    } else if (end < start && other.end >= other.start) {
      if (other.start > end && other.end < start)
        return null;

      if (end >= other.start && end >= other.end)
        return new ODHTRingInterval(other.start, other.end);

      if (end >= other.start && end < other.end)
        return new ODHTRingInterval(other.start, end);

      if (other.start < start && other.end >= start)
        return new ODHTRingInterval(start, other.end);

      return new ODHTRingInterval(other.start, other.end);
    }

    throw new UnsupportedOperationException("Intersection of intervals " + this + " and " + other + " is not supported.");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    ODHTRingInterval that = (ODHTRingInterval) o;

    if (start != that.start)
      return false;
    if (end != that.end)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (start ^ (start >>> 32));
    result = 31 * result + (int) (end ^ (end >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "ODHTRingInterval{" + "start=" + start + ", end=" + end + '}';
  }

  public static long increment(long value) {
    if (value < Long.MAX_VALUE)
      return value + 1;

    return 0;
  }

  public static long decrement(long value) {
    if (value == 0)
      return Long.MAX_VALUE;

    return value - 1;
  }
}
