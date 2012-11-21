package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.orient.core.id.ONodeId;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
public final class ODHTRingInterval {
  private final ONodeId start;
  private final ONodeId end;

  public ODHTRingInterval(ONodeId start, ONodeId end) {
    this.start = start;
    this.end = end;
  }

  public ONodeId getStart() {
    return start;
  }

  public ONodeId getEnd() {
    return end;
  }

  public boolean insideInterval(ONodeId value) {
    if (end.compareTo(start) >= 0)
      return start.compareTo(value) <= 0 && end.compareTo(value) >= 0;
    else
      return value.compareTo(end) <= 0 || value.compareTo(start) >= 0;
  }

  public ODHTRingInterval intersection(ODHTRingInterval other) {
    if (end.compareTo(start) >= 0 && other.end.compareTo(other.start) >= 0) {
      if (other.end.compareTo(start) < 0)
        return null;

      if (end.compareTo(other.start) < 0)
        return null;

      if (other.end.compareTo(start) >= 0 && other.start.compareTo(start) < 0)
        return new ODHTRingInterval(start, other.end);

      if (end.compareTo(other.start) >= 0 && end.compareTo(other.end) < 0)
        return new ODHTRingInterval(other.start, end);

      return new ODHTRingInterval(other.start, other.end);
    } else if (end.compareTo(start) < 0 && other.end.compareTo(other.start) >= 0) {
      if (other.start.compareTo(end) > 0 && other.end.compareTo(start) < 0)
        return null;

      if (end.compareTo(other.start) >= 0 && end.compareTo(other.end) >= 0)
        return new ODHTRingInterval(other.start, other.end);

      if (end.compareTo(other.start) >= 0 && end.compareTo(other.end) < 0)
        return new ODHTRingInterval(other.start, end);

      if (other.start.compareTo(start) < 0 && other.end.compareTo(start) >= 0)
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

    if (end != null ? !end.equals(that.end) : that.end != null)
      return false;
    if (start != null ? !start.equals(that.start) : that.start != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = start != null ? start.hashCode() : 0;
    result = 31 * result + (end != null ? end.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "ODHTRingInterval{" + "start=" + start + ", end=" + end + '}';
  }
}
