package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
@Test
public class DHTRingIntervalTest {

  public void testInsideNormalInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(1, 100);

    Assert.assertTrue(ringInterval.insideInterval(1));
    Assert.assertTrue(ringInterval.insideInterval(20));
    Assert.assertTrue(ringInterval.insideInterval(100));

    Assert.assertFalse(ringInterval.insideInterval(0));
    Assert.assertFalse(ringInterval.insideInterval(150));
    Assert.assertFalse(ringInterval.insideInterval(Long.MAX_VALUE));
  }

  public void testInsideOverlappedInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(100, 1);

    Assert.assertTrue(ringInterval.insideInterval(Long.MAX_VALUE));
    Assert.assertTrue(ringInterval.insideInterval(150));
    Assert.assertTrue(ringInterval.insideInterval(1));
    Assert.assertTrue(ringInterval.insideInterval(100));
    Assert.assertTrue(ringInterval.insideInterval(0));

    Assert.assertFalse(ringInterval.insideInterval(99));
    Assert.assertFalse(ringInterval.insideInterval(2));
    Assert.assertFalse(ringInterval.insideInterval(50));
  }

  public void testIntersectionNormalInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(5, 100);

    final ODHTRingInterval intersectionOne = ringInterval.intersection(new ODHTRingInterval(0, 10));
    Assert.assertEquals(5, intersectionOne.getStart());
    Assert.assertEquals(10, intersectionOne.getEnd());

    final ODHTRingInterval intersectionTwo = ringInterval.intersection(new ODHTRingInterval(0, 5));
    Assert.assertEquals(5, intersectionTwo.getStart());
    Assert.assertEquals(5, intersectionTwo.getEnd());

    final ODHTRingInterval intersectionThree = ringInterval.intersection(new ODHTRingInterval(100, 200));
    Assert.assertEquals(100, intersectionThree.getStart());
    Assert.assertEquals(100, intersectionThree.getEnd());

    final ODHTRingInterval intersectionFour = ringInterval.intersection(new ODHTRingInterval(80, 200));
    Assert.assertEquals(80, intersectionFour.getStart());
    Assert.assertEquals(100, intersectionFour.getEnd());

    final ODHTRingInterval intersectionFive = ringInterval.intersection(new ODHTRingInterval(20, 80));
    Assert.assertEquals(20, intersectionFive.getStart());
    Assert.assertEquals(80, intersectionFive.getEnd());

    final ODHTRingInterval intersectionSix = ringInterval.intersection(new ODHTRingInterval(5, 100));
    Assert.assertEquals(5, intersectionSix.getStart());
    Assert.assertEquals(100, intersectionSix.getEnd());

    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(0, 1)));
    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(101, 200)));
  }

  public void testIntersectionOverlappedInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(200, 100);

    final ODHTRingInterval intersectionOne = ringInterval.intersection(new ODHTRingInterval(1, 50));
    Assert.assertEquals(1, intersectionOne.getStart());
    Assert.assertEquals(50, intersectionOne.getEnd());

    final ODHTRingInterval intersectionTwo = ringInterval.intersection(new ODHTRingInterval(0, 100));
    Assert.assertEquals(0, intersectionTwo.getStart());
    Assert.assertEquals(100, intersectionTwo.getEnd());

    final ODHTRingInterval intersectionThree = ringInterval.intersection(new ODHTRingInterval(20, 150));
    Assert.assertEquals(20, intersectionThree.getStart());
    Assert.assertEquals(100, intersectionThree.getEnd());

    final ODHTRingInterval intersectionFour = ringInterval.intersection(new ODHTRingInterval(150, 250));
    Assert.assertEquals(200, intersectionFour.getStart());
    Assert.assertEquals(250, intersectionFour.getEnd());

    final ODHTRingInterval intersectionFive = ringInterval.intersection(new ODHTRingInterval(150, Long.MAX_VALUE));
    Assert.assertEquals(200, intersectionFive.getStart());
    Assert.assertEquals(Long.MAX_VALUE, intersectionFive.getEnd());

    final ODHTRingInterval intersectionSix = ringInterval.intersection(new ODHTRingInterval(250, Long.MAX_VALUE));
    Assert.assertEquals(250, intersectionSix.getStart());
    Assert.assertEquals(Long.MAX_VALUE, intersectionSix.getEnd());

    final ODHTRingInterval intersectionSeven = ringInterval.intersection(new ODHTRingInterval(250, 300));
    Assert.assertEquals(250, intersectionSeven.getStart());
    Assert.assertEquals(300, intersectionSeven.getEnd());

    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(110, 195)));
  }

  public void testOverlappedIntervals() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(200, 100);
    try {
      ringInterval.intersection(new ODHTRingInterval(100, 50));
      Assert.fail();
    } catch (UnsupportedOperationException e) {
    }
  }

  public void testNextValue() {
    Assert.assertEquals(26, ODHTRingInterval.increment(25));
    Assert.assertEquals(0, ODHTRingInterval.increment(Long.MAX_VALUE));
  }

  public void testPrevValue() {
    Assert.assertEquals(24, ODHTRingInterval.decrement(25));
    Assert.assertEquals(Long.MAX_VALUE, ODHTRingInterval.decrement(0));
  }
}
