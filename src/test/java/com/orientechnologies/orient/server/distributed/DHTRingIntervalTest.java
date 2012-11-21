package com.orientechnologies.orient.server.distributed;

import junit.framework.Assert;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ONodeId;

/**
 * @author Andrey Lomakin
 * @since 22.10.12
 */
@Test
public class DHTRingIntervalTest {

  public void testInsideNormalInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ONodeId.valueOf(1), ONodeId.valueOf(100));

    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(1)));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(20)));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(100)));

    Assert.assertFalse(ringInterval.insideInterval(ONodeId.valueOf(0)));
    Assert.assertFalse(ringInterval.insideInterval(ONodeId.valueOf(150)));
    Assert.assertFalse(ringInterval.insideInterval(ONodeId.MAX_VALUE));
  }

  public void testInsideOverlappedInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ONodeId.valueOf(100), ONodeId.valueOf(1));

    Assert.assertTrue(ringInterval.insideInterval(ONodeId.MAX_VALUE));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(150)));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(1)));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(100)));
    Assert.assertTrue(ringInterval.insideInterval(ONodeId.valueOf(0)));

    Assert.assertFalse(ringInterval.insideInterval(ONodeId.valueOf(99)));
    Assert.assertFalse(ringInterval.insideInterval(ONodeId.valueOf(2)));
    Assert.assertFalse(ringInterval.insideInterval(ONodeId.valueOf(50)));
  }

  public void testIntersectionNormalInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ONodeId.valueOf(5), ONodeId.valueOf(100));

    final ODHTRingInterval intersectionOne = ringInterval
        .intersection(new ODHTRingInterval(ONodeId.valueOf(0), ONodeId.valueOf(10)));
    Assert.assertEquals(ONodeId.valueOf(5), intersectionOne.getStart());
    Assert.assertEquals(ONodeId.valueOf(10), intersectionOne.getEnd());

    final ODHTRingInterval intersectionTwo = ringInterval
        .intersection(new ODHTRingInterval(ONodeId.valueOf(0), ONodeId.valueOf(5)));
    Assert.assertEquals(ONodeId.valueOf(5), intersectionTwo.getStart());
    Assert.assertEquals(ONodeId.valueOf(5), intersectionTwo.getEnd());

    final ODHTRingInterval intersectionThree = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(100), ONodeId
        .valueOf(200)));
    Assert.assertEquals(ONodeId.valueOf(100), intersectionThree.getStart());
    Assert.assertEquals(ONodeId.valueOf(100), intersectionThree.getEnd());

    final ODHTRingInterval intersectionFour = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(80), ONodeId
        .valueOf(200)));
    Assert.assertEquals(ONodeId.valueOf(80), intersectionFour.getStart());
    Assert.assertEquals(ONodeId.valueOf(100), intersectionFour.getEnd());

    final ODHTRingInterval intersectionFive = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(20), ONodeId
        .valueOf(80)));
    Assert.assertEquals(ONodeId.valueOf(20), intersectionFive.getStart());
    Assert.assertEquals(ONodeId.valueOf(80), intersectionFive.getEnd());

    final ODHTRingInterval intersectionSix = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(5), ONodeId
        .valueOf(100)));
    Assert.assertEquals(ONodeId.valueOf(5), intersectionSix.getStart());
    Assert.assertEquals(ONodeId.valueOf(100), intersectionSix.getEnd());

    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(0), ONodeId.valueOf(1))));
    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(101), ONodeId.valueOf(200))));
  }

  public void testIntersectionOverlappedInterval() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ONodeId.valueOf(200), ONodeId.valueOf(100));

    final ODHTRingInterval intersectionOne = ringInterval
        .intersection(new ODHTRingInterval(ONodeId.valueOf(1), ONodeId.valueOf(50)));
    Assert.assertEquals(ONodeId.valueOf(1), intersectionOne.getStart());
    Assert.assertEquals(ONodeId.valueOf(50), intersectionOne.getEnd());

    final ODHTRingInterval intersectionTwo = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(0), ONodeId
        .valueOf(100)));
    Assert.assertEquals(ONodeId.valueOf(0), intersectionTwo.getStart());
    Assert.assertEquals(ONodeId.valueOf(100), intersectionTwo.getEnd());

    final ODHTRingInterval intersectionThree = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(20), ONodeId
        .valueOf(150)));
    Assert.assertEquals(ONodeId.valueOf(20), intersectionThree.getStart());
    Assert.assertEquals(ONodeId.valueOf(100), intersectionThree.getEnd());

    final ODHTRingInterval intersectionFour = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(150), ONodeId
        .valueOf(250)));
    Assert.assertEquals(ONodeId.valueOf(200), intersectionFour.getStart());
    Assert.assertEquals(ONodeId.valueOf(250), intersectionFour.getEnd());

    final ODHTRingInterval intersectionFive = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(150),
        ONodeId.MAX_VALUE));
    Assert.assertEquals(ONodeId.valueOf(200), intersectionFive.getStart());
    Assert.assertEquals(ONodeId.MAX_VALUE, intersectionFive.getEnd());

    final ODHTRingInterval intersectionSix = ringInterval
        .intersection(new ODHTRingInterval(ONodeId.valueOf(250), ONodeId.MAX_VALUE));
    Assert.assertEquals(ONodeId.valueOf(250), intersectionSix.getStart());
    Assert.assertEquals(ONodeId.MAX_VALUE, intersectionSix.getEnd());

    final ODHTRingInterval intersectionSeven = ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(250), ONodeId
        .valueOf(300)));
    Assert.assertEquals(ONodeId.valueOf(250), intersectionSeven.getStart());
    Assert.assertEquals(ONodeId.valueOf(300), intersectionSeven.getEnd());

    Assert.assertNull(ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(110), ONodeId.valueOf(195))));
  }

  public void testOverlappedIntervals() {
    final ODHTRingInterval ringInterval = new ODHTRingInterval(ONodeId.valueOf(200), ONodeId.valueOf(100));
    try {
      ringInterval.intersection(new ODHTRingInterval(ONodeId.valueOf(100), ONodeId.valueOf(50)));
      Assert.fail();
    } catch (UnsupportedOperationException e) {
    }
  }
}
